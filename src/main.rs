// Serde helper module.
mod serde;
// Command line arguments and configuration.
mod config;
// How to parse and represent hosts.
mod host;
// How to parse and represent jobs.
mod job;
// Synchronization primitives.
mod sync;
// SSH session wrapper.
mod session;
// Provides utility for std::io::Writer
mod writer;
// Error handling.
mod error;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use clap::Parser;
use colourado::{ColorPalette, PaletteType};
use futures::future::join_all;
use handlebars::Handlebars;
use itertools::zip;
use job::JobQueue;
use tokio::sync::{broadcast, Barrier, Mutex};
use tokio::time;

use crate::config::{Config, Mode};
use crate::error::PegasusError;
use crate::host::get_hosts;
use crate::job::Cmd;
use crate::sync::LockedFile;

async fn run_broadcast(cli: &Config) -> Result<(), PegasusError> {
    let hosts = get_hosts(&cli.hosts_file);
    let num_hosts = hosts.len();

    // Set handler for Ctrl-c. This will set the `cancelled` variable to
    // true, which will be noticed by the scheduling loop.
    let cancelled = Arc::new(Mutex::new(false));
    let cancelled_handler = Arc::clone(&cancelled);
    tokio::spawn(async move {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to await ctrl_c.");
        eprintln!("\n[Pegasus] Ctrl-c detected. Sending out cancel notices.");
        *cancelled_handler.lock().await = true;
    });

    // Broadcast channel used to distribute commands to all hosts.
    let (command_tx, _) = broadcast::channel::<Cmd>(1);

    // Synchronization barrier that all SSH sessions report to when
    // they're done executing. The scheduler also waits on this barrier
    // to know when all sessions are done.
    let end_barrier = Arc::new(Barrier::new(num_hosts + 1));

    // An atomic variable set whenever a session errors. Later read by
    // the scheduling loop to determine whether or not to exit.
    // TODO: Make this a Vec of hostnames so that we can report which hosts
    //       failed specifically.
    let errored = Arc::new(AtomicBool::new(false));

    let mut tasks = Vec::with_capacity(num_hosts);
    let colors = ColorPalette::new(num_hosts as u32, PaletteType::Pastel, false).colors;
    for (color, host) in zip(colors, hosts) {
        let mut command_rx = command_tx.subscribe();
        let end_barrier = Arc::clone(&end_barrier);
        let errored = Arc::clone(&errored);
        let print_period = cli.print_period;
        // Open a new SSH session with the host.
        let session = host.connect(color).await?;
        tasks.push(tokio::spawn(async move {
            // Handlebars registry for filling in parameters.
            let mut registry = Handlebars::new();
            handlebars_misc_helpers::register(&mut registry);
            // When cancellation is triggered by the ctrlc handler, the
            // scheduling loop will see that, break, and drop `command_tx`.
            // Then `command_rx.recv()` will return `Err`, allowing the
            // session object to be dropped, and everyting gracefully
            // terminated.
            while let Ok(cmd) = command_rx.recv().await {
                let cmd = cmd.fill_template(&mut registry, &host);
                let result = session.run(cmd, print_period).await;
                if result.is_err() || result.unwrap().code() != Some(0) {
                    errored.store(true, Ordering::Relaxed);
                }
                end_barrier.wait().await;
            }
        }));
    }

    // The scheduling loop that fetches jobs from the queue file and distributes
    // them to SSH sessions. Wait 0.5s so that we don't touch the queue file
    // when some sesions fail to connect.
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    let mut job_queue = JobQueue::new(&cli.queue_file);
    loop {
        // Check cancel.
        if *cancelled.lock().await {
            eprintln!("[Pegasus] Ctrl-c detected. Stopping scheduling loop...");
            break;
        }
        // Try to fetch the next job and run it.
        if let Some(cmd) = job_queue.next().await {
            // Broadcast command to all sessions and wait for all to finish.
            command_tx.send(cmd).expect("command_tx");
            // Unleash the sessions. The sessions are guaranteed to
            end_barrier.wait().await;
            // Check if any errored. No task will be holding this lock now.
            if cli.error_aborts && errored.load(Ordering::SeqCst) {
                eprintln!("[Pegasus] Some commands failed. Aborting.");
                break;
            }
            continue;
        } else if !cli.daemon {
            // Queue empty and not in daemon mode.
            // Break out of the scheduling loop.
            break;
        }
        eprintln!("[Pegasus] Queue drained. Waiting 3 seconds...");
        time::sleep(time::Duration::from_secs(3)).await;
    }

    // Dropping this will make sessions break of the while loop.
    drop(command_tx);

    // Wait for all session tasks to finish cleanup.
    join_all(tasks).await;

    Ok(())
}

async fn run_queue(cli: &Config) -> Result<(), PegasusError> {
    let hosts = get_hosts(&cli.hosts_file);
    let num_hosts = hosts.len();

    // Set handler for Ctrl-c. This will set the `cancelled` variable to
    // true, which will be noticed by the scheduling loop.
    let cancelled = Arc::new(Mutex::new(false));
    let cancelled_handler = Arc::clone(&cancelled);
    tokio::spawn(async move {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to await ctrl_c.");
        eprintln!("\n[Pegasus] Ctrl-c detected. Sending out cancel notices.");
        *cancelled_handler.lock().await = true;
    });

    // MPMC channel (used as MPSC) for requesting the scheduling loop a new command.
    let (notify_tx, notify_rx) = flume::bounded(hosts.len());

    let mut tasks = Vec::with_capacity(num_hosts);
    let mut command_txs = Vec::with_capacity(num_hosts);
    let colors = ColorPalette::new(num_hosts as u32, PaletteType::Pastel, false).colors;
    for ((host_index, host), color) in zip(hosts.into_iter().enumerate(), colors) {
        // MPMC channel (used as SPSC) to send the actual command to the SSH session task.
        let (command_tx, command_rx) = flume::bounded::<Cmd>(1);
        command_txs.push(command_tx);
        let notify_tx = notify_tx.clone();
        let print_period = cli.print_period;
        // Open a new SSH session with the host.
        let session = host.connect(color).await?;
        tasks.push(tokio::spawn(async move {
            // Handlebars registry for filling in parameters.
            let mut registry = Handlebars::new();
            handlebars_misc_helpers::register(&mut registry);
            // When cancellation happens, the scheduling loop will detect that and drop
            // `notify_rx` and `command_tx`. Thus we can break out of the loop and
            // gracefully terminate the session.
            loop {
                // Request the scheduler a new command.
                if notify_tx.send_async(host_index).await.is_err() {
                    break;
                }
                // Receive and run the command.
                match command_rx.recv_async().await {
                    Ok(cmd) => {
                        let cmd = cmd.fill_template(&mut registry, &host);
                        let _ = session.run(cmd, print_period).await;
                    }
                    Err(_) => break,
                };
            }
        }));
    }

    // The scheduling loop that fetches jobs from the job queue and distributes
    // them to SSH sessions.
    let mut job_queue = JobQueue::new(&cli.queue_file);
    let mut host_index;
    loop {
        // `recv_async` will allow the scheduler to react to a new free session
        // immediately. However, the received `host_index` must be consumed in
        // some way.
        host_index = notify_rx.recv_async().await.expect("notify_rx");
        // Check cancel.
        if *cancelled.lock().await {
            eprintln!("[Pegasus] Ctrl-c detected. Stopping scheduling loop...");
            break;
        }
        if let Some(cmd) = job_queue.next().await {
            // Next command available.
            // Use blocking send because submitting jobs is more important
            // than streaming output from jobs that are already running.
            command_txs[host_index].send(cmd).expect("command_tx");
            continue;
        } else {
            // Queue empty.
            // Put the received host_index back to the channel.
            notify_tx.send_async(host_index).await.expect("notify_tx");
            // If not in daemon mode and all commands finished,
            // break out of the scheduling loop.
            if !cli.daemon && notify_rx.is_full() {
                break;
            }
        }
        // Queue is empty but either we're in deamon mode or not all commands
        // finished running. So we wait.
        eprintln!("[Pegasus] Queue drained. Waiting 3 seconds...");
        time::sleep(time::Duration::from_secs(3)).await;
    }

    // After this, tasks that call recv on command_rx or send on notify_tx will get an Err,
    // gracefully terminating the SSH session.
    drop(notify_rx);
    drop(command_txs);

    // The scheduling loop has terminated, but there should be commands still running.
    // Wait for all of them to finish.
    join_all(tasks).await;

    Ok(())
}

async fn run_lock(cli: &Config) {
    let editor = match &cli.editor {
        Some(editor) => editor.into(),
        None => match std::env::var("EDITOR") {
            Ok(editor) => editor,
            Err(_) => "vim".into(),
        },
    };

    let _queue_file = LockedFile::acquire(&cli.queue_file).await;
    let mut command = std::process::Command::new(&editor);
    command
        .arg(&cli.queue_file)
        .status()
        .unwrap_or_else(|_| panic!("Failed to execute '{} {}'.", editor, &cli.queue_file));
}

#[tokio::main]
async fn main() -> Result<(), PegasusError> {
    let cli = Config::parse();

    match cli.mode {
        Mode::Broadcast => {
            eprintln!("[Pegasus] Running in broadcast mode!");
            run_broadcast(&cli).await?;
        }
        Mode::Queue => {
            eprintln!("[Pegasus] Running in queue mode!");
            run_queue(&cli).await?;
        }
        Mode::Lock => {
            run_lock(&cli).await;
        }
    };

    Ok(())
}
