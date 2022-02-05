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
use crate::host::get_hosts;
use crate::job::Cmd;
use crate::session::Session;
use crate::sync::LockedFile;

async fn run_broadcast(cli: &Config) -> Result<(), openssh::Error> {
    let hosts = get_hosts();
    let num_hosts = hosts.len();

    // Broadcast channel used to distribute commands to all hosts.
    let (command_tx, _) = broadcast::channel::<Cmd>(1);

    // Synchronization barrier that all SSH sessions report to when
    // they're done executing. The scheduler also waits on this barrier
    // to know when all sessions are done.
    let barrier = Arc::new(Barrier::new(num_hosts + 1));

    let mut tasks = vec![];
    let errored = Arc::new(Mutex::new(false));
    let colors = ColorPalette::new(num_hosts as u32, PaletteType::Pastel, false).colors;
    for (color, host) in zip(colors, hosts) {
        let mut command_rx = command_tx.subscribe();
        let barrier = Arc::clone(&barrier);
        let errored = Arc::clone(&errored);
        tasks.push(tokio::spawn(async move {
            // Open a new SSH session with the host.
            let session = Session::connect(host, color).await;
            // Handlebars registry for filling in parameters.
            let mut registry = Handlebars::new();
            while let Ok(job) = command_rx.recv().await {
                let job = job.fill_template(&mut registry, &session.host);
                let result = session.run(job).await;
                if result.is_err() || result.unwrap().code() != Some(0) {
                    *errored.lock().await = true;
                }
                barrier.wait().await;
            }
        }));
    }

    // The scheduling loop that fetches jobs from queue.yaml and distributes them
    // to SSH sessions.
    let mut job_queue = JobQueue::new();
    loop {
        if let Some(cmd) = job_queue.next().await {
            // Broadcast command to all sessions and wait for all to finish.
            command_tx.send(cmd).expect("command_tx");
            barrier.wait().await;
            // Check if any errored.
            let mut errored = errored.lock().await;
            if *errored {
                *errored = false;
                if cli.error_aborts {
                    eprintln!("[Pegasus] Some commands failed. Aborting.");
                    break;
                }
            }
            continue;
        } else if !cli.daemon {
            // Queue empty and not in daemon mode.
            // Break out of the scheduling loop.
            break;
        }
        eprintln!("[Pegasus] Queue drained. Waiting 3 seconds...");
        time::sleep(time::Duration::from_secs(5)).await;
    }

    // After this, tasks that call recv on command_tx will get an Err, gracefully terminating the
    // SSH session.
    drop(command_tx);

    // The scheduling loop has terminated, but there should be commands still running.
    // Wait for all of them to finish.
    join_all(tasks).await;

    Ok(())
}

async fn run_queue(cli: &Config) -> Result<(), openssh::Error> {
    let hosts = get_hosts();
    let num_hosts = hosts.len();

    // MPMC channel (used as MPSC) for requesting1 the scheduling loop a new command.
    let (notify_tx, notify_rx) = flume::bounded(hosts.len());

    let mut tasks = vec![];
    let mut command_txs = vec![];
    let colors = ColorPalette::new(num_hosts as u32, PaletteType::Pastel, false).colors;
    for ((host_index, host), color) in zip(hosts.into_iter().enumerate(), colors) {
        // MPMC channel (used as SPSC) to send the actual command to the SSH session task.
        let (command_tx, command_rx) = flume::bounded::<Cmd>(1);
        command_txs.push(command_tx);
        let notify_tx = notify_tx.clone();
        tasks.push(tokio::spawn(async move {
            // Open a new SSH session with the host.
            let session = Session::connect(host, color).await;
            // Handlebars registry for filling in parameters.
            let mut registry = Handlebars::new();
            // Request a new command from the scheduler.
            if notify_tx.send_async(host_index).await.is_ok() {
                while let Ok(job) = command_rx.recv_async().await {
                    let job = job.fill_template(&mut registry, &session.host);
                    let _ = session.run(job).await;
                    if notify_tx.send_async(host_index).await.is_err() {
                        break;
                    }
                }
            }
        }));
    }

    // The scheduling loop that fetches jobs from the job queue and distributes
    // them to SSH sessions.
    let mut job_queue = JobQueue::new();
    let mut host_index;
    loop {
        // `recv_async` will allow the scheduler to react to a new free session
        // immediately. However, the received `host_index` must be consumed in
        // some way.
        host_index = notify_rx.recv_async().await.expect("notify_rx");
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

        // if !notify_rx.is_empty() {
        //     // At least one session is available to execute commands.
        //     if let Some(cmd) = job_queue.next().await {
        //         // Next command available.
        //         // Use blocking methods because submitting jobs is more important
        //         // than streaming output from jobs that are already running.
        //         let host_index = notify_rx.recv().expect("notify_rx");
        //         command_txs[host_index].send(cmd).expect("command_tx");
        //         continue;
        //     } else if !cli.daemon && notify_rx.is_full() {
        //         // Queue empty, not in daemon mode, and all commands finished.
        //         // Break out of the scheduling loop.
        //         break;
        //     }
        // }
        // time::sleep(time::Duration::from_secs(5)).await;
    }

    // let mut host_index = notify_rx
    //     .recv_async()
    //     .await
    //     .expect("Failed while receiving command request.");
    // 'sched: loop {
    //     if let Some(jobs) = get_one_job().await {
    //         // One job might consist of multiple jobs after parametrization.
    //         for job in jobs {
    //             command_txs[host_index]
    //                 .send_async(job)
    //                 .await
    //                 .expect("Failed while sending command.");
    //             host_index = notify_rx
    //                 .recv_async()
    //                 .await
    //                 .expect("Failed while receiving command request.");
    //         }
    //     } else if cli.daemon {
    //         // The queue file is empty at the moment, but since we're in daemon mode, wait.
    //         time::sleep(time::Duration::from_secs(5)).await;
    //     } else {
    //         // We drained everything in the queue file. Still, wait until all commands finish.
    //         // If new commands arrive in queue.yaml while we're waiting, we're willing to
    //         // execute those, too.
    //         // We already received one message from `notify_rx` in the last iteration of the
    //         // job loop. Thus, if the length of the channel is `num_hosts - 1`, it means that
    //         // all sessions are done executing.
    //         if notify_rx.len() == num_hosts - 1 {
    //             break 'sched;
    //         }
    //         time::sleep(time::Duration::from_secs(5)).await;
    //     }
    // }

    // After this, tasks that call recv on command_tx or send on notify_rx will get an Err,
    // gracefully terminating the SSH session.
    drop(notify_tx);
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
    let _queue_file = LockedFile::acquire(".lock", "queue.yaml").await;
    let mut command = std::process::Command::new(&editor);
    command
        .arg("queue.yaml")
        .status()
        .unwrap_or_else(|_| panic!("Failed to execute '{} queue.yaml'.", editor));
}

#[tokio::main]
async fn main() -> Result<(), openssh::Error> {
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
