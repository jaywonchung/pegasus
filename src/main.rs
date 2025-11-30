// Serde helper module.
mod serde;
// Command line arguments and configuration.
mod config;
// How to parse and represent hosts.
mod host;
// How to parse and represent jobs.
mod job;
// Resource-aware scheduling.
mod scheduler;
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
use crate::host::{get_hosts, Host};
use crate::job::{Cmd, FailedCmd};
use crate::scheduler::{find_host_for_job, HostSlotState, JobCompletion};
use crate::session::Session;
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
                let result = session.run(&cmd, print_period).await;
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
    let mut job_queue = JobQueue::new(&cli.queue_file, Arc::clone(&cancelled));
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
            if !cli.ignore_errors && errored.load(Ordering::SeqCst) {
                eprintln!("[Pegasus] Some commands failed. Aborting.");
                break;
            }
            continue;
        } else if !cli.daemon {
            // Queue empty and not in daemon mode.
            // Break out of the scheduling loop.
            break;
        }
        time::sleep(time::Duration::from_secs(3)).await;
    }

    // Dropping this will make sessions break of the while loop.
    drop(command_tx);

    // Wait for all session tasks to finish cleanup.
    join_all(tasks).await;

    // TODO: Better reporting of which command failed on which host.
    if errored.load(Ordering::SeqCst) {
        eprintln!("[Pegasus] Some commands failed.");
    } else {
        eprintln!("[Pegasus] All commands finished successfully.");
    }

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

    // Track errors from job executions.
    let errored = Arc::new(Mutex::new(vec![]));

    // Initialize slot state for each host.
    let mut slot_states: Vec<HostSlotState> =
        hosts.iter().map(|h| HostSlotState::new(h.slots)).collect();
    let max_host_slots = hosts.iter().map(|h| h.slots).max().unwrap_or(0);

    // Channel for job completion notifications.
    let (completion_tx, completion_rx) = flume::unbounded::<JobCompletion>();

    // Connect to all hosts and store sessions (shared across concurrent jobs).
    let colors = ColorPalette::new(num_hosts as u32, PaletteType::Pastel, false).colors;
    let mut sessions: Vec<Arc<Box<dyn Session + Send + Sync>>> = Vec::with_capacity(num_hosts);
    let mut host_data: Vec<Host> = Vec::with_capacity(num_hosts);
    for (host, color) in zip(hosts.into_iter(), colors) {
        let session = host.connect(color).await?;
        sessions.push(Arc::new(session));
        host_data.push(host);
    }

    // Track running job tasks.
    let mut running_tasks: Vec<tokio::task::JoinHandle<()>> = Vec::new();

    // The scheduling loop that fetches jobs from the job queue and schedules
    // them on hosts with sufficient slots.
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    let mut job_queue = JobQueue::new(&cli.queue_file, Arc::clone(&cancelled));
    let mut pending_cmd: Option<Cmd> = None;

    loop {
        // Check cancel.
        if *cancelled.lock().await {
            eprintln!("[Pegasus] Ctrl-c detected. Stopping scheduling loop...");
            break;
        }

        // Process any completions (non-blocking) - release slots.
        while let Ok(completion) = completion_rx.try_recv() {
            slot_states[completion.host_index].release(&completion.released_slots);
        }

        // Get next job if we don't have a pending one.
        if pending_cmd.is_none() {
            pending_cmd = job_queue.next().await;
        }

        // Try to schedule the pending job.
        if let Some(cmd) = pending_cmd.take() {
            // Validate job fits in at least one host.
            if cmd.slots_required > max_host_slots {
                eprintln!(
                    "[Pegasus] ERROR: Job requires {} slots but max host capacity is {}. Aborting.",
                    cmd.slots_required, max_host_slots
                );
                break;
            }

            // Find a host with enough free slots.
            if let Some((host_index, allocated_slots)) =
                find_host_for_job(&mut slot_states, cmd.slots_required)
            {
                // Spawn job execution task.
                let task = spawn_job(
                    Arc::clone(&sessions[host_index]),
                    host_data[host_index].clone(),
                    cmd,
                    allocated_slots,
                    completion_tx.clone(),
                    host_index,
                    cli.print_period,
                    Arc::clone(&errored),
                );
                running_tasks.push(task);
            } else {
                // No host has capacity. Put job back and wait.
                pending_cmd = Some(cmd);
            }
        }

        // If no pending job and no running tasks, check termination.
        if pending_cmd.is_none() {
            let slots_in_use: usize = slot_states
                .iter()
                .map(|s| s.total_slots() - s.free_slots())
                .sum();
            if !cli.daemon && slots_in_use == 0 {
                break;
            }
        }

        // Wait a bit before next iteration to avoid busy loop.
        // Use shorter sleep when we have capacity and might get completions soon.
        let slots_in_use: usize = slot_states
            .iter()
            .map(|s| s.total_slots() - s.free_slots())
            .sum();
        if pending_cmd.is_some() && slots_in_use > 0 {
            // We have a pending job but no capacity - wait for a completion.
            if let Ok(completion) = completion_rx.recv_async().await {
                slot_states[completion.host_index].release(&completion.released_slots);
            }
        } else if pending_cmd.is_none() && slots_in_use == 0 && cli.daemon {
            // Queue empty, no running jobs, daemon mode - wait for new jobs.
            time::sleep(time::Duration::from_secs(3)).await;
        } else {
            // Brief yield to allow other tasks to run.
            tokio::task::yield_now().await;
        }
    }

    // Wait for all running tasks to complete.
    join_all(running_tasks).await;

    let errored_commands = errored.lock().await;
    if !errored_commands.is_empty() {
        eprintln!("[Pegasus] The following commands failed:");
        eprintln!("{errored_commands:#?}");
    } else {
        eprintln!("[Pegasus] All commands finished successfully.");
    }

    Ok(())
}

/// Spawns an async task to execute a job on a host.
#[allow(clippy::too_many_arguments)]
fn spawn_job(
    session: Arc<Box<dyn Session + Send + Sync>>,
    host: Host,
    mut cmd: Cmd,
    allocated_slots: Vec<usize>,
    completion_tx: flume::Sender<JobCompletion>,
    host_index: usize,
    print_period: usize,
    errored: Arc<Mutex<Vec<FailedCmd>>>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        // Inject {{slots}} template variable with allocated slot indices.
        let slots_str = allocated_slots
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>()
            .join(",");
        cmd.insert_param("slots".to_string(), slots_str);

        // Fill template and run command.
        let mut registry = Handlebars::new();
        handlebars_misc_helpers::register(&mut registry);
        let filled_cmd = cmd.fill_template(&mut registry, &host);

        let result = session.run(&filled_cmd, print_period).await;

        // Record errors.
        match result {
            Ok(status) => {
                if status.code() != Some(0) {
                    errored.lock().await.push(FailedCmd::new(
                        host.to_string(),
                        filled_cmd,
                        status.to_string(),
                    ));
                }
            }
            Err(err) => {
                errored.lock().await.push(FailedCmd::new(
                    host.to_string(),
                    filled_cmd,
                    format!("Pegasus error: {}", err),
                ));
            }
        }

        // Notify scheduler that slots are released.
        let _ = completion_tx.send(JobCompletion {
            host_index,
            released_slots: allocated_slots,
        });
    })
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
            if cli.ignore_errors {
                eprintln!("[Pegasus] Will ignore errors and proceed.");
            }
            session::spawn_terminal_width_handler();
            run_broadcast(&cli).await?;
        }
        Mode::Queue => {
            eprintln!("[Pegasus] Running in queue mode!");
            session::spawn_terminal_width_handler();
            run_queue(&cli).await?;
        }
        Mode::Lock => {
            run_lock(&cli).await;
        }
    };

    Ok(())
}
