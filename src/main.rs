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

use clap::Parser;
use colourado::{ColorPalette, PaletteType};
use futures::future::join_all;
use handlebars::Handlebars;
use itertools::zip;
use tokio::sync::broadcast;

use crate::config::{Config, Mode};
use crate::host::get_hosts;
use crate::job::{get_one_job, Cmd};
use crate::session::Session;
use crate::sync::LockedFile;

async fn run_broadcast(cli: &Config) -> Result<(), openssh::Error> {
    let hosts = get_hosts();

    // Broadcast channel used to distribute commands to all hosts.
    let (command_tx, _) = broadcast::channel::<Cmd>(1);
    // MPMC channel (used as MPSC channel) for hosts to notify the scheduler that
    // the its command has finished.
    let (notify_tx, notify_rx) = flume::bounded(hosts.len());

    let mut tasks = vec![];
    let num_hosts = hosts.len();
    let colors = ColorPalette::new(num_hosts as u32, PaletteType::Pastel, false).colors;
    for (color, host) in zip(colors, hosts) {
        let mut command_rx = command_tx.subscribe();
        let notify_tx = notify_tx.clone();
        tasks.push(tokio::spawn(async move {
            // Open a new SSH session with the host.
            let session = Session::connect(host, color).await;
            // Handlebars registry for filling in parameters.
            let mut registry = Handlebars::new();
            while let Ok(job) = command_rx.recv().await {
                let job = job.fill_template(&mut registry, &session.host);
                let exitcode = session.run(job).await;
                notify_tx
                    .send_async(exitcode)
                    .await
                    .expect("Failed to send exit code.");
            }
        }));
    }
    drop(notify_tx);

    // TODO: Have the scheduling loop run at least until all commands finish.
    'sched: loop {
        if let Some(jobs) = get_one_job().await {
            // One job might consist of multiple jobs after parameterization.
            for job in jobs {
                // Send one job to tasks.
                command_tx.send(job).unwrap();
                // Wait for all of them to complete.
                let mut notifications = Vec::with_capacity(num_hosts);
                for _ in 0..num_hosts {
                    notifications.push(notify_rx.recv_async());
                }
                let mut exit_codes = join_all(notifications)
                    .await
                    .into_iter()
                    .map(|res| res.unwrap());
                // Check if all commands exited successfully.
                if !exit_codes.all(|code| matches!(code, Some(0))) {
                    eprint!("[Pegasus] Some commands exited with non-zero status. ");
                    if cli.error_aborts {
                        eprintln!("Aborting.");
                        break 'sched;
                    } else {
                        eprintln!("Just continuing.");
                    }
                }
            }
        } else if cli.daemon {
            // The queue file is empty at the moment, but since we're in daemon mode, wait.
            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
        } else {
            // We drained everything in the queue file, so exit.
            eprintln!("[Pegasus] queue.yaml drained, breaking out of scheduler loop");
            break 'sched;
        }
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

    // MPMC channel (used as MPSC) for requesting1 the scheduling loop a new command.
    let (notify_tx, notify_rx) = flume::bounded(hosts.len());

    let mut tasks = vec![];
    let mut command_txs = vec![];
    let colors = ColorPalette::new(hosts.len() as u32, PaletteType::Pastel, false).colors;
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
                    // TODO: Queue mode currently doesn't care about the exit code.
                    session.run(job).await;
                    if notify_tx.send_async(host_index).await.is_err() {
                        break;
                    }
                }
            }
        }));
    }
    drop(notify_tx);

    // TODO: Have the scheduling loop run at least until all commands finish.
    // Wait for a command request first before we actually remove entries from
    // queue.yaml.
    let mut host_index = notify_rx
        .recv_async()
        .await
        .expect("Failed while receiving command request.");
    loop {
        if let Some(jobs) = get_one_job().await {
            // One job might consist of multiple jobs after parametrization.
            for job in jobs {
                command_txs[host_index]
                    .send_async(job)
                    .await
                    .expect("Failed while sending command.");
                host_index = notify_rx
                    .recv_async()
                    .await
                    .expect("Failed while receiving command request.");
            }
        } else if cli.daemon {
            // The queue file is empty at the moment, but since we're in daemon mode, wait.
            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
        } else {
            // We drained everything in the queue file, so exit.
            eprintln!("[Pegasus] queue.yaml drained, breaking out of scheduler loop");
            break;
        }
    }

    // After this, tasks that call recv on command_tx or send on notify_rx will get an Err,
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
    let _queue_file = LockedFile::acquire("lock", "queue.yaml").await;
    let mut command = std::process::Command::new(&editor);
    command
        .arg("queue.yaml")
        .status()
        .expect(&format!("Failed to execute '{} queue.yaml'.", editor));
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
