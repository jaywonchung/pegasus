use std::fs::{File, OpenOptions};
use std::process::Stdio;

use tokio::sync::broadcast;
use tokio::io::{BufReader, AsyncBufReadExt};
use tokio::process::ChildStdout;
use futures::future::join_all;
use openssh::{Session, KnownHosts};
use clap::{Parser, ArgEnum};

#[derive(Parser)]
#[clap(version)]
struct Cli {
    /// Broadcast mode (b) runs each command on every host.
    /// Queue mode (q) runs each command once an one host.
    /// Lock mode (l) locks queue.yaml so that you can modify it.
    #[clap(arg_enum)]
    mode: Mode,

    /// When daemon mode is on, pegasus will wait for new commands in submit.json,
    /// instead of terminating after draining the initial queue.
    #[clap(long, short)]
    daemon: bool,

    /// Whether to abort Pegasus when a task returns a non-zero exit code.
    /// Think of it as `set -e` in shell scripts.
    #[clap(long, short)]
    error_aborts: bool,

    /// Which editor to use to open queue.yaml.
    #[clap(long)]
    editor: Option<String>,
}

#[derive(PartialEq, Clone, ArgEnum)]
enum Mode {
    #[clap(name = "b")]
    Broadcast,
    #[clap(name = "q")]
    Queue,
    #[clap(name = "l")]
    Lock,
}

fn get_hosts() -> Vec<String> {
    let host_file = File::open("hosts.yaml").expect("Failed to open hosts.yaml");
    let hosts: Vec<String> = serde_yaml::from_reader(host_file).expect("Failed to deserialize hosts.yaml");
    let hosts_string = hosts.clone().into_iter().reduce(|acc, host| format!("{}, {}", acc, host)).expect("No hosts found.");
    eprintln!("Hosts detected: {:?}", hosts_string);
    hosts
}

struct LockedFile {
    /// Path to the lock file.
    lock_path: &'static str,
    /// The file this lock protects.
    file_path: &'static str,
}

impl LockedFile {
    async fn acquire(lock_path: &'static str, file_path: &'static str) -> Self {
        while OpenOptions::new().write(true).create_new(true).open(lock_path).is_err() {
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        }
        Self { lock_path, file_path }
    }

    fn read_handle(&self) -> File {
        File::open(self.file_path)
            .expect(&format!("Failed to open {}", self.file_path))
    }

    fn write_handle(&self) -> File {
        File::create(self.file_path)
            .expect(&format!("Failed to create {}", self.file_path))
    }
}

impl Drop for LockedFile {
    fn drop(&mut self) {
        std::fs::remove_file(self.lock_path)
            .expect(&format!("Failed to remove lock file {}", self.lock_path));
    }
}

async fn get_one_job_line() -> Option<Vec<String>> {
    let mut error_count: usize = 0;
    loop {
        let queue_file = LockedFile::acquire("lock", "queue.yaml").await;
        let read_handle = queue_file.read_handle();
        let jobs: Result<Vec<String>, _> = serde_yaml::from_reader(&read_handle);
        match jobs {
            Ok(mut jobs) => {
                if jobs.is_empty() {
                    return None;
                }
                // Remove one job from queue.yaml
                let job = jobs.remove(0);
                let write_handle = queue_file.write_handle();
                serde_yaml::to_writer(write_handle, &jobs).expect("Failed to update queue.yaml");
                // Move the job to consumed.yaml
                let (mut consumed, write_handle): (Vec<String>, _) = if std::fs::metadata("consumed.yaml").is_ok() {
                    // consumed.yaml exists. So read it and deserialize it.
                    let handle = File::open("consumed.yaml").expect("Failed to open consumed.yaml");
                    if handle.metadata().expect("Failed to get metadata of consumed.yaml.").len() == 0 { 

                    }
                    let consumed = serde_yaml::from_reader(handle).expect("Failed to parse consumed.yaml.");
                    let write_handle = File::create("consumed.yaml").expect("Failed to create consumed.yaml.");
                    (consumed, write_handle)
                } else {
                    // consumed.yaml does not exist. Create one and return an empty vector.
                    let write_handle = File::create("consumed.yaml").expect("Failed to create consumed.yaml");
                    (Vec::with_capacity(1), write_handle)
                };
                consumed.push(job.clone());
                serde_yaml::to_writer(write_handle, &consumed).expect("Failed to update consumed.yaml");
                return Some(vec![job]);
            }
            Err(err) => {
                error_count += 1;
                if error_count > 2 {
                    eprintln!("Failed to parse queue.yaml too many times. Assuming empty queue.");
                    return None;
                }
                eprintln!("Failed to parse queue.yaml: {}", err);
                eprintln!("Waiting 5 seconds before retry...");
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
            }
        }
    }
}

async fn stream_stdout(host: &str, stdout: &mut ChildStdout) {
    let mut stdout_reader = BufReader::new(stdout);
    let mut line_buf = String::with_capacity(256);
    loop {
        let buflen;
        {
            let buf = stdout_reader.fill_buf().await.expect("Failed to read stdout");
            buflen = buf.len();
            // An empty buffer means that the stream has reached an EOF.
            if buf.is_empty() {
                break;
            }
            for c in buf.iter().map(|c| *c as char) {
                match c {
                    '\r' | '\n' => {
                        println!("[{}] {}", &host, line_buf);
                        line_buf.clear();
                    },
                    _ => line_buf.push(c),
                };
            }
        }
        stdout_reader.consume(buflen);
    }
}

async fn run_broadcast(cli: &Cli) -> Result<(), openssh::Error> {
    let hosts = get_hosts();

    // Broadcast channel used to distribute commands to all hosts.
    let (command_tx, _) = broadcast::channel(1);
    // MPMC channel (used as MPSC channel) for hosts to notify the scheduler that
    // the its command has finished.
    let (notify_tx, notify_rx) = flume::bounded(hosts.len());

    let mut tasks = vec![];
    let num_hosts = hosts.len();
    for host in hosts {
        let mut command_rx = command_tx.subscribe();
        let notify_tx = notify_tx.clone();
        tasks.push(tokio::spawn(async move {
            // Open a new SSH session with the host.
            let session = Session::connect(&host, KnownHosts::Add)
                .await
                .expect(&format!("[{}] Failed to connect to host.", &host));
            eprintln!("[{}] Connected to host.", &host);
            while let Ok(job) = command_rx.recv().await {
                println!("[{}] === run '{}' ===", &host, &job);
                let mut cmd = session.command("sh");
                let mut process = cmd
                    .arg("-c")
                    .raw_arg(format!("'{}'", &job))
                    .stdout(Stdio::piped())
                    .spawn()
                    .unwrap();
                stream_stdout(host.as_ref(), process.stdout().as_mut().unwrap()).await;
                let exitcode = process.wait().await
                    .expect(&format!("[{}] Waiting on child errored.", &host))
                    .code();
                println!("[{}] === done ({}) ===", &host, match exitcode {
                    Some(i) => format!("exit code: {}", i),
                    None => "killed by signal".into(),
                });
                notify_tx.send_async(exitcode).await.expect("Failed to send exit code.");
            }
        }));
    }
    drop(notify_tx);

    'sched: loop {
        if let Some(jobs) = get_one_job_line().await {
            // One job might consist of multiple jobs after parameterization.
            for job in jobs {
                // Send one job to tasks.
                command_tx.send(job).unwrap();
                // Wait for all of them to complete.
                let mut notifications = Vec::with_capacity(num_hosts);
                for _ in 0..num_hosts {
                    notifications.push(notify_rx.recv_async());
                }
                let mut exit_codes = join_all(notifications).await.into_iter().map(|res| res.unwrap());
                // Check if all commands exited successfully.
                if !exit_codes.all(|code| matches!(code, Some(0))) {
                    eprint!("Some commands exited with non-zero status. ");
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
            eprintln!("queue.yaml is empty. Waiting for 5 seconds...");
            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
        } else {
            // We drained everything in the queue file, so exit.
            eprintln!("queue.yaml is empty. Pegasus will exit when the running command finishes.");
            break 'sched;
        }
    }
    drop(command_tx);

    join_all(tasks).await;

    Ok(())
}

async fn run_queue(cli: &Cli) -> Result<(), openssh::Error> {
    let hosts = get_hosts();

    let (notify_tx, notify_rx) = flume::bounded(1);

    let mut tasks = vec![];
    let mut command_txs = vec![];
    for (host_index, host) in hosts.into_iter().enumerate() {
        let (command_tx, command_rx) = flume::bounded(1);
        command_txs.push(command_tx);
        let notify_tx = notify_tx.clone();
        tasks.push(tokio::spawn(async move {
            // Open a new SSH session with the host.
            let session = Session::connect(&host, KnownHosts::Add)
                .await
                .expect(&format!("[{}] Failed to connect to host.", &host));
            eprintln!("[{}] Connected to host.", &host);
            // Request a command to run from the scheduler.
            notify_tx.send_async(host_index).await.unwrap();
            while let Ok(job) = command_rx.recv_async().await {
                println!("[{}] === run '{}' ===", &host, &job);
                let mut cmd = session.command("sh");
                let mut process = cmd
                    .arg("-c")
                    .raw_arg(format!("'{}'", &job))
                    .stdout(Stdio::piped())
                    .spawn()
                    .unwrap();
                stream_stdout(host.as_ref(), process.stdout().as_mut().unwrap()).await;
                let exit_status = process.wait().await
                    .expect(&format!("[{}] Waiting on child errored.", &host));
                println!("[{}] === done ({}) ===", &host, exit_status);
                if notify_tx.send_async(host_index).await.is_err() {
                    eprintln!("[{}] Terminating connection.", &host);
                    break;
                }
            }
        }));
    }
    drop(notify_tx);

    loop {
        if let Some(jobs) = get_one_job_line().await {
            // One job might consist of multiple jobs after parametrization.
            for job in jobs {
                let host_index = notify_rx.recv_async().await.expect("Failed while receiving command request.");
                command_txs[host_index].send_async(job).await.expect("Failed while sending command.");
            }
        } else if cli.daemon {
            // The queue file is empty at the moment, but since we're in daemon mode, wait.
            eprintln!("queue.yaml is empty. Waiting for 5 seconds...");
            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
        } else {
            // We drained everything in the queue file, so exit.
            eprintln!("queue.yaml is empty. Pegasus will exit when all running commands finish.");
            break;
        }
    }
    drop(notify_rx);
    drop(command_txs);

    // The scheduling loop has terminated, but there should be commands still running.
    // Wait for all of them to finish.
    join_all(tasks).await;

    Ok(())
}


#[tokio::main]
async fn main() -> Result<(), openssh::Error> {
    let cli = Cli::parse();

    match cli.mode {
        Mode::Broadcast => {
            eprintln!("Running in broadcast mode!");
            run_broadcast(&cli).await?;
        },
        Mode::Queue => {
            eprintln!("Running in queue mode!");
            run_queue(&cli).await?;
        },
        Mode::Lock => {
            let editor = match cli.editor.as_ref() {
                Some(editor) => editor.into(),
                None => {
                    match std::env::var("EDITOR") {
                        Ok(editor) => editor,
                        Err(_) => "vim".into(),
                    }
                }
            };
            let _queue_file = LockedFile::acquire("lock", "queue.yaml").await;
            let mut command = std::process::Command::new(editor);
            command.arg("queue.yaml").status().expect("Failed to execute the editor.");
        }
    };

    Ok(())
}
