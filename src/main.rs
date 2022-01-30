use std::fs::File;
use std::io::Write;
use std::process::Stdio;

use tokio::sync::broadcast;
use tokio::io::{BufReader, AsyncBufReadExt};
use openssh::{Session, KnownHosts};


fn get_hosts() -> Vec<String> {
    let host_file = File::open("hosts.yaml").expect("Failed to open hosts.yaml");
    serde_yaml::from_reader(host_file).expect("Failed to deserialize hosts.yaml")
}

fn get_jobs(stderr_to_stdout: bool) -> Vec<String> {
    let job_file = File::open("jobs.yaml").expect("Failed to open jobs.yaml");
    let mut jobs: Vec<String> = serde_yaml::from_reader(job_file).expect("Failed to read jobs.yaml");
    if stderr_to_stdout {
        jobs = jobs.into_iter().map(|cmd| cmd + " 2>&1").collect();
    }
    jobs
}

async fn run_broadcast(hosts: Vec<String>, jobs: Vec<String>) -> Result<(), openssh::Error> {
    let (tx, _) = broadcast::channel(jobs.len());

    let mut handles = vec![];
    for host in hosts {
        let mut rx = tx.subscribe();
        handles.push(tokio::spawn(async move {
            let session = Session::connect(&host, KnownHosts::Add)
                .await
                .expect(&format!("Failed to connect to host '{}'", &host));
            while let Ok(job) = rx.recv().await {
                println!("[{}] === run '{}' ===", &host, &job);
                let mut cmd = session.command("sh");
                let mut child = cmd.arg("-c").raw_arg(format!("'{}'", &job))
                    .stdout(Stdio::piped())
                    // .stderr(Stdio::piped())
                    .spawn()
                    .unwrap();
                let stdout = child.stdout().as_mut().unwrap();
                let mut stdout_reader = BufReader::new(stdout);
                let mut line_buf = String::with_capacity(128);
                loop {
                    let buflen;
                    {
                        let buf = stdout_reader.fill_buf().await.expect("Failed to read stdout");
                        buflen = buf.len();
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
                let exit_status = child.wait().await.expect("Waiting on child errored.");
                println!("[{}] === done ({}) ===", &host, exit_status);
            }
        }));
    }

    for job in jobs {
        tx.send(job).unwrap();
    }
    drop(tx);

    futures::future::join_all(handles).await;

    Ok(())
}

async fn run_queue(hosts: Vec<String>, jobs: Vec<String>) -> Result<(), openssh::Error> {

    Ok(())
}


#[tokio::main]
async fn main() -> Result<(), openssh::Error> {
    let hosts = get_hosts();
    let jobs = get_jobs(true);

    print!("Broadcast mode [y/N]? ");
    std::io::stdout().flush().expect("Failed to flush stdout");
    let mut response = String::new();
    std::io::stdin().read_line(&mut response).expect("Failed to read from stdin");
    let broadcast = matches!(response.trim(), "y" | "Y");
    if broadcast {
        println!("Running in broadcast mode!");
        println!("Hosts: {:?}", hosts);
        println!("Jobs: {:?}", jobs);
        futures::executor::block_on(run_broadcast(hosts, jobs))?;
    } else {
        println!("Running in queue mode!");
        println!("Hosts: {:?}", hosts);
        println!("Jobs: {:?}", jobs);
        run_queue(hosts, jobs).await?;
    }

    Ok(())
}
