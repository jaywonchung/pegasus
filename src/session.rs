use std::process::Stdio;

use colored::ColoredString;
use colourado::Color;
use openssh::{KnownHosts, Session as SSHSession};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::ChildStdout;

use crate::host::Host;

pub struct Session {
    pub host: Host,
    colorhost: ColoredString,
    session: SSHSession,
}

impl Session {
    pub async fn connect(host: Host, color: Color) -> Self {
        let session = SSHSession::connect(&host.hostname, KnownHosts::Add)
            .await
            .expect(&format!("{} Failed to connect to host.", host));
        let colorhost = host.prettify(color);
        eprintln!("{} Connected to host.", colorhost);
        Self {
            host,
            colorhost,
            session,
        }
    }

    pub async fn run(&self, job: String) -> Option<i32> {
        println!("{} === run '{}' ===", self.colorhost, job);
        let mut cmd = self.session.command("sh");
        let mut process = cmd
            .arg("-c")
            .raw_arg(format!("'{}'", &job))
            .stdout(Stdio::piped())
            .spawn()
            .expect("Failed to spawn ssh command.");
        self.stream_stdout(process.stdout().as_mut().unwrap()).await;
        let status = process
            .wait()
            .await
            .expect(&format!("{} Waiting on ssh errored.", self.host));
        eprintln!("{} === done ({}) ===", self.colorhost, status);
        status.code()
    }

    async fn stream_stdout(&self, stdout: &mut ChildStdout) {
        let mut stdout_reader = BufReader::new(stdout);
        let mut line_buf = String::with_capacity(256);
        loop {
            let buflen;
            {
                let buf = stdout_reader
                    .fill_buf()
                    .await
                    .expect("Failed to read stdout");
                buflen = buf.len();
                // An empty buffer means that the stream has reached an EOF.
                if buf.is_empty() {
                    break;
                }
                for c in buf.iter().map(|c| *c as char) {
                    match c {
                        '\r' | '\n' => {
                            println!("{} {}", self.colorhost, line_buf);
                            line_buf.clear();
                        }
                        _ => line_buf.push(c),
                    };
                }
            }
            stdout_reader.consume(buflen);
        }
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        eprintln!("{} Terminating connection.", self.colorhost);
    }
}
