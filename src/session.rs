use std::process::Stdio;

use colored::ColoredString;
use colourado::Color;
use openssh::{KnownHosts, Session as SSHSession};
use tokio::io::{AsyncBufReadExt, AsyncRead, BufReader};
use futures::future::join;

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
            .stderr(Stdio::piped())
            .spawn()
            .expect("Failed to spawn ssh command.");
        join(
            self.stream(process.stdout().take().unwrap()),
            self.stream(process.stderr().take().unwrap())
        ).await;
        let status = process
            .wait()
            .await
            .expect(&format!("{} Waiting on ssh errored.", self.host));
        eprintln!("{} === done ({}) ===", self.colorhost, status);
        status.code()
    }

    async fn stream<B: AsyncRead + Unpin>(&self, stdout: B) {
        let mut reader = BufReader::new(stdout);
        let mut line_buf = String::with_capacity(256);
        loop {
            let buflen;
            {
                let buf = reader
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
            reader.consume(buflen);
        }
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        eprintln!("{} Terminating connection.", self.colorhost);
    }
}
