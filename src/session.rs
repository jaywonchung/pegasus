use std::process::Stdio;

use colored::ColoredString;
use colourado::Color;
use futures::future::join;
use openssh::{KnownHosts, Session as SSHSession};
use tokio::io::{AsyncBufReadExt, AsyncRead, BufReader};

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
            self.stream(process.stderr().take().unwrap()),
        )
        .await;
        let status = process
            .wait()
            .await
            .expect(&format!("{} Waiting on ssh errored.", self.host));
        eprintln!("{} === done ({}) ===", self.colorhost, status);
        status.code()
    }

    async fn stream<B: AsyncRead + Unpin>(&self, stream: B) {
        let mut reader = BufReader::new(stream);
        let mut line_buf = String::with_capacity(256);
        loop {
            // Tracks the number of bytes consumed from the internal buffer.
            let mut consumed_bytes = 0;
            // Read into the internal buffer of `reader`.
            let mut buf = reader.fill_buf().await.expect("Failed to read from stream");
            // An empty buffer means that the stream has reached an EOF.
            if buf.is_empty() {
                break;
            }
            // Decode bytes into a UTF-8 string (into `line_buf`).
            loop {
                match std::str::from_utf8(buf) {
                    Ok(valid) => {
                        line_buf.push_str(valid);
                        // We add, not assign, because `buf` might have been from the previous
                        // iteration of the inner decode loop (the case when an invalid UTF-8
                        // character was detected and we skipped error_len() bytes).
                        consumed_bytes += buf.len();
                        break;
                    }
                    Err(error) => {
                        // We'll consume only up to the valid byte and leave the rest of the bytes
                        // (excluding errored bytes) to the next iteration. This is because we're
                        // incrementally decoding a stream of bytes into UTF-8, and a UTF-8
                        // character might have been cut off at the end.
                        consumed_bytes += error.valid_up_to();
                        let (valid, after_valid) = buf.split_at(consumed_bytes);
                        // SAFETY: Splitting the buffer at `error.valid_up_to()` guarantees
                        // that `valid` is valid.
                        unsafe {
                            line_buf.push_str(std::str::from_utf8_unchecked(valid));
                        }
                        if let Some(invalid_sequence_length) = error.error_len() {
                            line_buf.push_str("\u{FFFD}");
                            buf = &after_valid[invalid_sequence_length..];
                        } else {
                            // We don't know if it's actually an invalid UTF-8 character.
                            break;
                        }
                    }
                };
            }
            // While loop because multiple \r or \n's might have been fetched.
            while let Some(index) = line_buf.find(['\r', '\n']) {
                println!("{} {}", self.colorhost, &line_buf[..index]);
                line_buf = String::from(&line_buf[index + 1..]);
            }
            reader.consume(consumed_bytes);
        }
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        eprintln!("{} Terminating connection.", self.colorhost);
    }
}
