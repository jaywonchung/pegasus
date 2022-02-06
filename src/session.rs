use std::io::Write;
use std::process::{ExitStatus, Stdio};

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
            .unwrap_or_else(|_| panic!("{} Failed to connect to host.", host));
        let colorhost = host.prettify(color);
        eprintln!("{} Connected to host.", colorhost);
        Self {
            host,
            colorhost,
            session,
        }
    }

    pub async fn run(&self, job: String) -> Result<ExitStatus, openssh::Error> {
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
        let result = process.wait().await;
        match &result {
            Ok(status) => println!("{} === done ({}) ===", self.colorhost, status),
            Err(error) => println!("{} === done (error: {}) ===", self.colorhost, error),
        };
        result
    }

    async fn stream<B: AsyncRead + Unpin>(&self, stream: B) {
        let mut reader = BufReader::new(stream);
        let mut buf = Vec::with_capacity(reader.buffer().len());
        loop {
            // Read into the buffer until either \r or \n is met.
            read_until2(&mut reader, b'\r', b'\n', &mut buf)
                .await
                .expect("Failed to read from stream.");
            // An empty buffer means that EOF was reached.
            if buf.is_empty() {
                break;
            }
            // Print as we decode.
            // Without the lock, when multiple commands output to stdout,
            // lines from different commands get mixed.
            let stdout = std::io::stdout();
            let mut lock = stdout.lock();
            write!(lock, "{} ", self.colorhost).unwrap();
            loop {
                // This loop will not infinitely loop because `from_utf8` returns `Ok`
                // when `buf` is empty.
                match std::str::from_utf8(&buf) {
                    Ok(valid) => {
                        // Ok means that the entire `buf` is valid. We print everything
                        // happily and break out of the loop.
                        // The buffer populated by `read_until2` includes the delimiter.
                        writeln!(lock, "{}", &valid[..valid.len() - 1]).unwrap();
                        buf.clear();
                        break;
                    }
                    Err(error) => {
                        // The decoder met an invaild UTF-8 byte sequence while decoding.
                        // We print up to `valid_len`, drain the buffer (including the
                        // length of the error'ed bytes) and try decoding again.
                        let valid_len = error.valid_up_to();
                        let error_len = error.error_len().expect("read_until2 cuts off UTF-8.");
                        write!(
                            lock,
                            "{}\u{FFFD}",
                            // SAFETY: `error.valid_up_to()` guarantees that up to that
                            //         index, all characters are valid UTF-8.
                            unsafe { std::str::from_utf8_unchecked(&buf[..valid_len]) },
                        )
                        .unwrap();
                        buf.drain(..valid_len + error_len);
                    }
                }
            }
            drop(lock);
        }
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        eprintln!("{} Terminating connection.", self.colorhost);
    }
}

async fn read_until2<B: AsyncRead + Unpin>(
    reader: &mut BufReader<B>,
    delimiter1: u8,
    delimiter2: u8,
    buf: &mut Vec<u8>,
) -> std::io::Result<()> {
    loop {
        let (done, used) = {
            let available = reader.fill_buf().await?;
            if let Some(i) = memchr::memchr2(delimiter1, delimiter2, available) {
                buf.extend_from_slice(&available[..=i]);
                (true, i + 1)
            } else {
                buf.extend_from_slice(available);
                (false, available.len())
            }
        };
        reader.consume(used);
        if done || used == 0 {
            return Ok(());
        }
    }
}
