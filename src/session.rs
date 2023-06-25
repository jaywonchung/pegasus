use std::io::Write;
use std::process::ExitStatus;

use colored::ColoredString;
use colourado::Color;
use futures::future::join;
use openssh::{KnownHosts, Session as SSHSession, Stdio};
use tokio::io::{AsyncBufReadExt, AsyncRead, BufReader};

use crate::host::Host;

pub struct Session {
    pub host: Host,
    colorhost: ColoredString,
    session: SSHSession,
}

impl Session {
    pub async fn connect(host: Host, color: Color) -> Result<Self, openssh::Error> {
        let colorhost = host.prettify(color);
        let session = match SSHSession::connect_mux(&host.hostname, KnownHosts::Add).await {
            Ok(session) => session,
            Err(e) => {
                eprintln!("{} Failed to connect to host: {:?}", colorhost, e);
                tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
                return Err(e);
            }
        };
        eprintln!("{} Connected to host.", colorhost);
        Ok(Self {
            host,
            colorhost,
            session,
        })
    }

    pub async fn run(
        &self,
        job: String,
        print_period: usize,
    ) -> Result<ExitStatus, openssh::Error> {
        println!("{} === run '{}' ===", self.colorhost, job);
        let mut cmd = self.session.command("sh");
        let mut process = cmd.arg("-c").raw_arg(format!("'{}'", &job));
        if print_period == 0 {
            process = process.stdout(Stdio::null()).stderr(Stdio::null());
        } else {
            process = process.stdout(Stdio::piped()).stderr(Stdio::piped())
        }
        let mut process = process.spawn().await.expect("Failed to spawn ssh command.");
        if print_period != 0 {
            join(
                self.stream(process.stdout().take().unwrap(), print_period),
                self.stream(process.stderr().take().unwrap(), print_period),
            )
            .await;
        }
        let result = process.wait().await;
        match &result {
            Ok(status) => println!("{} === done ({}) ===", self.colorhost, status),
            Err(error) => println!("{} === done (error: {}) ===", self.colorhost, error),
        };
        result
    }

    pub async fn close(self) {
        eprintln!("{} Terminating connection.", self.colorhost);
        if let Err(e) = self.session.close().await {
            eprintln!("{} Error while terminating: {}", self.colorhost, e);
        }
    }

    async fn stream<B: AsyncRead + Unpin>(&self, stream: B, print_period: usize) {
        let mut reader = BufReader::new(stream);
        let mut buf = Vec::with_capacity(reader.buffer().len());
        loop {
            // Read into the buffer until either \r or \n is met.
            // Skip the first `print_period-1` occurances.
            read_until2(&mut reader, b'\r', b'\n', &mut buf, print_period)
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
            let mut guard = stdout.lock();
            write!(guard, "{} ", self.colorhost).unwrap();
            loop {
                // This loop will not infinitely loop because `from_utf8` returns `Ok`
                // when `buf` is empty.
                match std::str::from_utf8(&buf) {
                    Ok(valid) => {
                        // Ok means that the entire `buf` is valid. We print everything
                        // happily and break out of the loop.
                        // The buffer populated by `read_until2` includes the delimiter.
                        writeln!(guard, "{}", &valid[..valid.len() - 1]).unwrap();
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
                            guard,
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
        }
    }
}

async fn read_until2<B: AsyncRead + Unpin>(
    reader: &mut BufReader<B>,
    delimiter1: u8,
    delimiter2: u8,
    buf: &mut Vec<u8>,
    mut skip: usize,
) -> std::io::Result<()> {
    loop {
        let (done, used) = {
            let available = reader.fill_buf().await?;
            if let Some(i) = memchr::memchr2(delimiter1, delimiter2, available) {
                if skip == 1 {
                    buf.extend_from_slice(&available[..=i]);
                    (true, i + 1)
                } else {
                    skip -= 1;
                    (false, i + 1)
                }
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
