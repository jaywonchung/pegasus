use std::fmt::Display;
use std::io::Write;
use std::process::ExitStatus;

use async_trait::async_trait;
use colored::ColoredString;
use futures::future::join;
use openssh::Session as SSHSession;
use tokio::io::{AsyncBufReadExt, AsyncRead, BufReader};
use tokio::process::Command;

use crate::error::PegasusError;

/// Wraps a command with a parent process monitor that kills the job if SSH dies.
/// Based on GNU Parallel's approach: monitor parent PID and kill child if parent changes.
fn parent_monitor_wrapper(cmd: &str) -> String {
    // Escape single quotes: ' becomes '\''
    let escaped_cmd = cmd.replace('\'', r"'\''");

    // Wrapper monitors parent PID and kills child if parent dies
    // Uses process groups (-$_c) to kill entire job tree
    format!(
        r#"_p=$PPID; (setsid sh -c '{}') & _c=$!; while kill -0 $_c 2>/dev/null; do _n=$(ps -o ppid= -p $$ 2>/dev/null | tr -d ' '); if [ -n "$_n" ] && [ "$_n" != "$_p" ]; then kill -TERM -$_c 2>/dev/null; sleep 0.2; kill -KILL -$_c 2>/dev/null; exit 143; fi; sleep 0.1; done; wait $_c"#,
        escaped_cmd
    )
}

#[async_trait]
pub trait Session {
    /// Runs a job with the session.
    async fn run(&self, job: &str, print_period: usize) -> Result<ExitStatus, PegasusError>;
}

pub struct RemoteSession {
    colorhost: ColoredString,
    session: SSHSession,
}

impl RemoteSession {
    pub fn new(colorhost: ColoredString, session: SSHSession) -> Self {
        Self { colorhost, session }
    }
}

#[async_trait]
impl Session for RemoteSession {
    async fn run(&self, job: &str, print_period: usize) -> Result<ExitStatus, PegasusError> {
        println!("{} === run '{}' ===", self.colorhost, job);
        // Wrap command with parent monitor to kill job if SSH dies
        let wrapped_job = parent_monitor_wrapper(job);
        let mut cmd = self.session.command("sh");
        let mut process = cmd.arg("-c").arg(&wrapped_job);
        if print_period == 0 {
            process = process
                .stdout(openssh::Stdio::null())
                .stderr(openssh::Stdio::null());
        } else {
            process = process
                .stdout(openssh::Stdio::piped())
                .stderr(openssh::Stdio::piped())
        }
        let mut process = process.spawn().await.expect("Failed to spawn ssh command.");
        if print_period != 0 {
            join(
                stream(
                    &self.colorhost,
                    process.stdout().take().unwrap(),
                    print_period,
                ),
                stream(
                    &self.colorhost,
                    process.stderr().take().unwrap(),
                    print_period,
                ),
            )
            .await;
        }
        let result = process.wait().await;
        match &result {
            Ok(status) => println!("{} === done ({}) ===", self.colorhost, status),
            Err(error) => println!("{} === done (error: {}) ===", self.colorhost, error),
        };
        result.map_err(PegasusError::SshError)
    }
}

pub struct LocalSession {
    colorhost: ColoredString,
}

impl LocalSession {
    pub fn new(colorhost: ColoredString) -> Self {
        Self { colorhost }
    }
}

#[async_trait]
impl Session for LocalSession {
    async fn run(&self, job: &str, print_period: usize) -> Result<ExitStatus, PegasusError> {
        println!("{} === run '{}' ===", self.colorhost, job);
        let mut cmd = Command::new("sh");
        let mut process = cmd.arg("-c").arg(job);
        if print_period == 0 {
            process = process
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null());
        } else {
            process = process
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
        }
        let mut process = process.spawn().expect("Failed to spawn command.");
        if print_period != 0 {
            join(
                stream(
                    &self.colorhost,
                    process.stdout.take().unwrap(),
                    print_period,
                ),
                stream(
                    &self.colorhost,
                    process.stderr.take().unwrap(),
                    print_period,
                ),
            )
            .await;
        }
        let result = process.wait().await;
        match &result {
            Ok(status) => println!("{} === done ({}) ===", self.colorhost, status),
            Err(error) => println!("{} === done (error: {}) ===", self.colorhost, error),
        };
        result.map_err(PegasusError::LocalCommandError)
    }
}

async fn stream<B: AsyncRead + Unpin, D: Display>(prefix: D, stream: B, print_period: usize) {
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
        write!(guard, "{} ", prefix).unwrap();
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
