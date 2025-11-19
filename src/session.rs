use std::fmt::Display;
use std::io::Write;
use std::process::ExitStatus;

use async_trait::async_trait;
use colored::ColoredString;
use futures::future::join;
use openssh::Session as SSHSession;
use tokio::io::{AsyncBufReadExt, AsyncRead, BufReader};
use tokio::process::Command;
use unicode_width::{UnicodeWidthChar, UnicodeWidthStr};

use crate::error::PegasusError;

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
        let mut cmd = self.session.command("sh");
        let mut process = cmd.arg("-c").raw_arg(format!("'{}'", job));
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
    // Get terminal width, use 120 as conservative default
    let term_width = terminal_size::terminal_size()
        .map(|(terminal_size::Width(w), _)| w as usize)
        .unwrap_or(120);

    // Format prefix with space
    let prefix_str = format!("{} ", prefix);

    // Calculate visual width of prefix (excluding ANSI codes)
    let prefix_display_width = strip_ansi_codes(&prefix_str).width();

    // Maximum content length per line (leave some margin to avoid edge cases)
    let max_content_len = term_width.saturating_sub(prefix_display_width).saturating_sub(1);

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

        loop {
            // This loop will not infinitely loop because `from_utf8` returns `Ok`
            // when `buf` is empty.
            match std::str::from_utf8(&buf) {
                Ok(valid) => {
                    // Ok means that the entire `buf` is valid. We print everything
                    // happily and break out of the loop.
                    // The buffer populated by `read_until2` includes the delimiter.
                    let content = &valid[..valid.len() - 1];
                    print_wrapped_line(&mut guard, &prefix_str, content, max_content_len);
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

/// Strip ANSI escape codes from a string to get the display text
fn strip_ansi_codes(s: &str) -> String {
    let mut result = String::new();
    let mut chars = s.chars().peekable();

    while let Some(c) = chars.next() {
        if c == '\x1b' {
            // Start of ANSI escape sequence
            if chars.peek() == Some(&'[') {
                chars.next(); // consume '['
                // Skip until we find a letter (end of escape sequence)
                while let Some(ch) = chars.next() {
                    if ch.is_ascii_alphabetic() {
                        break;
                    }
                }
            }
        } else {
            result.push(c);
        }
    }
    result
}

/// Print a line with proper wrapping, ensuring each wrapped segment has the prefix
fn print_wrapped_line(writer: &mut std::io::StdoutLock, prefix: &str, content: &str, max_content_len: usize) {
    if content.is_empty() {
        writeln!(writer, "{}", prefix.trim_end()).unwrap();
        return;
    }

    let content_width = content.width();
    if content_width <= max_content_len {
        // Content fits in one line
        writeln!(writer, "{}{}", prefix, content).unwrap();
    } else {
        // Need to split content across multiple lines
        let mut remaining = content;
        while !remaining.is_empty() {
            let split_pos = find_split_point(remaining, max_content_len);
            let (chunk, rest) = remaining.split_at(split_pos);
            writeln!(writer, "{}{}", prefix, chunk).unwrap();
            remaining = rest;
        }
    }
}

/// Find the byte position where we should split to fit within max_width display columns
fn find_split_point(s: &str, max_width: usize) -> usize {
    let mut width = 0;
    let mut last_pos = 0;

    for (pos, ch) in s.char_indices() {
        let ch_width = ch.width().unwrap_or(0);
        if width + ch_width > max_width {
            // Split here (at the previous character boundary)
            if last_pos == 0 {
                // If we haven't made any progress, at least include this character
                return pos + ch.len_utf8();
            }
            return last_pos;
        }
        width += ch_width;
        last_pos = pos + ch.len_utf8();
    }

    // The entire string fits
    s.len()
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
