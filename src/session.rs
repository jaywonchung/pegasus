use std::marker::PhantomPinned;
use std::pin::Pin;
use std::process::Stdio;
use std::task::{Context, Poll};

use colored::ColoredString;
use colourado::Color;
use futures::future::{join, Future};
use futures::ready;
use openssh::{KnownHosts, Session as SSHSession};
use tokio::io::{AsyncBufRead, AsyncRead, BufReader};

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
            print!("{} ", self.colorhost);
            loop {
                match std::str::from_utf8(&buf) {
                    Ok(valid) => {
                        // Ok means that the entire `buf` is valid. We print everything
                        // happily and break out of the loop.
                        // The buffer populated by `read_until2` includes the delimiter.
                        println!("{}", &valid[..valid.len() - 1]);
                        buf.clear();
                        break;
                    }
                    Err(error) => {
                        // The decoder met an invaild UTF-8 byte sequence while decoding.
                        // We print up to `valid_len`, drain the buffer (including the
                        // length of the error'ed bytes) and try decoding again.
                        let valid_len = error.valid_up_to();
                        let error_len = error.error_len().expect("read_until2 cuts off UTF-8.");
                        print!(
                            "{}\u{FFFD}",
                            // SAFETY: `error.valid_up_to()` guarantees that up to that
                            //         index, all characters are valid UTF-8.
                            unsafe { std::str::from_utf8_unchecked(&buf[..valid_len]) },
                        );
                        buf.drain(..valid_len + error_len);
                    }
                }
            }
        }
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        eprintln!("{} Terminating connection.", self.colorhost);
    }
}

// Stolen from https://docs.rs/tokio/latest/src/tokio/io/util/read_until.rs.html
// Changed memchr::memchr to memchr::memchr2.
pin_project_lite::pin_project! {
    struct ReadUntil2<'a, R: ?Sized> {
        reader: &'a mut R,
        delimiter1: u8,
        delimiter2: u8,
        buf: &'a mut Vec<u8>,
        read: usize,
        #[pin]
        _pin: PhantomPinned,
    }
}

fn read_until2<'a, R>(
    reader: &'a mut R,
    delimiter1: u8,
    delimiter2: u8,
    buf: &'a mut Vec<u8>,
) -> ReadUntil2<'a, R>
where
    R: AsyncBufRead + ?Sized + Unpin,
{
    ReadUntil2 {
        reader,
        delimiter1,
        delimiter2,
        buf,
        read: 0,
        _pin: PhantomPinned,
    }
}

fn read_until2_internal<R: AsyncBufRead + ?Sized>(
    mut reader: Pin<&mut R>,
    cx: &mut Context<'_>,
    delimiter1: u8,
    delimiter2: u8,
    buf: &mut Vec<u8>,
    read: &mut usize,
) -> Poll<std::io::Result<usize>> {
    loop {
        let (done, used) = {
            let available = ready!(reader.as_mut().poll_fill_buf(cx))?;
            if let Some(i) = memchr::memchr2(delimiter1, delimiter2, available) {
                buf.extend_from_slice(&available[..=i]);
                (true, i + 1)
            } else {
                buf.extend_from_slice(available);
                (false, available.len())
            }
        };
        reader.as_mut().consume(used);
        *read += used;
        if done || used == 0 {
            return Poll::Ready(Ok(std::mem::replace(read, 0)));
        }
    }
}

impl<R: AsyncBufRead + ?Sized + Unpin> Future for ReadUntil2<'_, R> {
    type Output = std::io::Result<usize>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let me = self.project();
        read_until2_internal(
            Pin::new(*me.reader),
            cx,
            *me.delimiter1,
            *me.delimiter2,
            me.buf,
            me.read,
        )
    }
}
