use std::io::{Result, Write};

#[derive(Debug)]
pub struct StripPrefixWriter<W>(W, usize);

impl<W> StripPrefixWriter<W> {
    pub fn new(writer: W, prefix_len: usize) -> Self {
        Self(writer, prefix_len)
    }
}

impl<W: Write> Write for StripPrefixWriter<W> {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        if self.1 == 0 {
            self.0.write(buf)
        } else if self.1 >= buf.len() {
            self.1 -= buf.len();
            Ok(buf.len())
        } else {
            // 0 < self.1 < buf.len()

            // Strip prefix
            let buf = &buf[self.1..];

            // The stripped prefix is also consiered as written.
            let n = self.0.write(buf)? + self.1;

            // Now all prefix is stripped
            self.1 = 0;

            Ok(n)
        }
    }

    fn flush(&mut self) -> Result<()> {
        self.0.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::StripPrefixWriter;
    use std::io::Write;

    #[test]
    fn test_zero_prefix_len() {
        let content = b"1324Hello, Wordl!".repeat(200);

        let mut buffer = Vec::new();

        StripPrefixWriter::new(&mut buffer, 0)
            .write_all(&content)
            .unwrap();

        assert_eq!(&*buffer, &*content);
    }

    /// Test prefix_len >= buf.len() and self.1 < buf.len()
    #[test]
    fn test1() {
        let content = b"1324Hello, Wordl!";

        let mut buffer = Vec::new();

        let mut writer = StripPrefixWriter::new(&mut buffer, content.len() + 2);
        writer.write_all(content).unwrap();
        writer.write_all(content).unwrap();

        assert_eq!(&*buffer, &content[2..]);
    }

    #[test]
    fn test2() {
        let content = b"1324Hello, Wordl!".repeat(200);

        let mut buffer = Vec::new();

        StripPrefixWriter::new(&mut buffer, 20)
            .write_all(&content)
            .unwrap();

        assert_eq!(&*buffer, &content[20..]);
    }
}
