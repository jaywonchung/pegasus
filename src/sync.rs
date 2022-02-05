use std::fs::{File, OpenOptions};

pub struct LockedFile {
    /// Path to the lock file.
    lock_path: &'static str,
    /// The file this lock protects.
    file_path: &'static str,
}

impl LockedFile {
    pub async fn acquire(lock_path: &'static str, file_path: &'static str) -> Self {
        while OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(lock_path)
            .is_err()
        {
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        }
        Self {
            lock_path,
            file_path,
        }
    }

    pub fn read_handle(&self) -> File {
        File::open(self.file_path).unwrap_or_else(|_| panic!("Failed to open {}", self.file_path))
    }

    pub fn write_handle(&self) -> File {
        File::create(self.file_path)
            .unwrap_or_else(|_| panic!("Failed to create {}", self.file_path))
    }
}

impl Drop for LockedFile {
    fn drop(&mut self) {
        std::fs::remove_file(self.lock_path)
            .unwrap_or_else(|_| panic!("Failed to remove lock file {}", self.lock_path));
    }
}
