//! Pegasus: A Multi-Node SSH Command Runner.

// Serde helper module.
mod serde;
// Command line arguments and configuration.
pub mod config;
// How to parse and represent hosts.
pub mod host;
// How to parse and represent jobs.
pub mod job;
// Resource-aware scheduling.
pub mod scheduler;
// Synchronization primitives.
pub mod sync;
// SSH session wrapper.
pub mod session;
// Error handling.
pub mod error;

pub use config::{Config, Mode};
pub use error::PegasusError;
pub use host::{Host, get_hosts};
pub use job::{Cmd, FailedCmd, JobQueue, validate_queue_file};
pub use scheduler::{HostSlotState, JobCompletion, find_host_for_job, spawn_job};
pub use session::Session;
pub use sync::LockedFile;
