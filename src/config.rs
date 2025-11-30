//! Configuration for Pegasus.
//!
//! Currently holds clap structs for command line arguments and flags.
//! When Pegasus ends up getting its own configuration file, that will
//! come to sit here, too.

use clap::{Parser, ValueEnum};

#[derive(Parser)]
#[command(version, author)]
pub struct Config {
    /// Broadcast (b), Queue (q), and Lock (l) mode
    #[arg(value_enum)]
    pub mode: Mode,

    /// Don't terminate when done; watch the queue file for more jobs
    #[arg(long, short)]
    pub daemon: bool,

    /// (Broadcast mode) Don't abort Pegasus even if a command fails
    #[arg(long, short)]
    pub ignore_errors: bool,

    /// (Lock mode) Which editor to use to open the queue file
    #[arg(long)]
    pub editor: Option<String>,

    /// How often to print output. Giving 0 will suppress stdout/stderr.
    #[arg(long, short, default_value = "1")]
    pub print_period: usize,

    /// Queue file to use. Defaults to `queue.yaml`
    #[arg(long, default_value = "queue.yaml")]
    pub queue_file: String,

    /// Host file to use. Defaults to `hosts.yaml`
    #[arg(long, default_value = "hosts.yaml")]
    pub hosts_file: String,
}

#[derive(PartialEq, Clone, ValueEnum)]
pub enum Mode {
    #[value(name = "b")]
    Broadcast,
    #[value(name = "q")]
    Queue,
    #[value(name = "l")]
    Lock,
}
