//! Configuration for Pegasus.
//!
//! Currently holds clap structs for command line arguments and flags.
//! When Pegasus ends up getting its own configuration file, that will
//! come to sit here, too.

use clap::{ArgEnum, Parser};

#[derive(Parser)]
#[clap(version)]
#[clap(author)]
pub struct Config {
    /// Broadcast (b), Queue (q), and Lock (l) mode
    #[clap(arg_enum)]
    pub mode: Mode,

    /// Don't terminate when done; watch the queue file for more jobs
    #[clap(long, short)]
    pub daemon: bool,

    /// (Broadcast mode) Don't abort Pegasus even if a command fails
    #[clap(long, short)]
    pub ignore_errors: bool,

    /// (Lock mode) Which editor to use to open the queue file
    #[clap(long)]
    pub editor: Option<String>,

    /// How often to print output. Giving 0 will suppress stdout/stderr.
    #[clap(long, short, default_value = "1")]
    pub print_period: usize,

    /// Queue file to use. Defaults to `queue.yaml`
    #[clap(long, default_value = "queue.yaml")]
    pub queue_file: String,

    /// Host file to use. Defaults to `hosts.yaml`
    #[clap(long, default_value = "hosts.yaml")]
    pub hosts_file: String,
}

#[derive(PartialEq, Clone, ArgEnum)]
pub enum Mode {
    #[clap(name = "b")]
    Broadcast,
    #[clap(name = "q")]
    Queue,
    #[clap(name = "l")]
    Lock,
}
