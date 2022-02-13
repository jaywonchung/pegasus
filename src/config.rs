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

    /// Don't terminate when done; watch queue.yaml for more jobs
    #[clap(long, short)]
    pub daemon: bool,

    /// (Broadcast mode) One non-zero exit code will stop Pegasus
    #[clap(long, short)]
    pub error_aborts: bool,

    /// (Lock mode) Which editor to use to open queue.yaml
    #[clap(long)]
    pub editor: Option<String>,

    /// How often to print output. Giving 0 will suppress stdout/stderr.
    #[clap(long, short, default_value = "1")]
    pub print_period: usize,
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
