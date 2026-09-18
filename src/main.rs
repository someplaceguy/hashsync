//! `hashsync` copies a remote command's stdout, replacing blocks that the
//! receiver already holds with verified local content. See `DESIGN.md`.
//!
//! This build implements the first milestone of `PLAN.md`: `update` and
//! `stats` for mutable roots.

mod blocks;
mod config;
mod die;
mod fsid;
mod fsops;
mod glob;
mod hash;
mod index;
mod roots;
mod scan;
mod stats;
mod sys;
mod update;

use std::ffi::OsString;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(
    name = "hashsync",
    version,
    about = "Block-level deduplicating transfer of a command's output"
)]
struct Cli {
    /// Configuration file [default: $XDG_CONFIG_HOME/hashsync/config.toml]
    #[arg(long, global = true, value_name = "PATH")]
    config: Option<OsString>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Update the index from the configured roots
    Update {
        /// Discard the index and build it again
        #[arg(long)]
        rebuild: bool,

        /// Override the configured thread count; 0 selects the default
        #[arg(short = 'j', value_name = "N")]
        jobs: Option<u32>,
    },
    /// Report index statistics
    Stats,
}

fn main() {
    die::install_panic_hook();
    let cli = Cli::parse();
    let config = config::load(cli.config.as_deref());
    match cli.command {
        Command::Update { rebuild, jobs } => update::run(&config, rebuild, jobs),
        Command::Stats => stats::run(&config),
    }
}
