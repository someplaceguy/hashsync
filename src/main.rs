//! `hashsync` copies a remote command's stdout, replacing blocks that the
//! receiver already holds with verified local content. See `DESIGN.md`.
//!
//! This build implements PLAN.md steps 1–5: `update` and `stats` for
//! mutable roots, and `run` transfers that replace indexed blocks with
//! verified references, send constant-byte runs compactly, compress the
//! data stream, spool the command's output and the data session, and, with
//! `--stdin`, send the receiver's stdin to the command, both in eager or
//! batch delivery.

mod attach;
mod blocks;
mod cache;
mod config;
mod die;
mod fsid;
mod fsops;
mod glob;
mod hash;
mod hashlist;
mod hashtable;
mod index;
mod input;
mod lookup;
mod names;
mod pins;
mod pool;
mod protocol;
mod rendezvous;
mod roots;
mod run;
mod scan;
mod scanner;
mod segments;
mod serve;
mod spool;
mod ssh;
mod stats;
mod sys;
mod units;
mod update;

use std::ffi::OsString;

use clap::{Parser, Subcommand};

use crate::protocol::Delivery;

#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

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

    /// Print no progress notes and summaries, only warnings and errors
    #[arg(short, long, global = true)]
    quiet: bool,

    /// Show amounts of bytes as whole bytes, without scale units such as KiB
    #[arg(long, global = true)]
    no_human_readable: bool,

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
    /// Run a remote command and reproduce its output locally
    Run {
        #[command(flatten)]
        deliveries: Deliveries,

        /// Share read-only index access instead of updating first
        #[arg(long)]
        no_update: bool,

        /// Override the configured SSH command for this run
        #[arg(long, value_name = "CMD")]
        ssh: Option<OsString>,

        /// The SSH destination, passed to SSH unchanged
        #[arg(value_name = "[USER@]HOST")]
        host: OsString,

        /// The remote command and its arguments, after `--`
        #[arg(value_name = "COMMAND", last = true, required = true)]
        command: Vec<OsString>,
    },
    /// Internal sender entry point: serve one transfer (DESIGN.md §6.1)
    #[command(hide = true)]
    Serve {
        #[arg(long)]
        token: String,
    },
    /// Internal sender entry point: relay the control session
    #[command(hide = true)]
    Attach {
        #[arg(long)]
        token: String,
    },
}

/// The options of `run` that decide whether the command reads this
/// process's stdin, and the delivery of that input and of the command's
/// output (DESIGN.md §6.2 and §8).
#[derive(clap::Args)]
struct Deliveries {
    /// Send stdin to the command's stdin, as `ssh` does; without this
    /// option or --batch-stdin, the command reads EOF at once, as with
    /// `ssh -n`
    #[arg(long)]
    stdin: bool,

    /// Send stdin to the command as --stdin does, but in batch delivery:
    /// only from a full buffer and at its end, not each time it waits,
    /// which compresses better
    #[arg(long)]
    batch_stdin: bool,

    /// Send the command's stdout in batch delivery: only from full buffers
    /// and at its end, not each time it waits, which keeps more references
    /// and compresses better
    #[arg(long)]
    batch_stdout: bool,
}

impl Deliveries {
    /// The delivery of this process's stdin to the command; `None` when the
    /// command reads no input. `--batch-stdin` sends stdin too.
    fn input(&self) -> Option<Delivery> {
        match (self.stdin, self.batch_stdin) {
            (_, true) => Some(Delivery::Batch),
            (true, false) => Some(Delivery::Eager),
            (false, false) => None,
        }
    }

    /// The delivery of the command's stdout.
    fn output(&self) -> Delivery {
        match self.batch_stdout {
            true => Delivery::Batch,
            false => Delivery::Eager,
        }
    }
}

fn main() {
    die::install_panic_hook();
    // Before any other thread starts, in every role: a write past the
    // file-size limit then fails with `EFBIG`, which the writer reports
    // as a failure, in place of SIGXFSZ, which skips the cleanup.
    die::block_file_size_signal();
    let cli = Cli::parse();
    if cli.quiet {
        die::disable_notes();
    }
    if cli.no_human_readable {
        units::disable_human_readable();
    }
    // The sender entry points run on the remote host, which needs no
    // configuration file.
    let load_config = || config::load(cli.config.as_deref());
    match cli.command {
        Command::Update { rebuild, jobs } => update::run(&load_config(), rebuild, jobs),
        Command::Stats => stats::run(&load_config()),
        Command::Run {
            deliveries,
            no_update,
            ssh,
            host,
            command,
        } => run::run(
            &load_config(),
            no_update,
            deliveries.input(),
            deliveries.output(),
            ssh.as_deref(),
            &host,
            &command,
        ),
        Command::Serve { token } => serve::run(&token),
        Command::Attach { token } => attach::run(&token),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The deliveries of `hashsync run` with `options`.
    fn deliveries(options: &[&str]) -> (Option<Delivery>, Delivery) {
        let words = ["hashsync", "run"]
            .iter()
            .chain(options)
            .chain(&["host", "--", "true"]);
        let Command::Run { deliveries, .. } = Cli::try_parse_from(words).unwrap().command else {
            panic!("not a run");
        };
        (deliveries.input(), deliveries.output())
    }

    #[test]
    fn the_deliveries_are_eager_unless_an_option_asks_for_batches() {
        // `--batch-stdin` also sends stdin, as `--stdin` does (DESIGN.md
        // §8).
        use Delivery::{Batch, Eager};
        assert_eq!(deliveries(&[]), (None, Eager));
        assert_eq!(deliveries(&["--stdin"]), (Some(Eager), Eager));
        assert_eq!(deliveries(&["--batch-stdin"]), (Some(Batch), Eager));
        assert_eq!(
            deliveries(&["--stdin", "--batch-stdin"]),
            (Some(Batch), Eager)
        );
        assert_eq!(deliveries(&["--batch-stdout"]), (None, Batch));
        assert_eq!(
            deliveries(&["--batch-stdin", "--batch-stdout"]),
            (Some(Batch), Batch)
        );
    }
}
