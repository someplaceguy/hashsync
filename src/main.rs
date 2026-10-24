//! `hashsync` runs a remote command through SSH, as ssh does, and
//! deduplicates one or both of its streams: the command's stdout, replacing
//! blocks that the client already holds with verified local content, and
//! the client's stdin, replacing blocks that the server holds. See
//! `docs/DESIGN.md`.
//!
//! This build implements PLAN.md steps 1–9: `update`, `prune`, `compact`, and
//! `stats` for mutable roots and for immutable ones, whose copies share a
//! group that an update scans once and then reuses, and `ssh`, which
//! deduplicates stdout by default, stdin with `--dedup-stdin`, and both with
//! `--dedup-stdin --dedup-stdout`. The command starts at once, and its
//! streams with it; the hash lists that deduplicate them follow while they
//! run, unless `--wait` holds each deduplicated stream until its list is in.
//! A transfer reuses the hash lists of earlier transfers, replaces indexed
//! blocks with verified references, read from any current copy of their root,
//! sends runs of one byte compactly, compresses both streams, and spools both
//! ends of each. Stdout and stdin each use eager or batch delivery.
//! `bench` measures how fast this host hashes blocks and scans output.

mod attach;
mod bench;
mod blocks;
mod cache;
mod client;
mod compact;
mod config;
mod control;
mod dedup;
mod die;
mod escapes;
mod fsid;
mod fsops;
mod glob;
mod hash;
mod hashlist;
mod hashtable;
mod index;
mod list_exchange;
mod lookup;
mod names;
mod pins;
mod pool;
mod protocol;
mod prune;
mod rendezvous;
mod report;
mod roots;
mod scan;
mod scanner;
mod segments;
mod server;
mod settings;
mod spool;
mod ssh;
mod stats;
mod stream;
mod sys;
mod units;
mod update;

use std::ffi::{OsStr, OsString};
use std::os::unix::ffi::OsStrExt;

use clap::builder::TypedValueParser;
use clap::error::ErrorKind;
use clap::{CommandFactory, Parser, Subcommand};

use crate::config::Source;
use crate::protocol::Request;
use crate::settings::{Deduplicated, RemoteCommand, RunSettings};

#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Parser)]
#[command(
    name = "hashsync",
    version,
    about = "Run a command through SSH, deduplicating its stdout, its stdin, or both against indexed blocks"
)]
struct Cli {
    /// The profile: the configuration file $XDG_CONFIG_HOME/hashsync/NAME.toml,
    /// whose index is $XDG_CACHE_HOME/hashsync/NAME.db unless it sets
    /// index_path [default: default]
    #[arg(long, global = true, value_name = "NAME", value_parser = ByteValue(profile_name))]
    profile: Option<OsString>,

    /// A configuration file anywhere, in place of a profile; unless it sets
    /// index_path, its index is named after it, as a profile's is
    #[arg(long, global = true, value_name = "PATH")]
    config: Option<OsString>,

    /// Print no progress notes and summaries, only warnings and errors
    #[arg(short, long, global = true)]
    quiet: bool,

    /// Show amounts of bytes as whole bytes, without scale units such as KiB,
    /// and the list sizes of bench as plain numbers
    #[arg(long, global = true)]
    no_human_readable: bool,

    #[command(subcommand)]
    command: Command,
}

impl Cli {
    /// The configuration file that the options select (DESIGN.md §7).
    /// `--profile` and `--config` both select it, so together they are a
    /// usage error. clap's `conflicts_with` misses a global option before
    /// the command with the other one after it, so this checks them
    /// wherever they are. `serve` reads the file that the client names in
    /// REQUEST, and `attach` and `bench` read none, so either option is a
    /// usage error there. The match names every command, so that a new
    /// one must decide whether it takes these options.
    fn source(&self) -> Result<Source, clap::Error> {
        let (option, source) = match (&self.profile, &self.config) {
            (Some(_), Some(_)) => {
                return Err(Cli::command().error(
                    ErrorKind::ArgumentConflict,
                    "the argument '--profile <NAME>' cannot be used with '--config <PATH>'",
                ));
            }
            (Some(name), None) => ("--profile <NAME>", Source::Profile(bytes(name))),
            (None, Some(path)) => ("--config <PATH>", Source::File(bytes(path))),
            (None, None) => return Ok(Source::Default),
        };
        let (command, reason) = match self.command {
            Command::Update { .. }
            | Command::Stats
            | Command::Prune { .. }
            | Command::Compact
            | Command::Ssh(_) => return Ok(source),
            Command::Serve { .. } => ("serve", "the configuration file that the client names"),
            Command::Attach { .. } => ("attach", "no configuration file"),
            Command::Bench { .. } => ("bench", "no configuration file"),
        };
        Err(Cli::command().error(
            ErrorKind::ArgumentConflict,
            format!(
                "the argument '{option}' cannot be used with '{command}', which reads {reason}"
            ),
        ))
    }
}

/// The value parser of the options whose values are byte strings that a
/// function checks, and may take apart, so that a bad value is a usage
/// error. Its own message shows the value as every message shows one
/// (DESIGN.md §8), which clap's message for a rejected value does not: it
/// replaces the bytes that are not UTF-8.
#[derive(Clone)]
struct ByteValue<T>(fn(&[u8]) -> Result<T, String>);

impl<T: Clone + Send + Sync + 'static> TypedValueParser for ByteValue<T> {
    type Value = T;

    fn parse_ref(
        &self,
        cmd: &clap::Command,
        arg: Option<&clap::Arg>,
        value: &OsStr,
    ) -> Result<T, clap::Error> {
        (self.0)(value.as_bytes()).map_err(|e| {
            let arg = arg.map_or_else(|| "...".to_owned(), ToString::to_string);
            let value = escapes::quoted(value.as_bytes());
            let message = format!("invalid value {value} for '{arg}': {e}");
            cmd.clone().error(ErrorKind::ValueValidation, message)
        })
    }
}

/// The value of an option that names a profile, `--profile` or
/// `--server-profile`: a profile name becomes part of the path of the
/// profile's file, so it must be a file name (DESIGN.md §7).
fn profile_name(name: &[u8]) -> Result<OsString, String> {
    config::check_profile_name(name)?;
    Ok(OsStr::from_bytes(name).to_owned())
}

/// The value of `--hashsync-path`, an executable name or path, which an
/// empty word cannot be.
fn hashsync_path(path: &[u8]) -> Result<OsString, String> {
    if path.is_empty() {
        return Err("empty path".into());
    }
    Ok(OsStr::from_bytes(path).to_owned())
}

/// The bytes of a command-line argument, as the configuration and the
/// protocol keep names and paths.
fn bytes(arg: &OsString) -> Vec<u8> {
    arg.as_bytes().to_vec()
}

#[derive(Subcommand)]
enum Command {
    /// Update the index from the configured roots
    Update {
        /// Discard the index and build it again
        #[arg(long)]
        rebuild: bool,

        /// Scan the completed immutable roots again too, which an update
        /// otherwise reuses without a scan
        #[arg(long)]
        scan_immutable: bool,

        /// Compact the index at the end, as the compact command does
        #[arg(long)]
        compact: bool,

        /// Override the configured thread count; 0 selects the default
        #[arg(short = 'j', value_name = "N")]
        jobs: Option<u32>,
    },
    /// Report index statistics
    Stats,
    /// Remove from the index the roots and paths that no current copy holds
    /// any more, without hashing, then compact it
    Prune {
        /// Look up the paths of immutable roots too, which prune otherwise
        /// leaves as they are while a copy of them is there
        #[arg(long)]
        scan_immutable: bool,
    },
    /// Compact the index: rewrite its files without the entries that
    /// updates and prunes removed
    Compact,
    /// Run a remote command through SSH, as ssh does, deduplicating its
    /// stdout against the local index, or with --dedup-stdin, stdin against
    /// the remote one, or both
    Ssh(Box<SshArgs>),
    /// Measure how fast this host computes the hashes that deduplication
    /// uses at a few common block sizes, on one thread, and scans output
    /// against hash lists of a few sizes, on one thread and on the probe
    /// threads
    Bench {
        /// Measure only this block size, and scan with it; it takes the
        /// values that block_size takes in a configuration file
        #[arg(long, value_name = "SIZE", value_parser = config::parse_block_size)]
        block_size: Option<u32>,
        /// The probe threads of the second scan rate, as for ssh; 0 selects
        /// half the cores, and 1 leaves that rate out
        #[arg(long, value_name = "N", default_value_t = 0)]
        probe_threads: u32,
    },
    /// Internal server entry point: serve one transfer (DESIGN.md §6.1)
    #[command(hide = true)]
    Serve {
        #[arg(long)]
        token: String,
    },
    /// Internal server entry point: relay the control session
    #[command(hide = true)]
    Attach {
        #[arg(long)]
        token: String,
    },
}

/// `hashsync ssh [OPTIONS] [-l USER] [USER@]HOST [--] COMMAND [ARGS...]`
/// takes the arguments that rsync gives its remote shell (DESIGN.md §8).
/// As with ssh, options can also follow HOST: the command starts at the
/// first argument after HOST that is neither an option nor the value of
/// one, or right after `--`. So a command that starts with `-` needs `--`.
#[derive(clap::Args)]
struct SshArgs {
    /// Log in as USER on the remote host; goes to SSH as it is
    #[arg(short = 'l', value_name = "USER")]
    login: Option<OsString>,

    /// Run the command without a shell: send COMMAND and ARGS as they are,
    /// not joined into one line for the remote user's shell. With rsync,
    /// use it only together with rsync's own -s, which keeps the paths off
    /// the command line: without it, rsync escapes them for a shell
    #[arg(short = 's', long)]
    no_shell: bool,

    /// Send the command an empty stdin, as ssh -n does with /dev/null: the
    /// command reads EOF at once, and nothing is read from stdin
    #[arg(short = 'n', long, conflicts_with_all = ["batch_stdin", "dedup_stdin"])]
    no_stdin: bool,

    /// Deduplicate stdin against the index of the remote host, which then
    /// needs a configuration file; without --dedup-stdout, the command's
    /// stdout comes back plain, and the local host needs no index
    #[arg(long)]
    dedup_stdin: bool,

    /// Deduplicate the command's stdout against the index of the local
    /// host, the default without --dedup-stdin; with it, both streams are
    /// deduplicated
    #[arg(long)]
    dedup_stdout: bool,

    /// The SSH command, shell-split; the first word is the executable
    #[arg(
        long,
        value_name = "CMD",
        default_value = "ssh",
        value_parser = ByteValue(ssh::Program::parse)
    )]
    ssh: ssh::Program,

    /// The hashsync executable on the remote host, a name or a path; a
    /// wrapper script there can set its environment
    #[arg(
        long,
        value_name = "PATH",
        default_value = "hashsync",
        value_parser = ByteValue(hashsync_path)
    )]
    hashsync_path: OsString,

    /// The profile of the remote host, whose file there is
    /// $XDG_CONFIG_HOME/hashsync/NAME.toml [default: default]
    #[arg(
        long,
        value_name = "NAME",
        value_parser = ByteValue(profile_name),
        conflicts_with = "server_config"
    )]
    server_profile: Option<OsString>,

    /// A configuration file on the remote host, in place of a profile; a
    /// relative path starts at the remote home directory
    #[arg(long, value_name = "PATH")]
    server_config: Option<OsString>,

    #[command(flatten)]
    settings: RunSettings,

    /// The SSH destination, passed to SSH unchanged
    #[arg(value_name = "[USER@]HOST")]
    host: OsString,

    /// The remote command and its arguments
    #[arg(value_name = "COMMAND", required = true, trailing_var_arg = true)]
    command: Vec<OsString>,
}

impl SshArgs {
    /// What the client sends to the server in REQUEST: the command and the
    /// settings of the run, with the global `-q` and `--no-human-readable`,
    /// which the server applies to its own notes, and the server's
    /// configuration file, by default its profile `default` (DESIGN.md §7).
    fn request(&self, quiet: bool, no_human_readable: bool) -> Request {
        Request {
            command: RemoteCommand::from_words(&self.command, self.no_shell),
            deduplicated: match (self.dedup_stdin, self.dedup_stdout) {
                (true, true) => Deduplicated::Both,
                (true, false) => Deduplicated::Stdin,
                (false, _) => Deduplicated::Stdout,
            },
            settings: self.settings.clone(),
            quiet,
            human_readable: !no_human_readable,
            config: match (&self.server_profile, &self.server_config) {
                (Some(name), _) => Source::Profile(bytes(name)),
                (_, Some(path)) => Source::File(bytes(path)),
                (None, None) => Source::Default,
            },
        }
    }

    fn remote(&self) -> ssh::Remote {
        ssh::Remote {
            ssh: self.ssh.clone(),
            login: self.login.as_ref().map(bytes),
            host: bytes(&self.host),
            hashsync_path: bytes(&self.hashsync_path),
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
    let source = cli.source().unwrap_or_else(|e| e.exit());
    if cli.quiet {
        report::disable_notes();
    }
    if cli.no_human_readable {
        units::disable_human_readable();
    }
    let load_config = || config::load(&source, true);
    match &cli.command {
        Command::Update {
            rebuild,
            scan_immutable,
            compact,
            jobs,
        } => {
            let options = update::Options {
                rebuild: *rebuild,
                scan_immutable: *scan_immutable,
                compact: *compact,
                jobs: *jobs,
            };
            update::run(&load_config(), options)
        }
        Command::Stats => stats::run(&load_config()),
        Command::Prune { scan_immutable } => prune::run(&load_config(), *scan_immutable),
        Command::Compact => compact::run(&load_config()),
        // The client receives the backward stream: it needs its
        // configuration file when that stream is deduplicated, for its index
        // (DESIGN.md §7).
        Command::Ssh(args) => {
            let request = args.request(cli.quiet, cli.no_human_readable);
            let config = config::load(&source, request.deduplicated.stdout());
            client::run(config, &args.remote(), &request, args.no_stdin)
        }
        Command::Bench {
            block_size,
            probe_threads,
        } => bench::run(*block_size, *probe_threads),
        // The server reads the configuration file that REQUEST names, once
        // REQUEST has told it its roles.
        Command::Serve { token } => server::run(token),
        Command::Attach { token } => attach::run(token),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use super::*;

    /// The parsed arguments of `hashsync WORDS...`, or the usage error.
    fn parse(words: &[&str]) -> Result<Cli, clap::Error> {
        Cli::try_parse_from(["hashsync"].iter().chain(words))
    }

    /// The `ssh` arguments of `hashsync ssh WORDS...`.
    fn ssh(words: &[&str]) -> SshArgs {
        let words = ["ssh"].iter().chain(words).copied().collect::<Vec<_>>();
        let Command::Ssh(args) = parse(&words).unwrap().command else {
            panic!("not ssh");
        };
        *args
    }

    fn words(args: &SshArgs) -> Vec<&str> {
        args.command.iter().map(|w| w.to_str().unwrap()).collect()
    }

    /// rsync gives its remote shell `[-l USER] HOST COMMAND...`, with no
    /// `--`, and the command's own options follow (DESIGN.md §8).
    #[test]
    fn the_command_starts_at_the_first_word_after_host() {
        let args = ssh(&[
            "-l",
            "backup",
            "nas",
            "rsync",
            "--server",
            "-logDtpre.iLsfxCIvu",
            "--no-update",
            ".",
            "dst dir/",
        ]);
        assert_eq!(args.login.unwrap(), "backup");
        assert_eq!(args.host, "nas");
        assert_eq!(
            words(&ssh(&["nas", "rsync", "--server", "-l", "x"])),
            ["rsync", "--server", "-l", "x"]
        );
        assert!(!args.settings.no_update, "an option of the command");
        assert_eq!(
            args.command,
            [
                "rsync",
                "--server",
                "-logDtpre.iLsfxCIvu",
                "--no-update",
                ".",
                "dst dir/"
            ]
        );
    }

    /// As with ssh, options can also follow HOST, until the first word of
    /// the command.
    #[test]
    fn options_can_follow_host() {
        let args = ssh(&["nas", "--no-update", "-l", "backup", "-n", "true", "-n"]);
        assert!(args.settings.no_update);
        assert!(args.no_stdin);
        assert_eq!(args.login.unwrap(), "backup");
        assert_eq!(words(&ssh(&["nas", "-s", "true", "-n"])), ["true", "-n"]);
        // The global options too.
        let cli = parse(&["ssh", "nas", "-q", "--config", "/c", "true"]).unwrap();
        assert!(cli.quiet);
        assert_eq!(cli.config.unwrap(), "/c");
        let cli = parse(&["ssh", "nas", "--profile", "work", "true"]).unwrap();
        assert_eq!(cli.profile.unwrap(), "work");
    }

    /// `--profile NAME` selects the file of a profile, and `--config PATH` a
    /// file anywhere, before or after the command. Without either, the
    /// command uses the default profile, and both together are a usage
    /// error (DESIGN.md §7).
    #[test]
    fn the_options_select_the_configuration_file() {
        let source = |words: &[&str]| parse(words).unwrap().source().unwrap();
        assert_eq!(source(&["stats"]), Source::Default);
        for words in [
            &["--profile", "work", "stats"][..],
            &["stats", "--profile", "work"],
            &["update", "--profile=work"],
        ] {
            let work = Source::Profile(b"work".to_vec());
            assert_eq!(source(words), work, "{words:?}");
        }
        let file = Source::File(b"/c".to_vec());
        assert_eq!(source(&["update", "--config", "/c"]), file);
        for words in [
            &["--profile", "work", "--config", "/c", "stats"][..],
            &["--config", "/c", "stats", "--profile", "work"],
            &["stats", "--profile=work", "--config=/c"],
        ] {
            let error = parse(words).unwrap().source().err().map(|e| e.kind());
            assert_eq!(error, Some(ErrorKind::ArgumentConflict), "{words:?}");
        }
        // A profile name becomes part of a file name.
        for name in ["", "a/b", "/"] {
            let error = parse(&["--profile", name, "stats"]).err().map(|e| e.kind());
            assert_eq!(error, Some(ErrorKind::ValueValidation), "{name:?}");
        }
        // The message shows a rejected name as messages show names, with
        // its bytes that are not UTF-8 and its control characters escaped
        // (DESIGN.md §8).
        let words = [&b"hashsync"[..], b"--profile", b"a/\xff\n", b"stats"];
        let error = Cli::try_parse_from(words.map(OsStr::from_bytes))
            .err()
            .unwrap();
        assert_eq!(error.kind(), ErrorKind::ValueValidation);
        let message = r"invalid value 'a/\xFF\x0A' for '--profile <NAME>': a profile name";
        assert!(error.to_string().contains(message), "{error}");
    }

    /// `serve` reads the configuration file that the client names in
    /// REQUEST, and `attach` and `bench` read none, so an option of their
    /// own that selects one would have no meaning, before the command or
    /// after it (DESIGN.md §8).
    #[test]
    fn serve_attach_and_bench_reject_profile_and_config() {
        for command in [
            &["serve", "--token", "t"][..],
            &["attach", "--token", "t"],
            &["bench"],
        ] {
            let source = parse(command).unwrap().source().unwrap();
            assert_eq!(source, Source::Default, "{command:?}");
            for option in [["--profile", "work"], ["--config", "/c"]] {
                for words in [
                    [&option[..], command].concat(),
                    [command, &option[..]].concat(),
                ] {
                    let error = parse(&words).unwrap().source().err().map(|e| e.kind());
                    assert_eq!(error, Some(ErrorKind::ArgumentConflict), "{words:?}");
                }
            }
        }
    }

    /// The block size and the probe threads of `hashsync WORDS...`, which
    /// must be `bench`.
    fn bench(words: &[&str]) -> Result<(Option<u32>, u32), clap::Error> {
        match parse(words)?.command {
            Command::Bench {
                block_size,
                probe_threads,
            } => Ok((block_size, probe_threads)),
            _ => panic!("not bench"),
        }
    }

    /// `--block-size` takes the values that `block_size` takes in a
    /// configuration file (DESIGN.md §7 and §8).
    #[test]
    fn bench_takes_the_block_sizes_of_the_configuration() {
        let block_size = |words: &[&str]| bench(words).map(|(size, _)| size);
        assert_eq!(block_size(&["bench"]).unwrap(), None);
        let size = block_size(&["bench", "--block-size", "64KiB"]).unwrap();
        assert_eq!(size, Some(64 * 1024));
        assert_eq!(
            block_size(&["bench", "--block-size=512"]).unwrap(),
            Some(512)
        );
        for (size, message) in [
            ("511", "must be at least 512 bytes"),
            ("4GiB", "must be at most 4294967295 bytes"),
            ("16XiB", "unknown unit 'XiB'"),
        ] {
            let error = block_size(&["bench", "--block-size", size]).unwrap_err();
            assert_eq!(error.kind(), ErrorKind::ValueValidation, "{size}");
            assert!(error.to_string().contains(message), "{error}");
        }
    }

    /// `--probe-threads` takes the counts that it takes for `ssh`, and
    /// selects half the cores by default, with 0 (DESIGN.md §8).
    #[test]
    fn bench_takes_the_probe_threads_of_ssh() {
        let threads = |words: &[&str]| bench(words).map(|(_, threads)| threads);
        assert_eq!(threads(&["bench"]).unwrap(), 0);
        assert_eq!(threads(&["bench", "--probe-threads", "3"]).unwrap(), 3);
        assert!(threads(&["bench", "--probe-threads", "-1"]).is_err());
    }

    /// By default, the server uses its profile `default`, whatever file the
    /// client's own options select. `--server-profile` and `--server-config`
    /// select another profile or a file anywhere on the server, and together
    /// they are a usage error (DESIGN.md §7).
    #[test]
    fn the_client_names_the_configuration_file_of_the_server() {
        let config = |words: &[&str]| {
            let cli = parse(&[words, &["nas", "true"]].concat()).unwrap();
            let Command::Ssh(args) = &cli.command else {
                panic!("not ssh");
            };
            args.request(false, false).config
        };
        assert_eq!(config(&["ssh", "--profile", "work"]), Source::Default);
        for client in [&[][..], &["--profile", "work"], &["--config", "/c"]] {
            assert_eq!(config(&[client, &["ssh"]].concat()), Source::Default);
            let words = [client, &["ssh", "--server-profile", "nas"]].concat();
            assert_eq!(config(&words), Source::Profile(b"nas".to_vec()));
            let words = [client, &["ssh", "--server-config", "nas.toml"]].concat();
            assert_eq!(config(&words), Source::File(b"nas.toml".to_vec()));
        }
        let both = ["--server-profile", "a", "--server-config", "/b"];
        assert!(parse(&[&["ssh"], &both[..], &["nas", "true"]].concat()).is_err());
        assert!(parse(&[&["ssh", "nas"], &both[..], &["true"]].concat()).is_err());
        for name in ["", "a/b"] {
            let words = ["ssh", "--server-profile", name, "nas", "true"];
            let error = parse(&words).err().map(|e| e.kind());
            assert_eq!(error, Some(ErrorKind::ValueValidation), "{name:?}");
        }
    }

    /// `--` ends the options, so a command can start with `-`; without it,
    /// such a word is an option, here an unknown one.
    #[test]
    fn a_command_that_starts_with_a_dash_needs_two_dashes() {
        assert_eq!(words(&ssh(&["nas", "--", "-x", "y"])), ["-x", "y"]);
        assert_eq!(words(&ssh(&["--", "nas", "-x"])), ["-x"]);
        assert!(parse(&["ssh", "nas", "-x"]).is_err());
        assert!(parse(&["ssh", "nas"]).is_err(), "a command is required");
    }

    /// The command is one line for the remote user's shell, COMMAND and
    /// ARGS joined with single spaces as ssh joins them, unless `-s` keeps
    /// the words as they are (DESIGN.md §8).
    #[test]
    fn s_keeps_the_words_of_the_command() {
        let line = ssh(&["nas", "printf", "%s\\n", "a b"]).request(false, false);
        assert_eq!(
            line.command,
            RemoteCommand::Shell(b"printf %s\\n a b".to_vec())
        );
        let argv = ssh(&["-s", "nas", "printf", "%s\\n", "a b"]).request(false, false);
        assert_eq!(
            argv.command,
            RemoteCommand::Argv(vec![b"printf".to_vec(), b"%s\\n".to_vec(), b"a b".to_vec()])
        );
    }

    /// The global `-q` and `--no-human-readable` travel in REQUEST.
    #[test]
    fn the_request_carries_the_global_options() {
        let args = ssh(&["--batch-stdout", "nas", "true"]);
        let request = args.request(true, true);
        assert!(request.quiet);
        assert!(!request.human_readable);
        assert!(request.settings.batch_stdout);
        let request = args.request(false, false);
        assert!(!request.quiet);
        assert!(request.human_readable);
    }

    /// `-n` makes the command read EOF at once, so neither a delivery nor
    /// the deduplication of stdin can apply: `-n` with `--batch-stdin` or
    /// `--dedup-stdin` is a usage error (DESIGN.md §8).
    #[test]
    fn no_stdin_excludes_the_options_of_stdin() {
        for option in ["--batch-stdin", "--dedup-stdin"] {
            assert!(parse(&["ssh", "-n", option, "nas", "true"]).is_err());
            assert!(parse(&["ssh", "nas", option, "--no-stdin", "true"]).is_err());
        }
        assert!(ssh(&["-n", "--batch-stdout", "nas", "true"]).no_stdin);
        let push = ssh(&["--dedup-stdin", "--batch-stdin", "nas", "true"]);
        assert!(push.settings.batch_stdin);
    }

    /// The stdout of the command is deduplicated, unless `--dedup-stdin`
    /// asks for stdin instead, or `--dedup-stdin --dedup-stdout` for both
    /// streams, and REQUEST tells the server which (DESIGN.md §8).
    #[test]
    fn the_dedup_options_choose_the_deduplicated_streams() {
        let deduplicated = |words: &[&str]| {
            let words = [&["nas"], words, &["true"]].concat();
            ssh(&words).request(false, false).deduplicated
        };
        assert_eq!(deduplicated(&[]), Deduplicated::Stdout);
        assert_eq!(deduplicated(&["--dedup-stdout"]), Deduplicated::Stdout);
        assert_eq!(deduplicated(&["--dedup-stdin"]), Deduplicated::Stdin);
        let both = ["--dedup-stdin", "--dedup-stdout"];
        assert_eq!(deduplicated(&both), Deduplicated::Both);
        // `-n` excludes the deduplication of stdin, also of both streams.
        assert!(parse(&[&["ssh", "-n"], &both[..], &["nas", "true"]].concat()).is_err());
        assert!(ssh(&["-n", "--dedup-stdout", "nas", "true"]).no_stdin);
    }

    /// The man page, `docs/hashsync.1`, documents the commands and options
    /// of the program and no others, so that a new one cannot go
    /// undocumented and a removed one cannot stay. Each option has an entry
    /// of its own: an item of an option list whose head names it with an
    /// mdoc `Fl` macro. The entries of the options of all commands are in
    /// the sections before COMMANDS, and each command but the internal ones
    /// has an item in COMMANDS, with the entries of its options. A mention
    /// in the text of another entry does not count, so the test fails when
    /// an entry is deleted, even if other entries still name its option.
    #[test]
    fn the_man_page_documents_every_command_and_option() {
        let page = include_str!("../docs/hashsync.1");
        let cli = Cli::command();
        // What the entries `items` get wrong about the options of
        // `command`, as the user writes them.
        let wrong_options = |command: &clap::Command, items: &HashSet<&str>| {
            let mut options = HashSet::new();
            for arg in command.get_arguments() {
                options.extend(arg.get_long().map(|long| format!("-{long}")));
                options.extend(arg.get_short().map(String::from));
            }
            let name = command.get_name();
            let undocumented = options
                .iter()
                .filter(|option| !items.contains(option.as_str()))
                .map(|option| format!("undocumented option {name} -{option}"));
            let unknown = items
                .iter()
                .filter(|item| !options.contains(**item))
                .map(|item| format!("unknown option {name} -{item}"));
            undocumented.chain(unknown).collect::<Vec<_>>()
        };
        let before_commands = page.lines().take_while(|line| *line != ".Sh COMMANDS");
        let mut wrong = wrong_options(&cli, &item_flags(before_commands));
        let commands = man_page_commands(page);
        let visible = |command: &&clap::Command| !command.is_hide_set();
        for command in cli.get_subcommands().filter(visible) {
            match commands.get(command.get_name()) {
                Some(items) => wrong.extend(wrong_options(command, items)),
                None => wrong.push(format!("undocumented command {}", command.get_name())),
            }
        }
        for name in commands.keys() {
            if cli.find_subcommand(name).filter(visible).is_none() {
                wrong.push(format!("unknown command {name}"));
            }
        }
        assert!(wrong.is_empty(), "hashsync.1: {wrong:?}");
    }

    /// The options that have entries in each item of the COMMANDS section
    /// of the man page (`item_flags`), by the name of its command: the
    /// first word after a `Cm` macro in it.
    fn man_page_commands(page: &str) -> HashMap<&str, HashSet<&str>> {
        let mut items = Vec::new();
        let mut item = Vec::new();
        let mut depth = 0;
        let section = page
            .lines()
            .skip_while(|line| *line != ".Sh COMMANDS")
            .skip(1)
            .take_while(|line| !line.starts_with(".Sh "));
        for line in section {
            // The items of the outermost list are the commands; the lists
            // in them hold their options.
            if depth == 1 && line.starts_with(".It") {
                items.push(std::mem::take(&mut item));
            }
            if line.starts_with(".Bl") {
                depth += 1;
            } else if line.starts_with(".El") {
                depth -= 1;
            }
            item.push(line);
        }
        items.push(item);
        items
            .into_iter()
            .filter_map(|item| {
                let mut words = item.iter().copied().flat_map(str::split_whitespace);
                words.find(|word| matches!(*word, "Cm" | ".Cm"))?;
                let name = words.next()?;
                Some((name, item_flags(item.iter().copied())))
            })
            .collect()
    }

    /// The options that have entries in a part of the man page: those that
    /// follow an `Fl` macro on the `.It` lines that head its items, up to
    /// the next macro, as mdoc writes them, without their first `-`.
    /// `.It Fl q , -quiet` gives `q` for `-q` and `-quiet` for `--quiet`.
    fn item_flags<'a>(lines: impl Iterator<Item = &'a str>) -> HashSet<&'a str> {
        let is_macro = |word: &str| {
            let mut letters = word.trim_start_matches('.').chars();
            word.len() >= 2
                && letters.next().is_some_and(|c| c.is_ascii_uppercase())
                && letters.all(|c| c.is_ascii_lowercase())
        };
        // A short option is a letter or a digit, and a long one a `-` and a
        // name. Other words, such as the `,` between the two forms of an
        // option, or `-` for `--`, are no options.
        let is_option = |word: &str| match word.as_bytes() {
            [short] => short.is_ascii_alphanumeric(),
            [b'-', _, ..] => true,
            _ => false,
        };
        let mut flags = HashSet::new();
        for line in lines.filter(|line| line.starts_with(".It ")) {
            let mut after_fl = false;
            for word in line.split_whitespace() {
                if is_macro(word) {
                    after_fl = word.trim_start_matches('.') == "Fl";
                } else if after_fl && is_option(word) {
                    flags.insert(word);
                }
            }
        }
        flags
    }

    /// `--ssh` and `--hashsync-path` say how the client starts the server,
    /// with `ssh` and `hashsync` by default (DESIGN.md §8).
    #[test]
    fn the_remote_takes_the_ssh_options() {
        let args = ssh(&[
            "--ssh",
            "ssh -p 2222",
            "--hashsync-path",
            "/opt/hashsync",
            "-l",
            "u",
            "nas",
            "true",
        ]);
        assert_eq!(
            args.remote(),
            ssh::Remote {
                ssh: ssh::Program {
                    executable: b"ssh".to_vec(),
                    options: vec![b"-p".to_vec(), b"2222".to_vec()],
                },
                login: Some(b"u".to_vec()),
                host: b"nas".to_vec(),
                hashsync_path: b"/opt/hashsync".to_vec(),
            }
        );
        let remote = ssh(&["nas", "true"]).remote();
        assert_eq!(remote.ssh, ssh::Program::parse(b"ssh").unwrap());
        assert_eq!(remote.hashsync_path, b"hashsync");
    }

    /// An `--ssh` that has no words or that does not split as a shell
    /// splits it, and an empty `--hashsync-path`, are usage errors, whose
    /// message shows the value as messages show values (DESIGN.md §8).
    #[test]
    fn bad_ssh_commands_and_hashsync_paths_are_usage_errors() {
        for (option, value, message) in [
            (
                "--ssh",
                "",
                "invalid value '' for '--ssh <CMD>': empty command",
            ),
            (
                "--ssh",
                " ",
                "invalid value ' ' for '--ssh <CMD>': empty command",
            ),
            (
                "--ssh",
                "ssh 'x",
                r"invalid value 'ssh \x27x' for '--ssh <CMD>': unterminated single quote",
            ),
            (
                "--hashsync-path",
                "",
                "invalid value '' for '--hashsync-path <PATH>': empty path",
            ),
        ] {
            let error = parse(&["ssh", option, value, "nas", "true"]).err().unwrap();
            assert_eq!(
                error.kind(),
                ErrorKind::ValueValidation,
                "{option} {value:?}"
            );
            assert!(error.to_string().contains(message), "{error}");
        }
    }
}
