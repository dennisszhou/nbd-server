use clap::{Parser, Subcommand};
use nbd_control_plane::ExportEngineKind;
use std::path::PathBuf;

const DEFAULT_BLOCK_SIZE: u64 = 4096;

#[derive(Debug, Parser)]
#[command(name = "nbdcli")]
#[command(about = "Manage NBD exports")]
pub struct Cli {
    #[arg(long, global = true)]
    pub config: Option<PathBuf>,

    #[arg(long, global = true)]
    pub json: bool,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    Create(CreateArgs),
    List(ListArgs),
    Inspect(InspectArgs),
    Clone(CloneArgs),
    Delete(DeleteArgs),
}

#[derive(Debug, Clone, PartialEq, Eq, clap::Args)]
pub struct CreateArgs {
    pub name: String,

    #[arg(long)]
    pub size: u64,

    #[arg(long, default_value_t = DEFAULT_BLOCK_SIZE)]
    pub block_size: u64,

    #[arg(long, default_value_t = ExportEngineKind::Memory)]
    pub engine: ExportEngineKind,
}

#[derive(Debug, Clone, PartialEq, Eq, clap::Args)]
pub struct ListArgs {
    #[arg(long)]
    pub include_deleted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, clap::Args)]
pub struct InspectArgs {
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, clap::Args)]
pub struct CloneArgs {
    pub source: String,
    pub destination: String,
}

#[derive(Debug, Clone, PartialEq, Eq, clap::Args)]
pub struct DeleteArgs {
    pub name: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_global_json_before_command() {
        let parsed = Cli::try_parse_from(["nbdcli", "--json", "list"]).expect("parse args");

        assert!(parsed.json);
        let Command::List(args) = parsed.command else {
            panic!("expected list command");
        };
        assert!(!args.include_deleted);
    }

    #[test]
    fn list_accepts_compat_json_after_command() {
        let parsed = Cli::try_parse_from(["nbdcli", "list", "--json"]).expect("parse args");

        assert!(parsed.json);
        let Command::List(_) = parsed.command else {
            panic!("expected list command");
        };
    }

    #[test]
    fn inspect_accepts_compat_json_after_command() {
        let parsed =
            Cli::try_parse_from(["nbdcli", "inspect", "disk-a", "--json"]).expect("parse args");

        assert!(parsed.json);
        let Command::Inspect(args) = parsed.command else {
            panic!("expected inspect command");
        };
        assert_eq!(args.name, "disk-a");
    }

    #[test]
    fn create_preserves_explicit_config_and_engine() {
        let parsed = Cli::try_parse_from([
            "nbdcli",
            "--config",
            "/tmp/nbd/config.toml",
            "create",
            "disk-a",
            "--size",
            "1048576",
            "--engine",
            "wal_durable",
        ])
        .expect("parse args");

        assert_eq!(parsed.config, Some(PathBuf::from("/tmp/nbd/config.toml")));
        let Command::Create(args) = parsed.command else {
            panic!("expected create command");
        };
        assert_eq!(args.name, "disk-a");
        assert_eq!(args.size, 1048576);
        assert_eq!(args.block_size, DEFAULT_BLOCK_SIZE);
        assert_eq!(args.engine, ExportEngineKind::WalDurable);
    }
}
