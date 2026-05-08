use clap::{Parser, Subcommand};
use nbd_config::ConfigKey;
use std::net::SocketAddr;
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(name = "nbd-server")]
#[command(about = "Run and inspect the NBD server")]
pub struct Cli {
    #[arg(long, global = true)]
    pub config: Option<PathBuf>,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    Serve(ServeArgs),
    Config(ConfigArgs),
    Doctor(DoctorArgs),
}

#[derive(Debug, Clone, PartialEq, Eq, clap::Args)]
pub struct ServeArgs {
    #[arg(long, default_value = "127.0.0.1:10809")]
    pub listen: SocketAddr,

    #[arg(long)]
    pub log_stdout: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, clap::Args)]
pub struct ConfigArgs {
    #[arg(long)]
    pub path: bool,

    #[command(subcommand)]
    pub action: Option<ConfigAction>,
}

#[derive(Debug, Clone, PartialEq, Eq, Subcommand)]
pub enum ConfigAction {
    Get { key: ConfigKey },
    Init,
}

#[derive(Debug, Clone, PartialEq, Eq, clap::Args)]
pub struct DoctorArgs {
    #[arg(long)]
    pub json: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use nbd_config::ConfigKey;

    #[test]
    fn serve_uses_default_config_source_without_config_arg() {
        let parsed = Cli::try_parse_from(["nbd-server", "serve", "--listen", "127.0.0.1:12000"])
            .expect("parse args");

        let Command::Serve(parsed_serve) = parsed.command else {
            panic!("expected serve command");
        };
        assert_eq!(parsed.config, None);
        assert_eq!(parsed_serve.listen, "127.0.0.1:12000".parse().unwrap());
        assert!(!parsed_serve.log_stdout);
    }

    #[test]
    fn serve_preserves_explicit_config_source() {
        let parsed = Cli::try_parse_from([
            "nbd-server",
            "serve",
            "--config",
            "/tmp/nbd/config.toml",
            "--listen",
            "127.0.0.1:12001",
        ])
        .expect("parse args");

        let Command::Serve(parsed_serve) = parsed.command else {
            panic!("expected serve command");
        };
        assert_eq!(parsed.config, Some(PathBuf::from("/tmp/nbd/config.toml")));
        assert_eq!(parsed_serve.listen, "127.0.0.1:12001".parse().unwrap());
        assert!(!parsed_serve.log_stdout);
    }

    #[test]
    fn serve_parses_stdout_logging_flag() {
        let parsed = Cli::try_parse_from([
            "nbd-server",
            "serve",
            "--log-stdout",
            "--listen",
            "127.0.0.1:12002",
        ])
        .expect("parse args");

        let Command::Serve(parsed_serve) = parsed.command else {
            panic!("expected serve command");
        };
        assert!(parsed_serve.log_stdout);
        assert_eq!(parsed_serve.listen, "127.0.0.1:12002".parse().unwrap());
    }

    #[test]
    fn serve_rejects_json_result_mode() {
        let error = Cli::try_parse_from(["nbd-server", "serve", "--json"])
            .expect_err("serve should not accept JSON result mode");

        assert_eq!(error.kind(), clap::error::ErrorKind::UnknownArgument);
    }

    #[test]
    fn doctor_parses_json_flag() {
        let parsed = Cli::try_parse_from([
            "nbd-server",
            "doctor",
            "--config",
            "/tmp/nbd/config.toml",
            "--json",
        ])
        .expect("parse args");

        let Command::Doctor(parsed_doctor) = parsed.command else {
            panic!("expected doctor command");
        };
        assert_eq!(parsed.config, Some(PathBuf::from("/tmp/nbd/config.toml")));
        assert!(parsed_doctor.json);
    }

    #[test]
    fn config_print_uses_default_path_without_config_arg() {
        let parsed = Cli::try_parse_from(["nbd-server", "config"]).expect("parse args");

        let Command::Config(parsed_config) = parsed.command else {
            panic!("expected config command");
        };
        assert_eq!(parsed.config, None);
        assert!(!parsed_config.path);
        assert_eq!(parsed_config.action, None);
    }

    #[test]
    fn config_print_preserves_explicit_path() {
        let parsed =
            Cli::try_parse_from(["nbd-server", "config", "--config", "/tmp/nbd/custom.toml"])
                .expect("parse args");

        let Command::Config(parsed_config) = parsed.command else {
            panic!("expected config command");
        };
        assert_eq!(parsed.config, Some(PathBuf::from("/tmp/nbd/custom.toml")));
        assert!(!parsed_config.path);
        assert_eq!(parsed_config.action, None);
    }

    #[test]
    fn config_path_preserves_explicit_path() {
        let parsed = Cli::try_parse_from([
            "nbd-server",
            "config",
            "--path",
            "--config",
            "/tmp/nbd/custom.toml",
        ])
        .expect("parse args");

        let Command::Config(parsed_config) = parsed.command else {
            panic!("expected config command");
        };
        assert_eq!(parsed.config, Some(PathBuf::from("/tmp/nbd/custom.toml")));
        assert!(parsed_config.path);
        assert_eq!(parsed_config.action, None);
    }

    #[test]
    fn config_get_parses_key() {
        let parsed = Cli::try_parse_from([
            "nbd-server",
            "config",
            "--config",
            "/tmp/nbd/custom.toml",
            "get",
            "server.export_queue_depth",
        ])
        .expect("parse args");

        let Command::Config(parsed_config) = parsed.command else {
            panic!("expected config command");
        };
        assert_eq!(parsed.config, Some(PathBuf::from("/tmp/nbd/custom.toml")));
        assert_eq!(
            parsed_config.action,
            Some(ConfigAction::Get {
                key: ConfigKey::ServerExportQueueDepth
            })
        );
    }

    #[test]
    fn config_init_preserves_explicit_path_after_subcommand() {
        let parsed = Cli::try_parse_from([
            "nbd-server",
            "config",
            "init",
            "--config",
            "/tmp/nbd/custom.toml",
        ])
        .expect("parse args");

        let Command::Config(parsed_config) = parsed.command else {
            panic!("expected config command");
        };
        assert_eq!(parsed.config, Some(PathBuf::from("/tmp/nbd/custom.toml")));
        assert_eq!(parsed_config.action, Some(ConfigAction::Init));
    }
}
