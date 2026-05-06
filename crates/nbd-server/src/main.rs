#![forbid(unsafe_code)]

mod logging;

use nbd_config::{ConfigFile, ConfigKey, ConfigSource, NbdConfig};
use nbd_server::NbdServer;
use nbd_server::observability::{self, event, target};
use std::env;
use std::error::Error;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("error: {error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn Error>> {
    let raw_args: Vec<String> = env::args().skip(1).collect();
    let command = parse_args(&raw_args)?;

    match command {
        Command::Serve(args) => run_serve(args).await,
        Command::Config(args) => run_config(args),
    }
}

async fn run_serve(args: ServeArgs) -> Result<(), Box<dyn Error>> {
    let config_source = format!("{:?}", args.config_source);
    let config = NbdConfig::load(args.config_source)?;
    let logging_policy = logging::LoggingPolicy::from_options(logging::LoggingOptions {
        file_path: config.logging.file_path.clone(),
        log_stdout: args.log_stdout,
        env_filter: env::var("RUST_LOG").ok(),
    });
    let _logging_guard = logging::init_logging(logging_policy)?;

    tracing::info!(
        target: target::OPS,
        event = event::LOGGING_INITIALIZED,
        service = observability::SERVICE_NAME,
        server_instance_id = observability::server_instance_id(),
        pid = observability::pid(),
        log_file_path = %config.logging.file_path.display(),
    );

    tracing::info!(
        target: target::OPS,
        event = event::SERVER_STARTING,
        service = observability::SERVICE_NAME,
        server_instance_id = observability::server_instance_id(),
        pid = observability::pid(),
        listen_addr = %args.listen,
        config_source = %config_source,
        log_file_path = %config.logging.file_path.display(),
    );

    let server = NbdServer::start_on(config, args.listen).await?;
    tracing::info!(
        target: target::OPS,
        event = event::SERVER_LISTENING,
        service = observability::SERVICE_NAME,
        server_instance_id = observability::server_instance_id(),
        pid = observability::pid(),
        listen_addr = %server.addr(),
    );

    std::future::pending::<()>().await;
    Ok(())
}

fn run_config(args: ConfigArgs) -> Result<(), Box<dyn Error>> {
    let config_file = match args.config_path {
        Some(path) => ConfigFile::explicit(path),
        None => ConfigFile::local()?,
    };
    let loaded = config_file.load_or_default()?;

    match args.action {
        ConfigAction::Print => {
            print!("{}", loaded.config().to_toml_string()?);
        }
        ConfigAction::Get(key) => {
            println!("{}", key.value(loaded.config()));
        }
    }

    Ok(())
}

#[derive(Debug, PartialEq, Eq)]
enum Command {
    Serve(ServeArgs),
    Config(ConfigArgs),
}

#[derive(Debug, PartialEq, Eq)]
struct ServeArgs {
    config_source: ConfigSource,
    listen: SocketAddr,
    log_stdout: bool,
}

#[derive(Debug, PartialEq, Eq)]
struct ConfigArgs {
    config_path: Option<PathBuf>,
    action: ConfigAction,
}

#[derive(Debug, PartialEq, Eq)]
enum ConfigAction {
    Print,
    Get(ConfigKey),
}

fn parse_args(args: &[String]) -> Result<Command, Box<dyn Error>> {
    match args.first().map(String::as_str) {
        Some("serve") => parse_serve_args(&args[1..]).map(Command::Serve),
        Some("config") => parse_config_args(&args[1..]).map(Command::Config),
        _ => Err(usage().into()),
    }
}

fn parse_serve_args(args: &[String]) -> Result<ServeArgs, Box<dyn Error>> {
    let mut config_path = None;
    let mut listen = "127.0.0.1:10809".parse::<SocketAddr>()?;
    let mut log_stdout = false;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--config" => {
                index += 1;
                let value = args.get(index).ok_or("missing value for --config")?;
                config_path = Some(PathBuf::from(value));
            }
            "--listen" => {
                index += 1;
                let value = args.get(index).ok_or("missing value for --listen")?;
                listen = value.parse()?;
            }
            "--log-stdout" => {
                log_stdout = true;
            }
            _ => return Err(usage().into()),
        }
        index += 1;
    }

    let config_source = config_path
        .map(ConfigSource::ExplicitPath)
        .unwrap_or(ConfigSource::DefaultUserPath);

    Ok(ServeArgs {
        config_source,
        listen,
        log_stdout,
    })
}

fn parse_config_args(args: &[String]) -> Result<ConfigArgs, Box<dyn Error>> {
    let mut config_path = None;
    let mut action = ConfigAction::Print;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--config" => {
                index += 1;
                let value = args.get(index).ok_or("missing value for --config")?;
                config_path = Some(PathBuf::from(value));
            }
            "get" => {
                index += 1;
                let value = args.get(index).ok_or("missing key for config get")?;
                action = ConfigAction::Get(ConfigKey::from_str(value)?);
            }
            _ => return Err(usage().into()),
        }
        index += 1;
    }

    Ok(ConfigArgs {
        config_path,
        action,
    })
}

fn usage() -> &'static str {
    "usage: nbd-server serve [--config <path>] [--listen <addr:port>] [--log-stdout]\n       nbd-server config [--config <path>] [get <key>]"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serve_uses_default_config_source_without_config_arg() {
        let args = strings(&["serve", "--listen", "127.0.0.1:12000"]);

        let parsed = parse_args(&args).expect("parse args");
        let Command::Serve(parsed) = parsed else {
            panic!("expected serve command");
        };

        assert_eq!(parsed.config_source, ConfigSource::DefaultUserPath);
        assert_eq!(parsed.listen, "127.0.0.1:12000".parse().unwrap());
        assert!(!parsed.log_stdout);
    }

    #[test]
    fn serve_preserves_explicit_config_source() {
        let args = strings(&[
            "serve",
            "--config",
            "/tmp/nbd/config.toml",
            "--listen",
            "127.0.0.1:12001",
        ]);

        let parsed = parse_args(&args).expect("parse args");
        let Command::Serve(parsed) = parsed else {
            panic!("expected serve command");
        };

        assert_eq!(
            parsed.config_source,
            ConfigSource::ExplicitPath(PathBuf::from("/tmp/nbd/config.toml"))
        );
        assert_eq!(parsed.listen, "127.0.0.1:12001".parse().unwrap());
        assert!(!parsed.log_stdout);
    }

    #[test]
    fn serve_parses_stdout_logging_flag() {
        let args = strings(&["serve", "--log-stdout", "--listen", "127.0.0.1:12002"]);

        let parsed = parse_args(&args).expect("parse args");
        let Command::Serve(parsed) = parsed else {
            panic!("expected serve command");
        };

        assert!(parsed.log_stdout);
        assert_eq!(parsed.listen, "127.0.0.1:12002".parse().unwrap());
    }

    #[test]
    fn config_print_uses_default_path_without_config_arg() {
        let args = strings(&["config"]);

        let parsed = parse_args(&args).expect("parse args");
        let Command::Config(parsed) = parsed else {
            panic!("expected config command");
        };

        assert_eq!(parsed.config_path, None);
        assert_eq!(parsed.action, ConfigAction::Print);
    }

    #[test]
    fn config_print_preserves_explicit_path() {
        let args = strings(&["config", "--config", "/tmp/nbd/custom.toml"]);

        let parsed = parse_args(&args).expect("parse args");
        let Command::Config(parsed) = parsed else {
            panic!("expected config command");
        };

        assert_eq!(
            parsed.config_path,
            Some(PathBuf::from("/tmp/nbd/custom.toml"))
        );
        assert_eq!(parsed.action, ConfigAction::Print);
    }

    #[test]
    fn config_get_parses_key() {
        let args = strings(&[
            "config",
            "--config",
            "/tmp/nbd/custom.toml",
            "get",
            "server.export_queue_depth",
        ]);

        let parsed = parse_args(&args).expect("parse args");
        let Command::Config(parsed) = parsed else {
            panic!("expected config command");
        };

        assert_eq!(
            parsed.config_path,
            Some(PathBuf::from("/tmp/nbd/custom.toml"))
        );
        assert_eq!(
            parsed.action,
            ConfigAction::Get(ConfigKey::ServerExportQueueDepth)
        );
    }

    fn strings(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| (*value).to_owned()).collect()
    }
}
