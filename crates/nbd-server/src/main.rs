#![forbid(unsafe_code)]

mod logging;

use nbd_config::{ConfigSource, NbdConfig};
use nbd_server::NbdServer;
use std::env;
use std::error::Error;
use std::net::SocketAddr;
use std::path::PathBuf;

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("error: {error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn Error>> {
    let raw_args: Vec<String> = env::args().skip(1).collect();
    let args = parse_serve_args(&raw_args)?;
    let config_source = format!("{:?}", args.config_source);
    let config = NbdConfig::load(args.config_source)?;
    let logging_policy = logging::LoggingPolicy::from_options(logging::LoggingOptions {
        file_path: config.logging.file_path.clone(),
        log_stdout: args.log_stdout,
        env_filter: env::var("RUST_LOG").ok(),
    });
    let _logging_guard = logging::init_logging(logging_policy)?;

    tracing::info!(
        target: "nbd_server::ops",
        event = "server.starting",
        listen_addr = %args.listen,
        config_source = %config_source,
        log_file_path = %config.logging.file_path.display(),
    );

    let server = NbdServer::start_on(config, args.listen).await?;
    tracing::info!(
        target: "nbd_server::ops",
        event = "server.listening",
        listen_addr = %server.addr(),
    );

    std::future::pending::<()>().await;
    Ok(())
}

#[derive(Debug, PartialEq, Eq)]
struct ServeArgs {
    config_source: ConfigSource,
    listen: SocketAddr,
    log_stdout: bool,
}

fn parse_serve_args(args: &[String]) -> Result<ServeArgs, Box<dyn Error>> {
    let command = args.first().map(String::as_str);
    if command != Some("serve") {
        return Err(usage().into());
    }

    let mut config_path = None;
    let mut listen = "127.0.0.1:10809".parse::<SocketAddr>()?;
    let mut log_stdout = false;
    let mut index = 1;
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

fn usage() -> &'static str {
    "usage: nbd-server serve [--config <path>] [--listen <addr:port>] [--log-stdout]"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serve_uses_default_config_source_without_config_arg() {
        let args = strings(&["serve", "--listen", "127.0.0.1:12000"]);

        let parsed = parse_serve_args(&args).expect("parse args");

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

        let parsed = parse_serve_args(&args).expect("parse args");

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

        let parsed = parse_serve_args(&args).expect("parse args");

        assert!(parsed.log_stdout);
        assert_eq!(parsed.listen, "127.0.0.1:12002".parse().unwrap());
    }

    fn strings(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| (*value).to_owned()).collect()
    }
}
