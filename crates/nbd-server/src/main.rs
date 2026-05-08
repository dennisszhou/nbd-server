#![forbid(unsafe_code)]

mod cli;
mod doctor;
mod logging;
mod output;

use clap::Parser;
use cli::{Cli, Command, ConfigAction, ConfigArgs, DoctorArgs, ServeArgs};
use nbd_config::{ConfigFile, ConfigSource, NbdConfig};
use nbd_server::NbdServer;
use nbd_server::observability::{self, event, target};
use std::env;
use std::error::Error;
use std::path::PathBuf;

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("error: {error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();

    match cli.command {
        Command::Serve(args) => run_serve(cli.config, args).await,
        Command::Config(args) => run_config(cli.config, args),
        Command::Doctor(args) => run_doctor(cli.config, args).await,
    }
}

async fn run_serve(config_path: Option<PathBuf>, args: ServeArgs) -> Result<(), Box<dyn Error>> {
    let config_source = config_source(config_path);
    let config_source_description = format!("{config_source:?}");
    let config = NbdConfig::load(config_source)?;
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
        config_source = %config_source_description,
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

    tokio::signal::ctrl_c().await?;
    server.shutdown().await?;
    Ok(())
}

fn run_config(config_path: Option<PathBuf>, args: ConfigArgs) -> Result<(), Box<dyn Error>> {
    let config_file = config_file(config_path)?;

    if args.path {
        if args.action.is_some() {
            return Err("config --path cannot be combined with a subcommand".into());
        }
        output::print_config_path(config_file.path());
        return Ok(());
    }

    match args.action {
        Some(ConfigAction::Get { key }) => {
            let loaded = config_file.load_or_default()?;
            output::print_config_value(key, &loaded);
        }
        Some(ConfigAction::Init) => {
            let initialized = config_file.init()?;
            output::print_config_initialized(&initialized);
        }
        None => {
            let loaded = config_file.load_or_default()?;
            output::print_config(&loaded)?;
        }
    }

    Ok(())
}

async fn run_doctor(config_path: Option<PathBuf>, args: DoctorArgs) -> Result<(), Box<dyn Error>> {
    let report = doctor::check(config_path).await;
    output::print_doctor_report(&report, args.json)?;
    if output::doctor_failed(&report) {
        return Err("doctor checks failed".into());
    }

    Ok(())
}

fn config_source(path: Option<PathBuf>) -> ConfigSource {
    path.map(ConfigSource::ExplicitPath)
        .unwrap_or(ConfigSource::DefaultUserPath)
}

fn config_file(path: Option<PathBuf>) -> Result<ConfigFile, nbd_config::ConfigError> {
    match path {
        Some(path) => Ok(ConfigFile::explicit(path)),
        None => ConfigFile::local(),
    }
}
