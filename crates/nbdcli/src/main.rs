#![forbid(unsafe_code)]

mod cli;
mod doctor;
mod output;

use clap::Parser;
use cli::{Cli, Command};
use nbd_config::{ConfigFile, NbdConfig};
use nbd_control_plane::{
    CatalogUrl, CloneExport, CreateExport, DeleteExport, ExportName, InspectExport, ListExports,
    open_catalog,
};
use std::error::Error;
use std::path::PathBuf;

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let output_mode = output::OutputMode::from_json(cli.json);
    let error_context = output::ErrorContext::from_command(&cli.command);

    if let Err(error) = run(cli).await {
        output::print_error(error.as_ref(), output_mode, &error_context);
        std::process::exit(1);
    }
}

async fn run(cli: Cli) -> Result<(), Box<dyn Error>> {
    let output_mode = output::OutputMode::from_json(cli.json);
    match cli.command {
        Command::Doctor(_) => {
            let report = doctor::check(cli.config).await;
            output::print_doctor_report(&report, output_mode)?;
            if output::doctor_failed(&report) {
                return Err("doctor checks failed".into());
            }
        }
        command => {
            let config = load_config(cli.config)?;
            run_catalog_command(command, config, output_mode).await?;
        }
    }

    Ok(())
}

async fn run_catalog_command(
    command: Command,
    config: NbdConfig,
    output_mode: output::OutputMode,
) -> Result<(), Box<dyn Error>> {
    let catalog_url = CatalogUrl::parse(&config.catalog.url)?;
    let catalog = open_catalog(&catalog_url).await?.export_catalog();

    match command {
        Command::Create(args) => {
            let request = CreateExport::new(
                ExportName::new(args.name)?,
                args.size,
                args.block_size,
                args.engine,
            )?;
            let meta = catalog.create_export(request).await?;
            output::print_created(&meta, output_mode)?;
        }
        Command::List(args) => {
            let exports = catalog
                .list_exports(ListExports::new(args.include_deleted))
                .await?;
            output::print_export_list(&exports, output_mode)?;
        }
        Command::Inspect(args) => {
            let meta = catalog
                .inspect_export(InspectExport::new(ExportName::new(args.name)?))
                .await?;
            output::print_export(&meta, output_mode)?;
        }
        Command::Clone(args) => {
            let request = CloneExport::new(
                ExportName::new(args.source)?,
                ExportName::new(args.destination)?,
            )?;
            let cloned = catalog.clone_export(request).await?;
            output::print_cloned(&cloned, output_mode)?;
        }
        Command::Delete(args) => {
            let name = ExportName::new(args.name)?;
            catalog
                .delete_export(DeleteExport::new(name.clone()))
                .await?;
            output::print_deleted(&name, output_mode)?;
        }
        Command::Doctor(_) => unreachable!("doctor is handled before opening the catalog"),
    }

    Ok(())
}

fn load_config(path: Option<PathBuf>) -> Result<NbdConfig, Box<dyn Error>> {
    let loaded = match path {
        Some(path) => ConfigFile::explicit(path).load()?,
        None => ConfigFile::local()?.load()?,
    };

    Ok(loaded.into_config())
}
