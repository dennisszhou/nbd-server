#![forbid(unsafe_code)]

use clap::{Parser, Subcommand};
use nbd_config::{ConfigSource, NbdConfig};
use nbd_control_plane::{
    CatalogUrl, CreateExport, DeleteExport, ExportCatalog, ExportEngineKind, ExportMeta,
    ExportName, InspectExport, ListExports, SQLiteExportCatalog,
};
use std::error::Error;
use std::path::PathBuf;

const DEFAULT_BLOCK_SIZE: u64 = 4096;

#[derive(Debug, Parser)]
#[command(name = "nbdcli")]
#[command(about = "Manage NBD exports")]
struct Cli {
    #[arg(long, global = true)]
    config: Option<PathBuf>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Create {
        name: String,

        #[arg(long)]
        size: u64,

        #[arg(long, default_value_t = DEFAULT_BLOCK_SIZE)]
        block_size: u64,

        #[arg(long, default_value_t = ExportEngineKind::Memory)]
        engine: ExportEngineKind,
    },
    List {
        #[arg(long)]
        include_deleted: bool,

        #[arg(long)]
        json: bool,
    },
    Inspect {
        name: String,

        #[arg(long)]
        json: bool,
    },
    Delete {
        name: String,
    },
}

#[tokio::main]
async fn main() {
    if let Err(error) = run(Cli::parse()).await {
        eprintln!("error: {error}");
        std::process::exit(1);
    }
}

async fn run(cli: Cli) -> Result<(), Box<dyn Error>> {
    let config = load_config(cli.config)?;
    let catalog_url = CatalogUrl::parse(&config.catalog.url)?;
    let catalog = SQLiteExportCatalog::connect(&catalog_url).await?;

    match cli.command {
        Command::Create {
            name,
            size,
            block_size,
            engine,
        } => {
            let request = CreateExport::new(ExportName::new(name)?, size, block_size, engine)?;
            let meta = catalog.create_export(request).await?;
            println!(
                "created export {} size={} block_size={} engine={}",
                meta.name(),
                meta.size_bytes(),
                meta.block_size(),
                meta.engine_kind()
            );
        }
        Command::List {
            include_deleted,
            json,
        } => {
            let exports = catalog
                .list_exports(ListExports::new(include_deleted))
                .await?;
            if json {
                println!("{}", serde_json::to_string_pretty(&exports)?);
            } else {
                print_export_list(&exports);
            }
        }
        Command::Inspect { name, json } => {
            let meta = catalog
                .inspect_export(InspectExport::new(ExportName::new(name)?))
                .await?;
            if json {
                println!("{}", serde_json::to_string_pretty(&meta)?);
            } else {
                print_export(&meta);
            }
        }
        Command::Delete { name } => {
            let name = ExportName::new(name)?;
            catalog
                .delete_export(DeleteExport::new(name.clone()))
                .await?;
            println!("deleted export {name}");
        }
    }

    Ok(())
}

fn load_config(path: Option<PathBuf>) -> Result<NbdConfig, Box<dyn Error>> {
    let source = path
        .map(ConfigSource::ExplicitPath)
        .unwrap_or(ConfigSource::DefaultUserPath);

    Ok(NbdConfig::load(source)?)
}

fn print_export_list(exports: &[ExportMeta]) {
    for export in exports {
        println!(
            "{}\t{}\tsize={}\tblock_size={}\tengine={}\tlayout={}",
            export.name(),
            export.state(),
            export.size_bytes(),
            export.block_size(),
            export.engine_kind(),
            export.head().layout_kind()
        );
    }
}

fn print_export(export: &ExportMeta) {
    println!("name: {}", export.name());
    println!("state: {}", export.state());
    println!("size: {}", export.size_bytes());
    println!("block_size: {}", export.block_size());
    println!("engine: {}", export.engine_kind());
    println!("layout: {}", export.head().layout_kind());
    println!("checkpoint_wal_seq: {}", export.head().checkpoint_wal_seq());
    match export.head().root_node_id() {
        Some(root_node_id) => println!("root_node_id: {root_node_id}"),
        None => println!("root_node_id: <empty>"),
    }
}
