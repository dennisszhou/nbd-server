use crate::cli::{CloneArgs, Command};
use nbd_control_plane::{CloneExportResult, ExportName, ExportRecord};
use std::error::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputMode {
    Human,
    Json,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ErrorContext {
    operation: Option<&'static str>,
    resource: Option<String>,
}

impl OutputMode {
    pub fn from_json(json: bool) -> Self {
        if json { Self::Json } else { Self::Human }
    }
}

impl ErrorContext {
    pub fn from_command(command: &Command) -> Self {
        match command {
            Command::Create(args) => Self::new("create", Some(args.name.clone())),
            Command::List(_) => Self::new("list", None),
            Command::Inspect(args) => Self::new("inspect", Some(args.name.clone())),
            Command::Clone(args) => Self::from_clone(args),
            Command::Delete(args) => Self::new("delete", Some(args.name.clone())),
        }
    }

    fn new(operation: &'static str, resource: Option<String>) -> Self {
        Self {
            operation: Some(operation),
            resource,
        }
    }

    fn from_clone(args: &CloneArgs) -> Self {
        Self::new(
            "clone",
            Some(format!("{} -> {}", args.source, args.destination)),
        )
    }
}

pub fn print_created(export: &ExportRecord, mode: OutputMode) -> Result<(), Box<dyn Error>> {
    match mode {
        OutputMode::Human => println!(
            "created export {} size={} block_size={} engine={}",
            export.name(),
            export.size_bytes(),
            export.block_size(),
            export.engine_kind()
        ),
        OutputMode::Json => print_json_value(&serde_json::json!({
            "status": "created",
            "export": export,
        }))?,
    }
    Ok(())
}

pub fn print_export_list(exports: &[ExportRecord], mode: OutputMode) -> Result<(), Box<dyn Error>> {
    match mode {
        OutputMode::Human => {
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
        OutputMode::Json => println!("{}", serde_json::to_string_pretty(exports)?),
    }
    Ok(())
}

pub fn print_export(export: &ExportRecord, mode: OutputMode) -> Result<(), Box<dyn Error>> {
    match mode {
        OutputMode::Human => {
            println!("name: {}", export.name());
            println!("state: {}", export.state());
            println!("size: {}", export.size_bytes());
            println!("block_size: {}", export.block_size());
            println!("engine: {}", export.engine_kind());
            println!("layout: {}", export.head().layout_kind());
            println!("base_wal_seq: {}", export.head().base_wal_seq());
            match export.head().root_node_id() {
                Some(root_node_id) => println!("root_node_id: {root_node_id}"),
                None => println!("root_node_id: <empty>"),
            }
        }
        OutputMode::Json => println!("{}", serde_json::to_string_pretty(export)?),
    }
    Ok(())
}

pub fn print_cloned(result: &CloneExportResult, mode: OutputMode) -> Result<(), Box<dyn Error>> {
    match mode {
        OutputMode::Human => {
            println!(
                "cloned export {} from {} source_base_wal_seq={} destination_base_wal_seq={}",
                result.destination().name(),
                result.source().name(),
                result.source().head().base_wal_seq(),
                result.destination().head().base_wal_seq(),
            );
            println!("note: copied committed checkpoint only; source WAL was not cloned");
        }
        OutputMode::Json => print_json_value(&serde_json::json!({
            "status": "cloned",
            "source": result.source(),
            "destination": result.destination(),
            "source_wal_cloned": false,
        }))?,
    }
    Ok(())
}

pub fn print_deleted(name: &ExportName, mode: OutputMode) -> Result<(), Box<dyn Error>> {
    match mode {
        OutputMode::Human => println!("deleted export {name}"),
        OutputMode::Json => print_json_value(&serde_json::json!({
            "status": "deleted",
            "name": name,
        }))?,
    }
    Ok(())
}

pub fn print_error(error: &(dyn Error + 'static), mode: OutputMode, context: &ErrorContext) {
    match mode {
        OutputMode::Human => eprintln!("error: {error}"),
        OutputMode::Json => {
            let report = serde_json::json!({
                "status": "error",
                "code": error_code(error),
                "message": error.to_string(),
                "operation": context.operation,
                "resource": context.resource,
            });
            eprintln!(
                "{}",
                serde_json::to_string_pretty(&report).expect("error report should serialize")
            );
        }
    }
}

fn error_code(error: &(dyn Error + 'static)) -> &'static str {
    if error.downcast_ref::<nbd_config::ConfigError>().is_some() {
        "config_error"
    } else if error
        .downcast_ref::<nbd_control_plane::CatalogError>()
        .is_some()
    {
        "catalog_error"
    } else {
        "runtime_error"
    }
}

fn print_json_value(value: &serde_json::Value) -> Result<(), serde_json::Error> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}
