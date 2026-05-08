use crate::doctor::{DoctorReport, DoctorStatus};
use nbd_config::{ConfigError, ConfigKey, InitializedConfig, LoadedConfig};
use std::error::Error;
use std::path::Path;

pub fn print_config(loaded: &LoadedConfig) -> Result<(), ConfigError> {
    print!("{}", loaded.config().to_toml_string()?);
    Ok(())
}

pub fn print_config_value(key: ConfigKey, loaded: &LoadedConfig) {
    println!("{}", key.value(loaded.config()));
}

pub fn print_config_path(path: &Path) {
    println!("{}", path.display());
}

pub fn print_config_initialized(initialized: &InitializedConfig) {
    println!("initialized config {}", initialized.path().display());
}

pub fn print_doctor_report(report: &DoctorReport, json: bool) -> Result<(), Box<dyn Error>> {
    if json {
        println!("{}", serde_json::to_string_pretty(report)?);
    } else {
        println!("status: {}", report.status());
        for check in report.checks() {
            match (check.detail(), check.remediation()) {
                (Some(detail), Some(remediation)) => println!(
                    "{}: {} ({detail}; remediation: {remediation})",
                    check.name(),
                    check.status()
                ),
                (Some(detail), None) => {
                    println!("{}: {} ({detail})", check.name(), check.status());
                }
                (None, Some(remediation)) => println!(
                    "{}: {} (remediation: {remediation})",
                    check.name(),
                    check.status()
                ),
                (None, None) => println!("{}: {}", check.name(), check.status()),
            }
        }
    }
    Ok(())
}

pub fn doctor_failed(report: &DoctorReport) -> bool {
    report.status() == DoctorStatus::Failed
}
