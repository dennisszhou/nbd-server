use nbd_config::{ConfigError, ConfigKey, InitializedConfig, LoadedConfig};
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
