//! SQLite adapter for the storage-neutral control-plane API.

#![forbid(unsafe_code)]

mod adapter;

pub use adapter::SQLiteExportCatalog;
