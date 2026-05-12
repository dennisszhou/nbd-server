//! SQLite adapter for the storage-neutral control-plane API.

#![forbid(unsafe_code)]

mod adapter;
mod transaction;
mod tree_rows;

pub use adapter::SQLiteExportCatalog;
