//! Small userspace NBD validation client.

#![forbid(unsafe_code)]

pub mod client;
pub mod error;

pub use client::NbdClient;
pub use error::{ClientError, Result};
