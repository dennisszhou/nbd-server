//! NBD wire protocol primitives.

#![forbid(unsafe_code)]

pub mod constants;
pub mod error;
pub mod wire;

pub use error::{ProtocolError, Result};
pub use wire::{NbdCommandFlags, NbdCommandType, NbdCookie, NbdOptionCode};
