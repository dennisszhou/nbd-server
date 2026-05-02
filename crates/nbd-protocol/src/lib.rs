//! NBD wire protocol primitives.

#![forbid(unsafe_code)]

pub mod constants;
pub mod error;
pub mod handshake;
pub mod option;
pub mod wire;

pub use error::{ProtocolError, Result};
pub use handshake::{decode_client_flags, encode_server_handshake, ClientFlags};
pub use option::{
    encode_ack_reply, encode_export_info_reply, encode_option_reply,
    encode_unsupported_option_reply, parse_option_request, GoRequest, OptionRequest,
};
pub use wire::{NbdCommandFlags, NbdCommandType, NbdCookie, NbdOptionCode};
