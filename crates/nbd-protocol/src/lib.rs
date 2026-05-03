//! NBD wire protocol primitives.

#![forbid(unsafe_code)]

pub mod constants;
pub mod error;
pub mod handshake;
pub mod option;
pub mod transmission;
pub mod wire;

pub use error::{ProtocolError, Result};
pub use handshake::{
    decode_client_flags, encode_client_flags, encode_server_handshake, ClientFlags,
};
pub use option::{
    encode_abort_request, encode_ack_reply, encode_export_info_reply, encode_go_request,
    encode_option_reply, encode_option_request, encode_policy_option_reply,
    encode_unknown_export_reply, encode_unsupported_option_reply, parse_option_reply,
    parse_option_reply_header, parse_option_request, parse_option_request_header, GoRequest,
    OptionReply, OptionReplyHeader, OptionRequest, OptionRequestHeader, MAX_OPTION_PAYLOAD_BYTES,
    OPTION_REPLY_HEADER_BYTES, OPTION_REQUEST_HEADER_BYTES,
};
pub use transmission::{
    encode_disconnect_request, encode_flush_request, encode_read_reply, encode_read_request,
    encode_request_header, encode_simple_reply, encode_success_reply, encode_write_request,
    parse_read_reply, parse_request, parse_request_header, parse_simple_reply, ReadReply,
    RequestHeader, SimpleReply, TransmissionRequest, MAX_IO_BYTES, REQUEST_HEADER_BYTES,
    SIMPLE_REPLY_BYTES,
};
pub use wire::{NbdCommandFlags, NbdCommandType, NbdCookie, NbdOptionCode};
