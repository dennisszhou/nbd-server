use crate::constants::{
    IHAVEOPT_MAGIC, INIT_PASSWD, NBD_FLAG_C_FIXED_NEWSTYLE, NBD_FLAG_C_NO_ZEROES,
    NBD_FLAG_FIXED_NEWSTYLE, NBD_FLAG_NO_ZEROES,
};
use crate::wire::{write_u16, write_u64, WireReader};
use crate::{ProtocolError, Result};

pub const SERVER_HANDSHAKE_FLAGS: u16 = NBD_FLAG_FIXED_NEWSTYLE | NBD_FLAG_NO_ZEROES;
pub const SUPPORTED_CLIENT_FLAGS: u32 = NBD_FLAG_C_FIXED_NEWSTYLE | NBD_FLAG_C_NO_ZEROES;

/// Client flags accepted for the supported fixed-newstyle handshake path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClientFlags {
    raw: u32,
}

impl ClientFlags {
    pub fn raw(self) -> u32 {
        self.raw
    }

    pub fn no_zeroes(self) -> bool {
        self.raw & NBD_FLAG_C_NO_ZEROES != 0
    }
}

pub fn encode_server_handshake() -> Vec<u8> {
    let mut out = Vec::with_capacity(18);
    write_u64(&mut out, INIT_PASSWD);
    write_u64(&mut out, IHAVEOPT_MAGIC);
    write_u16(&mut out, SERVER_HANDSHAKE_FLAGS);
    out
}

pub fn encode_client_flags(no_zeroes: bool) -> Vec<u8> {
    let mut raw = NBD_FLAG_C_FIXED_NEWSTYLE;
    if no_zeroes {
        raw |= NBD_FLAG_C_NO_ZEROES;
    }
    raw.to_be_bytes().to_vec()
}

pub fn decode_client_flags(input: &[u8]) -> Result<ClientFlags> {
    let mut reader = WireReader::new(input);
    let raw = reader.read_u32()?;

    if reader.remaining() != 0 {
        return Err(ProtocolError::TrailingBytes {
            remaining: reader.remaining(),
        });
    }

    if raw & NBD_FLAG_C_FIXED_NEWSTYLE == 0 {
        return Err(ProtocolError::MissingClientFlag {
            flag: "NBD_FLAG_C_FIXED_NEWSTYLE",
        });
    }

    let unsupported = raw & !SUPPORTED_CLIENT_FLAGS;
    if unsupported != 0 {
        return Err(ProtocolError::UnsupportedClientFlags { raw, unsupported });
    }

    Ok(ClientFlags { raw })
}
