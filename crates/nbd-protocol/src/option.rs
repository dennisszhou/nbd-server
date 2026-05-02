use std::str;

use crate::constants::{
    IHAVEOPT_MAGIC, MAX_STRING_BYTES, NBD_INFO_EXPORT, NBD_OPT_ABORT, NBD_OPT_GO, NBD_REP_ACK,
    NBD_REP_ERR_UNSUP, NBD_REP_INFO, OPTION_REPLY_MAGIC,
};
use crate::wire::{write_u16, write_u32, write_u64, NbdOptionCode, WireReader};
use crate::{ProtocolError, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OptionRequest {
    Go(GoRequest),
    Abort {
        payload: Vec<u8>,
    },
    Unknown {
        code: NbdOptionCode,
        payload: Vec<u8>,
    },
}

impl OptionRequest {
    pub fn code(&self) -> NbdOptionCode {
        match self {
            Self::Go(_) => NbdOptionCode::new(NBD_OPT_GO),
            Self::Abort { .. } => NbdOptionCode::new(NBD_OPT_ABORT),
            Self::Unknown { code, .. } => *code,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GoRequest {
    export_name: String,
    info_requests: Vec<u16>,
}

impl GoRequest {
    pub fn export_name(&self) -> &str {
        &self.export_name
    }

    pub fn info_requests(&self) -> &[u16] {
        &self.info_requests
    }
}

pub fn parse_option_request(input: &[u8]) -> Result<OptionRequest> {
    let mut reader = WireReader::new(input);
    let magic = reader.read_u64()?;
    if magic != IHAVEOPT_MAGIC {
        return Err(ProtocolError::InvalidMagic {
            context: "option request",
            expected: IHAVEOPT_MAGIC,
            actual: magic,
        });
    }

    let code = NbdOptionCode::new(reader.read_u32()?);
    let len = reader.read_u32()? as usize;
    let payload = reader.read_bytes(len)?;

    if reader.remaining() != 0 {
        return Err(ProtocolError::TrailingBytes {
            remaining: reader.remaining(),
        });
    }

    match code.raw() {
        NBD_OPT_GO => Ok(OptionRequest::Go(parse_go_payload(payload)?)),
        NBD_OPT_ABORT => Ok(OptionRequest::Abort {
            payload: payload.to_vec(),
        }),
        _ => Ok(OptionRequest::Unknown {
            code,
            payload: payload.to_vec(),
        }),
    }
}

pub fn encode_option_reply(
    option: NbdOptionCode,
    reply_type: u32,
    payload: &[u8],
) -> Result<Vec<u8>> {
    let payload_len = u32::try_from(payload.len()).map_err(|_| ProtocolError::LengthTooLarge {
        field: "option reply payload",
        len: payload.len(),
        max: u32::MAX as usize,
    })?;

    let mut out = Vec::with_capacity(20 + payload.len());
    write_u64(&mut out, OPTION_REPLY_MAGIC);
    write_u32(&mut out, option.raw());
    write_u32(&mut out, reply_type);
    write_u32(&mut out, payload_len);
    out.extend_from_slice(payload);
    Ok(out)
}

pub fn encode_export_info_reply(
    option: NbdOptionCode,
    export_size_bytes: u64,
    transmission_flags: u16,
) -> Result<Vec<u8>> {
    let mut payload = Vec::with_capacity(12);
    write_u16(&mut payload, NBD_INFO_EXPORT);
    write_u64(&mut payload, export_size_bytes);
    write_u16(&mut payload, transmission_flags);
    encode_option_reply(option, NBD_REP_INFO, &payload)
}

pub fn encode_ack_reply(option: NbdOptionCode) -> Result<Vec<u8>> {
    encode_option_reply(option, NBD_REP_ACK, &[])
}

pub fn encode_unsupported_option_reply(option: NbdOptionCode, message: &[u8]) -> Result<Vec<u8>> {
    encode_option_reply(option, NBD_REP_ERR_UNSUP, message)
}

fn parse_go_payload(payload: &[u8]) -> Result<GoRequest> {
    let mut reader = WireReader::new(payload);
    let name_len = reader.read_u32()? as usize;

    if name_len > MAX_STRING_BYTES {
        return Err(ProtocolError::LengthTooLarge {
            field: "export name",
            len: name_len,
            max: MAX_STRING_BYTES,
        });
    }

    let name = reader.read_bytes(name_len)?;
    if name.contains(&0) {
        return Err(ProtocolError::InvalidString {
            field: "export name",
            reason: "contains NUL byte",
        });
    }

    let export_name = str::from_utf8(name)
        .map_err(|_| ProtocolError::InvalidUtf8 {
            field: "export name",
        })?
        .to_owned();

    let info_count = reader.read_u16()? as usize;
    let mut info_requests = Vec::with_capacity(info_count);
    for _ in 0..info_count {
        info_requests.push(reader.read_u16()?);
    }

    if reader.remaining() != 0 {
        return Err(ProtocolError::TrailingBytes {
            remaining: reader.remaining(),
        });
    }

    Ok(GoRequest {
        export_name,
        info_requests,
    })
}
