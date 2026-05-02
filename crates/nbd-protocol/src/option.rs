use std::str;

use crate::constants::{
    IHAVEOPT_MAGIC, MAX_STRING_BYTES, NBD_INFO_EXPORT, NBD_OPT_ABORT, NBD_OPT_GO, NBD_REP_ACK,
    NBD_REP_ERR_UNKNOWN, NBD_REP_ERR_UNSUP, NBD_REP_INFO, OPTION_REPLY_MAGIC,
};
use crate::wire::{write_u16, write_u32, write_u64, NbdOptionCode, WireReader};
use crate::{ProtocolError, Result};

pub const OPTION_REQUEST_HEADER_BYTES: usize = 16;
pub const OPTION_REPLY_HEADER_BYTES: usize = 20;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OptionReplyHeader {
    option: NbdOptionCode,
    reply_type: u32,
    payload_len: u32,
}

impl OptionReplyHeader {
    pub fn option(self) -> NbdOptionCode {
        self.option
    }

    pub fn reply_type(self) -> u32 {
        self.reply_type
    }

    pub fn payload_len(self) -> u32 {
        self.payload_len
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OptionReply {
    InfoExport {
        option: NbdOptionCode,
        export_size_bytes: u64,
        transmission_flags: u16,
    },
    Ack {
        option: NbdOptionCode,
    },
    Error {
        option: NbdOptionCode,
        reply_type: u32,
        message: Vec<u8>,
    },
    Other {
        option: NbdOptionCode,
        reply_type: u32,
        payload: Vec<u8>,
    },
}

impl OptionReply {
    pub fn option(&self) -> NbdOptionCode {
        match self {
            Self::InfoExport { option, .. }
            | Self::Ack { option }
            | Self::Error { option, .. }
            | Self::Other { option, .. } => *option,
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

pub fn encode_option_request(option: NbdOptionCode, payload: &[u8]) -> Result<Vec<u8>> {
    let payload_len = u32::try_from(payload.len()).map_err(|_| ProtocolError::LengthTooLarge {
        field: "option request payload",
        len: payload.len(),
        max: u32::MAX as usize,
    })?;

    let mut out = Vec::with_capacity(OPTION_REQUEST_HEADER_BYTES + payload.len());
    write_u64(&mut out, IHAVEOPT_MAGIC);
    write_u32(&mut out, option.raw());
    write_u32(&mut out, payload_len);
    out.extend_from_slice(payload);
    Ok(out)
}

pub fn encode_go_request(export_name: &str, info_requests: &[u16]) -> Result<Vec<u8>> {
    if export_name.len() > MAX_STRING_BYTES {
        return Err(ProtocolError::LengthTooLarge {
            field: "export name",
            len: export_name.len(),
            max: MAX_STRING_BYTES,
        });
    }
    if export_name.as_bytes().contains(&0) {
        return Err(ProtocolError::InvalidString {
            field: "export name",
            reason: "contains NUL byte",
        });
    }

    let info_count =
        u16::try_from(info_requests.len()).map_err(|_| ProtocolError::LengthTooLarge {
            field: "info request count",
            len: info_requests.len(),
            max: u16::MAX as usize,
        })?;

    let mut payload = Vec::with_capacity(4 + export_name.len() + 2 + info_requests.len() * 2);
    write_u32(&mut payload, export_name.len() as u32);
    payload.extend_from_slice(export_name.as_bytes());
    write_u16(&mut payload, info_count);
    for info in info_requests {
        write_u16(&mut payload, *info);
    }

    encode_option_request(NbdOptionCode::new(NBD_OPT_GO), &payload)
}

pub fn encode_abort_request(payload: &[u8]) -> Result<Vec<u8>> {
    encode_option_request(NbdOptionCode::new(NBD_OPT_ABORT), payload)
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

pub fn parse_option_reply_header(input: &[u8]) -> Result<OptionReplyHeader> {
    let mut reader = WireReader::new(input);
    let magic = reader.read_u64()?;
    if magic != OPTION_REPLY_MAGIC {
        return Err(ProtocolError::InvalidMagic {
            context: "option reply",
            expected: OPTION_REPLY_MAGIC,
            actual: magic,
        });
    }

    let header = OptionReplyHeader {
        option: NbdOptionCode::new(reader.read_u32()?),
        reply_type: reader.read_u32()?,
        payload_len: reader.read_u32()?,
    };

    if reader.remaining() != 0 {
        return Err(ProtocolError::TrailingBytes {
            remaining: reader.remaining(),
        });
    }

    Ok(header)
}

pub fn parse_option_reply(input: &[u8]) -> Result<OptionReply> {
    let mut reader = WireReader::new(input);
    let header = parse_option_reply_header(reader.read_bytes(OPTION_REPLY_HEADER_BYTES)?)?;
    let payload = reader.read_bytes(header.payload_len() as usize)?;

    if reader.remaining() != 0 {
        return Err(ProtocolError::TrailingBytes {
            remaining: reader.remaining(),
        });
    }

    match header.reply_type() {
        NBD_REP_INFO => parse_info_reply(header.option(), payload),
        NBD_REP_ACK => {
            if !payload.is_empty() {
                return Err(ProtocolError::InvalidReply {
                    reply: "NBD_REP_ACK",
                    reason: "payload must be empty",
                });
            }
            Ok(OptionReply::Ack {
                option: header.option(),
            })
        }
        reply_type if is_error_reply(reply_type) => Ok(OptionReply::Error {
            option: header.option(),
            reply_type,
            message: payload.to_vec(),
        }),
        reply_type => Ok(OptionReply::Other {
            option: header.option(),
            reply_type,
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

pub fn encode_unknown_export_reply(option: NbdOptionCode, message: &[u8]) -> Result<Vec<u8>> {
    encode_option_reply(option, NBD_REP_ERR_UNKNOWN, message)
}

fn parse_info_reply(option: NbdOptionCode, payload: &[u8]) -> Result<OptionReply> {
    let mut reader = WireReader::new(payload);
    let info_type = reader.read_u16()?;
    match info_type {
        NBD_INFO_EXPORT => {
            let export_size_bytes = reader.read_u64()?;
            let transmission_flags = reader.read_u16()?;
            if reader.remaining() != 0 {
                return Err(ProtocolError::TrailingBytes {
                    remaining: reader.remaining(),
                });
            }
            Ok(OptionReply::InfoExport {
                option,
                export_size_bytes,
                transmission_flags,
            })
        }
        _ => Ok(OptionReply::Other {
            option,
            reply_type: NBD_REP_INFO,
            payload: payload.to_vec(),
        }),
    }
}

fn is_error_reply(reply_type: u32) -> bool {
    reply_type & (1 << 31) != 0
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
