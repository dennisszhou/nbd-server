use crate::constants::{
    NBD_CMD_DISC, NBD_CMD_FLUSH, NBD_CMD_READ, NBD_CMD_WRITE, NBD_REQUEST_MAGIC,
    NBD_SIMPLE_REPLY_MAGIC,
};
use crate::wire::{write_u32, write_u64, NbdCommandFlags, NbdCommandType, NbdCookie, WireReader};
use crate::{ProtocolError, Result};

pub const REQUEST_HEADER_BYTES: usize = 28;
pub const MAX_WRITE_PAYLOAD_BYTES: u32 = 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestHeader {
    pub flags: NbdCommandFlags,
    pub command: NbdCommandType,
    pub cookie: NbdCookie,
    pub offset: u64,
    pub length: u32,
}

impl RequestHeader {
    pub fn payload_len(self, max_write_payload_bytes: u32) -> Result<usize> {
        validate_common_header(self)?;

        match self.command.raw() {
            NBD_CMD_READ => {
                validate_nonzero_len("NBD_CMD_READ", self.length)?;
                validate_effect_range(self.offset, self.length)?;
                Ok(0)
            }
            NBD_CMD_WRITE => {
                validate_nonzero_len("NBD_CMD_WRITE", self.length)?;
                validate_effect_range(self.offset, self.length)?;
                validate_write_payload_len(self.length, max_write_payload_bytes)?;
                Ok(self.length as usize)
            }
            NBD_CMD_FLUSH => {
                validate_no_range("NBD_CMD_FLUSH", self.offset, self.length)?;
                Ok(0)
            }
            NBD_CMD_DISC => {
                validate_no_range("NBD_CMD_DISC", self.offset, self.length)?;
                Ok(0)
            }
            command => Err(ProtocolError::UnsupportedCommand { command }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransmissionRequest {
    Read {
        cookie: NbdCookie,
        offset: u64,
        length: u32,
    },
    Write {
        cookie: NbdCookie,
        offset: u64,
        data: Vec<u8>,
    },
    Flush {
        cookie: NbdCookie,
    },
    Disconnect {
        cookie: NbdCookie,
    },
}

impl TransmissionRequest {
    pub fn cookie(&self) -> NbdCookie {
        match self {
            Self::Read { cookie, .. }
            | Self::Write { cookie, .. }
            | Self::Flush { cookie }
            | Self::Disconnect { cookie } => *cookie,
        }
    }
}

pub fn parse_request(input: &[u8], max_write_payload_bytes: u32) -> Result<TransmissionRequest> {
    let mut reader = WireReader::new(input);
    let header = parse_header(&mut reader)?;
    let payload_len = header.payload_len(max_write_payload_bytes)?;

    match header.command.raw() {
        NBD_CMD_READ => {
            require_no_payload(&reader)?;
            Ok(TransmissionRequest::Read {
                cookie: header.cookie,
                offset: header.offset,
                length: header.length,
            })
        }
        NBD_CMD_WRITE => {
            let data = reader.read_bytes(payload_len)?;
            if reader.remaining() != 0 {
                return Err(ProtocolError::TrailingBytes {
                    remaining: reader.remaining(),
                });
            }

            Ok(TransmissionRequest::Write {
                cookie: header.cookie,
                offset: header.offset,
                data: data.to_vec(),
            })
        }
        NBD_CMD_FLUSH => {
            require_no_payload(&reader)?;
            Ok(TransmissionRequest::Flush {
                cookie: header.cookie,
            })
        }
        NBD_CMD_DISC => {
            require_no_payload(&reader)?;
            Ok(TransmissionRequest::Disconnect {
                cookie: header.cookie,
            })
        }
        command => Err(ProtocolError::UnsupportedCommand { command }),
    }
}

pub fn parse_request_header(input: &[u8]) -> Result<RequestHeader> {
    let mut reader = WireReader::new(input);
    let header = parse_header(&mut reader)?;
    require_no_payload(&reader)?;
    Ok(header)
}

pub fn encode_simple_reply(cookie: NbdCookie, error: u32) -> Vec<u8> {
    let mut out = Vec::with_capacity(16);
    write_u32(&mut out, NBD_SIMPLE_REPLY_MAGIC);
    write_u32(&mut out, error);
    write_u64(&mut out, cookie.raw());
    out
}

pub fn encode_success_reply(cookie: NbdCookie) -> Vec<u8> {
    encode_simple_reply(cookie, 0)
}

pub fn encode_read_reply(cookie: NbdCookie, data: &[u8]) -> Vec<u8> {
    let mut out = encode_success_reply(cookie);
    out.extend_from_slice(data);
    out
}

fn parse_header(reader: &mut WireReader<'_>) -> Result<RequestHeader> {
    let magic = reader.read_u32()?;
    if magic != NBD_REQUEST_MAGIC {
        return Err(ProtocolError::InvalidMagic {
            context: "transmission request",
            expected: NBD_REQUEST_MAGIC as u64,
            actual: magic as u64,
        });
    }

    Ok(RequestHeader {
        flags: NbdCommandFlags::new(reader.read_u16()?),
        command: NbdCommandType::new(reader.read_u16()?),
        cookie: NbdCookie::new(reader.read_u64()?),
        offset: reader.read_u64()?,
        length: reader.read_u32()?,
    })
}

fn validate_common_header(header: RequestHeader) -> Result<()> {
    if header.flags.raw() != 0 {
        return Err(ProtocolError::UnsupportedCommandFlags {
            raw: header.flags.raw(),
        });
    }

    Ok(())
}

fn validate_nonzero_len(command: &'static str, length: u32) -> Result<()> {
    if length == 0 {
        return Err(ProtocolError::InvalidRequest {
            command,
            reason: "zero length is unsupported",
        });
    }

    Ok(())
}

fn validate_effect_range(offset: u64, length: u32) -> Result<()> {
    offset
        .checked_add(u64::from(length))
        .map(|_| ())
        .ok_or(ProtocolError::LengthOverflow { offset, length })
}

fn validate_write_payload_len(length: u32, max_write_payload_bytes: u32) -> Result<()> {
    if length > max_write_payload_bytes {
        return Err(ProtocolError::LengthTooLarge {
            field: "write payload",
            len: length as usize,
            max: max_write_payload_bytes as usize,
        });
    }

    Ok(())
}

fn validate_no_range(command: &'static str, offset: u64, length: u32) -> Result<()> {
    if offset != 0 || length != 0 {
        return Err(ProtocolError::InvalidRequest {
            command,
            reason: "offset and length must be zero",
        });
    }

    Ok(())
}

fn require_no_payload(reader: &WireReader<'_>) -> Result<()> {
    if reader.remaining() != 0 {
        return Err(ProtocolError::TrailingBytes {
            remaining: reader.remaining(),
        });
    }

    Ok(())
}
