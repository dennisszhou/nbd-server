use crate::constants::{
    NBD_CMD_DISC, NBD_CMD_FLUSH, NBD_CMD_READ, NBD_CMD_WRITE, NBD_REQUEST_MAGIC,
    NBD_SIMPLE_REPLY_MAGIC,
};
use crate::wire::{
    write_u16, write_u32, write_u64, NbdCommandFlags, NbdCommandType, NbdCookie, WireReader,
};
use crate::{ProtocolError, Result};

pub const REQUEST_HEADER_BYTES: usize = 28;
pub const SIMPLE_REPLY_BYTES: usize = 16;
pub const MAX_IO_BYTES: u32 = 64 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestHeader {
    pub flags: NbdCommandFlags,
    pub command: NbdCommandType,
    pub cookie: NbdCookie,
    pub offset: u64,
    pub length: u32,
}

impl RequestHeader {
    pub fn payload_len(self, max_io_bytes: u32) -> Result<usize> {
        validate_common_header(self)?;

        match self.command.raw() {
            NBD_CMD_READ => {
                validate_nonzero_len("NBD_CMD_READ", self.length)?;
                validate_effect_range(self.offset, self.length)?;
                validate_io_len("read length", self.length, max_io_bytes)?;
                Ok(0)
            }
            NBD_CMD_WRITE => {
                validate_nonzero_len("NBD_CMD_WRITE", self.length)?;
                validate_effect_range(self.offset, self.length)?;
                validate_io_len("write payload", self.length, max_io_bytes)?;
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SimpleReply {
    pub cookie: NbdCookie,
    pub error: u32,
}

impl SimpleReply {
    pub fn is_success(self) -> bool {
        self.error == 0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadReply {
    pub cookie: NbdCookie,
    pub error: u32,
    pub data: Vec<u8>,
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

pub fn encode_read_request(cookie: NbdCookie, offset: u64, length: u32) -> Result<Vec<u8>> {
    let header = RequestHeader {
        flags: NbdCommandFlags::new(0),
        command: NbdCommandType::new(NBD_CMD_READ),
        cookie,
        offset,
        length,
    };
    header.payload_len(MAX_IO_BYTES)?;
    Ok(encode_request_header(header))
}

pub fn encode_write_request(cookie: NbdCookie, offset: u64, data: &[u8]) -> Result<Vec<u8>> {
    let length = u32::try_from(data.len()).map_err(|_| ProtocolError::LengthTooLarge {
        field: "write payload",
        len: data.len(),
        max: u32::MAX as usize,
    })?;
    let header = RequestHeader {
        flags: NbdCommandFlags::new(0),
        command: NbdCommandType::new(NBD_CMD_WRITE),
        cookie,
        offset,
        length,
    };
    header.payload_len(MAX_IO_BYTES)?;

    let mut out = encode_request_header(header);
    out.extend_from_slice(data);
    Ok(out)
}

pub fn encode_flush_request(cookie: NbdCookie) -> Result<Vec<u8>> {
    encode_zero_range_request(cookie, NBD_CMD_FLUSH)
}

pub fn encode_disconnect_request(cookie: NbdCookie) -> Result<Vec<u8>> {
    encode_zero_range_request(cookie, NBD_CMD_DISC)
}

pub fn encode_request_header(header: RequestHeader) -> Vec<u8> {
    let mut out = Vec::with_capacity(REQUEST_HEADER_BYTES);
    write_u32(&mut out, NBD_REQUEST_MAGIC);
    write_u16(&mut out, header.flags.raw());
    write_u16(&mut out, header.command.raw());
    write_u64(&mut out, header.cookie.raw());
    write_u64(&mut out, header.offset);
    write_u32(&mut out, header.length);
    out
}

pub fn parse_request(input: &[u8], max_io_bytes: u32) -> Result<TransmissionRequest> {
    let mut reader = WireReader::new(input);
    let header = parse_header(&mut reader)?;
    let payload_len = header.payload_len(max_io_bytes)?;

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

pub fn parse_simple_reply(input: &[u8]) -> Result<SimpleReply> {
    let mut reader = WireReader::new(input);
    let magic = reader.read_u32()?;
    if magic != NBD_SIMPLE_REPLY_MAGIC {
        return Err(ProtocolError::InvalidMagic {
            context: "simple reply",
            expected: NBD_SIMPLE_REPLY_MAGIC as u64,
            actual: magic as u64,
        });
    }

    let reply = SimpleReply {
        error: reader.read_u32()?,
        cookie: NbdCookie::new(reader.read_u64()?),
    };

    if reader.remaining() != 0 {
        return Err(ProtocolError::TrailingBytes {
            remaining: reader.remaining(),
        });
    }

    Ok(reply)
}

pub fn parse_read_reply(input: &[u8], expected_len: u32) -> Result<ReadReply> {
    let mut reader = WireReader::new(input);
    let reply = parse_simple_reply(reader.read_bytes(SIMPLE_REPLY_BYTES)?)?;
    let data_len = if reply.is_success() {
        expected_len as usize
    } else {
        0
    };
    let data = reader.read_bytes(data_len)?;

    if reader.remaining() != 0 {
        return Err(ProtocolError::TrailingBytes {
            remaining: reader.remaining(),
        });
    }

    Ok(ReadReply {
        cookie: reply.cookie,
        error: reply.error,
        data: data.to_vec(),
    })
}

fn encode_zero_range_request(cookie: NbdCookie, command: u16) -> Result<Vec<u8>> {
    let header = RequestHeader {
        flags: NbdCommandFlags::new(0),
        command: NbdCommandType::new(command),
        cookie,
        offset: 0,
        length: 0,
    };
    header.payload_len(MAX_IO_BYTES)?;
    Ok(encode_request_header(header))
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

fn validate_io_len(field: &'static str, length: u32, max_io_bytes: u32) -> Result<()> {
    if length > max_io_bytes {
        return Err(ProtocolError::LengthTooLarge {
            field,
            len: length as usize,
            max: max_io_bytes as usize,
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
