use crate::{ProtocolError, Result};

/// Opaque client cookie that must be echoed by the server reply.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NbdCookie(u64);

impl NbdCookie {
    pub fn new(raw: u64) -> Self {
        Self(raw)
    }

    pub fn raw(self) -> u64 {
        self.0
    }
}

/// Raw command flags from a transmission request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NbdCommandFlags(u16);

impl NbdCommandFlags {
    pub fn new(raw: u16) -> Self {
        Self(raw)
    }

    pub fn raw(self) -> u16 {
        self.0
    }
}

/// Raw command type from a transmission request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NbdCommandType(u16);

impl NbdCommandType {
    pub fn new(raw: u16) -> Self {
        Self(raw)
    }

    pub fn raw(self) -> u16 {
        self.0
    }
}

/// Raw option code from fixed-newstyle option negotiation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NbdOptionCode(u32);

impl NbdOptionCode {
    pub fn new(raw: u32) -> Self {
        Self(raw)
    }

    pub fn raw(self) -> u32 {
        self.0
    }
}

/// Cursor over big-endian NBD wire bytes.
#[derive(Debug, Clone)]
pub struct WireReader<'a> {
    input: &'a [u8],
    offset: usize,
}

impl<'a> WireReader<'a> {
    pub fn new(input: &'a [u8]) -> Self {
        Self { input, offset: 0 }
    }

    pub fn position(&self) -> usize {
        self.offset
    }

    pub fn remaining(&self) -> usize {
        self.input.len().saturating_sub(self.offset)
    }

    pub fn read_u16(&mut self) -> Result<u16> {
        let bytes = self.read_array::<2>()?;
        Ok(u16::from_be_bytes(bytes))
    }

    pub fn read_u32(&mut self) -> Result<u32> {
        let bytes = self.read_array::<4>()?;
        Ok(u32::from_be_bytes(bytes))
    }

    pub fn read_u64(&mut self) -> Result<u64> {
        let bytes = self.read_array::<8>()?;
        Ok(u64::from_be_bytes(bytes))
    }

    pub fn read_bytes(&mut self, len: usize) -> Result<&'a [u8]> {
        if self.remaining() < len {
            return Err(ProtocolError::UnexpectedEof {
                needed: len,
                remaining: self.remaining(),
            });
        }

        let start = self.offset;
        self.offset += len;
        Ok(&self.input[start..self.offset])
    }

    fn read_array<const N: usize>(&mut self) -> Result<[u8; N]> {
        let bytes = self.read_bytes(N)?;
        let mut out = [0; N];
        out.copy_from_slice(bytes);
        Ok(out)
    }
}

pub fn write_u16(out: &mut Vec<u8>, value: u16) {
    out.extend_from_slice(&value.to_be_bytes());
}

pub fn write_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_be_bytes());
}

pub fn write_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_be_bytes());
}
