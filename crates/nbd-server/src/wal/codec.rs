use super::WalRecord;
use crate::error::{Result, ServerError};
use crate::range::ByteRange;
use crc::{CRC_32_ISCSI, Crc};
use nbd_control_plane::WalSeq;
use std::path::Path;

pub(super) const WAL_SEGMENT_EXTENSION: &str = "wal";
const SEGMENT_MAGIC: &[u8; 8] = b"NBDWALSG";
const RECORD_MAGIC: &[u8; 8] = b"NBDWALRC";
const WAL_FORMAT_VERSION: u16 = 1;
pub(super) const SEGMENT_HEADER_LEN: usize = 24;
const RECORD_KIND_WRITE: u16 = 1;
const RECORD_HEADER_LEN: usize = 40;
const RECORD_CRC_OFFSET: usize = 36;
const CRC32C: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);

#[derive(Debug)]
pub(super) struct RecordDecodeError {
    kind: RecordDecodeErrorKind,
    error: ServerError,
}

#[derive(Debug)]
enum RecordDecodeErrorKind {
    PartialHeader,
    PartialPayload,
    ChecksumMismatch { next_offset: usize },
    Corrupt,
}

pub(super) fn encode_segment_header(first_seq: WalSeq) -> Vec<u8> {
    let mut header = Vec::with_capacity(SEGMENT_HEADER_LEN);
    header.extend_from_slice(SEGMENT_MAGIC);
    header.extend_from_slice(&WAL_FORMAT_VERSION.to_le_bytes());
    header.extend_from_slice(&(SEGMENT_HEADER_LEN as u16).to_le_bytes());
    header.extend_from_slice(&first_seq.get().to_le_bytes());
    let checksum = CRC32C.checksum(&header);
    header.extend_from_slice(&checksum.to_le_bytes());
    header
}

pub(super) fn decode_segment_header(path: &Path, data: &[u8]) -> Result<WalSeq> {
    if data.len() < SEGMENT_HEADER_LEN {
        return Err(ServerError::wal(
            "read WAL segment header",
            format!("segment {} is shorter than header", path.display()),
        ));
    }
    if &data[0..8] != SEGMENT_MAGIC {
        return Err(ServerError::wal(
            "read WAL segment header",
            format!("segment {} has invalid magic", path.display()),
        ));
    }
    let version = u16_at(data, 8);
    if version != WAL_FORMAT_VERSION {
        return Err(ServerError::wal(
            "read WAL segment header",
            format!(
                "segment {} has unsupported version {}",
                path.display(),
                version
            ),
        ));
    }
    let header_len = u16_at(data, 10) as usize;
    if header_len != SEGMENT_HEADER_LEN {
        return Err(ServerError::wal(
            "read WAL segment header",
            format!(
                "segment {} has invalid header length {}",
                path.display(),
                header_len
            ),
        ));
    }
    let expected = u32_at(data, 20);
    let actual = CRC32C.checksum(&data[..20]);
    if expected != actual {
        return Err(ServerError::wal(
            "read WAL segment header",
            format!("segment {} header checksum mismatch", path.display()),
        ));
    }

    Ok(WalSeq::new(u64_at(data, 12)))
}

pub(super) fn encode_record(record: &WalRecord) -> Result<Vec<u8>> {
    let mut header = Vec::with_capacity(RECORD_HEADER_LEN);
    header.extend_from_slice(RECORD_MAGIC);
    header.extend_from_slice(&WAL_FORMAT_VERSION.to_le_bytes());
    header.extend_from_slice(&(RECORD_HEADER_LEN as u16).to_le_bytes());
    header.extend_from_slice(&RECORD_KIND_WRITE.to_le_bytes());
    header.extend_from_slice(&0u16.to_le_bytes());
    header.extend_from_slice(&record.seq().get().to_le_bytes());
    header.extend_from_slice(&record.range().start().to_le_bytes());
    header.extend_from_slice(&(record.data().len() as u32).to_le_bytes());

    let mut digest = CRC32C.digest();
    digest.update(&header);
    digest.update(record.data());
    let checksum = digest.finalize();
    header.extend_from_slice(&checksum.to_le_bytes());
    header.extend_from_slice(record.data());
    Ok(header)
}

pub(super) fn decode_record_at(
    path: &Path,
    data: &[u8],
    offset: usize,
) -> std::result::Result<(WalRecord, usize), RecordDecodeError> {
    if data.len() - offset < RECORD_HEADER_LEN {
        return Err(RecordDecodeError::partial_header(path));
    }
    let header = &data[offset..offset + RECORD_HEADER_LEN];
    if &header[0..8] != RECORD_MAGIC {
        return Err(RecordDecodeError::corrupt(ServerError::wal(
            "read WAL record",
            format!("segment {} has invalid record magic", path.display()),
        )));
    }
    let version = u16_at(header, 8);
    if version != WAL_FORMAT_VERSION {
        return Err(RecordDecodeError::corrupt(ServerError::wal(
            "read WAL record",
            format!(
                "segment {} has unsupported record version {}",
                path.display(),
                version
            ),
        )));
    }
    let header_len = u16_at(header, 10) as usize;
    if header_len != RECORD_HEADER_LEN {
        return Err(RecordDecodeError::corrupt(ServerError::wal(
            "read WAL record",
            format!(
                "segment {} has invalid record header length {}",
                path.display(),
                header_len
            ),
        )));
    }
    let record_kind = u16_at(header, 12);
    if record_kind != RECORD_KIND_WRITE {
        return Err(RecordDecodeError::corrupt(ServerError::wal(
            "read WAL record",
            format!(
                "segment {} has unsupported record kind {}",
                path.display(),
                record_kind
            ),
        )));
    }

    let seq = WalSeq::new(u64_at(header, 16));
    let start = u64_at(header, 24);
    let data_len = u32_at(header, 32);
    let next_offset = offset
        .checked_add(RECORD_HEADER_LEN)
        .and_then(|value| value.checked_add(data_len as usize))
        .ok_or_else(|| {
            RecordDecodeError::corrupt(ServerError::wal(
                "read WAL record",
                "record length overflow",
            ))
        })?;
    if next_offset > data.len() {
        return Err(RecordDecodeError::partial_payload(path));
    }

    let payload = &data[offset + RECORD_HEADER_LEN..next_offset];
    let mut digest = CRC32C.digest();
    digest.update(&header[..RECORD_CRC_OFFSET]);
    digest.update(payload);
    let actual = digest.finalize();
    let expected = u32_at(header, RECORD_CRC_OFFSET);
    if expected != actual {
        return Err(RecordDecodeError::checksum_mismatch(path, next_offset));
    }

    let record = WalRecord::new(seq, ByteRange::new(start, data_len), payload.to_vec())
        .map_err(RecordDecodeError::corrupt)?;
    Ok((record, next_offset))
}

impl RecordDecodeError {
    fn partial_header(path: &Path) -> Self {
        Self {
            kind: RecordDecodeErrorKind::PartialHeader,
            error: ServerError::wal(
                "read WAL record",
                format!("segment {} has partial record header", path.display()),
            ),
        }
    }

    fn partial_payload(path: &Path) -> Self {
        Self {
            kind: RecordDecodeErrorKind::PartialPayload,
            error: ServerError::wal(
                "read WAL record",
                format!("segment {} has partial record payload", path.display()),
            ),
        }
    }

    fn checksum_mismatch(path: &Path, next_offset: usize) -> Self {
        Self {
            kind: RecordDecodeErrorKind::ChecksumMismatch { next_offset },
            error: ServerError::wal(
                "read WAL record",
                format!("segment {} record checksum mismatch", path.display()),
            ),
        }
    }

    fn corrupt(error: ServerError) -> Self {
        Self {
            kind: RecordDecodeErrorKind::Corrupt,
            error,
        }
    }

    pub(super) fn repair_offset(&self, data_len: usize, offset: usize) -> Option<usize> {
        match self.kind {
            RecordDecodeErrorKind::PartialHeader | RecordDecodeErrorKind::PartialPayload => {
                Some(offset)
            }
            RecordDecodeErrorKind::ChecksumMismatch { next_offset } if next_offset == data_len => {
                Some(offset)
            }
            RecordDecodeErrorKind::ChecksumMismatch { .. } | RecordDecodeErrorKind::Corrupt => None,
        }
    }

    pub(super) fn into_error(self) -> ServerError {
        self.error
    }
}

fn u16_at(data: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes([data[offset], data[offset + 1]])
}

fn u32_at(data: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes([
        data[offset],
        data[offset + 1],
        data[offset + 2],
        data[offset + 3],
    ])
}

fn u64_at(data: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes([
        data[offset],
        data[offset + 1],
        data[offset + 2],
        data[offset + 3],
        data[offset + 4],
        data[offset + 5],
        data[offset + 6],
        data[offset + 7],
    ])
}
