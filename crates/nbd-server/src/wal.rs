use crate::{ByteRange, Result, ServerError};
use crc::{Crc, CRC_32_ISCSI};
use nbd_control_plane::{ExportId, WalSeq};
use std::collections::VecDeque;
use std::ffi::OsStr;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs::{self, OpenOptions};
use tokio::io::AsyncWriteExt;

const LOCAL_WAL_SEGMENT_TARGET_BYTES: u64 = 128 * 1024 * 1024;
const WAL_SEGMENT_EXTENSION: &str = "wal";
const SEGMENT_MAGIC: &[u8; 8] = b"NBDWALSG";
const RECORD_MAGIC: &[u8; 8] = b"NBDWALRC";
const WAL_FORMAT_VERSION: u16 = 1;
const SEGMENT_HEADER_LEN: usize = 24;
const RECORD_KIND_WRITE: u16 = 1;
const RECORD_HEADER_LEN: usize = 40;
const RECORD_CRC_OFFSET: usize = 36;
const CRC32C: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);

pub type ExportWalHandle = Arc<dyn ExportWal>;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WalDomain {
    export_id: ExportId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenWal {
    domain: WalDomain,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalRequest {
    range: ByteRange,
    data: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalRecord {
    seq: WalSeq,
    range: ByteRange,
    data: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalBounds {
    pub pruned_through: WalSeq,
    pub last_durable: WalSeq,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalPruneResult {
    pub requested_through: WalSeq,
    pub pruned_through: WalSeq,
    pub removed_segments: u64,
}

#[derive(Debug, Clone)]
pub struct WalReplay {
    records: VecDeque<WalRecord>,
}

#[derive(Debug, Clone)]
pub struct LocalWalProvider {
    root: PathBuf,
}

#[derive(Debug)]
pub struct LocalExportWal {
    dir: PathBuf,
    segment_target_bytes: u64,
    state: tokio::sync::Mutex<LocalWalState>,
}

#[derive(Debug, Clone)]
struct LocalWalState {
    bounds: WalBounds,
    active: Option<ActiveSegment>,
}

#[derive(Debug, Clone)]
struct ActiveSegment {
    path: PathBuf,
    len_bytes: u64,
}

#[derive(Debug, Clone)]
struct SegmentScan {
    first_seq: WalSeq,
    path: PathBuf,
    len_bytes: u64,
    records: Vec<WalRecord>,
}

impl WalDomain {
    pub fn for_export_id(export_id: ExportId) -> Self {
        Self { export_id }
    }

    pub fn export_id(&self) -> &ExportId {
        &self.export_id
    }
}

impl OpenWal {
    pub fn new(domain: WalDomain) -> Self {
        Self { domain }
    }

    pub fn domain(&self) -> &WalDomain {
        &self.domain
    }
}

impl WalRequest {
    pub fn new(range: ByteRange, data: Vec<u8>) -> Result<Self> {
        if data.is_empty() {
            return Err(ServerError::wal(
                "create WAL request",
                "write payload must not be empty",
            ));
        }
        if data.len() as u64 != range.len() {
            return Err(ServerError::wal(
                "create WAL request",
                format!(
                    "payload length {} does not match range length {}",
                    data.len(),
                    range.len()
                ),
            ));
        }

        Ok(Self { range, data })
    }

    pub fn range(&self) -> ByteRange {
        self.range
    }

    pub fn data(&self) -> &[u8] {
        &self.data
    }

    pub fn into_parts(self) -> (ByteRange, Vec<u8>) {
        (self.range, self.data)
    }
}

impl WalRecord {
    pub fn new(seq: WalSeq, range: ByteRange, data: Vec<u8>) -> Result<Self> {
        let request = WalRequest::new(range, data)?;
        Self::from_request(seq, request)
    }

    fn from_request(seq: WalSeq, request: WalRequest) -> Result<Self> {
        if seq == WalSeq::zero() {
            return Err(ServerError::wal(
                "create WAL record",
                "record sequence must be nonzero",
            ));
        }
        let (range, data) = request.into_parts();
        Ok(Self { seq, range, data })
    }

    pub fn seq(&self) -> WalSeq {
        self.seq
    }

    pub fn range(&self) -> ByteRange {
        self.range
    }

    pub fn data(&self) -> &[u8] {
        &self.data
    }

    pub fn into_parts(self) -> (WalSeq, ByteRange, Vec<u8>) {
        (self.seq, self.range, self.data)
    }
}

impl WalBounds {
    pub fn new(pruned_through: WalSeq, last_durable: WalSeq) -> Result<Self> {
        if pruned_through > last_durable {
            return Err(ServerError::wal(
                "create WAL bounds",
                format!(
                    "pruned_through {} is greater than last_durable {}",
                    pruned_through, last_durable
                ),
            ));
        }

        Ok(Self {
            pruned_through,
            last_durable,
        })
    }

    pub fn empty() -> Self {
        Self {
            pruned_through: WalSeq::zero(),
            last_durable: WalSeq::zero(),
        }
    }
}

impl WalPruneResult {
    pub fn new(
        requested_through: WalSeq,
        pruned_through: WalSeq,
        removed_segments: u64,
    ) -> Result<Self> {
        if pruned_through > requested_through {
            return Err(ServerError::wal(
                "create WAL prune result",
                format!(
                    "pruned_through {} is greater than requested_through {}",
                    pruned_through, requested_through
                ),
            ));
        }

        Ok(Self {
            requested_through,
            pruned_through,
            removed_segments,
        })
    }
}

impl WalReplay {
    pub fn empty() -> Self {
        Self::from_records(Vec::new()).expect("empty WAL replay is ordered")
    }

    pub(crate) fn from_records(records: Vec<WalRecord>) -> Result<Self> {
        let mut previous = WalSeq::zero();
        for record in &records {
            if record.seq() <= previous {
                return Err(ServerError::wal(
                    "create WAL replay",
                    "records must be strictly ordered by sequence",
                ));
            }
            previous = record.seq();
        }

        Ok(Self {
            records: VecDeque::from(records),
        })
    }

    pub async fn next_record(&mut self) -> Result<Option<WalRecord>> {
        Ok(self.records.pop_front())
    }
}

#[async_trait::async_trait]
pub trait WalProvider: Send + Sync {
    async fn open_export(&self, request: OpenWal) -> Result<ExportWalHandle>;
}

#[async_trait::async_trait]
pub trait ExportWal: Send + Sync {
    async fn append(&self, request: WalRequest) -> Result<WalRecord>;

    async fn bounds(&self) -> Result<WalBounds>;

    async fn replay_after(&self, after: WalSeq) -> Result<WalReplay>;

    async fn replay_range(&self, after: WalSeq, through: WalSeq) -> Result<WalReplay>;

    async fn prune_through(&self, seq: WalSeq) -> Result<WalPruneResult>;
}

impl LocalWalProvider {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }
}

impl LocalExportWal {
    async fn open(dir: PathBuf, segment_target_bytes: u64) -> Result<Self> {
        fs::create_dir_all(&dir)
            .await
            .map_err(|source| ServerError::io("create WAL directory", source))?;
        if let Some(parent) = dir.parent() {
            sync_directory(parent.to_path_buf()).await?;
        }
        let state = scan_wal_dir(&dir).await?;

        Ok(Self {
            dir,
            segment_target_bytes,
            state: tokio::sync::Mutex::new(state),
        })
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    async fn ensure_active_segment(&self, state: &mut LocalWalState) -> Result<()> {
        let next_seq = next_seq_after(state.bounds.last_durable)?;
        let should_create = state
            .active
            .as_ref()
            .is_none_or(|active| active.len_bytes >= self.segment_target_bytes);
        if !should_create {
            return Ok(());
        }

        let path = segment_path(&self.dir, next_seq);
        write_new_segment(&path, next_seq).await?;
        sync_directory(self.dir.clone()).await?;
        state.active = Some(ActiveSegment {
            path,
            len_bytes: SEGMENT_HEADER_LEN as u64,
        });
        Ok(())
    }

    async fn load_records(&self) -> Result<Vec<WalRecord>> {
        let scans = scan_segments(&self.dir).await?;
        Ok(scans
            .into_iter()
            .flat_map(|segment| segment.records)
            .collect())
    }
}

#[async_trait::async_trait]
impl WalProvider for LocalWalProvider {
    async fn open_export(&self, request: OpenWal) -> Result<ExportWalHandle> {
        fs::create_dir_all(&self.root)
            .await
            .map_err(|source| ServerError::io("create WAL root directory", source))?;

        let dir = export_wal_dir(&self.root, request.domain().export_id())?;
        Ok(Arc::new(
            LocalExportWal::open(dir, LOCAL_WAL_SEGMENT_TARGET_BYTES).await?,
        ))
    }
}

#[async_trait::async_trait]
impl ExportWal for LocalExportWal {
    async fn append(&self, request: WalRequest) -> Result<WalRecord> {
        let mut state = self.state.lock().await;
        self.ensure_active_segment(&mut state).await?;
        let seq = next_seq_after(state.bounds.last_durable)?;
        let record = WalRecord::from_request(seq, request)?;
        let frame = encode_record(&record)?;
        let active = state
            .active
            .as_mut()
            .expect("active segment exists after ensure_active_segment");

        let mut file = OpenOptions::new()
            .append(true)
            .open(&active.path)
            .await
            .map_err(|source| ServerError::io("open WAL segment for append", source))?;
        file.write_all(&frame)
            .await
            .map_err(|source| ServerError::io("write WAL record", source))?;
        file.sync_all()
            .await
            .map_err(|source| ServerError::io("sync WAL segment", source))?;

        active.len_bytes += frame.len() as u64;
        state.bounds.last_durable = seq;
        Ok(record)
    }

    async fn bounds(&self) -> Result<WalBounds> {
        Ok(self.state.lock().await.bounds)
    }

    async fn replay_after(&self, after: WalSeq) -> Result<WalReplay> {
        let last_durable = self.state.lock().await.bounds.last_durable;
        self.replay_range(after, last_durable).await
    }

    async fn replay_range(&self, after: WalSeq, through: WalSeq) -> Result<WalReplay> {
        let state = self.state.lock().await;
        validate_replay_range(state.bounds, after, through)?;
        let records = self
            .load_records()
            .await?
            .into_iter()
            .filter(|record| record.seq() > after && record.seq() <= through)
            .collect();

        WalReplay::from_records(records)
    }

    async fn prune_through(&self, seq: WalSeq) -> Result<WalPruneResult> {
        let bounds = self.state.lock().await.bounds;
        if seq > bounds.last_durable {
            return Err(ServerError::wal(
                "prune WAL",
                format!(
                    "requested prune sequence {} is greater than last durable {}",
                    seq, bounds.last_durable
                ),
            ));
        }

        Err(ServerError::wal(
            "prune WAL",
            "local WAL pruning is not implemented yet",
        ))
    }
}

async fn scan_wal_dir(dir: &Path) -> Result<LocalWalState> {
    let scans = scan_segments(dir).await?;
    let mut last_durable = WalSeq::zero();
    let mut active = None;

    for scan in scans {
        if let Some(record) = scan.records.last() {
            last_durable = record.seq();
        }
        active = Some(ActiveSegment {
            path: scan.path,
            len_bytes: scan.len_bytes,
        });
    }

    Ok(LocalWalState {
        bounds: WalBounds::new(WalSeq::zero(), last_durable)?,
        active,
    })
}

async fn scan_segments(dir: &Path) -> Result<Vec<SegmentScan>> {
    let mut entries = Vec::new();
    let mut read_dir = fs::read_dir(dir)
        .await
        .map_err(|source| ServerError::io("read WAL directory", source))?;

    while let Some(entry) = read_dir
        .next_entry()
        .await
        .map_err(|source| ServerError::io("read WAL directory entry", source))?
    {
        let path = entry.path();
        if path.extension() != Some(OsStr::new(WAL_SEGMENT_EXTENSION)) {
            continue;
        }
        let first_seq = parse_segment_file_name(&path)?;
        entries.push((first_seq, path));
    }

    entries.sort_by_key(|(first_seq, _)| *first_seq);

    let mut scans = Vec::new();
    let mut expected_seq = WalSeq::new(1);
    for (first_seq, path) in entries {
        if first_seq != expected_seq {
            return Err(ServerError::wal(
                "scan WAL",
                format!(
                    "segment {} starts at {}, expected {}",
                    path.display(),
                    first_seq,
                    expected_seq
                ),
            ));
        }
        let scan = scan_segment(path, expected_seq).await?;
        expected_seq = match scan.records.last() {
            Some(record) => next_seq_after(record.seq())?,
            None => scan.first_seq,
        };
        scans.push(scan);
    }

    Ok(scans)
}

async fn scan_segment(path: PathBuf, expected_first_seq: WalSeq) -> Result<SegmentScan> {
    let data = fs::read(&path)
        .await
        .map_err(|source| ServerError::io("read WAL segment", source))?;
    let first_seq = decode_segment_header(&path, &data)?;
    if first_seq != expected_first_seq {
        return Err(ServerError::wal(
            "scan WAL segment",
            format!(
                "segment header first_seq {} does not match expected {}",
                first_seq, expected_first_seq
            ),
        ));
    }

    let mut offset = SEGMENT_HEADER_LEN;
    let mut expected_seq = first_seq;
    let mut records = Vec::new();
    while offset < data.len() {
        let (record, next_offset) = decode_record_at(&path, &data, offset)?;
        if record.seq() != expected_seq {
            return Err(ServerError::wal(
                "scan WAL segment",
                format!(
                    "record sequence {} does not match expected {}",
                    record.seq(),
                    expected_seq
                ),
            ));
        }
        expected_seq = next_seq_after(record.seq())?;
        offset = next_offset;
        records.push(record);
    }

    Ok(SegmentScan {
        first_seq,
        path,
        len_bytes: data.len() as u64,
        records,
    })
}

fn validate_replay_range(bounds: WalBounds, after: WalSeq, through: WalSeq) -> Result<()> {
    if through < after {
        return Err(ServerError::wal(
            "replay WAL",
            format!("through sequence {through} is less than after sequence {after}"),
        ));
    }
    if after < bounds.pruned_through {
        return Err(ServerError::wal(
            "replay WAL",
            format!(
                "requested checkpoint {} is older than pruned WAL prefix {}",
                after, bounds.pruned_through
            ),
        ));
    }
    if through > bounds.last_durable {
        return Err(ServerError::wal(
            "replay WAL",
            format!(
                "through sequence {} is greater than last durable {}",
                through, bounds.last_durable
            ),
        ));
    }
    Ok(())
}

async fn write_new_segment(path: &Path, first_seq: WalSeq) -> Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .await
        .map_err(|source| ServerError::io("create WAL segment", source))?;
    file.write_all(&encode_segment_header(first_seq))
        .await
        .map_err(|source| ServerError::io("write WAL segment header", source))?;
    file.sync_all()
        .await
        .map_err(|source| ServerError::io("sync WAL segment header", source))
}

fn encode_segment_header(first_seq: WalSeq) -> Vec<u8> {
    let mut header = Vec::with_capacity(SEGMENT_HEADER_LEN);
    header.extend_from_slice(SEGMENT_MAGIC);
    header.extend_from_slice(&WAL_FORMAT_VERSION.to_le_bytes());
    header.extend_from_slice(&(SEGMENT_HEADER_LEN as u16).to_le_bytes());
    header.extend_from_slice(&first_seq.get().to_le_bytes());
    let checksum = CRC32C.checksum(&header);
    header.extend_from_slice(&checksum.to_le_bytes());
    header
}

fn decode_segment_header(path: &Path, data: &[u8]) -> Result<WalSeq> {
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

fn encode_record(record: &WalRecord) -> Result<Vec<u8>> {
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

fn decode_record_at(path: &Path, data: &[u8], offset: usize) -> Result<(WalRecord, usize)> {
    if data.len() - offset < RECORD_HEADER_LEN {
        return Err(ServerError::wal(
            "read WAL record",
            format!("segment {} has partial record header", path.display()),
        ));
    }
    let header = &data[offset..offset + RECORD_HEADER_LEN];
    if &header[0..8] != RECORD_MAGIC {
        return Err(ServerError::wal(
            "read WAL record",
            format!("segment {} has invalid record magic", path.display()),
        ));
    }
    let version = u16_at(header, 8);
    if version != WAL_FORMAT_VERSION {
        return Err(ServerError::wal(
            "read WAL record",
            format!(
                "segment {} has unsupported record version {}",
                path.display(),
                version
            ),
        ));
    }
    let header_len = u16_at(header, 10) as usize;
    if header_len != RECORD_HEADER_LEN {
        return Err(ServerError::wal(
            "read WAL record",
            format!(
                "segment {} has invalid record header length {}",
                path.display(),
                header_len
            ),
        ));
    }
    let record_kind = u16_at(header, 12);
    if record_kind != RECORD_KIND_WRITE {
        return Err(ServerError::wal(
            "read WAL record",
            format!(
                "segment {} has unsupported record kind {}",
                path.display(),
                record_kind
            ),
        ));
    }

    let seq = WalSeq::new(u64_at(header, 16));
    let start = u64_at(header, 24);
    let data_len = u32_at(header, 32);
    let next_offset = offset
        .checked_add(RECORD_HEADER_LEN)
        .and_then(|value| value.checked_add(data_len as usize))
        .ok_or_else(|| ServerError::wal("read WAL record", "record length overflow"))?;
    if next_offset > data.len() {
        return Err(ServerError::wal(
            "read WAL record",
            format!("segment {} has partial record payload", path.display()),
        ));
    }

    let payload = &data[offset + RECORD_HEADER_LEN..next_offset];
    let mut digest = CRC32C.digest();
    digest.update(&header[..RECORD_CRC_OFFSET]);
    digest.update(payload);
    let actual = digest.finalize();
    let expected = u32_at(header, RECORD_CRC_OFFSET);
    if expected != actual {
        return Err(ServerError::wal(
            "read WAL record",
            format!("segment {} record checksum mismatch", path.display()),
        ));
    }

    let record = WalRecord::new(seq, ByteRange::new(start, data_len), payload.to_vec())?;
    Ok((record, next_offset))
}

fn export_wal_dir(root: &Path, export_id: &ExportId) -> Result<PathBuf> {
    let encoded = hex_encode(export_id.as_str().as_bytes());
    let path = root.join(encoded);
    if path.parent() != Some(root) || !path.starts_with(root) {
        return Err(ServerError::wal(
            "resolve WAL directory",
            format!("export id `{export_id}` escaped WAL root"),
        ));
    }
    Ok(path)
}

fn segment_path(dir: &Path, first_seq: WalSeq) -> PathBuf {
    dir.join(format!("{:016}.wal", first_seq.get()))
}

fn parse_segment_file_name(path: &Path) -> Result<WalSeq> {
    let stem = path.file_stem().and_then(OsStr::to_str).ok_or_else(|| {
        ServerError::wal(
            "parse WAL segment name",
            format!("segment path {} has no UTF-8 file stem", path.display()),
        )
    })?;
    let seq = stem.parse::<u64>().map_err(|source| {
        ServerError::wal(
            "parse WAL segment name",
            format!(
                "segment path {} has invalid sequence: {source}",
                path.display()
            ),
        )
    })?;
    if seq == 0 {
        return Err(ServerError::wal(
            "parse WAL segment name",
            format!("segment path {} starts at sequence zero", path.display()),
        ));
    }
    Ok(WalSeq::new(seq))
}

fn next_seq_after(seq: WalSeq) -> Result<WalSeq> {
    seq.get()
        .checked_add(1)
        .map(WalSeq::new)
        .ok_or_else(|| ServerError::wal("assign WAL sequence", "WAL sequence overflow"))
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
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

async fn sync_directory(path: PathBuf) -> Result<()> {
    tokio::task::spawn_blocking(move || {
        let dir = std::fs::File::open(path)?;
        match dir.sync_all() {
            Ok(()) => Ok(()),
            Err(error)
                if error.kind() == io::ErrorKind::InvalidInput
                    || error.raw_os_error() == Some(libc_einval()) =>
            {
                Ok(())
            }
            Err(error) => Err(error),
        }
    })
    .await
    .map_err(|error| ServerError::Wal {
        context: "sync WAL directory",
        message: error.to_string(),
    })?
    .map_err(|source| ServerError::io("sync WAL directory", source))
}

#[cfg(unix)]
fn libc_einval() -> i32 {
    22
}

#[cfg(not(unix))]
fn libc_einval() -> i32 {
    i32::MIN
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn replay_yields_records_in_order() {
        let first = WalRecord::new(WalSeq::new(1), ByteRange::new(0, 3), b"one".to_vec())
            .expect("first record");
        let second = WalRecord::new(WalSeq::new(2), ByteRange::new(3, 3), b"two".to_vec())
            .expect("second record");
        let mut replay =
            WalReplay::from_records(vec![first.clone(), second.clone()]).expect("ordered replay");

        assert_eq!(replay.next_record().await.expect("next"), Some(first));
        assert_eq!(replay.next_record().await.expect("next"), Some(second));
        assert_eq!(replay.next_record().await.expect("next"), None);
    }

    #[test]
    fn replay_rejects_non_increasing_records() {
        let first = WalRecord::new(WalSeq::new(2), ByteRange::new(0, 3), b"one".to_vec())
            .expect("first record");
        let second = WalRecord::new(WalSeq::new(2), ByteRange::new(3, 3), b"two".to_vec())
            .expect("second record");

        assert!(matches!(
            WalReplay::from_records(vec![first, second]),
            Err(ServerError::Wal {
                context: "create WAL replay",
                ..
            }),
        ));
    }
}
