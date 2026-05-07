mod codec;
mod replay;

pub use replay::WalReplay;

use crate::{ByteRange, Result, ServerError};
use codec::{
    SEGMENT_HEADER_LEN, WAL_SEGMENT_EXTENSION, decode_record_at, decode_segment_header,
    encode_record, encode_segment_header,
};
use nbd_control_plane::{ExportId, WalSeq};
use std::ffi::OsStr;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs::{self, OpenOptions};
use tokio::io::AsyncWriteExt;

const LOCAL_WAL_SEGMENT_TARGET_BYTES: u64 = 128 * 1024 * 1024;

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
pub struct LocalWalProvider {
    root: PathBuf,
    segment_target_bytes: u64,
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
    pub fn new(requested_through: WalSeq, pruned_through: WalSeq, removed_segments: u64) -> Self {
        Self {
            requested_through,
            pruned_through,
            removed_segments,
        }
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
        Self {
            root: root.into(),
            segment_target_bytes: LOCAL_WAL_SEGMENT_TARGET_BYTES,
        }
    }

    pub fn with_segment_target_bytes(
        root: impl Into<PathBuf>,
        segment_target_bytes: u64,
    ) -> Result<Self> {
        if segment_target_bytes <= SEGMENT_HEADER_LEN as u64 {
            return Err(ServerError::wal(
                "create local WAL provider",
                format!(
                    "segment target {} must be greater than header length {}",
                    segment_target_bytes, SEGMENT_HEADER_LEN
                ),
            ));
        }
        Ok(Self {
            root: root.into(),
            segment_target_bytes,
        })
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
            LocalExportWal::open(dir, self.segment_target_bytes).await?,
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
        let mut state = self.state.lock().await;
        if seq > state.bounds.last_durable {
            return Err(ServerError::wal(
                "prune WAL",
                format!(
                    "requested prune sequence {} is greater than last durable {}",
                    seq, state.bounds.last_durable
                ),
            ));
        }

        let scans = scan_segments(&self.dir).await?;
        let active_path = state.active.as_ref().map(|active| active.path.clone());
        let mut removed_segments = 0;
        let mut directory_changed = false;

        for scan in &scans {
            if Some(&scan.path) == active_path.as_ref() {
                continue;
            }
            if scan.max_seq()? <= seq {
                if let Err(source) = fs::remove_file(&scan.path).await {
                    if directory_changed {
                        sync_and_reload_wal_state(&mut state, &self.dir).await?;
                    }
                    return Err(ServerError::io("remove WAL segment", source));
                }
                removed_segments += 1;
                directory_changed = true;
            }
        }

        if let Some(active_scan) = scans
            .iter()
            .find(|scan| Some(&scan.path) == active_path.as_ref())
        {
            if !active_scan.records.is_empty() && active_scan.max_seq()? <= seq {
                let next_seq = next_seq_after(state.bounds.last_durable)?;
                let new_path = segment_path(&self.dir, next_seq);
                write_new_segment(&new_path, next_seq).await?;
                sync_directory(self.dir.clone()).await?;
                state.active = Some(ActiveSegment {
                    path: new_path,
                    len_bytes: SEGMENT_HEADER_LEN as u64,
                });
                directory_changed = true;

                if let Err(source) = fs::remove_file(&active_scan.path).await {
                    sync_and_reload_wal_state(&mut state, &self.dir).await?;
                    return Err(ServerError::io("remove active WAL segment", source));
                }
                removed_segments += 1;
            }
        }

        if directory_changed {
            sync_and_reload_wal_state(&mut state, &self.dir).await?;
        }

        Ok(WalPruneResult::new(
            seq,
            state.bounds.pruned_through,
            removed_segments,
        ))
    }
}

async fn sync_and_reload_wal_state(state: &mut LocalWalState, dir: &Path) -> Result<()> {
    sync_directory(dir.to_path_buf()).await?;
    *state = scan_wal_dir(dir).await?;
    Ok(())
}

async fn scan_wal_dir(dir: &Path) -> Result<LocalWalState> {
    let scans = scan_segments(dir).await?;
    let pruned_through = scans
        .first()
        .map(|scan| previous_seq_before(scan.first_seq))
        .transpose()?
        .unwrap_or_else(WalSeq::zero);
    let mut last_durable = pruned_through;
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
        bounds: WalBounds::new(pruned_through, last_durable)?,
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
    let mut expected_seq = entries
        .first()
        .map(|(first_seq, _)| *first_seq)
        .unwrap_or_else(|| WalSeq::new(1));
    let entry_count = entries.len();
    for (index, (first_seq, path)) in entries.into_iter().enumerate() {
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
        let scan = scan_segment(path, expected_seq, index + 1 == entry_count).await?;
        expected_seq = match scan.records.last() {
            Some(record) => next_seq_after(record.seq())?,
            None => scan.first_seq,
        };
        scans.push(scan);
    }

    Ok(scans)
}

impl SegmentScan {
    fn max_seq(&self) -> Result<WalSeq> {
        self.records
            .last()
            .map(WalRecord::seq)
            .map(Ok)
            .unwrap_or_else(|| previous_seq_before(self.first_seq))
    }
}

async fn scan_segment(
    path: PathBuf,
    expected_first_seq: WalSeq,
    is_newest: bool,
) -> Result<SegmentScan> {
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
    let mut len_bytes = data.len();
    while offset < data.len() {
        let (record, next_offset) = match decode_record_at(&path, &data, offset) {
            Ok(decoded) => decoded,
            Err(error) if is_newest && error.repair_offset(data.len(), offset).is_some() => {
                truncate_segment_tail(&path, offset).await?;
                len_bytes = offset;
                break;
            }
            Err(error) => return Err(error.into_error()),
        };
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
        len_bytes: len_bytes as u64,
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

async fn truncate_segment_tail(path: &Path, offset: usize) -> Result<()> {
    let file = OpenOptions::new()
        .write(true)
        .open(path)
        .await
        .map_err(|source| ServerError::io("open WAL segment for tail repair", source))?;
    file.set_len(offset as u64)
        .await
        .map_err(|source| ServerError::io("truncate WAL segment tail", source))?;
    file.sync_all()
        .await
        .map_err(|source| ServerError::io("sync WAL segment after tail repair", source))
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

fn previous_seq_before(seq: WalSeq) -> Result<WalSeq> {
    seq.get()
        .checked_sub(1)
        .map(WalSeq::new)
        .ok_or_else(|| ServerError::wal("read WAL segment", "segment starts at sequence zero"))
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
