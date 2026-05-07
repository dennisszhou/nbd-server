/// Logical byte range used by export admission, engines, WAL, and read views.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ByteRange {
    start: u64,
    len: u32,
}

impl ByteRange {
    pub fn new(start: u64, len: u32) -> Self {
        Self { start, len }
    }

    pub fn start(self) -> u64 {
        self.start
    }

    pub fn len(self) -> u64 {
        u64::from(self.len)
    }

    pub fn is_empty(self) -> bool {
        self.len == 0
    }

    pub(crate) fn checked_end(self) -> Option<u64> {
        self.start.checked_add(self.len())
    }

    fn end(self) -> u64 {
        self.start.saturating_add(u64::from(self.len))
    }

    pub(crate) fn overlaps(self, other: Self) -> bool {
        self.start < other.end() && other.start < self.end()
    }
}
