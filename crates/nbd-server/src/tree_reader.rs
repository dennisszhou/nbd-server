use crate::{ByteRange, Result};
use std::fmt;

#[async_trait::async_trait]
pub(crate) trait TreeReader<R>: fmt::Debug + Send + Sync {
    async fn read_committed(&self, root: &R, range: ByteRange) -> Result<Vec<u8>>;
}
