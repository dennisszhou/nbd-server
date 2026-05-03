use nbd_config::{ConfigSource, NbdConfig};
use nbd_control_plane::{
    CatalogUrl, CreateExport, DeleteExport, ExportCatalog, ExportEngineKind, ExportMeta,
    ExportName, SQLiteExportCatalog,
};
use nbd_protocol::constants::{
    IHAVEOPT_MAGIC, INIT_PASSWD, NBD_FLAG_FIXED_NEWSTYLE, NBD_FLAG_NO_ZEROES, NBD_INFO_EXPORT,
};
use nbd_protocol::handshake::encode_client_flags;
use nbd_protocol::option::{
    encode_abort_request, encode_go_request, encode_option_request, parse_option_reply,
    parse_option_reply_header, OptionReply, OPTION_REPLY_HEADER_BYTES,
};
use nbd_protocol::transmission::{
    encode_disconnect_request, encode_flush_request, encode_read_request, encode_request_header,
    encode_write_request, parse_simple_reply, RequestHeader, SimpleReply, SIMPLE_REPLY_BYTES,
};
use nbd_protocol::wire::{NbdCookie, WireReader};
use nbd_server::NbdServer;
use nbd_test_support::TestRuntime;
use std::collections::HashMap;
use std::error::Error;
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const MIGRATION: &str =
    include_str!("../../../../prisma/migrations/20260501000000_init/migration.sql");

pub type TestResult<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EngineProfile {
    kind: ExportEngineKind,
}

impl EngineProfile {
    pub const MEMORY: Self = Self {
        kind: ExportEngineKind::Memory,
    };

    pub fn kind(self) -> ExportEngineKind {
        self.kind
    }
}

pub struct ServerFixture {
    runtime: TestRuntime,
    catalog: SQLiteExportCatalog,
    engine: EngineProfile,
}

impl ServerFixture {
    pub async fn new(engine: EngineProfile) -> TestResult<Self> {
        let runtime = TestRuntime::new()?;
        let catalog = migrated_catalog(&runtime).await?;

        Ok(Self {
            runtime,
            catalog,
            engine,
        })
    }

    pub async fn create_export(
        &self,
        name: &str,
        size_bytes: u64,
        block_size: u64,
    ) -> TestResult<ExportMeta> {
        Ok(self
            .catalog
            .create_export(create_export(name, size_bytes, block_size, self.engine))
            .await?)
    }

    pub async fn delete_export(&self, name: &str) -> TestResult<()> {
        Ok(self
            .catalog
            .delete_export(DeleteExport::new(export_name(name)))
            .await?)
    }

    pub async fn start_server(&self) -> TestResult<NbdServer> {
        Ok(NbdServer::start(load_config(&self.runtime)?).await?)
    }
}

pub struct RawNbdConnection {
    stream: TcpStream,
    export_size_bytes: u64,
    transmission_flags: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RawNbdReply {
    Simple(SimpleReply),
    Read {
        cookie: NbdCookie,
        error: u32,
        data: Vec<u8>,
    },
}

impl RawNbdReply {
    pub fn cookie(&self) -> NbdCookie {
        match self {
            Self::Simple(reply) => reply.cookie,
            Self::Read { cookie, .. } => *cookie,
        }
    }

    pub fn error(&self) -> u32 {
        match self {
            Self::Simple(reply) => reply.error,
            Self::Read { error, .. } => *error,
        }
    }

    pub fn read_data(&self) -> Option<&[u8]> {
        match self {
            Self::Read { data, .. } => Some(data),
            Self::Simple(_) => None,
        }
    }
}

impl RawNbdConnection {
    pub async fn connect(addr: SocketAddr, export_name: &str) -> TestResult<Self> {
        let mut option_client = RawNbdOptionClient::connect(addr).await?;
        option_client
            .stream
            .write_all(&encode_go_request(export_name, &[NBD_INFO_EXPORT])?)
            .await?;

        let (export_size_bytes, transmission_flags) =
            read_go_replies(&mut option_client.stream).await?;

        Ok(Self {
            stream: option_client.stream,
            export_size_bytes,
            transmission_flags,
        })
    }

    pub fn export_size_bytes(&self) -> u64 {
        self.export_size_bytes
    }

    pub fn transmission_flags(&self) -> u16 {
        self.transmission_flags
    }

    pub async fn send_read(&mut self, cookie: NbdCookie, offset: u64, len: u32) -> TestResult<()> {
        Ok(self
            .stream
            .write_all(&encode_read_request(cookie, offset, len)?)
            .await?)
    }

    pub async fn send_write(
        &mut self,
        cookie: NbdCookie,
        offset: u64,
        data: &[u8],
    ) -> TestResult<()> {
        Ok(self
            .stream
            .write_all(&encode_write_request(cookie, offset, data)?)
            .await?)
    }

    pub async fn send_flush(&mut self, cookie: NbdCookie) -> TestResult<()> {
        Ok(self
            .stream
            .write_all(&encode_flush_request(cookie)?)
            .await?)
    }

    pub async fn send_request_header(&mut self, header: RequestHeader) -> TestResult<()> {
        Ok(self
            .stream
            .write_all(&encode_request_header(header))
            .await?)
    }

    pub async fn send_raw_bytes(&mut self, bytes: &[u8]) -> TestResult<()> {
        Ok(self.stream.write_all(bytes).await?)
    }

    pub async fn read_simple_reply(
        &mut self,
        expected_cookie: NbdCookie,
    ) -> TestResult<SimpleReply> {
        let mut header = [0; SIMPLE_REPLY_BYTES];
        self.stream.read_exact(&mut header).await?;
        let reply = parse_simple_reply(&header)?;
        if reply.cookie != expected_cookie {
            return Err(test_error(format!(
                "expected cookie {}, got {}",
                expected_cookie.raw(),
                reply.cookie.raw()
            )));
        }

        Ok(reply)
    }

    pub async fn read_reply(
        &mut self,
        read_lengths: &HashMap<NbdCookie, u32>,
    ) -> TestResult<RawNbdReply> {
        let mut header = [0; SIMPLE_REPLY_BYTES];
        self.stream.read_exact(&mut header).await?;
        let reply = parse_simple_reply(&header)?;

        if reply.error == 0 {
            if let Some(length) = read_lengths.get(&reply.cookie) {
                let mut data = vec![0; *length as usize];
                self.stream.read_exact(&mut data).await?;
                return Ok(RawNbdReply::Read {
                    cookie: reply.cookie,
                    error: reply.error,
                    data,
                });
            }
        }

        Ok(RawNbdReply::Simple(reply))
    }

    pub async fn read_successful_read(
        &mut self,
        expected_cookie: NbdCookie,
        expected_len: u32,
    ) -> TestResult<Vec<u8>> {
        let mut read_lengths = HashMap::new();
        read_lengths.insert(expected_cookie, expected_len);
        let reply = self.read_reply(&read_lengths).await?;
        if reply.cookie() != expected_cookie {
            return Err(test_error(format!(
                "expected cookie {}, got {}",
                expected_cookie.raw(),
                reply.cookie().raw()
            )));
        }
        if reply.error() != 0 {
            return Err(test_error(format!(
                "expected successful read, got NBD error {}",
                reply.error()
            )));
        }

        reply
            .read_data()
            .map(<[u8]>::to_vec)
            .ok_or_else(|| test_error("expected read payload"))
    }

    pub async fn disconnect(mut self, cookie: NbdCookie) -> TestResult<()> {
        self.stream
            .write_all(&encode_disconnect_request(cookie)?)
            .await?;
        Ok(self.stream.shutdown().await?)
    }

    pub async fn shutdown_write(&mut self) -> TestResult<()> {
        Ok(self.stream.shutdown().await?)
    }
}

pub struct RawNbdOptionClient {
    stream: TcpStream,
}

impl RawNbdOptionClient {
    pub async fn connect(addr: SocketAddr) -> TestResult<Self> {
        let mut stream = TcpStream::connect(addr).await?;

        read_server_handshake(&mut stream).await?;
        stream.write_all(&encode_client_flags(true)).await?;

        Ok(Self { stream })
    }

    pub async fn send_option(
        &mut self,
        option: nbd_protocol::wire::NbdOptionCode,
        payload: &[u8],
    ) -> TestResult<()> {
        Ok(self
            .stream
            .write_all(&encode_option_request(option, payload)?)
            .await?)
    }

    pub async fn send_abort(&mut self) -> TestResult<()> {
        Ok(self.stream.write_all(&encode_abort_request(&[])?).await?)
    }

    pub async fn read_option_reply(&mut self) -> TestResult<OptionReply> {
        read_option_reply(&mut self.stream).await
    }
}

pub fn export_name(name: &str) -> ExportName {
    ExportName::new(name).expect("valid export name")
}

fn create_export(
    name: &str,
    size_bytes: u64,
    block_size: u64,
    engine: EngineProfile,
) -> CreateExport {
    CreateExport::new(export_name(name), size_bytes, block_size, engine.kind())
        .expect("valid create export request")
}

async fn migrated_catalog(runtime: &TestRuntime) -> TestResult<SQLiteExportCatalog> {
    let url = CatalogUrl::parse(runtime.catalog_url())?;
    let catalog = SQLiteExportCatalog::connect(&url).await?;

    sqlx::raw_sql(MIGRATION).execute(catalog.pool()).await?;

    Ok(catalog)
}

fn load_config(runtime: &TestRuntime) -> Result<NbdConfig, nbd_config::ConfigError> {
    NbdConfig::load(ConfigSource::ExplicitPath(
        runtime.config_path().to_path_buf(),
    ))
}

async fn read_server_handshake(stream: &mut TcpStream) -> TestResult<()> {
    let mut bytes = [0; 18];
    stream.read_exact(&mut bytes).await?;

    let mut reader = WireReader::new(&bytes);
    let init = reader.read_u64()?;
    if init != INIT_PASSWD {
        return Err(test_error(format!(
            "expected NBD init magic {INIT_PASSWD}, got {init}"
        )));
    }

    let option_magic = reader.read_u64()?;
    if option_magic != IHAVEOPT_MAGIC {
        return Err(test_error(format!(
            "expected NBD option magic {IHAVEOPT_MAGIC}, got {option_magic}"
        )));
    }

    let flags = reader.read_u16()?;
    if flags & NBD_FLAG_FIXED_NEWSTYLE == 0 || flags & NBD_FLAG_NO_ZEROES == 0 {
        return Err(test_error(format!("unsupported server flags {flags}")));
    }

    Ok(())
}

async fn read_go_replies(stream: &mut TcpStream) -> TestResult<(u64, u16)> {
    let mut export_info = None;
    loop {
        match read_option_reply(stream).await? {
            OptionReply::InfoExport {
                export_size_bytes,
                transmission_flags,
                ..
            } => export_info = Some((export_size_bytes, transmission_flags)),
            OptionReply::Ack { .. } => {
                return export_info.ok_or_else(|| test_error("NBD_REP_ACK before NBD_INFO_EXPORT"));
            }
            OptionReply::Error {
                reply_type,
                message,
                ..
            } => {
                return Err(test_error(format!(
                    "NBD_OPT_GO failed with reply type {reply_type}: {}",
                    String::from_utf8_lossy(&message)
                )));
            }
            OptionReply::Other { .. } => {}
        }
    }
}

async fn read_option_reply(stream: &mut TcpStream) -> TestResult<OptionReply> {
    let mut header = [0; OPTION_REPLY_HEADER_BYTES];
    stream.read_exact(&mut header).await?;
    let parsed_header = parse_option_reply_header(&header)?;

    let mut bytes = header.to_vec();
    let mut payload = vec![0; parsed_header.bounded_payload_len()?];
    stream.read_exact(&mut payload).await?;
    bytes.extend_from_slice(&payload);

    Ok(parse_option_reply(&bytes)?)
}

fn test_error(message: impl Into<String>) -> Box<dyn Error + Send + Sync> {
    Box::new(std::io::Error::other(message.into()))
}
