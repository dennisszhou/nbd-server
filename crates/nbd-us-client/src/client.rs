use crate::{ClientError, Result};
use nbd_protocol::constants::{
    IHAVEOPT_MAGIC, INIT_PASSWD, NBD_FLAG_FIXED_NEWSTYLE, NBD_FLAG_HAS_FLAGS, NBD_FLAG_NO_ZEROES,
    NBD_INFO_EXPORT,
};
use nbd_protocol::handshake::encode_client_flags;
use nbd_protocol::option::{
    OPTION_REPLY_HEADER_BYTES, OptionReply, encode_go_request, parse_option_reply,
    parse_option_reply_header,
};
use nbd_protocol::transmission::{
    SIMPLE_REPLY_BYTES, encode_disconnect_request, encode_flush_request, encode_read_request,
    encode_write_request, parse_simple_reply,
};
use nbd_protocol::wire::NbdCookie;
use nbd_protocol::wire::WireReader;
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

pub struct NbdClient {
    stream: TcpStream,
    export_size_bytes: u64,
    transmission_flags: u16,
    next_cookie: u64,
}

impl NbdClient {
    pub async fn connect(addr: SocketAddr, export_name: &str) -> Result<Self> {
        let mut stream = TcpStream::connect(addr)
            .await
            .map_err(|source| ClientError::io("connect to NBD server", source))?;

        read_server_handshake(&mut stream).await?;
        stream
            .write_all(&encode_client_flags(true))
            .await
            .map_err(|source| ClientError::io("write NBD client flags", source))?;
        stream
            .write_all(&encode_go_request(export_name, &[NBD_INFO_EXPORT])?)
            .await
            .map_err(|source| ClientError::io("write NBD_OPT_GO", source))?;

        let (export_size_bytes, transmission_flags) = read_go_replies(&mut stream).await?;

        Ok(Self {
            stream,
            export_size_bytes,
            transmission_flags,
            next_cookie: 1,
        })
    }

    pub fn export_size_bytes(&self) -> u64 {
        self.export_size_bytes
    }

    pub fn transmission_flags(&self) -> u16 {
        self.transmission_flags
    }

    pub fn has_transmission_flags(&self) -> bool {
        self.transmission_flags & NBD_FLAG_HAS_FLAGS != 0
    }

    pub fn peer_addr(&self) -> std::io::Result<SocketAddr> {
        self.stream.peer_addr()
    }

    pub async fn read(&mut self, offset: u64, len: u32) -> Result<Vec<u8>> {
        let cookie = self.next_cookie();
        self.stream
            .write_all(&encode_read_request(cookie, offset, len)?)
            .await
            .map_err(|source| ClientError::io("write NBD read request", source))?;

        let reply = self.read_simple_reply(cookie).await?;
        if reply.error != 0 {
            return Err(ClientError::CommandError {
                command: "READ",
                error: reply.error,
            });
        }

        let mut data = vec![0; len as usize];
        self.stream
            .read_exact(&mut data)
            .await
            .map_err(|source| ClientError::io("read NBD read payload", source))?;
        Ok(data)
    }

    pub async fn write(&mut self, offset: u64, data: &[u8]) -> Result<()> {
        let cookie = self.next_cookie();
        self.stream
            .write_all(&encode_write_request(cookie, offset, data)?)
            .await
            .map_err(|source| ClientError::io("write NBD write request", source))?;
        self.expect_success(cookie, "WRITE").await
    }

    pub async fn flush(&mut self) -> Result<()> {
        let cookie = self.next_cookie();
        self.stream
            .write_all(&encode_flush_request(cookie)?)
            .await
            .map_err(|source| ClientError::io("write NBD flush request", source))?;
        self.expect_success(cookie, "FLUSH").await
    }

    pub async fn disconnect(mut self) -> Result<()> {
        let cookie = self.next_cookie();
        self.stream
            .write_all(&encode_disconnect_request(cookie)?)
            .await
            .map_err(|source| ClientError::io("write NBD disconnect request", source))?;
        self.stream
            .shutdown()
            .await
            .map_err(|source| ClientError::io("shutdown NBD client socket", source))
    }

    fn next_cookie(&mut self) -> NbdCookie {
        let cookie = NbdCookie::new(self.next_cookie);
        self.next_cookie = self.next_cookie.wrapping_add(1);
        cookie
    }

    async fn expect_success(&mut self, cookie: NbdCookie, command: &'static str) -> Result<()> {
        let reply = self.read_simple_reply(cookie).await?;
        if reply.error == 0 {
            Ok(())
        } else {
            Err(ClientError::CommandError {
                command,
                error: reply.error,
            })
        }
    }

    async fn read_simple_reply(
        &mut self,
        expected_cookie: NbdCookie,
    ) -> Result<nbd_protocol::SimpleReply> {
        let mut bytes = [0; SIMPLE_REPLY_BYTES];
        self.stream
            .read_exact(&mut bytes)
            .await
            .map_err(|source| ClientError::io("read NBD simple reply", source))?;
        let reply = parse_simple_reply(&bytes)?;
        if reply.cookie != expected_cookie {
            return Err(ClientError::CookieMismatch {
                expected: expected_cookie,
                actual: reply.cookie,
            });
        }
        Ok(reply)
    }
}

async fn read_server_handshake(stream: &mut TcpStream) -> Result<()> {
    let mut bytes = [0; 18];
    stream
        .read_exact(&mut bytes)
        .await
        .map_err(|source| ClientError::io("read NBD server handshake", source))?;

    let mut reader = WireReader::new(&bytes);
    let init = reader.read_u64()?;
    if init != INIT_PASSWD {
        return Err(nbd_protocol::ProtocolError::InvalidMagic {
            context: "server handshake",
            expected: INIT_PASSWD,
            actual: init,
        }
        .into());
    }

    let option_magic = reader.read_u64()?;
    if option_magic != IHAVEOPT_MAGIC {
        return Err(nbd_protocol::ProtocolError::InvalidMagic {
            context: "server option handshake",
            expected: IHAVEOPT_MAGIC,
            actual: option_magic,
        }
        .into());
    }

    let flags = reader.read_u16()?;
    if flags & NBD_FLAG_FIXED_NEWSTYLE == 0 || flags & NBD_FLAG_NO_ZEROES == 0 {
        return Err(ClientError::UnsupportedServerFlags { flags });
    }

    Ok(())
}

async fn read_go_replies(stream: &mut TcpStream) -> Result<(u64, u16)> {
    let mut export_info = None;
    loop {
        match read_option_reply(stream).await? {
            OptionReply::InfoExport {
                export_size_bytes,
                transmission_flags,
                ..
            } => export_info = Some((export_size_bytes, transmission_flags)),
            OptionReply::Ack { .. } => {
                return export_info.ok_or(ClientError::UnexpectedOptionReply {
                    reply: "NBD_REP_ACK before NBD_INFO_EXPORT",
                });
            }
            OptionReply::Error {
                reply_type,
                message,
                ..
            } => {
                return Err(ClientError::OptionError {
                    reply_type,
                    message,
                });
            }
            OptionReply::Other { .. } => {}
        }
    }
}

async fn read_option_reply(stream: &mut TcpStream) -> Result<OptionReply> {
    let mut header = [0; OPTION_REPLY_HEADER_BYTES];
    stream
        .read_exact(&mut header)
        .await
        .map_err(|source| ClientError::io("read NBD option reply header", source))?;
    let parsed_header = parse_option_reply_header(&header)?;

    let mut bytes = header.to_vec();
    let mut payload = vec![0; parsed_header.bounded_payload_len()?];
    stream
        .read_exact(&mut payload)
        .await
        .map_err(|source| ClientError::io("read NBD option reply payload", source))?;
    bytes.extend_from_slice(&payload);

    Ok(parse_option_reply(&bytes)?)
}
