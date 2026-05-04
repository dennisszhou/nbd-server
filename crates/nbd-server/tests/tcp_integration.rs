mod support;

use nbd_config::{ExportRuntimeKind, ServerConfig};
use nbd_protocol::constants::{
    NBD_CMD_WRITE, NBD_EINVAL, NBD_FLAG_HAS_FLAGS, NBD_FLAG_SEND_FLUSH, NBD_OPT_ABORT,
    NBD_REP_ERR_POLICY, NBD_REP_ERR_UNKNOWN, NBD_REP_ERR_UNSUP,
};
use nbd_protocol::wire::{NbdCommandFlags, NbdCommandType, NbdCookie, NbdOptionCode};
use nbd_protocol::{OptionReply, RequestHeader};
use nbd_us_client::{ClientError, NbdClient};
use std::collections::HashMap;
use std::num::NonZeroUsize;
use support::nbd::{
    EngineProfile, RawNbdConnection, RawNbdOptionClient, RawNbdReply, ServerFixture,
};

#[tokio::test]
async fn active_export_negotiates_over_tcp() {
    let fixture = ServerFixture::new(EngineProfile::MEMORY)
        .await
        .expect("server fixture");
    fixture
        .create_export("disk-a", 4096, 4096)
        .await
        .expect("create export");

    let server = fixture.start_server().await.expect("start server");
    let client = NbdClient::connect(server.addr(), "disk-a")
        .await
        .expect("connect client");

    assert_eq!(client.export_size_bytes(), 4096);
    assert_eq!(
        client.transmission_flags(),
        NBD_FLAG_HAS_FLAGS | NBD_FLAG_SEND_FLUSH,
    );
    assert!(client.has_transmission_flags());
    assert_eq!(client.peer_addr().expect("peer addr"), server.addr());

    server.shutdown().await.expect("shutdown server");
}

#[tokio::test]
async fn client_reads_writes_flushes_and_disconnects() {
    let fixture = ServerFixture::new(EngineProfile::MEMORY)
        .await
        .expect("server fixture");
    fixture
        .create_export("disk-a", 4096, 4096)
        .await
        .expect("create export");

    let server = fixture.start_server().await.expect("start server");
    let mut client = NbdClient::connect(server.addr(), "disk-a")
        .await
        .expect("connect client");

    assert_eq!(client.read(0, 8).await.expect("zero read"), vec![0; 8]);
    client.write(2, b"hello").await.expect("write");
    assert_eq!(
        client.read(0, 10).await.expect("readback"),
        vec![0, 0, b'h', b'e', b'l', b'l', b'o', 0, 0, 0],
    );
    client.flush().await.expect("flush");
    client.disconnect().await.expect("disconnect");

    server.shutdown().await.expect("shutdown server");
}

#[tokio::test]
async fn different_exports_have_independent_in_memory_contents() {
    let fixture = ServerFixture::new(EngineProfile::MEMORY)
        .await
        .expect("server fixture");
    fixture
        .create_export("disk-a", 4096, 4096)
        .await
        .expect("create disk-a");
    fixture
        .create_export("disk-b", 4096, 4096)
        .await
        .expect("create disk-b");

    let server = fixture.start_server().await.expect("start server");
    let mut disk_a = NbdClient::connect(server.addr(), "disk-a")
        .await
        .expect("connect disk-a");
    let mut disk_b = NbdClient::connect(server.addr(), "disk-b")
        .await
        .expect("connect disk-b");

    disk_a.write(0, b"aaaa").await.expect("write disk-a");
    assert_eq!(
        disk_a.read(0, 4).await.expect("read disk-a"),
        b"aaaa".to_vec(),
    );
    assert_eq!(disk_b.read(0, 4).await.expect("read disk-b"), vec![0; 4]);

    disk_a.disconnect().await.expect("disconnect disk-a");
    disk_b.disconnect().await.expect("disconnect disk-b");
    server.shutdown().await.expect("shutdown server");
}

#[tokio::test]
async fn out_of_bounds_reads_return_nbd_error() {
    let fixture = ServerFixture::new(EngineProfile::MEMORY)
        .await
        .expect("server fixture");
    fixture
        .create_export("disk-a", 8, 4096)
        .await
        .expect("create export");

    let server = fixture.start_server().await.expect("start server");
    let mut client = NbdClient::connect(server.addr(), "disk-a")
        .await
        .expect("connect client");

    assert!(matches!(
        client.read(7, 2).await,
        Err(ClientError::CommandError {
            command: "READ",
            error: NBD_EINVAL,
        }),
    ));

    client.disconnect().await.expect("disconnect");
    server.shutdown().await.expect("shutdown server");
}

#[tokio::test]
async fn out_of_bounds_writes_return_nbd_error() {
    let fixture = ServerFixture::new(EngineProfile::MEMORY)
        .await
        .expect("server fixture");
    fixture
        .create_export("disk-a", 8, 4096)
        .await
        .expect("create export");

    let server = fixture.start_server().await.expect("start server");
    let mut client = NbdClient::connect(server.addr(), "disk-a")
        .await
        .expect("connect client");

    assert!(matches!(
        client.write(7, b"xx").await,
        Err(ClientError::CommandError {
            command: "WRITE",
            error: NBD_EINVAL,
        }),
    ));

    client.disconnect().await.expect("disconnect");
    server.shutdown().await.expect("shutdown server");
}

#[tokio::test]
async fn missing_or_deleted_exports_fail_during_go() {
    let fixture = ServerFixture::new(EngineProfile::MEMORY)
        .await
        .expect("server fixture");
    fixture
        .create_export("deleted", 4096, 4096)
        .await
        .expect("create export");
    fixture
        .delete_export("deleted")
        .await
        .expect("delete export");

    let server = fixture.start_server().await.expect("start server");

    assert_unknown_export(NbdClient::connect(server.addr(), "missing").await);
    assert_unknown_export(NbdClient::connect(server.addr(), "deleted").await);

    server.shutdown().await.expect("shutdown server");
}

#[tokio::test]
async fn active_export_rejects_second_mounter_until_disconnect() {
    let fixture = ServerFixture::new(EngineProfile::MEMORY)
        .await
        .expect("server fixture");
    fixture
        .create_export("disk-a", 4096, 4096)
        .await
        .expect("create export");

    let server = fixture.start_server().await.expect("start server");
    let first = NbdClient::connect(server.addr(), "disk-a")
        .await
        .expect("connect first mounter");

    assert_policy_error(NbdClient::connect(server.addr(), "disk-a").await);

    first.disconnect().await.expect("disconnect first");

    let reopened = reconnect_after_disconnect(server.addr()).await;
    reopened.disconnect().await.expect("disconnect reopened");
    server.shutdown().await.expect("shutdown server");
}

#[tokio::test]
async fn unsupported_option_returns_error_and_keeps_negotiation_open() {
    let fixture = ServerFixture::new(EngineProfile::MEMORY)
        .await
        .expect("server fixture");
    let server = fixture.start_server().await.expect("start server");
    let mut client = RawNbdOptionClient::connect(server.addr())
        .await
        .expect("connect raw option client");

    let unsupported = NbdOptionCode::new(0xfeed_beef);
    client
        .send_option(unsupported, b"ignored")
        .await
        .expect("send unsupported option");

    assert_option_error(
        client
            .read_option_reply()
            .await
            .expect("read unsupported option reply"),
        unsupported,
        NBD_REP_ERR_UNSUP,
    );

    client.send_abort().await.expect("send abort");
    assert_option_ack(
        client.read_option_reply().await.expect("read abort ack"),
        NbdOptionCode::new(NBD_OPT_ABORT),
    );
    server.shutdown().await.expect("shutdown server");
}

#[tokio::test]
async fn abort_option_is_acknowledged() {
    let fixture = ServerFixture::new(EngineProfile::MEMORY)
        .await
        .expect("server fixture");
    let server = fixture.start_server().await.expect("start server");
    let mut client = RawNbdOptionClient::connect(server.addr())
        .await
        .expect("connect raw option client");

    client.send_abort().await.expect("send abort");
    assert_option_ack(
        client.read_option_reply().await.expect("read abort ack"),
        NbdOptionCode::new(NBD_OPT_ABORT),
    );
    server.shutdown().await.expect("shutdown server");
}

#[tokio::test]
async fn raw_protocol_helper_reads_with_explicit_cookie() {
    let fixture = ServerFixture::new(EngineProfile::MEMORY)
        .await
        .expect("server fixture");
    fixture
        .create_export("disk-a", 4096, 4096)
        .await
        .expect("create export");

    let server = fixture.start_server().await.expect("start server");
    let mut client = RawNbdConnection::connect(server.addr(), "disk-a")
        .await
        .expect("connect raw client");

    assert_eq!(client.export_size_bytes(), 4096);
    assert_eq!(
        client.transmission_flags(),
        NBD_FLAG_HAS_FLAGS | NBD_FLAG_SEND_FLUSH,
    );

    let read_cookie = NbdCookie::new(42);
    client
        .send_read(read_cookie, 0, 8)
        .await
        .expect("send raw read");
    assert_eq!(
        client
            .read_successful_read(read_cookie, 8)
            .await
            .expect("read raw reply"),
        vec![0; 8],
    );

    client
        .disconnect(NbdCookie::new(43))
        .await
        .expect("disconnect raw client");
    server.shutdown().await.expect("shutdown server");
}

#[tokio::test]
async fn raw_write_flush_and_read_replies_echo_cookies() {
    let fixture = ServerFixture::new(EngineProfile::MEMORY)
        .await
        .expect("server fixture");
    fixture
        .create_export("disk-a", 4096, 4096)
        .await
        .expect("create export");

    let server = fixture.start_server().await.expect("start server");
    let mut client = RawNbdConnection::connect(server.addr(), "disk-a")
        .await
        .expect("connect raw client");

    let write_cookie = NbdCookie::new(100);
    client
        .send_write(write_cookie, 2, b"abcd")
        .await
        .expect("send write");
    assert!(client
        .read_simple_reply(write_cookie)
        .await
        .expect("read write reply")
        .is_success());

    let flush_cookie = NbdCookie::new(101);
    client.send_flush(flush_cookie).await.expect("send flush");
    assert!(client
        .read_simple_reply(flush_cookie)
        .await
        .expect("read flush reply")
        .is_success());

    let read_cookie = NbdCookie::new(102);
    client
        .send_read(read_cookie, 0, 8)
        .await
        .expect("send read");
    assert_eq!(
        client
            .read_successful_read(read_cookie, 8)
            .await
            .expect("read reply"),
        vec![0, 0, b'a', b'b', b'c', b'd', 0, 0],
    );

    client
        .disconnect(NbdCookie::new(103))
        .await
        .expect("disconnect raw client");
    server.shutdown().await.expect("shutdown server");
}

#[tokio::test]
async fn default_runtime_handles_pipelined_protocol_smoke() {
    let fixture = ServerFixture::new(EngineProfile::MEMORY)
        .await
        .expect("server fixture");
    fixture
        .create_export("disk-a", 4096, 4096)
        .await
        .expect("create export");

    let server = fixture.start_server().await.expect("start server");
    let mut client = RawNbdConnection::connect(server.addr(), "disk-a")
        .await
        .expect("connect raw client");

    let first_read = NbdCookie::new(150);
    let write = NbdCookie::new(151);
    let independent_read = NbdCookie::new(152);
    let conflicting_read = NbdCookie::new(153);

    client
        .send_read(first_read, 0, 4)
        .await
        .expect("send first read");
    client
        .send_write(write, 0, b"zzzz")
        .await
        .expect("send write");
    client
        .send_read(independent_read, 4, 4)
        .await
        .expect("send independent read");
    client
        .send_read(conflicting_read, 0, 4)
        .await
        .expect("send conflicting read");

    let replies = collect_replies(
        &mut client,
        &[
            (first_read, 4),
            (independent_read, 4),
            (conflicting_read, 4),
        ],
        4,
    )
    .await;
    assert_read_data(&replies, first_read, &[0; 4]);
    assert_simple_success(&replies, write);
    assert_read_data(&replies, independent_read, &[0; 4]);
    assert_read_data(&replies, conflicting_read, b"zzzz");

    client
        .disconnect(NbdCookie::new(154))
        .await
        .expect("disconnect raw client");
    server.shutdown().await.expect("shutdown server");
}

#[tokio::test]
async fn serial_runtime_handles_pipelined_protocol_smoke() {
    let fixture = ServerFixture::new(EngineProfile::MEMORY)
        .await
        .expect("server fixture");
    fixture
        .configure_server(ServerConfig {
            export_runtime: ExportRuntimeKind::Serial,
            export_queue_depth: NonZeroUsize::new(4).expect("nonzero queue depth"),
            ..ServerConfig::default()
        })
        .expect("configure serial runtime");
    fixture
        .create_export("disk-a", 4096, 4096)
        .await
        .expect("create export");

    let server = fixture.start_server().await.expect("start server");
    let mut client = RawNbdConnection::connect(server.addr(), "disk-a")
        .await
        .expect("connect raw client");

    let first_read = NbdCookie::new(160);
    let write = NbdCookie::new(161);
    let independent_read = NbdCookie::new(162);
    let conflicting_read = NbdCookie::new(163);

    client
        .send_read(first_read, 0, 4)
        .await
        .expect("send first read");
    client
        .send_write(write, 0, b"yyyy")
        .await
        .expect("send write");
    client
        .send_read(independent_read, 4, 4)
        .await
        .expect("send independent read");
    client
        .send_read(conflicting_read, 0, 4)
        .await
        .expect("send conflicting read");

    let replies = collect_replies(
        &mut client,
        &[
            (first_read, 4),
            (independent_read, 4),
            (conflicting_read, 4),
        ],
        4,
    )
    .await;
    assert_read_data(&replies, first_read, &[0; 4]);
    assert_simple_success(&replies, write);
    assert_read_data(&replies, independent_read, &[0; 4]);
    assert_read_data(&replies, conflicting_read, b"yyyy");

    client
        .disconnect(NbdCookie::new(164))
        .await
        .expect("disconnect raw client");
    server.shutdown().await.expect("shutdown server");
}

#[tokio::test]
async fn unsupported_transmission_command_returns_nbd_error() {
    let fixture = ServerFixture::new(EngineProfile::MEMORY)
        .await
        .expect("server fixture");
    fixture
        .create_export("disk-a", 4096, 4096)
        .await
        .expect("create export");

    let server = fixture.start_server().await.expect("start server");
    let mut client = RawNbdConnection::connect(server.addr(), "disk-a")
        .await
        .expect("connect raw client");

    let cookie = NbdCookie::new(200);
    client
        .send_request_header(RequestHeader {
            flags: NbdCommandFlags::new(0),
            command: NbdCommandType::new(99),
            cookie,
            offset: 0,
            length: 0,
        })
        .await
        .expect("send unsupported command");

    let reply = client
        .read_simple_reply(cookie)
        .await
        .expect("read unsupported command reply");
    assert_eq!(reply.error, NBD_EINVAL);
    server.shutdown().await.expect("shutdown server");
}

#[tokio::test]
async fn connection_eof_releases_active_export() {
    let fixture = ServerFixture::new(EngineProfile::MEMORY)
        .await
        .expect("server fixture");
    fixture
        .create_export("disk-a", 4096, 4096)
        .await
        .expect("create export");

    let server = fixture.start_server().await.expect("start server");
    let mut client = RawNbdConnection::connect(server.addr(), "disk-a")
        .await
        .expect("connect raw client");

    client
        .shutdown_write()
        .await
        .expect("shutdown client write half");
    drop(client);

    let reopened = reconnect_after_disconnect(server.addr()).await;
    reopened.disconnect().await.expect("disconnect reopened");
    server.shutdown().await.expect("shutdown server");
}

#[tokio::test]
async fn malformed_write_payload_eof_releases_active_export() {
    let fixture = ServerFixture::new(EngineProfile::MEMORY)
        .await
        .expect("server fixture");
    fixture
        .create_export("disk-a", 4096, 4096)
        .await
        .expect("create export");

    let server = fixture.start_server().await.expect("start server");
    let mut client = RawNbdConnection::connect(server.addr(), "disk-a")
        .await
        .expect("connect raw client");

    client
        .send_request_header(RequestHeader {
            flags: NbdCommandFlags::new(0),
            command: NbdCommandType::new(NBD_CMD_WRITE),
            cookie: NbdCookie::new(300),
            offset: 0,
            length: 4,
        })
        .await
        .expect("send write header");
    client
        .send_raw_bytes(b"ab")
        .await
        .expect("send partial payload");
    client
        .shutdown_write()
        .await
        .expect("shutdown client write half");
    drop(client);

    let reopened = reconnect_after_disconnect(server.addr()).await;
    reopened.disconnect().await.expect("disconnect reopened");
    server.shutdown().await.expect("shutdown server");
}

#[tokio::test]
async fn pipelined_read_write_read_visibility_allows_independent_read() {
    let fixture = ServerFixture::new(EngineProfile::MEMORY)
        .await
        .expect("server fixture");
    fixture
        .create_export("disk-a", 4096, 4096)
        .await
        .expect("create export");

    let server = fixture.start_server().await.expect("start server");
    let mut client = RawNbdConnection::connect(server.addr(), "disk-a")
        .await
        .expect("connect raw client");

    let first_read = NbdCookie::new(400);
    let write = NbdCookie::new(401);
    let second_read = NbdCookie::new(402);
    let independent_read = NbdCookie::new(403);

    client
        .send_read(first_read, 0, 4)
        .await
        .expect("send first read");
    client
        .send_write(write, 0, b"aaaa")
        .await
        .expect("send write");
    client
        .send_read(second_read, 0, 4)
        .await
        .expect("send second read");
    client
        .send_read(independent_read, 4, 4)
        .await
        .expect("send independent read");

    let replies = collect_replies(
        &mut client,
        &[(first_read, 4), (second_read, 4), (independent_read, 4)],
        4,
    )
    .await;
    assert_read_data(&replies, first_read, &[0; 4]);
    assert_simple_success(&replies, write);
    assert_read_data(&replies, second_read, b"aaaa");
    assert_read_data(&replies, independent_read, &[0; 4]);

    client
        .disconnect(NbdCookie::new(404))
        .await
        .expect("disconnect raw client");
    server.shutdown().await.expect("shutdown server");
}

#[tokio::test]
async fn pipelined_independent_read_between_write_and_conflicting_read() {
    let fixture = ServerFixture::new(EngineProfile::MEMORY)
        .await
        .expect("server fixture");
    fixture
        .create_export("disk-a", 4096, 4096)
        .await
        .expect("create export");

    let server = fixture.start_server().await.expect("start server");
    let mut client = RawNbdConnection::connect(server.addr(), "disk-a")
        .await
        .expect("connect raw client");

    let first_read = NbdCookie::new(410);
    let write = NbdCookie::new(411);
    let independent_read = NbdCookie::new(412);
    let conflicting_read = NbdCookie::new(413);

    client
        .send_read(first_read, 0, 4)
        .await
        .expect("send first read");
    client
        .send_write(write, 0, b"bbbb")
        .await
        .expect("send write");
    client
        .send_read(independent_read, 4, 4)
        .await
        .expect("send independent read");
    client
        .send_read(conflicting_read, 0, 4)
        .await
        .expect("send conflicting read");

    let replies = collect_replies(
        &mut client,
        &[
            (first_read, 4),
            (independent_read, 4),
            (conflicting_read, 4),
        ],
        4,
    )
    .await;
    assert_read_data(&replies, first_read, &[0; 4]);
    assert_simple_success(&replies, write);
    assert_read_data(&replies, independent_read, &[0; 4]);
    assert_read_data(&replies, conflicting_read, b"bbbb");

    client
        .disconnect(NbdCookie::new(414))
        .await
        .expect("disconnect raw client");
    server.shutdown().await.expect("shutdown server");
}

#[tokio::test]
async fn pipelined_overlapping_writes_are_visible_to_later_read() {
    let fixture = ServerFixture::new(EngineProfile::MEMORY)
        .await
        .expect("server fixture");
    fixture
        .create_export("disk-a", 4096, 4096)
        .await
        .expect("create export");

    let server = fixture.start_server().await.expect("start server");
    let mut client = RawNbdConnection::connect(server.addr(), "disk-a")
        .await
        .expect("connect raw client");

    let first_write = NbdCookie::new(420);
    let second_write = NbdCookie::new(421);
    let read = NbdCookie::new(422);

    client
        .send_write(first_write, 0, b"cccc")
        .await
        .expect("send first write");
    client
        .send_write(second_write, 0, b"dddd")
        .await
        .expect("send second write");
    client.send_read(read, 0, 4).await.expect("send read");

    let replies = collect_replies(&mut client, &[(read, 4)], 3).await;
    assert_simple_success(&replies, first_write);
    assert_simple_success(&replies, second_write);
    assert_read_data(&replies, read, b"dddd");

    client
        .disconnect(NbdCookie::new(423))
        .await
        .expect("disconnect raw client");
    server.shutdown().await.expect("shutdown server");
}

#[tokio::test]
async fn pipelined_flush_preserves_prior_write_visibility() {
    let fixture = ServerFixture::new(EngineProfile::MEMORY)
        .await
        .expect("server fixture");
    fixture
        .create_export("disk-a", 4096, 4096)
        .await
        .expect("create export");

    let server = fixture.start_server().await.expect("start server");
    let mut client = RawNbdConnection::connect(server.addr(), "disk-a")
        .await
        .expect("connect raw client");

    let write = NbdCookie::new(430);
    let flush = NbdCookie::new(431);
    let read = NbdCookie::new(432);

    client
        .send_write(write, 0, b"eeee")
        .await
        .expect("send write");
    client.send_flush(flush).await.expect("send flush");
    client.send_read(read, 0, 4).await.expect("send read");

    let replies = collect_replies(&mut client, &[(read, 4)], 3).await;
    assert_simple_success(&replies, write);
    assert_simple_success(&replies, flush);
    assert_read_data(&replies, read, b"eeee");

    client
        .disconnect(NbdCookie::new(433))
        .await
        .expect("disconnect raw client");
    server.shutdown().await.expect("shutdown server");
}

fn assert_unknown_export(result: nbd_us_client::Result<NbdClient>) {
    assert!(matches!(
        result,
        Err(ClientError::OptionError {
            reply_type: NBD_REP_ERR_UNKNOWN,
            ..
        }),
    ));
}

fn assert_policy_error(result: nbd_us_client::Result<NbdClient>) {
    assert!(matches!(
        result,
        Err(ClientError::OptionError {
            reply_type: NBD_REP_ERR_POLICY,
            ..
        }),
    ));
}

async fn collect_replies(
    client: &mut RawNbdConnection,
    read_lengths: &[(NbdCookie, u32)],
    count: usize,
) -> HashMap<NbdCookie, RawNbdReply> {
    let read_lengths = read_lengths.iter().copied().collect::<HashMap<_, _>>();
    let mut replies = HashMap::new();
    for _ in 0..count {
        let reply = client
            .read_reply(&read_lengths)
            .await
            .expect("read pipelined reply");
        let cookie = reply.cookie();
        assert!(
            replies.insert(cookie, reply).is_none(),
            "duplicate reply for cookie {}",
            cookie.raw()
        );
    }
    replies
}

fn assert_simple_success(replies: &HashMap<NbdCookie, RawNbdReply>, cookie: NbdCookie) {
    let reply = replies
        .get(&cookie)
        .unwrap_or_else(|| panic!("missing reply for cookie {}", cookie.raw()));
    assert_eq!(reply.error(), 0);
    assert!(
        reply.read_data().is_none(),
        "expected simple reply for cookie {}",
        cookie.raw()
    );
}

fn assert_read_data(replies: &HashMap<NbdCookie, RawNbdReply>, cookie: NbdCookie, expected: &[u8]) {
    let reply = replies
        .get(&cookie)
        .unwrap_or_else(|| panic!("missing reply for cookie {}", cookie.raw()));
    assert_eq!(reply.error(), 0);
    assert_eq!(
        reply.read_data(),
        Some(expected),
        "read payload mismatch for cookie {}",
        cookie.raw()
    );
}

fn assert_option_ack(reply: OptionReply, expected_option: NbdOptionCode) {
    match reply {
        OptionReply::Ack { option } => {
            assert_eq!(option, expected_option);
        }
        reply => panic!("expected NBD_REP_ACK, got {reply:?}"),
    }
}

fn assert_option_error(
    reply: OptionReply,
    expected_option: NbdOptionCode,
    expected_reply_type: u32,
) {
    match reply {
        OptionReply::Error {
            option, reply_type, ..
        } => {
            assert_eq!(option, expected_option);
            assert_eq!(reply_type, expected_reply_type);
        }
        reply => panic!("expected option error, got {reply:?}"),
    }
}

async fn reconnect_after_disconnect(addr: std::net::SocketAddr) -> NbdClient {
    for _ in 0..10 {
        match NbdClient::connect(addr, "disk-a").await {
            Ok(client) => return client,
            Err(ClientError::OptionError {
                reply_type: NBD_REP_ERR_POLICY,
                ..
            }) => tokio::task::yield_now().await,
            Err(error) => panic!("unexpected reconnect error: {error}"),
        }
    }

    panic!("export remained busy after disconnect");
}
