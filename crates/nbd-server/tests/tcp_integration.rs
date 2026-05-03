mod support;

use nbd_protocol::constants::{
    NBD_EINVAL, NBD_FLAG_HAS_FLAGS, NBD_FLAG_SEND_FLUSH, NBD_OPT_ABORT, NBD_REP_ERR_POLICY,
    NBD_REP_ERR_UNKNOWN, NBD_REP_ERR_UNSUP,
};
use nbd_protocol::wire::{NbdCookie, NbdOptionCode};
use nbd_protocol::OptionReply;
use nbd_us_client::{ClientError, NbdClient};
use support::nbd::{EngineProfile, RawNbdConnection, RawNbdOptionClient, ServerFixture};

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
