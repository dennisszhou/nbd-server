use crate::error::{Result, ServerError};
use crate::export::{ConnectionId, ExportRuntimeHandle};
use crate::observability::{self, event, target};
use crate::registry::LocalExportRegistry;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

mod handshake;
mod io;
mod options;
mod replies;
mod shutdown;
mod transmission;

use handshake::write_handshake;
use options::negotiate_options;
use replies::write_replies;
use shutdown::ConnectionShutdown;
pub(crate) use shutdown::ServerConnectionShutdown;
use transmission::{ConnectionReplyDrain, RequestReaderExit, read_requests};

struct ConnectionRuntime {
    connection_id: ConnectionId,
    runtime: ExportRuntimeHandle,
    reply_capacity: usize,
}

impl ConnectionRuntime {
    fn new(
        connection_id: ConnectionId,
        runtime: ExportRuntimeHandle,
        reply_capacity: usize,
    ) -> Self {
        Self {
            connection_id,
            runtime,
            reply_capacity,
        }
    }

    async fn run_with_shutdown(
        self,
        stream: TcpStream,
        shutdown: ConnectionShutdown,
    ) -> Result<()> {
        let (reader, writer) = stream.into_split();
        self.run_io(reader, writer, shutdown).await
    }

    async fn run_io<R, W>(self, reader: R, writer: W, shutdown: ConnectionShutdown) -> Result<()>
    where
        R: AsyncRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin + Send + 'static,
    {
        let (reply_sender, reply_receiver) = mpsc::channel(self.reply_capacity);
        let writer_shutdown = shutdown.clone();
        let reader_task = tokio::spawn(read_requests(
            reader,
            self.connection_id,
            self.runtime,
            reply_sender,
            shutdown,
        ));
        let writer_task = tokio::spawn(write_replies(writer, reply_receiver, writer_shutdown));

        run_connection_tasks(reader_task, writer_task).await
    }
}

pub(crate) async fn serve_with_shutdown(
    mut stream: TcpStream,
    registry: Arc<LocalExportRegistry>,
    reply_capacity: usize,
    connection_id: ConnectionId,
    peer_addr: SocketAddr,
    mut shutdown: ConnectionShutdown,
) -> Result<()> {
    if !write_handshake(&mut stream, &mut shutdown).await? {
        return Ok(());
    }
    tracing::debug!(
        target: target::CONNECTION,
        event = event::CONNECTION_HANDSHAKE_COMPLETED,
        service = observability::SERVICE_NAME,
        server_instance_id = observability::server_instance_id(),
        pid = observability::pid(),
        connection_id = connection_id.raw(),
        peer_addr = %peer_addr,
    );
    let Some(export) = negotiate_options(
        &mut stream,
        registry.clone(),
        connection_id,
        peer_addr,
        &mut shutdown,
    )
    .await?
    else {
        tracing::info!(
            target: target::CONNECTION,
            event = event::CONNECTION_CLOSED,
            service = observability::SERVICE_NAME,
            server_instance_id = observability::server_instance_id(),
            pid = observability::pid(),
            connection_id = connection_id.raw(),
            peer_addr = %peer_addr,
            status = "no_export",
        );
        return Ok(());
    };
    let result = ConnectionRuntime::new(connection_id, export.runtime.clone(), reply_capacity)
        .run_with_shutdown(stream, shutdown)
        .await;
    let close_result = registry.close(&export.name, &export.owner).await;

    match (result, close_result) {
        (Err(error), _) => Err(error),
        (Ok(()), Err(error)) => Err(error),
        (Ok(()), Ok(())) => {
            tracing::info!(
                target: target::CONNECTION,
                event = event::CONNECTION_CLOSED,
                service = observability::SERVICE_NAME,
                server_instance_id = observability::server_instance_id(),
                pid = observability::pid(),
                connection_id = connection_id.raw(),
                peer_addr = %peer_addr,
                export_name = %export.name,
                owner_id = export.owner.id().raw(),
                status = "ok",
            );
            Ok(())
        }
    }
}

async fn run_connection_tasks(
    mut reader_task: JoinHandle<RequestReaderExit>,
    mut writer_task: JoinHandle<Result<()>>,
) -> Result<()> {
    tokio::select! {
        biased;

        reader_result = &mut reader_task => {
            let reader_exit = match reader_result {
                Ok(exit) => exit,
                Err(_) => {
                    writer_task.abort();
                    let _ = writer_task.await;
                    return Err(ServerError::RuntimeClosed {
                        resource: "connection request reader",
                    });
                }
            };

            if reader_exit.reply_drain == ConnectionReplyDrain::DrainQueued {
                match writer_task.await {
                    Ok(Ok(())) => reader_exit.result,
                    Ok(Err(error)) => Err(error),
                    Err(_) => Err(ServerError::RuntimeClosed {
                        resource: "connection reply writer",
                    }),
                }
            } else {
                writer_task.abort();
                let _ = writer_task.await;
                reader_exit.result
            }
        }
        writer_result = &mut writer_task => {
            reader_task.abort();
            let _ = reader_task.await;
            match writer_result {
                Ok(result) => result,
                Err(_) => Err(ServerError::RuntimeClosed {
                    resource: "connection reply writer",
                }),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::{
        replies::{ConnectionReply, ReplyKind, write_connection_reply},
        transmission::{ConnectionReplyDrain, read_requests},
    };
    use crate::{
        AdmittedExportRequest, ExportAdmissionPolicyHandle, ExportEngine, ExportJob,
        ExportJobContext, ExportQueueSlot, ExportReply, ExportRequest, ExportResult, ExportRuntime,
        ExportRuntimeHandle, MemoryAdmissionPolicy, RequestCookie, SerialExportRuntime,
    };
    use nbd_control_plane::{
        ExportEngineKind, ExportHead, ExportId, ExportName, ExportRecord, ExportState, Timestamp,
    };
    use nbd_protocol::constants::NBD_CMD_WRITE;
    use nbd_protocol::transmission::{
        RequestHeader, SIMPLE_REPLY_BYTES, encode_disconnect_request, encode_read_request,
        encode_request_header, parse_simple_reply,
    };
    use nbd_protocol::wire::{NbdCommandFlags, NbdCommandType, NbdCookie};
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream, duplex, split};
    use tokio::sync::{
        mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
        watch,
    };

    #[tokio::test]
    async fn connection_runtime_writes_out_of_order_completions_by_cookie() {
        let (runtime, mut submitted, _reserve_started, _reserve_acquired) = controllable_runtime(2);
        let (mut client, server_task) = spawn_connection(runtime, 2);
        let first_cookie = NbdCookie::new(101);
        let second_cookie = NbdCookie::new(102);

        client
            .write_all(&encode_read_request(first_cookie, 0, 4).expect("first read"))
            .await
            .expect("send first read");
        client
            .write_all(&encode_read_request(second_cookie, 4, 4).expect("second read"))
            .await
            .expect("send second read");

        let first_job = submitted.recv().await.expect("first job");
        let second_job = submitted.recv().await.expect("second job");
        assert_eq!(
            first_job.context().cookie(),
            RequestCookie::new(first_cookie.raw()),
        );
        assert_eq!(first_job.context().request_sequence().raw(), 1);
        assert_eq!(first_job.context().offset(), Some(0));
        assert_eq!(first_job.context().length(), Some(4));
        assert_eq!(
            second_job.context().cookie(),
            RequestCookie::new(second_cookie.raw()),
        );
        assert_eq!(second_job.context().request_sequence().raw(), 2);
        assert_eq!(second_job.context().offset(), Some(4));
        assert_eq!(second_job.context().length(), Some(4));

        complete_job(
            second_job,
            ExportRequest::Read { offset: 4, len: 4 },
            Ok(ExportReply::Read {
                data: b"bbbb".to_vec(),
            }),
        )
        .await;
        assert_eq!(
            read_successful_read(&mut client, 4).await,
            (second_cookie, b"bbbb".to_vec()),
        );

        complete_job(
            first_job,
            ExportRequest::Read { offset: 0, len: 4 },
            Ok(ExportReply::Read {
                data: b"aaaa".to_vec(),
            }),
        )
        .await;
        assert_eq!(
            read_successful_read(&mut client, 4).await,
            (first_cookie, b"aaaa".to_vec()),
        );

        disconnect_and_join(client, server_task).await;
    }

    #[tokio::test]
    async fn connection_runtime_backpressures_before_write_payload() {
        let (runtime, mut submitted, mut reserve_started, mut reserve_acquired) =
            controllable_runtime(1);
        let (mut client, server_task) = spawn_connection(runtime, 1);
        let first_cookie = NbdCookie::new(201);
        let write_cookie = NbdCookie::new(202);

        client
            .write_all(&encode_read_request(first_cookie, 0, 4).expect("first read"))
            .await
            .expect("send first read");
        expect_event(&mut reserve_started).await;
        expect_event(&mut reserve_acquired).await;
        let first_job = submitted.recv().await.expect("first job");

        client
            .write_all(&encode_request_header(RequestHeader {
                flags: NbdCommandFlags::new(0),
                command: NbdCommandType::new(NBD_CMD_WRITE),
                cookie: write_cookie,
                offset: 8,
                length: 4,
            }))
            .await
            .expect("send write header");
        expect_event(&mut reserve_started).await;
        assert_no_event(&mut reserve_acquired, "second reserve should wait").await;
        assert!(
            submitted.try_recv().is_err(),
            "write should not submit before queue depth is available",
        );

        complete_job(
            first_job,
            ExportRequest::Read { offset: 0, len: 4 },
            Ok(ExportReply::Read {
                data: b"aaaa".to_vec(),
            }),
        )
        .await;
        assert_eq!(
            read_successful_read(&mut client, 4).await,
            (first_cookie, b"aaaa".to_vec()),
        );

        expect_event(&mut reserve_acquired).await;
        assert!(
            submitted.try_recv().is_err(),
            "write should wait for payload after reserving queue depth",
        );

        client.write_all(b"zzzz").await.expect("send write payload");
        let write_job = submitted.recv().await.expect("write job");
        complete_job(
            write_job,
            ExportRequest::Write {
                offset: 8,
                data: b"zzzz".to_vec(),
            },
            Ok(ExportReply::Done),
        )
        .await;
        assert_success_reply(&mut client, write_cookie).await;

        disconnect_and_join(client, server_task).await;
    }

    #[tokio::test]
    async fn reply_write_holds_queue_slot_until_socket_write_finishes() {
        let meta = export_record("disk-a", 4096);
        let engine = Arc::new(NoopEngine);
        let runtime = SerialExportRuntime::with_capacity(meta, engine, 1);
        let queue_slot = runtime.reserve().await.expect("reserve queue slot");
        let reply = ConnectionReply::export_result(
            ExportJobContext::internal(RequestCookie::new(301), "read"),
            ReplyKind::Read,
            Ok(ExportReply::Read {
                data: vec![7; 1024],
            }),
            queue_slot,
        );
        let (mut writer, mut reader) = duplex(16);

        let write_task =
            tokio::spawn(async move { write_connection_reply(&mut writer, reply).await });
        tokio::task::yield_now().await;
        assert!(
            !write_task.is_finished(),
            "small duplex buffer should block the reply write",
        );

        let waiter_runtime = runtime.clone();
        let waiter =
            tokio::spawn(async move { waiter_runtime.reserve().await.expect("reserve again") });
        tokio::task::yield_now().await;
        assert!(
            !waiter.is_finished(),
            "reply write should hold queue depth until write_all finishes",
        );

        let mut bytes = vec![0; SIMPLE_REPLY_BYTES + 1024];
        reader
            .read_exact(&mut bytes)
            .await
            .expect("drain blocked reply");
        write_task
            .await
            .expect("reply write task")
            .expect("reply write");
        let next_slot = waiter.await.expect("reservation task");
        drop(next_slot);
    }

    #[tokio::test]
    async fn connection_shutdown_stops_blocked_request_reader() {
        let (runtime, _submitted, _reserve_started, _reserve_acquired) = controllable_runtime(1);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (client, server) = duplex(64 * 1024);
        let (reader, _server_writer) = split(server);
        let (reply_sender, _reply_receiver) = mpsc::channel(1);
        let task = tokio::spawn(read_requests(
            reader,
            ConnectionId::next(),
            runtime,
            reply_sender,
            ConnectionShutdown::from_receiver(shutdown_rx),
        ));

        shutdown_tx.send(true).expect("signal shutdown");

        let exit = task.await.expect("request reader task");
        assert_eq!(exit.reply_drain, ConnectionReplyDrain::DropPending);
        exit.result.expect("reader shutdown");
        drop(client);
    }

    async fn complete_job(job: ExportJob, expected: ExportRequest, result: ExportResult) {
        let (_context, request, completion, queue_slot) = job.into_parts();
        assert_eq!(request, expected);
        completion.complete(result, queue_slot).await;
    }

    async fn read_successful_read(client: &mut DuplexStream, len: usize) -> (NbdCookie, Vec<u8>) {
        let reply = read_simple_reply(client).await;
        assert_eq!(reply.error, 0);

        let mut data = vec![0; len];
        client
            .read_exact(&mut data)
            .await
            .expect("read reply payload");
        (reply.cookie, data)
    }

    async fn assert_success_reply(client: &mut DuplexStream, expected_cookie: NbdCookie) {
        let reply = read_simple_reply(client).await;
        assert_eq!(reply.cookie, expected_cookie);
        assert_eq!(reply.error, 0);
    }

    async fn read_simple_reply(client: &mut DuplexStream) -> nbd_protocol::SimpleReply {
        let mut bytes = [0; SIMPLE_REPLY_BYTES];
        client.read_exact(&mut bytes).await.expect("read reply");
        parse_simple_reply(&bytes).expect("simple reply")
    }

    async fn disconnect_and_join(mut client: DuplexStream, server_task: JoinHandle<Result<()>>) {
        client
            .write_all(&encode_disconnect_request(NbdCookie::new(999)).expect("disconnect"))
            .await
            .expect("send disconnect");
        client.shutdown().await.expect("shutdown client");
        server_task
            .await
            .expect("connection task")
            .expect("connection runtime");
    }

    async fn expect_event(receiver: &mut UnboundedReceiver<()>) {
        receiver.recv().await.expect("runtime event");
    }

    async fn assert_no_event(receiver: &mut UnboundedReceiver<()>, message: &str) {
        for _ in 0..4 {
            assert!(receiver.try_recv().is_err(), "{message}");
            tokio::task::yield_now().await;
        }
    }

    fn spawn_connection(
        runtime: ExportRuntimeHandle,
        reply_capacity: usize,
    ) -> (DuplexStream, JoinHandle<Result<()>>) {
        let (client, server) = duplex(64 * 1024);
        let (reader, writer) = split(server);
        let task = tokio::spawn(
            ConnectionRuntime::new(ConnectionId::next(), runtime, reply_capacity).run_io(
                reader,
                writer,
                ConnectionShutdown::not_cancelled(),
            ),
        );
        (client, task)
    }

    fn controllable_runtime(
        capacity: usize,
    ) -> (
        ExportRuntimeHandle,
        mpsc::Receiver<ExportJob>,
        UnboundedReceiver<()>,
        UnboundedReceiver<()>,
    ) {
        let meta = export_record("disk-a", 4096);
        let engine = Arc::new(NoopEngine);
        let reservations = SerialExportRuntime::with_capacity(meta.clone(), engine, capacity);
        let (submitted_sender, submitted_receiver) = mpsc::channel(8);
        let (reserve_started_sender, reserve_started_receiver) = unbounded_channel();
        let (reserve_acquired_sender, reserve_acquired_receiver) = unbounded_channel();

        (
            Arc::new(ControllableRuntime {
                meta,
                reservations,
                submitted: submitted_sender,
                reserve_started: reserve_started_sender,
                reserve_acquired: reserve_acquired_sender,
            }),
            submitted_receiver,
            reserve_started_receiver,
            reserve_acquired_receiver,
        )
    }

    #[derive(Clone)]
    struct ControllableRuntime {
        meta: ExportRecord,
        reservations: SerialExportRuntime,
        submitted: mpsc::Sender<ExportJob>,
        reserve_started: UnboundedSender<()>,
        reserve_acquired: UnboundedSender<()>,
    }

    #[async_trait::async_trait]
    impl crate::ExportRuntime for ControllableRuntime {
        fn export_record(&self) -> ExportRecord {
            self.meta.clone()
        }

        async fn reserve(&self) -> Result<ExportQueueSlot> {
            let _ = self.reserve_started.send(());
            let queue_slot = self.reservations.reserve().await?;
            let _ = self.reserve_acquired.send(());
            Ok(queue_slot)
        }

        async fn submit(&self, job: ExportJob) -> Result<()> {
            self.submitted
                .send(job)
                .await
                .map_err(|_| ServerError::RuntimeClosed {
                    resource: "controllable runtime",
                })
        }

        async fn close(&self) -> Result<()> {
            self.reservations.close().await
        }
    }

    struct NoopEngine;

    #[async_trait::async_trait]
    impl ExportEngine for NoopEngine {
        fn admission_policy(&self) -> ExportAdmissionPolicyHandle {
            Arc::new(MemoryAdmissionPolicy::new(4096))
        }

        async fn execute_admitted(&self, _request: AdmittedExportRequest) -> ExportResult {
            Ok(ExportReply::Done)
        }
    }

    fn export_record(name: &str, size_bytes: u64) -> ExportRecord {
        ExportRecord::new(
            ExportId::new(format!("export-{name}")).expect("export id"),
            ExportName::new(name).expect("export name"),
            4096,
            ExportEngineKind::Memory,
            ExportState::Active,
            ExportHead::memory_empty(size_bytes).expect("memory head"),
            Timestamp::new("created").expect("created timestamp"),
            Timestamp::new("updated").expect("updated timestamp"),
            None,
        )
        .expect("export meta")
    }
}
