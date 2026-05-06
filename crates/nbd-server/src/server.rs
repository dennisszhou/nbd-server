use crate::{
    ConnectionId, ExportFactory, LocalExportRegistry, LocalWalProvider, Result, ServerError,
    connection,
    observability::{self, event, target},
};
use nbd_config::NbdConfig;
use nbd_control_plane::{CatalogProvider, CatalogUrl, open_catalog};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tracing::Instrument;

pub struct NbdServer {
    addr: SocketAddr,
    shutdown: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
}

impl NbdServer {
    pub async fn start(config: NbdConfig) -> Result<Self> {
        Self::start_on(config, SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).await
    }

    pub async fn start_on(config: NbdConfig, listen: SocketAddr) -> Result<Self> {
        let catalog_url = CatalogUrl::parse(&config.catalog.url).map_err(ServerError::catalog)?;
        tracing::debug!(
            target: target::CATALOG,
            event = event::CATALOG_CONNECT_STARTED,
            service = observability::SERVICE_NAME,
            server_instance_id = observability::server_instance_id(),
            pid = observability::pid(),
            catalog_provider = catalog_provider_name(catalog_url.provider()),
        );
        let catalog = open_catalog(&catalog_url)
            .await
            .map_err(ServerError::catalog)?;
        tracing::debug!(
            target: target::CATALOG,
            event = event::CATALOG_CONNECT_COMPLETED,
            service = observability::SERVICE_NAME,
            server_instance_id = observability::server_instance_id(),
            pid = observability::pid(),
            catalog_provider = catalog_provider_name(catalog_url.provider()),
        );
        let export_catalog = catalog.export_catalog();
        let simple_tree_store = catalog.simple_tree_store();
        let cow_tree_store = catalog.cow_tree_store();
        let wal_provider = Arc::new(LocalWalProvider::new(config.runtime.wal_dir.clone()));
        let factory = Arc::new(ExportFactory::new(
            config.server.clone(),
            config.runtime.blob_dir.clone(),
            export_catalog.clone(),
            simple_tree_store,
            cow_tree_store,
            wal_provider,
        ));
        let registry = Arc::new(LocalExportRegistry::new(export_catalog, factory));
        let reply_capacity = config.server.export_queue_depth.get();
        let listener = TcpListener::bind(listen)
            .await
            .map_err(|source| ServerError::io("bind NBD server", source))?;
        let addr = listener
            .local_addr()
            .map_err(|source| ServerError::io("read NBD server address", source))?;
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();

        let task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = &mut shutdown_rx => break,
                    accepted = listener.accept() => {
                        let Ok((stream, peer_addr)) = accepted else {
                            break;
                        };
                        let connection_id = ConnectionId::next();
                        tracing::info!(
                            target: target::CONNECTION,
                            event = event::CONNECTION_ACCEPTED,
                            service = observability::SERVICE_NAME,
                            server_instance_id = observability::server_instance_id(),
                            pid = observability::pid(),
                            connection_id = connection_id.raw(),
                            peer_addr = %peer_addr,
                        );
                        let registry = registry.clone();
                        let span = tracing::debug_span!(
                            target: target::CONNECTION,
                            "connection",
                            service = observability::SERVICE_NAME,
                            server_instance_id = observability::server_instance_id(),
                            pid = observability::pid(),
                            connection_id = connection_id.raw(),
                            peer_addr = %peer_addr,
                        );
                        tokio::spawn(async move {
                            if let Err(error) = connection::serve(
                                stream,
                                registry,
                                reply_capacity,
                                connection_id,
                                peer_addr,
                            )
                            .await
                            {
                                tracing::warn!(
                                    target: target::CONNECTION,
                                    event = event::CONNECTION_ERROR,
                                    service = observability::SERVICE_NAME,
                                    server_instance_id = observability::server_instance_id(),
                                    pid = observability::pid(),
                                    connection_id = connection_id.raw(),
                                    peer_addr = %peer_addr,
                                    error = %error,
                                );
                            }
                        }.instrument(span));
                    }
                }
            }
        });

        Ok(Self {
            addr,
            shutdown: Some(shutdown_tx),
            task: Some(task),
        })
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    pub async fn shutdown(mut self) -> Result<()> {
        tracing::info!(
            target: target::OPS,
            event = event::SERVER_SHUTDOWN_STARTED,
            service = observability::SERVICE_NAME,
            server_instance_id = observability::server_instance_id(),
            pid = observability::pid(),
            listen_addr = %self.addr,
        );
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(task) = self.task.take() {
            task.await.map_err(|source| {
                ServerError::io(
                    "join NBD server task",
                    std::io::Error::other(source.to_string()),
                )
            })?;
        }
        tracing::info!(
            target: target::OPS,
            event = event::SERVER_SHUTDOWN_COMPLETED,
            service = observability::SERVICE_NAME,
            server_instance_id = observability::server_instance_id(),
            pid = observability::pid(),
            listen_addr = %self.addr,
        );
        Ok(())
    }
}

fn catalog_provider_name(provider: CatalogProvider) -> &'static str {
    match provider {
        CatalogProvider::Sqlite => "sqlite",
        CatalogProvider::Postgres => "postgres",
    }
}

impl Drop for NbdServer {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}
