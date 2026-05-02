use crate::{connection, Result, ServerError};
use nbd_config::NbdConfig;
use nbd_control_plane::{CatalogUrl, SQLiteExportCatalog};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

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
        let catalog = SQLiteExportCatalog::connect(&catalog_url)
            .await
            .map_err(ServerError::catalog)?;
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
                        let Ok((stream, _peer)) = accepted else {
                            break;
                        };
                        let catalog = catalog.clone();
                        tokio::spawn(async move {
                            let _ = connection::serve(stream, catalog).await;
                        });
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
        Ok(())
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
