use std::future;
use tokio::sync::watch;

#[derive(Clone)]
pub(crate) struct ServerConnectionShutdown {
    tx: watch::Sender<bool>,
}

#[derive(Clone)]
pub(crate) struct ConnectionShutdown {
    rx: Option<watch::Receiver<bool>>,
}

impl ServerConnectionShutdown {
    pub(crate) fn new() -> Self {
        let (tx, _rx) = watch::channel(false);
        Self { tx }
    }

    pub(crate) fn subscribe(&self) -> ConnectionShutdown {
        ConnectionShutdown {
            rx: Some(self.tx.subscribe()),
        }
    }

    pub(crate) fn shutdown(&self) {
        let _ = self.tx.send(true);
    }
}

impl ConnectionShutdown {
    #[cfg(test)]
    pub(crate) fn not_cancelled() -> Self {
        Self { rx: None }
    }

    #[cfg(test)]
    pub(crate) fn from_receiver(rx: watch::Receiver<bool>) -> Self {
        Self { rx: Some(rx) }
    }

    pub(super) async fn cancelled(&mut self) {
        let Some(rx) = &mut self.rx else {
            future::pending::<()>().await;
            return;
        };

        loop {
            if *rx.borrow() {
                return;
            }
            if rx.changed().await.is_err() {
                future::pending::<()>().await;
            }
        }
    }

    pub(super) fn is_cancelled(&self) -> bool {
        self.rx.as_ref().is_some_and(|rx| *rx.borrow())
    }
}
