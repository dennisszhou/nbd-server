use std::error::Error;
use std::fmt as std_fmt;
use std::fs::{self, OpenOptions};
use std::io;
use std::path::PathBuf;
use tracing::Dispatch;
use tracing_appender::non_blocking::{NonBlocking, NonBlockingBuilder, WorkerGuard};
use tracing_subscriber::fmt;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::{EnvFilter, Layer};

pub(crate) const DEFAULT_LOG_FILTER: &str =
    "info,nbd_server::request=warn,nbd_server::admission=warn,nbd_server::storage=warn";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LoggingOptions {
    pub(crate) file_path: PathBuf,
    pub(crate) log_stdout: bool,
    pub(crate) env_filter: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LogFormat {
    JsonLines,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LogDestination {
    File { path: PathBuf },
    Stdout,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LogWriterQueuePolicy {
    Lossless,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LoggingPolicy {
    pub(crate) destinations: Vec<LogDestination>,
    pub(crate) format: LogFormat,
    pub(crate) filter: String,
    pub(crate) append: bool,
    pub(crate) writer_queue_policy: LogWriterQueuePolicy,
}

pub(crate) struct LoggingGuard {
    _guards: Vec<WorkerGuard>,
}

#[derive(Debug)]
pub(crate) enum LoggingError {
    CreateLogDir { path: PathBuf, source: io::Error },
    OpenLogFile { path: PathBuf, source: io::Error },
    ParseFilter { filter: String, source: String },
    SetGlobalDefault { source: String },
}

impl LoggingPolicy {
    pub(crate) fn from_options(options: LoggingOptions) -> Self {
        let mut destinations = vec![LogDestination::File {
            path: options.file_path,
        }];
        if options.log_stdout {
            destinations.push(LogDestination::Stdout);
        }

        Self {
            destinations,
            format: LogFormat::JsonLines,
            filter: options
                .env_filter
                .unwrap_or_else(|| DEFAULT_LOG_FILTER.to_owned()),
            append: true,
            writer_queue_policy: LogWriterQueuePolicy::Lossless,
        }
    }
}

pub(crate) fn init_logging(policy: LoggingPolicy) -> Result<LoggingGuard, LoggingError> {
    let (dispatch, guard) = build_dispatch(policy)?;
    tracing::dispatcher::set_global_default(dispatch).map_err(|source| {
        LoggingError::SetGlobalDefault {
            source: source.to_string(),
        }
    })?;
    Ok(guard)
}

fn build_dispatch(policy: LoggingPolicy) -> Result<(Dispatch, LoggingGuard), LoggingError> {
    let filter =
        EnvFilter::try_new(policy.filter.clone()).map_err(|source| LoggingError::ParseFilter {
            filter: policy.filter.clone(),
            source: source.to_string(),
        })?;
    let mut guards = Vec::new();
    let mut file_writer = None;
    let mut stdout_writer = None;

    for destination in &policy.destinations {
        match destination {
            LogDestination::File { path } => {
                let (writer, guard) = file_log_writer(path, policy.append)?;
                file_writer = Some(writer);
                guards.push(guard);
            }
            LogDestination::Stdout => {
                let (writer, guard) = non_blocking_writer(std::io::stdout());
                stdout_writer = Some(writer);
                guards.push(guard);
            }
        }
    }

    let file_writer = file_writer.expect("logging policy always includes a file destination");
    let file_layer = json_layer(file_writer);

    let registry = tracing_subscriber::registry().with(filter).with(file_layer);
    let dispatch = if let Some(stdout_writer) = stdout_writer {
        Dispatch::new(registry.with(json_layer(stdout_writer)))
    } else {
        Dispatch::new(registry)
    };

    Ok((dispatch, LoggingGuard { _guards: guards }))
}

fn file_log_writer(
    path: &PathBuf,
    append: bool,
) -> Result<(NonBlocking, WorkerGuard), LoggingError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| LoggingError::CreateLogDir {
            path: parent.to_path_buf(),
            source,
        })?;
    }

    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .append(append)
        .truncate(!append)
        .open(path)
        .map_err(|source| LoggingError::OpenLogFile {
            path: path.clone(),
            source,
        })?;

    Ok(non_blocking_writer(file))
}

fn non_blocking_writer<W>(writer: W) -> (NonBlocking, WorkerGuard)
where
    W: io::Write + Send + 'static,
{
    NonBlockingBuilder::default().lossy(false).finish(writer)
}

fn json_layer<S>(writer: NonBlocking) -> impl Layer<S> + Send + Sync + 'static
where
    S: tracing::Subscriber,
    for<'a> S: tracing_subscriber::registry::LookupSpan<'a>,
{
    fmt::layer()
        .json()
        .with_ansi(false)
        .with_current_span(true)
        .with_span_list(true)
        .with_writer(writer)
}

impl std_fmt::Display for LoggingError {
    fn fmt(&self, f: &mut std_fmt::Formatter<'_>) -> std_fmt::Result {
        match self {
            Self::CreateLogDir { path, source } => {
                write!(
                    f,
                    "failed to create log directory {}: {source}",
                    path.display()
                )
            }
            Self::OpenLogFile { path, source } => {
                write!(f, "failed to open log file {}: {source}", path.display())
            }
            Self::ParseFilter { filter, source } => {
                write!(f, "failed to parse log filter {filter:?}: {source}")
            }
            Self::SetGlobalDefault { source } => {
                write!(f, "failed to initialize process logging: {source}")
            }
        }
    }
}

impl Error for LoggingError {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(1);

    #[test]
    fn policy_defaults_to_file_json_lossless_append() {
        let temp = TempRoot::new();
        let log_path = temp.path().join("current.log");

        let policy = LoggingPolicy::from_options(LoggingOptions {
            file_path: log_path.clone(),
            log_stdout: false,
            env_filter: None,
        });

        assert_eq!(
            policy.destinations,
            vec![LogDestination::File { path: log_path }]
        );
        assert_eq!(policy.format, LogFormat::JsonLines);
        assert_eq!(policy.filter, DEFAULT_LOG_FILTER);
        assert!(policy.append);
        assert_eq!(policy.writer_queue_policy, LogWriterQueuePolicy::Lossless);
    }

    #[test]
    fn policy_adds_stdout_destination_when_requested() {
        let temp = TempRoot::new();
        let log_path = temp.path().join("current.log");

        let policy = LoggingPolicy::from_options(LoggingOptions {
            file_path: log_path.clone(),
            log_stdout: true,
            env_filter: Some("debug".to_owned()),
        });

        assert_eq!(
            policy.destinations,
            vec![
                LogDestination::File { path: log_path },
                LogDestination::Stdout,
            ]
        );
        assert_eq!(policy.filter, "debug");
    }

    #[test]
    fn file_logging_creates_parent_and_writes_json_lines() {
        let temp = TempRoot::new();
        let log_path = temp.path().join("nested").join("current.log");
        let policy = LoggingPolicy::from_options(LoggingOptions {
            file_path: log_path.clone(),
            log_stdout: false,
            env_filter: Some("info".to_owned()),
        });
        let (dispatch, guard) = build_dispatch(policy).expect("build dispatch");

        tracing::dispatcher::with_default(&dispatch, || {
            tracing::info!(target: "nbd_server::ops", event = "logging.test", answer = 42);
        });
        drop(guard);

        let contents = fs::read_to_string(log_path).expect("read log file");
        assert!(contents.contains("\"event\":\"logging.test\""));
        assert!(contents.contains("\"answer\":42"));
        assert!(contents.ends_with('\n'));
    }

    struct TempRoot {
        path: PathBuf,
    }

    impl TempRoot {
        fn new() -> Self {
            let path = temp_path();
            Self { path }
        }

        fn path(&self) -> &std::path::Path {
            &self.path
        }
    }

    impl Drop for TempRoot {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    fn temp_path() -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let counter = NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "nbd-server-logging-test-{}-{unique}-{counter}",
            std::process::id()
        ));
        fs::create_dir_all(&path).unwrap();
        path
    }
}
