//! Centralized logging setup for runtime and test observability.

use std::fs;
use std::path::Path;

use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

/// Initialize tracing with console + file output under project logs directory.
pub fn init_centralized_logging(
    logs_dir: impl AsRef<Path>,
    file_name: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let logs_dir = logs_dir.as_ref();
    fs::create_dir_all(logs_dir)?;
    let log_path = logs_dir.join(file_name);
    let _ = std::fs::File::create(&log_path)?;

    let file_appender = tracing_appender::rolling::never(logs_dir, file_name);

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| {
                    "hft_lead_lag=info,hft_lead_lag::infrastructure::exchanges=warn,tokio_tungstenite=warn,hyper=warn,reqwest=warn".into()
                }),
        )
        .with(
            tracing_subscriber::fmt::layer()
                .compact()
                .with_target(false),
        )
        .with(
            tracing_subscriber::fmt::layer()
                .compact()
                .with_ansi(false)
                .with_target(false)
                .with_writer(file_appender),
        )
        .init();

    Ok(())
}
