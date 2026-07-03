use std::sync::Arc;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use haruki_sekai_asset_updater::core::config::AppConfig;
use haruki_sekai_asset_updater::service::http::{build_router, AppState};
use haruki_sekai_asset_updater::service::logging::init_logging;
use haruki_sekai_asset_updater::service::poller::Poller;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Arc::new(AppConfig::load_default().await?);
    let _logging_guards = init_logging(&config)?;
    info!(
        "========================= Haruki Sekai Asset Updater v{} =========================",
        env!("CARGO_PKG_VERSION")
    );
    info!("Powered by Haruki Dev Team");

    let bind_addr = format!("{}:{}", config.server.host, config.server.port);
    info!(
        bind_addr = %bind_addr,
        config_version = config.config_version,
        enabled_regions = ?config.enabled_regions(),
        poller_enabled = config.poller.enabled,
        hip_enabled = config.hip.enabled,
        hip_endpoint = %config.hip.endpoint,
        "starting haruki-sekai-asset-updater"
    );

    let cancel = CancellationToken::new();
    let poller = Poller::new(config.clone()).await?;
    let poller_handle = poller.handle();

    // Spawn poller in the background if enabled.
    let poller_task = if config.poller.enabled {
        let cancel_child = cancel.clone();
        Some(tokio::spawn(async move { poller.run(cancel_child).await }))
    } else {
        info!("poller.enabled=false; only mini HTTP is running");
        None
    };

    let state = AppState::new(config.clone(), poller_handle);
    let router = build_router(state);
    let listener = tokio::net::TcpListener::bind(&bind_addr).await?;
    let local_addr = listener
        .local_addr()
        .map(|addr| addr.to_string())
        .unwrap_or_else(|_| bind_addr.clone());
    info!(addr = %local_addr, "listening at http://{local_addr}");

    let shutdown = shutdown_signal(cancel.clone());
    axum::serve(listener, router.into_make_service())
        .with_graceful_shutdown(shutdown)
        .await?;

    cancel.cancel();
    if let Some(task) = poller_task {
        let _ = task.await;
    }

    info!("haruki-sekai-asset-updater shutdown complete");
    Ok(())
}

async fn shutdown_signal(cancel: CancellationToken) {
    let ctrl_c = async {
        if let Err(err) = tokio::signal::ctrl_c().await {
            warn!(error = %err, "failed to install ctrl-c handler");
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                let _ = signal.recv().await;
            }
            Err(err) => warn!(error = %err, "failed to install terminate handler"),
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    info!("shutdown signal received");
    cancel.cancel();
}
