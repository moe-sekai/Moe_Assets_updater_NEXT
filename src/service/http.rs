//! Minimal HTTP sidecar. Only exposes `/healthz` and `/trigger/{region}`.
//!
//! The main workload runs inside `Poller` on its own tasks; this HTTP layer
//! only provides:
//!   * `/healthz` — liveness + per-region last tick / last commit info.
//!   * `/trigger/{region}` — force the poller to run a region on its next
//!     immediate wake-up. Requires bearer token when auth is enabled.

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::middleware::from_fn_with_state;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use serde::Serialize;
use tracing::warn;

use crate::core::config::AppConfig;
use crate::service::logging::access_log_middleware;
use crate::service::poller::PollerHandle;
use crate::service::watermark::RegionWatermark;

#[derive(Clone)]
pub struct AppState {
    config: Arc<AppConfig>,
    poller: PollerHandle,
    started_at: DateTime<Utc>,
}

impl AppState {
    pub fn new(config: Arc<AppConfig>, poller: PollerHandle) -> Self {
        Self {
            config,
            poller,
            started_at: Utc::now(),
        }
    }

    pub fn config(&self) -> &Arc<AppConfig> {
        &self.config
    }
}

pub fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/trigger/{region}", post(trigger))
        .layer(from_fn_with_state(state.clone(), access_log_middleware))
        .with_state(state)
}

#[derive(Debug, Serialize)]
struct HealthResponse {
    status: &'static str,
    service: &'static str,
    version: &'static str,
    started_at: DateTime<Utc>,
    config_version: u32,
    enabled_regions: Vec<String>,
    regions: Vec<RegionHealth>,
}

#[derive(Debug, Serialize)]
struct RegionHealth {
    region: String,
    last_tick_at: Option<DateTime<Utc>>,
    last_success_at: Option<DateTime<Utc>>,
    last_error: Option<String>,
    in_flight: bool,
    watermark: Option<RegionWatermark>,
}

async fn healthz(State(state): State<AppState>) -> Json<HealthResponse> {
    let region_snapshots = state.poller.region_snapshots().await;
    let regions = region_snapshots
        .into_iter()
        .map(|snap| RegionHealth {
            region: snap.region,
            last_tick_at: snap.last_tick_at,
            last_success_at: snap.last_success_at,
            last_error: snap.last_error,
            in_flight: snap.in_flight,
            watermark: snap.watermark,
        })
        .collect();
    Json(HealthResponse {
        status: "ok",
        service: "haruki-sekai-asset-updater",
        version: env!("CARGO_PKG_VERSION"),
        started_at: state.started_at,
        config_version: state.config.config_version,
        enabled_regions: state.config.enabled_regions(),
        regions,
    })
}

#[derive(Debug, Serialize)]
struct TriggerResponse {
    message: String,
    region: String,
}

async fn trigger(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(region): Path<String>,
) -> Result<(StatusCode, Json<TriggerResponse>), ApiError> {
    authorize(&state.config, &headers)?;
    if !state.config.regions.contains_key(&region) {
        return Err(ApiError::NotFound(format!("region `{region}` not defined")));
    }
    if !state
        .config
        .regions
        .get(&region)
        .map(|cfg| cfg.enabled)
        .unwrap_or(false)
    {
        return Err(ApiError::Conflict(format!(
            "region `{region}` is disabled in config"
        )));
    }
    state.poller.trigger(&region).await;
    Ok((
        StatusCode::ACCEPTED,
        Json(TriggerResponse {
            message: "trigger accepted".to_string(),
            region,
        }),
    ))
}

fn authorize(config: &AppConfig, headers: &HeaderMap) -> Result<(), ApiError> {
    let auth = &config.server.auth;
    if !auth.enabled {
        return Ok(());
    }
    if let Some(prefix) = &auth.user_agent_prefix {
        let user_agent = headers
            .get(axum::http::header::USER_AGENT)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default();
        if !user_agent.starts_with(prefix) {
            return Err(ApiError::Unauthorized("invalid user-agent".to_string()));
        }
    }
    if let Some(token) = &auth.bearer_token {
        let authorization = headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default();
        if authorization != format!("Bearer {token}") {
            return Err(ApiError::Unauthorized("invalid bearer token".to_string()));
        }
    }
    Ok(())
}

#[derive(Debug)]
enum ApiError {
    Unauthorized(String),
    NotFound(String),
    Conflict(String),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            Self::Unauthorized(message) => (StatusCode::UNAUTHORIZED, message),
            Self::NotFound(message) => (StatusCode::NOT_FOUND, message),
            Self::Conflict(message) => (StatusCode::CONFLICT, message),
        };
        warn!(status = %status, error = %message, "request failed");
        (status, Json(sonic_rs::json!({ "message": message }))).into_response()
    }
}
