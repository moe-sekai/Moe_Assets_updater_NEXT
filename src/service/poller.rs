//! Region poller: every N seconds pulls current_version.json for each
//! enabled region, applies Layer-0 watermark pruning, then triggers a full
//! asset execution + HIP session on version change.

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use tokio::sync::{broadcast, RwLock, Semaphore};
use tokio::time::{interval, MissedTickBehavior};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::core::asset_execution::{
    fetch_current_version_info, fetch_live_asset_bundle_info, AssetExecutionContext,
    CurrentVersionInfo, DownloadTask, ExecutionProgressUpdate, PreDownloadTaskFilter,
};
use crate::core::bundle_diff;
use crate::core::config::AppConfig;
use crate::core::hip::{CheckAction, CheckBatchItem, CommitStats, HelloParams, HipClient};
use crate::core::models::{AssetUpdateMode, AssetUpdateRequest};
use crate::core::regions::select_region;
use crate::service::watermark::{RegionWatermark, WatermarkStore};

#[derive(Debug, Clone)]
pub struct RegionSnapshot {
    pub region: String,
    pub last_tick_at: Option<DateTime<Utc>>,
    pub last_success_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub in_flight: bool,
    pub watermark: Option<RegionWatermark>,
}

#[derive(Default)]
struct PollerState {
    per_region: HashMap<String, RegionRuntimeState>,
}

#[derive(Default, Clone)]
struct RegionRuntimeState {
    last_tick_at: Option<DateTime<Utc>>,
    last_success_at: Option<DateTime<Utc>>,
    last_error: Option<String>,
    in_flight: bool,
}

#[derive(Clone)]
pub struct PollerHandle {
    trigger_tx: broadcast::Sender<String>,
    state: Arc<RwLock<PollerState>>,
    watermarks: WatermarkStore,
}

impl PollerHandle {
    pub async fn trigger(&self, region: &str) {
        let _ = self.trigger_tx.send(region.to_string());
    }

    pub async fn region_snapshots(&self) -> Vec<RegionSnapshot> {
        let state = self.state.read().await;
        let wm_file = self.watermarks.snapshot().await;
        let mut out = Vec::new();
        for (region, runtime) in &state.per_region {
            out.push(RegionSnapshot {
                region: region.clone(),
                last_tick_at: runtime.last_tick_at,
                last_success_at: runtime.last_success_at,
                last_error: runtime.last_error.clone(),
                in_flight: runtime.in_flight,
                watermark: wm_file.regions.get(region).cloned(),
            });
        }
        out.sort_by(|a, b| a.region.cmp(&b.region));
        out
    }
}

pub struct Poller {
    config: Arc<AppConfig>,
    watermarks: WatermarkStore,
    state: Arc<RwLock<PollerState>>,
    trigger_tx: broadcast::Sender<String>,
}

impl Poller {
    pub async fn new(
        config: Arc<AppConfig>,
    ) -> Result<Self, crate::core::errors::AssetExecutionError> {
        let watermarks = WatermarkStore::open(&config.poller.watermark_file).await?;
        let (trigger_tx, _) = broadcast::channel(64);
        Ok(Self {
            config,
            watermarks,
            state: Arc::new(RwLock::new(PollerState::default())),
            trigger_tx,
        })
    }

    pub fn handle(&self) -> PollerHandle {
        PollerHandle {
            trigger_tx: self.trigger_tx.clone(),
            state: self.state.clone(),
            watermarks: self.watermarks.clone(),
        }
    }

    pub async fn run(self, cancel: CancellationToken) {
        let interval_secs = self.config.poller.interval_seconds.max(5);
        let enabled_regions = self.config.enabled_regions();
        let max_concurrent = self.config.poller.max_concurrent_regions.max(1);
        info!(
            interval_seconds = interval_secs,
            max_concurrent_regions = max_concurrent,
            regions = ?enabled_regions,
            "poller starting"
        );

        // Seed state map so healthz shows every enabled region even before tick 1.
        {
            let mut state = self.state.write().await;
            for region in &enabled_regions {
                state
                    .per_region
                    .entry(region.clone())
                    .or_insert_with(RegionRuntimeState::default);
            }
        }

        // Shared semaphore: caps the number of regions that may run a full
        // execution simultaneously. Layer-0 (watermark) skips do not take a
        // permit — only actual asset processing does.
        let region_permits = Arc::new(Semaphore::new(max_concurrent));

        let mut tasks = Vec::new();
        for region in enabled_regions {
            let ctx = RegionLoopCtx {
                config: self.config.clone(),
                watermarks: self.watermarks.clone(),
                state: self.state.clone(),
                trigger_rx: self.trigger_tx.subscribe(),
                cancel: cancel.clone(),
                region_name: region.clone(),
                interval: Duration::from_secs(interval_secs),
                region_permits: region_permits.clone(),
            };
            tasks.push(tokio::spawn(async move { run_region_loop(ctx).await }));
        }

        // Wait until cancelled.
        cancel.cancelled().await;
        for task in tasks {
            let _ = task.await;
        }
        info!("poller stopped");
    }
}

struct RegionLoopCtx {
    config: Arc<AppConfig>,
    watermarks: WatermarkStore,
    state: Arc<RwLock<PollerState>>,
    trigger_rx: broadcast::Receiver<String>,
    cancel: CancellationToken,
    region_name: String,
    interval: Duration,
    region_permits: Arc<Semaphore>,
}

async fn run_region_loop(mut ctx: RegionLoopCtx) {
    let mut ticker = interval(ctx.interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    // Skip the first immediate tick to avoid a thundering herd on startup.
    ticker.tick().await;

    loop {
        tokio::select! {
            _ = ctx.cancel.cancelled() => {
                debug!(region = %ctx.region_name, "region loop cancelled");
                return;
            }
            _ = ticker.tick() => {
                run_region_once(&ctx).await;
            }
            recv = ctx.trigger_rx.recv() => {
                match recv {
                    Ok(region) if region == ctx.region_name => {
                        run_region_once(&ctx).await;
                    }
                    Ok(_) => {}
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        warn!(region = %ctx.region_name, missed = n, "trigger channel lagged");
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        debug!(region = %ctx.region_name, "trigger channel closed");
                        return;
                    }
                }
            }
        }
    }
}

async fn run_region_once(ctx: &RegionLoopCtx) {
    let start = Utc::now();
    mark_state(ctx, |runtime| {
        runtime.last_tick_at = Some(start);
        runtime.in_flight = true;
        runtime.last_error = None;
    })
    .await;

    let outcome = execute_once(ctx).await;

    let now = Utc::now();
    match outcome {
        Ok(RunOutcome::SkippedByWatermark) => {
            debug!(region = %ctx.region_name, "poll skipped: watermark unchanged");
            mark_state(ctx, |runtime| {
                runtime.in_flight = false;
                runtime.last_success_at = Some(now);
            })
            .await;
        }
        Ok(RunOutcome::Completed) => {
            info!(region = %ctx.region_name, "poll completed successfully");
            mark_state(ctx, |runtime| {
                runtime.in_flight = false;
                runtime.last_success_at = Some(now);
            })
            .await;
        }
        Err(err) => {
            error!(region = %ctx.region_name, error = %err, "poll failed");
            mark_state(ctx, |runtime| {
                runtime.in_flight = false;
                runtime.last_error = Some(err.to_string());
            })
            .await;
        }
    }
}

enum RunOutcome {
    SkippedByWatermark,
    Completed,
}

async fn execute_once(ctx: &RegionLoopCtx) -> Result<RunOutcome, PollError> {
    let region = select_region(&ctx.config, &ctx.region_name)
        .map_err(|err| PollError::Config(err.to_string()))?;

    // Layer 0: fetch current_version.json and compare against watermark.
    let http_client = build_reusable_client(&ctx.config)?;
    let current = fetch_current_version_info(&http_client, region, &ctx.region_name)
        .await
        .map_err(|err| PollError::Execution(err.to_string()))?;

    // Pass the already-resolved asset_version / asset_hash into the request so
    // the executor doesn't need to re-fetch current_version.json (and won't
    // report MissingAssetVersionOrHash if that second fetch is flaky).
    let request = AssetUpdateRequest {
        region: ctx.region_name.clone(),
        asset_version: current.asset_version.clone(),
        asset_hash: current.asset_hash.clone(),
        dry_run: false,
        mode: AssetUpdateMode::Update,
    };

    if let Some(watermark) = ctx.watermarks.get(&ctx.region_name).await {
        if !watermark.asset_version.is_empty()
            && watermark.asset_version == current.asset_version_or_default()
            && watermark.asset_hash == current.asset_hash_or_default()
        {
            remove_region_work_dir_after_success(&ctx.config, &ctx.region_name).await?;
            return Ok(RunOutcome::SkippedByWatermark);
        }
    }

    // Acquire a region-execution permit before doing any heavy work. This is
    // what makes `poller.max_concurrent_regions` actually cap parallelism —
    // Layer-0 watermark skips above never take a permit, so idle regions don't
    // block busy ones. If cancellation fires while waiting, bail cleanly.
    let _permit = tokio::select! {
        permit = ctx.region_permits.clone().acquire_owned() => permit
            .map_err(|err| PollError::Execution(format!("region semaphore closed: {err}")))?,
        _ = ctx.cancel.cancelled() => {
            debug!(region = %ctx.region_name, "poll aborted while waiting for region permit");
            return Ok(RunOutcome::SkippedByWatermark);
        }
    };

    // Layer 1: fetch full AssetBundleInfo, diff against the previous
    // committed snapshot. `changed` = bundles that are either new or whose
    // fingerprint has changed. Only these need to be considered further.
    let new_info = fetch_live_asset_bundle_info(&ctx.config, &ctx.region_name, region, &request)
        .await
        .map_err(|err| PollError::Execution(err.to_string()))?;

    let snapshot_path =
        bundle_diff::snapshot_path(&ctx.config.poller.last_info_dir, &ctx.region_name);
    let old_info = bundle_diff::load_snapshot(&snapshot_path)
        .map_err(|err| PollError::Execution(err.to_string()))?;
    let diff = bundle_diff::diff(old_info.as_ref(), &new_info);
    info!(
        region = %ctx.region_name,
        added = diff.stats.added,
        changed = diff.stats.changed,
        unchanged = diff.stats.unchanged,
        removed = diff.stats.removed,
        "layer1 diff"
    );

    // Layer 2 + execute
    let run_id = uuid::Uuid::new_v4().to_string();
    // A dedicated *check* session lives for the whole region-poll: it never
    // sends UPLOAD_BEGIN / COMMIT, so the server-side state machine stays in
    // stateHandshaked / stateRunning and we can keep reusing it. Upload
    // sessions are opened one-per-batch by the uploader task below (a HIP
    // session can only COMMIT once — after COMMIT it becomes stateFinalized
    // and rejects further CHECK / UPLOAD / COMMIT).
    let check_session = if ctx.config.hip.enabled {
        Some(connect_hip(ctx, &current, &run_id).await?)
    } else {
        None
    };

    let cancel_flag = Arc::new(AtomicBool::new(false));
    // Wire the poller's CancellationToken to the execution-level cancel flag
    // so a SIGTERM propagated by `Poller::run` interrupts an in-flight
    // execute mid-download instead of stalling.
    {
        let cancel_flag = cancel_flag.clone();
        let token = ctx.cancel.clone();
        tokio::spawn(async move {
            token.cancelled().await;
            cancel_flag.store(true, std::sync::atomic::Ordering::Release);
        });
    }
    let executor = AssetExecutionContext::new(&ctx.config, &ctx.region_name, region, &request)
        .map_err(|err| PollError::Execution(err.to_string()))?;

    let changed_names: HashSet<String> =
        diff.changed.iter().map(|c| c.bundle_name.clone()).collect();
    let layer1_skipped_before_check = diff.stats.unchanged;

    let check_skipped_counter = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let check_skipped_bundles: Arc<tokio::sync::Mutex<HashSet<String>>> =
        Arc::new(tokio::sync::Mutex::new(HashSet::new()));
    let filter = CombinedFilter {
        layer1_allow: Arc::new(changed_names),
        layer1_skipped: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        hip: check_session.as_ref().map(|session| HipCheckFilter {
            session: session.clone(),
            region_name: ctx.region_name.clone(),
            check_batch_size: ctx.config.hip.check_batch_size.max(1),
            skipped_counter: check_skipped_counter.clone(),
            skipped_bundles: check_skipped_bundles.clone(),
        }),
    };
    let layer1_counter = filter.layer1_skipped.clone();

    // Progress channel from executor → forwarder. Executor treats it as an
    // unbounded sink (matches the existing API); forwarder converts to the
    // bounded artefact channel so slow uploads eventually throttle downloads.
    let (progress_tx, mut progress_rx) = tokio::sync::mpsc::unbounded_channel();

    // Uploader input channel. Bounded (32 = ~a few dozen bundles of runway)
    // so upload lag can't let the artefact queue grow unbounded in memory —
    // once full, the forwarder task blocks on `send`, and further executor
    // `progress` sends pile up in the unbounded mpsc which is fine because
    // each event is tiny (bundle path + few file paths).
    let uploader_setup = if ctx.config.hip.enabled {
        let (artefact_tx, artefact_rx) = tokio::sync::mpsc::channel::<BundleArtefacts>(32);
        let ctx_config = ctx.config.clone();
        let region_name = ctx.region_name.clone();
        let current_for_upload = current.clone();
        let run_id_for_upload = run_id.clone();
        let hip_config = ctx.config.hip.clone();
        let snapshot_path_for_upload = snapshot_path.clone();
        let new_info_for_upload = new_info.clone();
        let layer1_skipped_at_filter_for_upload = layer1_counter.clone();
        let check_skipped_counter_for_upload = check_skipped_counter.clone();
        let cancel_for_upload = ctx.cancel.clone();
        let handle = tokio::spawn(async move {
            run_uploader(
                ctx_config,
                region_name,
                current_for_upload,
                run_id_for_upload,
                hip_config,
                snapshot_path_for_upload,
                new_info_for_upload,
                artefact_rx,
                layer1_skipped_at_filter_for_upload,
                check_skipped_counter_for_upload,
                cancel_for_upload,
            )
            .await
        });
        Some((artefact_tx, handle))
    } else {
        None
    };

    // Forwarder: bridges executor's unbounded progress channel → uploader's
    // bounded artefact channel. Runs concurrently with the executor.
    let artefact_tx_for_forwarder = uploader_setup.as_ref().map(|(tx, _)| tx.clone());
    let region_name_for_progress = ctx.region_name.clone();
    let layer1_skipped_for_progress = layer1_skipped_before_check;
    let check_skipped_for_progress = check_skipped_counter.clone();
    let forwarder = tokio::spawn(async move {
        let mut reporter = ProgressReporter::new(
            region_name_for_progress,
            layer1_skipped_for_progress,
            check_skipped_for_progress,
        );
        while let Some(event) = progress_rx.recv().await {
            reporter.observe(&event);
            if let ExecutionProgressUpdate::BundleArtefactsProduced {
                bundle,
                bundle_hash,
                export_root,
                files,
            } = event
            {
                let art = BundleArtefacts {
                    bundle_path: bundle,
                    bundle_hash,
                    export_root,
                    files,
                };
                if let Some(tx) = &artefact_tx_for_forwarder {
                    // If the uploader has died / dropped its rx, we silently
                    // drop the event — those bundles won't be reflected in
                    // the layer1 snapshot, so next tick they'll be seen as
                    // changed and retried. That's the safe fallback.
                    if tx.send(art).await.is_err() {
                        // uploader gone; keep draining events so the executor
                        // doesn't stall on its unbounded sender.
                        continue;
                    }
                }
            }
        }
        reporter.finish();
    });

    let execution_result = executor
        .execute_with_filter(
            &ctx.config,
            Some(progress_tx),
            Some(cancel_flag.clone()),
            Some(filter),
        )
        .await
        .map_err(|err| PollError::Execution(err.to_string()));

    // Drain forwarder — its `recv()` returns None once the executor drops
    // its progress_tx, which happened inside execute_with_filter above.
    let _ = forwarder.await;

    // Signal uploader by dropping the last artefact sender (our copy lives
    // inside `uploader_setup`); then await its final result.
    let mut processed_bundles: HashSet<String> = check_skipped_bundles.lock().await.clone();
    let mut uploader_result = Ok(());
    if let Some((artefact_tx, handle)) = uploader_setup {
        drop(artefact_tx);
        uploader_result =
            handle_uploader_result(&ctx.region_name, handle.await, &mut processed_bundles);
    }

    // Close the long-lived check session cleanly.
    if let Some(session) = check_session {
        if let Ok(owned) = Arc::try_unwrap(session) {
            let _ = owned.close().await;
        }
        // If try_unwrap fails there's a leaked filter clone somewhere; the
        // writer task will exit on its own when the TCP connection drops.
    }

    let layer1_skipped_at_filter = layer1_counter.load(std::sync::atomic::Ordering::Relaxed);
    debug!(
        region = %ctx.region_name,
        layer1_snapshot_unchanged = layer1_skipped_before_check,
        layer1_filtered_at_runtime = layer1_skipped_at_filter,
        "layer1 stats"
    );

    if let Err(err) = &execution_result {
        persist_processed_snapshot(
            &ctx.region_name,
            &snapshot_path,
            &new_info,
            old_info.as_ref(),
            &processed_bundles,
        );
        if let Err(upload_err) = uploader_result {
            warn!(
                region = %ctx.region_name,
                error = %upload_err,
                "uploader also failed while preserving partial poll progress",
            );
        }
        return Err(err.clone());
    }

    if let Err(err) = uploader_result {
        persist_processed_snapshot(
            &ctx.region_name,
            &snapshot_path,
            &new_info,
            old_info.as_ref(),
            &processed_bundles,
        );
        return Err(err);
    }

    let summary = execution_result.expect("execution_result is Ok after error branch");

    // Persist Layer 1 snapshot + watermark for next tick's diff.
    //
    // Watermark is only bumped when the whole region-poll finishes, so a
    // mid-poll crash leaves it stale → next tick re-runs the poll. The
    // layer1 snapshot has been incrementally merged by the uploader after
    // each successful commit, so a re-run's diff already skips whatever
    // batches did land, and only the un-committed remainder is re-processed.
    // Build the snapshot for next-tick Layer-1 diff:
    //   * include every bundle that was unchanged (already in old_info with
    //     matching fingerprint — carry forward from new_info),
    //   * include every bundle whose current run marked processed
    //     (Layer 2 SKIP or upload OK),
    //   * exclude everything else (they'll be seen again as `added` /
    //     `changed` next tick and re-attempted).
    persist_processed_snapshot(
        &ctx.region_name,
        &snapshot_path,
        &new_info,
        old_info.as_ref(),
        &processed_bundles,
    );

    remove_region_work_dir_after_success(&ctx.config, &ctx.region_name).await?;

    let wm = RegionWatermark {
        asset_version: current.asset_version_or_default(),
        asset_hash: current.asset_hash_or_default(),
        app_version: current.app_version_or_default(),
        bundle_count: summary.discovered_bundles as u64,
        committed_at: Utc::now(),
    };
    ctx.watermarks
        .set(&ctx.region_name, wm)
        .await
        .map_err(|err| PollError::Execution(err.to_string()))?;

    Ok(RunOutcome::Completed)
}

/// Streaming uploader: consumes `BundleArtefacts` from the executor, uploads
/// them in bounded batches, and issues a HIP COMMIT every
/// `commit_batch_bundles` bundles (or `commit_batch_bytes` bytes) so a
/// crash mid-poll only loses the last un-committed batch.
///
/// Each COMMIT terminates the current session (HIP servers enforce
/// commit-once-per-session), then a fresh session is opened for the next
/// batch. The layer-1 snapshot is merged after every successful commit so
/// the resume-after-crash path is entirely file-system driven.
#[allow(clippy::too_many_arguments)]
async fn run_uploader(
    config: Arc<AppConfig>,
    region_name: String,
    current: CurrentVersionInfo,
    run_id: String,
    hip_config: crate::core::config::HipConfig,
    snapshot_path: std::path::PathBuf,
    new_info: crate::core::asset_execution::AssetBundleInfo,
    mut artefact_rx: tokio::sync::mpsc::Receiver<BundleArtefacts>,
    layer1_skipped_at_filter: Arc<std::sync::atomic::AtomicU64>,
    check_skipped_counter: Arc<std::sync::atomic::AtomicU64>,
    cancel: CancellationToken,
) -> Result<HashSet<String>, PollError> {
    // Copy of the effective thresholds (either > 0 means the trigger is on).
    let batch_bundles_threshold = hip_config.commit_batch_bundles;
    let batch_bytes_threshold = hip_config.commit_batch_bytes;

    let mut batch: Vec<BundleArtefacts> = Vec::new();
    let mut batch_bytes: u64 = 0;
    let mut processed_all: HashSet<String> = HashSet::new();
    let mut batch_index: u32 = 0;
    // Only the very first COMMIT carries the layer1/CHECK skip counters —
    // those are region-wide totals, we don't want to double-count them.
    let mut first_commit_pending = true;

    let mut session_opt: Option<Arc<crate::core::hip::HipSession>> = None;

    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                info!(region = %region_name, "uploader: cancel signalled, flushing residual batch and exiting");
                break;
            }
            maybe = artefact_rx.recv() => {
                match maybe {
                    None => break, // executor + forwarder both done
                    Some(art) => {
                        // Best-effort byte accounting for the batch-bytes
                        // threshold. Missing files (bundle got moved / cleaned)
                        // just don't contribute — the bundle-count trigger
                        // still applies.
                        let bytes_this: u64 = art
                            .files
                            .iter()
                            .filter_map(|p| std::fs::metadata(p).ok().map(|m| m.len()))
                            .sum();
                        batch_bytes += bytes_this;
                        batch.push(art);

                        let should_flush = (batch_bundles_threshold > 0
                            && batch.len() >= batch_bundles_threshold)
                            || (batch_bytes_threshold > 0
                                && batch_bytes >= batch_bytes_threshold);
                        if should_flush {
                            batch_index += 1;
                            flush_batch(
                                &config,
                                &region_name,
                                &current,
                                &run_id,
                                &snapshot_path,
                                &new_info,
                                &mut session_opt,
                                &mut batch,
                                &mut batch_bytes,
                                &mut processed_all,
                                batch_index,
                                &mut first_commit_pending,
                                &layer1_skipped_at_filter,
                                &check_skipped_counter,
                            )
                            .await?;
                        }
                    }
                }
            }
        }
    }

    // Final residual batch (or first-ever batch if we never crossed a threshold).
    if !batch.is_empty() {
        batch_index += 1;
        flush_batch(
            &config,
            &region_name,
            &current,
            &run_id,
            &snapshot_path,
            &new_info,
            &mut session_opt,
            &mut batch,
            &mut batch_bytes,
            &mut processed_all,
            batch_index,
            &mut first_commit_pending,
            &layer1_skipped_at_filter,
            &check_skipped_counter,
        )
        .await?;
    } else if let Some(session_arc) = session_opt.take() {
        // Nothing to commit — just say goodbye to whatever session was open
        // (e.g. a batch had 0 successful uploads and we already closed it,
        // or we never opened one). Best-effort.
        if let Ok(owned) = Arc::try_unwrap(session_arc) {
            let _ = owned.close().await;
        }
    }

    Ok(processed_all)
}

fn handle_uploader_result(
    region_name: &str,
    result: Result<Result<HashSet<String>, PollError>, tokio::task::JoinError>,
    processed_bundles: &mut HashSet<String>,
) -> Result<(), PollError> {
    match result {
        Ok(Ok(uploaded)) => {
            processed_bundles.extend(uploaded);
            Ok(())
        }
        Ok(Err(err)) => {
            warn!(
                region = %region_name,
                error = %err,
                "uploader task failed; committed batches (if any) are still durable on the gateway",
            );
            Err(err)
        }
        Err(join_err) => {
            warn!(
                region = %region_name,
                error = %join_err,
                "uploader task panicked",
            );
            Err(PollError::Execution(format!(
                "uploader task panicked: {join_err}"
            )))
        }
    }
}

/// Upload the current in-memory batch on the currently-held session
/// (opening one if needed), COMMIT it, close the session, and merge the
/// successfully-uploaded bundles into the on-disk layer-1 snapshot so a
/// later crash can resume.
#[allow(clippy::too_many_arguments)]
async fn flush_batch(
    config: &Arc<AppConfig>,
    region_name: &str,
    current: &CurrentVersionInfo,
    run_id: &str,
    snapshot_path: &std::path::Path,
    new_info: &crate::core::asset_execution::AssetBundleInfo,
    session_opt: &mut Option<Arc<crate::core::hip::HipSession>>,
    batch: &mut Vec<BundleArtefacts>,
    batch_bytes: &mut u64,
    processed_all: &mut HashSet<String>,
    batch_index: u32,
    first_commit_pending: &mut bool,
    layer1_skipped_at_filter: &Arc<std::sync::atomic::AtomicU64>,
    check_skipped_counter: &Arc<std::sync::atomic::AtomicU64>,
) -> Result<(), PollError> {
    if batch.is_empty() {
        return Ok(());
    }

    if session_opt.is_none() {
        *session_opt = Some(connect_hip_with(config, region_name, current, run_id).await?);
    }
    let session = session_opt
        .as_ref()
        .expect("session must exist after connect_hip");

    let bundles_in_batch = batch.len();
    info!(
        region = %region_name,
        batch = batch_index,
        bundles = bundles_in_batch,
        files = batch.iter().map(|b| b.files.len()).sum::<usize>(),
        bytes = *batch_bytes,
        "uploading batch to HIP gateway",
    );

    let outcome = upload_bundle_artefacts(region_name, session, batch).await?;
    let mut commit_stats = CommitStats {
        skipped_by_layer1: 0,
        skipped_by_check: 0,
        uploaded_shared: outcome.stats.uploaded_shared,
        uploaded_override: outcome.stats.uploaded_override,
    };
    if *first_commit_pending {
        commit_stats.skipped_by_layer1 =
            layer1_skipped_at_filter.load(std::sync::atomic::Ordering::Relaxed);
        commit_stats.skipped_by_check =
            check_skipped_counter.load(std::sync::atomic::Ordering::Relaxed);
        *first_commit_pending = false;
    }

    let succeeded_bundles = outcome.succeeded_bundles.clone();

    // COMMIT + close. `bundle_count` on COMMIT is a stats field the server
    // records into `versions.stats_json`; we pass the number of bundles that
    // just landed in this batch. Server treats each COMMIT as a new
    // `versions` row scoped to (region, app_version, asset_version,
    // asset_hash) — see hipserver `store.InsertVersion`. Multiple COMMITs
    // for the same version-tuple are additive; each rebuilds the read-path
    // index.
    let session_arc = session_opt.take().expect("session_opt was Some");
    session_arc
        .commit(succeeded_bundles.len() as u64, commit_stats)
        .await
        .map_err(|err| PollError::Hip(err.to_string()))?;
    if let Ok(owned) = Arc::try_unwrap(session_arc) {
        let _ = owned.close().await;
    }

    // Merge the successfully-committed bundles into the on-disk snapshot so
    // a subsequent crash resumes from here.
    let details_this_batch: Vec<(String, crate::core::asset_execution::AssetBundleDetail)> =
        succeeded_bundles
            .iter()
            .filter_map(|name| {
                new_info
                    .bundles
                    .get(name)
                    .map(|d| (name.clone(), d.clone()))
            })
            .collect();
    if !details_this_batch.is_empty() {
        if let Err(err) = bundle_diff::merge_snapshot(
            snapshot_path,
            new_info.version.as_deref(),
            new_info.os.as_deref(),
            &details_this_batch,
        ) {
            // Snapshot flush failure is not fatal — data on the gateway is
            // durable, we just lose crash-recovery precision. Log and go.
            warn!(
                region = %region_name,
                batch = batch_index,
                error = %err,
                "failed to merge layer1 snapshot after successful commit",
            );
        }
    }

    info!(
        region = %region_name,
        batch = batch_index,
        uploaded_bundles = succeeded_bundles.len(),
        total_batch_bundles = bundles_in_batch,
        uploaded_shared = outcome.stats.uploaded_shared,
        uploaded_override = outcome.stats.uploaded_override,
        "batch committed",
    );

    if config
        .regions
        .get(region_name)
        .is_some_and(|region_config| region_config.upload.remove_local_after_upload)
    {
        let cleanup =
            remove_committed_bundle_artefacts(region_name, batch_index, batch, &succeeded_bundles)
                .await;
        info!(
            region = %region_name,
            batch = batch_index,
            removed_files = cleanup.removed_files,
            removed_dirs = cleanup.removed_dirs,
            failed_files = cleanup.failed_files,
            failed_dirs = cleanup.failed_dirs,
            "removed local artefacts after HIP commit",
        );
        if cleanup.has_failures() {
            return Err(PollError::Execution(format!(
                "failed to remove local artefacts after HIP commit: {} file(s), {} dir(s)",
                cleanup.failed_files, cleanup.failed_dirs
            )));
        }
    }

    processed_all.extend(succeeded_bundles);
    batch.clear();
    *batch_bytes = 0;
    Ok(())
}

/// Copy of `connect_hip` that takes plain args instead of the poller's
/// `RegionLoopCtx`, so the uploader task (spawned into 'static) can call it.
async fn connect_hip_with(
    config: &Arc<AppConfig>,
    region_name: &str,
    current: &CurrentVersionInfo,
    run_id: &str,
) -> Result<Arc<crate::core::hip::HipSession>, PollError> {
    let hip_config = &config.hip;
    let mut cfg = crate::core::hip::client::HipClientConfig {
        endpoint: hip_config.endpoint.clone(),
        bearer_token: hip_config.bearer_token.clone().unwrap_or_default(),
        tls_enabled: hip_config.tls.enabled,
        tls_ca_file: hip_config.tls.ca_file.clone(),
        handshake_timeout: Duration::from_millis(hip_config.handshake_timeout_ms),
        request_timeout: Duration::from_millis(hip_config.request_timeout_ms),
        max_frame_bytes: hip_config.max_frame_bytes,
        chunk_size_bytes: hip_config.chunk_size_bytes,
        heartbeat_interval: Duration::from_secs(hip_config.heartbeat_interval_seconds.max(1)),
        unpacker_version: env!("CARGO_PKG_VERSION").to_string(),
    };
    cfg.chunk_size_bytes = cfg.chunk_size_bytes.max(64 * 1024);
    let hello = HelloParams {
        region: region_name.to_string(),
        app_version: current.app_version_or_default(),
        asset_version: current.asset_version_or_default(),
        asset_hash: current.asset_hash_or_default(),
        run_id: run_id.to_string(),
    };
    let session = HipClient::connect(cfg, hello)
        .await
        .map_err(|err| PollError::Hip(err.to_string()))?;
    Ok(Arc::new(session))
}

/// Return an `AssetBundleInfo` suitable for persisting as the next
/// Layer-1 snapshot. Keeps only bundles we can be sure the gateway has for
/// this region, so a failure to process one bundle is retried next tick
/// instead of being silently dropped.
fn build_next_snapshot(
    new_info: &crate::core::asset_execution::AssetBundleInfo,
    old_info: Option<&crate::core::asset_execution::AssetBundleInfo>,
    processed_this_run: &HashSet<String>,
) -> crate::core::asset_execution::AssetBundleInfo {
    let mut carry = crate::core::asset_execution::AssetBundleInfo {
        version: new_info.version.clone(),
        os: new_info.os.clone(),
        bundles: Default::default(),
    };
    for (name, detail) in &new_info.bundles {
        // Case A: processed this run — include the fresh detail.
        if processed_this_run.contains(name) {
            carry.bundles.insert(name.clone(), detail.clone());
            continue;
        }
        // Case B: unchanged relative to previous snapshot — carry forward.
        if let Some(old) = old_info {
            if let Some(prev) = old.bundles.get(name) {
                if prev.crc == detail.crc {
                    carry.bundles.insert(name.clone(), detail.clone());
                }
            }
        }
        // Case C: everything else (new or changed but not processed) is omitted.
    }
    carry
}

fn persist_processed_snapshot(
    region_name: &str,
    snapshot_path: &Path,
    new_info: &crate::core::asset_execution::AssetBundleInfo,
    old_info: Option<&crate::core::asset_execution::AssetBundleInfo>,
    processed_bundles: &HashSet<String>,
) {
    let snapshot_to_save = build_next_snapshot(new_info, old_info, processed_bundles);
    let dropped = new_info
        .bundles
        .len()
        .saturating_sub(snapshot_to_save.bundles.len());
    if dropped > 0 {
        info!(
            region = %region_name,
            dropped_from_snapshot = dropped,
            "layer1 snapshot omits bundles not processed this run"
        );
    }
    if let Err(err) = bundle_diff::save_snapshot(snapshot_path, &snapshot_to_save) {
        warn!(
            region = %region_name,
            error = %err,
            "failed to save layer1 snapshot; next tick will still work but layer1 diff will be less effective"
        );
    }
}

fn build_reusable_client(config: &AppConfig) -> Result<reqwest::Client, PollError> {
    let mut builder = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(30));
    if let Some(proxy) = config
        .server
        .proxy
        .as_deref()
        .filter(|value| !value.is_empty())
    {
        builder = builder.proxy(
            reqwest::Proxy::all(proxy)
                .map_err(|err| PollError::Config(format!("invalid proxy `{proxy}`: {err}")))?,
        );
    }
    builder
        .build()
        .map_err(|err| PollError::Config(err.to_string()))
}

async fn connect_hip(
    ctx: &RegionLoopCtx,
    current: &CurrentVersionInfo,
    run_id: &str,
) -> Result<Arc<crate::core::hip::HipSession>, PollError> {
    let hip_config = &ctx.config.hip;
    let mut cfg = crate::core::hip::client::HipClientConfig {
        endpoint: hip_config.endpoint.clone(),
        bearer_token: hip_config.bearer_token.clone().unwrap_or_default(),
        tls_enabled: hip_config.tls.enabled,
        tls_ca_file: hip_config.tls.ca_file.clone(),
        handshake_timeout: Duration::from_millis(hip_config.handshake_timeout_ms),
        request_timeout: Duration::from_millis(hip_config.request_timeout_ms),
        max_frame_bytes: hip_config.max_frame_bytes,
        chunk_size_bytes: hip_config.chunk_size_bytes,
        heartbeat_interval: Duration::from_secs(hip_config.heartbeat_interval_seconds.max(1)),
        unpacker_version: env!("CARGO_PKG_VERSION").to_string(),
    };
    cfg.chunk_size_bytes = cfg.chunk_size_bytes.max(64 * 1024);
    let hello = HelloParams {
        region: ctx.region_name.clone(),
        app_version: current.app_version_or_default(),
        asset_version: current.asset_version_or_default(),
        asset_hash: current.asset_hash_or_default(),
        run_id: run_id.to_string(),
    };
    let session = HipClient::connect(cfg, hello)
        .await
        .map_err(|err| PollError::Hip(err.to_string()))?;
    Ok(Arc::new(session))
}

struct ProgressReporter {
    region_name: String,
    phase: String,
    planned: usize,
    started: usize,
    downloaded: usize,
    exported: usize,
    artefacts_ready: usize,
    completed: usize,
    failed: usize,
    layer1_skipped: u64,
    hip_skipped: Arc<std::sync::atomic::AtomicU64>,
    last_logged_at: Instant,
    last_logged_percent_bucket: usize,
}

impl ProgressReporter {
    const LOG_INTERVAL: Duration = Duration::from_secs(15);
    const PERCENT_BUCKET: usize = 5;

    fn new(
        region_name: String,
        layer1_skipped: u64,
        hip_skipped: Arc<std::sync::atomic::AtomicU64>,
    ) -> Self {
        Self {
            region_name,
            phase: "starting".to_string(),
            planned: 0,
            started: 0,
            downloaded: 0,
            exported: 0,
            artefacts_ready: 0,
            completed: 0,
            failed: 0,
            layer1_skipped,
            hip_skipped,
            last_logged_at: Instant::now() - Self::LOG_INTERVAL,
            last_logged_percent_bucket: 0,
        }
    }

    fn observe(&mut self, event: &ExecutionProgressUpdate) {
        let mut force = false;
        match event {
            ExecutionProgressUpdate::Phase { phase, .. } => {
                self.phase = format!("{phase:?}");
                force = true;
            }
            ExecutionProgressUpdate::DownloadsPlanned { total } => {
                self.planned = *total;
                force = true;
            }
            ExecutionProgressUpdate::BundleStarted { .. } => self.started += 1,
            ExecutionProgressUpdate::BundleDownloaded { .. } => self.downloaded += 1,
            ExecutionProgressUpdate::BundleExported { .. } => self.exported += 1,
            ExecutionProgressUpdate::BundleArtefactsProduced { .. } => self.artefacts_ready += 1,
            ExecutionProgressUpdate::BundleCompleted { .. } => {
                self.completed += 1;
                force = self.is_finished();
            }
            ExecutionProgressUpdate::BundleFailed { .. } => {
                self.failed += 1;
                force = true;
            }
            ExecutionProgressUpdate::RecordSaved { .. }
            | ExecutionProgressUpdate::ChartHashSyncFinished { .. } => force = true,
            ExecutionProgressUpdate::BundleFetchDetails { .. }
            | ExecutionProgressUpdate::BundleDeobfuscated { .. }
            | ExecutionProgressUpdate::BundleTempWritten { .. }
            | ExecutionProgressUpdate::BundleFfiExportPhases { .. }
            | ExecutionProgressUpdate::BundleFfiSkippedObjectReads { .. }
            | ExecutionProgressUpdate::BundleFfiObjectReadPlan { .. }
            | ExecutionProgressUpdate::SchedulerTelemetry { .. } => {}
        }
        self.maybe_log(force);
    }

    fn finish(&mut self) {
        self.maybe_log(true);
    }

    fn is_finished(&self) -> bool {
        self.planned > 0 && self.completed + self.failed >= self.planned
    }

    fn percent_bucket(&self) -> usize {
        if self.planned == 0 {
            return 0;
        }
        let percent = ((self.completed + self.failed) * 100) / self.planned;
        percent / Self::PERCENT_BUCKET
    }

    fn maybe_log(&mut self, force: bool) {
        let bucket = self.percent_bucket();
        let bucket_changed = bucket > self.last_logged_percent_bucket;
        let interval_elapsed = self.last_logged_at.elapsed() >= Self::LOG_INTERVAL;
        if !force && !bucket_changed && !interval_elapsed {
            return;
        }
        self.last_logged_at = Instant::now();
        self.last_logged_percent_bucket = bucket;
        let processed = self.completed + self.failed;
        let percent_x10 = if self.planned == 0 {
            0
        } else {
            processed * 1000 / self.planned
        };
        info!(
            region = %self.region_name,
            phase = %self.phase,
            planned = self.planned,
            started = self.started,
            downloaded = self.downloaded,
            exported = self.exported,
            artefacts_ready = self.artefacts_ready,
            completed = self.completed,
            failed = self.failed,
            percent = %format_args!("{}.{:01}%", percent_x10 / 10, percent_x10 % 10),
            skipped_layer1 = self.layer1_skipped,
            skipped_hip = self
                .hip_skipped
                .load(std::sync::atomic::Ordering::Relaxed),
            "asset execution progress"
        );
    }
}

struct CombinedFilter {
    layer1_allow: Arc<HashSet<String>>,
    layer1_skipped: Arc<std::sync::atomic::AtomicU64>,
    hip: Option<HipCheckFilter>,
}

impl PreDownloadTaskFilter for CombinedFilter {
    fn filter(
        self,
        tasks: Vec<DownloadTask>,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<Vec<DownloadTask>, crate::core::errors::AssetExecutionError>,
                > + Send,
        >,
    > {
        Box::pin(async move {
            // Layer 1: keep only tasks whose bundle_path is in the diff set.
            let mut kept = Vec::with_capacity(tasks.len());
            let mut skipped_by_layer1 = 0u64;
            for task in tasks {
                if self.layer1_allow.contains(&task.bundle_path) {
                    kept.push(task);
                } else {
                    skipped_by_layer1 += 1;
                }
            }
            self.layer1_skipped
                .store(skipped_by_layer1, std::sync::atomic::Ordering::Relaxed);

            // Layer 2: hand off to HIP check_batch if configured.
            if let Some(hip) = self.hip {
                hip.filter(kept).await
            } else {
                Ok(kept)
            }
        })
    }
}

#[derive(Clone)]
struct HipCheckFilter {
    session: Arc<crate::core::hip::HipSession>,
    region_name: String,
    check_batch_size: usize,
    skipped_counter: Arc<std::sync::atomic::AtomicU64>,
    /// Bundle paths that Layer 2 (HIP CHECK) marked as SKIP. Used to
    /// preserve them in the next snapshot (they are effectively "processed"
    /// because the gateway already has them from another region).
    skipped_bundles: Arc<tokio::sync::Mutex<HashSet<String>>>,
}

impl PreDownloadTaskFilter for HipCheckFilter {
    fn filter(
        self,
        tasks: Vec<DownloadTask>,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<Vec<DownloadTask>, crate::core::errors::AssetExecutionError>,
                > + Send,
        >,
    > {
        Box::pin(async move {
            if tasks.is_empty() {
                return Ok(tasks);
            }
            let session = self.session.clone();
            let region_name = self.region_name;
            let mut keep: Vec<DownloadTask> = Vec::with_capacity(tasks.len());
            for chunk in tasks.chunks(self.check_batch_size) {
                let items: Vec<CheckBatchItem> = chunk
                    .iter()
                    .map(|task| CheckBatchItem {
                        path: task.bundle_path.clone(),
                        fingerprint: task.bundle_hash.clone(),
                        size: task.file_size.max(0) as u64,
                        provider: region_name.clone(),
                    })
                    .collect();
                let results = session.check_batch(items).await.map_err(|err| {
                    crate::core::errors::AssetExecutionError::BlockingTask(format!(
                        "hip check_batch failed: {err}"
                    ))
                })?;
                let decisions: HashMap<String, CheckAction> = results
                    .into_iter()
                    .map(|item| (item.path, item.action))
                    .collect();
                let mut skipped_set = self.skipped_bundles.lock().await;
                for task in chunk {
                    match decisions.get(&task.bundle_path) {
                        Some(CheckAction::Skip) => {
                            self.skipped_counter
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            skipped_set.insert(task.bundle_path.clone());
                            debug!(
                                region = %region_name,
                                bundle = %task.bundle_path,
                                "hip check: SKIP"
                            );
                        }
                        _ => {
                            keep.push(task.clone());
                        }
                    }
                }
                drop(skipped_set);
            }
            Ok(keep)
        })
    }
}

async fn mark_state<F>(ctx: &RegionLoopCtx, f: F)
where
    F: FnOnce(&mut RegionRuntimeState),
{
    let mut state = ctx.state.write().await;
    let runtime = state
        .per_region
        .entry(ctx.region_name.clone())
        .or_insert_with(RegionRuntimeState::default);
    f(runtime);
}

#[derive(Default)]
struct CleanupSummary {
    removed_files: usize,
    removed_dirs: usize,
    failed_files: usize,
    failed_dirs: usize,
}

impl CleanupSummary {
    fn has_failures(&self) -> bool {
        self.failed_files > 0 || self.failed_dirs > 0
    }
}

async fn remove_region_work_dir_after_success(
    config: &AppConfig,
    region_name: &str,
) -> Result<(), PollError> {
    if !config.hip.enabled {
        return Ok(());
    }
    let Some(region_config) = config.regions.get(region_name) else {
        return Ok(());
    };
    if !region_config.upload.remove_local_after_upload {
        return Ok(());
    }
    let Some(asset_save_dir) = region_config
        .paths
        .asset_save_dir
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return Ok(());
    };

    let Some(cleanup_dir) = resolve_region_cleanup_dir(region_name, Path::new(asset_save_dir))?
    else {
        return Ok(());
    };
    tokio::fs::remove_dir_all(&cleanup_dir)
        .await
        .map_err(|err| {
            PollError::Execution(format!(
                "remove region local work dir {} after HIP success: {err}",
                cleanup_dir.display()
            ))
        })?;
    info!(
        region = %region_name,
        path = %cleanup_dir.display(),
        "removed region local work dir after HIP success",
    );
    Ok(())
}

fn resolve_region_cleanup_dir(
    region_name: &str,
    configured_path: &Path,
) -> Result<Option<std::path::PathBuf>, PollError> {
    if configured_path.as_os_str().is_empty() {
        return Err(PollError::Config(format!(
            "region `{region_name}` asset_save_dir is empty; refusing cleanup"
        )));
    }

    let cleanup_dir = match std::fs::canonicalize(configured_path) {
        Ok(path) => path,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => {
            return Err(PollError::Execution(format!(
                "resolve region local work dir {}: {err}",
                configured_path.display()
            )));
        }
    };

    let metadata = std::fs::metadata(&cleanup_dir).map_err(|err| {
        PollError::Execution(format!(
            "inspect region local work dir {}: {err}",
            cleanup_dir.display()
        ))
    })?;
    if !metadata.is_dir() {
        return Err(PollError::Config(format!(
            "region `{region_name}` asset_save_dir {} is not a directory; refusing cleanup",
            cleanup_dir.display()
        )));
    }
    if cleanup_dir.parent().is_none() {
        return Err(PollError::Config(format!(
            "region `{region_name}` asset_save_dir {} resolves to a filesystem root; refusing cleanup",
            cleanup_dir.display()
        )));
    }

    let cwd = std::env::current_dir()
        .and_then(std::fs::canonicalize)
        .map_err(|err| PollError::Execution(format!("resolve current directory: {err}")))?;
    if cleanup_dir == cwd || cwd.starts_with(&cleanup_dir) {
        return Err(PollError::Config(format!(
            "region `{region_name}` asset_save_dir {} contains the process cwd; refusing cleanup",
            cleanup_dir.display()
        )));
    }

    let Some(dir_name) = cleanup_dir.file_name().and_then(|name| name.to_str()) else {
        return Err(PollError::Config(format!(
            "region `{region_name}` asset_save_dir {} has no final directory name; refusing cleanup",
            cleanup_dir.display()
        )));
    };
    if !dir_name
        .to_ascii_lowercase()
        .contains(&region_name.to_ascii_lowercase())
    {
        return Err(PollError::Config(format!(
            "region `{region_name}` asset_save_dir {} does not look region-scoped; refusing cleanup",
            cleanup_dir.display()
        )));
    }

    Ok(Some(cleanup_dir))
}

async fn remove_committed_bundle_artefacts(
    region_name: &str,
    batch_index: u32,
    batch: &[BundleArtefacts],
    succeeded_bundles: &HashSet<String>,
) -> CleanupSummary {
    let mut summary = CleanupSummary::default();
    for bundle in batch {
        if !succeeded_bundles.contains(&bundle.bundle_path) {
            continue;
        }
        for file in &bundle.files {
            match tokio::fs::remove_file(file).await {
                Ok(()) => {
                    summary.removed_files += 1;
                    match prune_empty_parent_dirs(file, &bundle.export_root) {
                        Ok(removed) => summary.removed_dirs += removed,
                        Err(err) => {
                            summary.failed_dirs += 1;
                            warn!(
                                region = %region_name,
                                batch = batch_index,
                                bundle = %bundle.bundle_path,
                                path = %file.display(),
                                error = %err,
                                "failed to prune empty local artefact directories after HIP commit",
                            );
                        }
                    }
                }
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => {
                    summary.failed_files += 1;
                    warn!(
                        region = %region_name,
                        batch = batch_index,
                        bundle = %bundle.bundle_path,
                        path = %file.display(),
                        error = %err,
                        "failed to remove local artefact after HIP commit",
                    );
                }
            }
        }
    }
    summary
}

fn prune_empty_parent_dirs(path: &Path, stop_root: &Path) -> std::io::Result<usize> {
    let stop_root = match std::fs::canonicalize(stop_root) {
        Ok(path) => path,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(err) => return Err(err),
    };
    let mut removed = 0usize;
    let mut current = match path.parent() {
        Some(parent) => parent.to_path_buf(),
        None => return Ok(0),
    };

    loop {
        let current_canon = match std::fs::canonicalize(&current) {
            Ok(path) => path,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => break,
            Err(err) => return Err(err),
        };
        if current_canon == stop_root || !current_canon.starts_with(&stop_root) {
            break;
        }
        match std::fs::remove_dir(&current_canon) {
            Ok(()) => removed += 1,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) if err.kind() == std::io::ErrorKind::DirectoryNotEmpty => break,
            Err(err) => return Err(err),
        }
        let Some(parent) = current_canon.parent() else {
            break;
        };
        current = parent.to_path_buf();
    }

    Ok(removed)
}

#[derive(Debug, Clone)]
struct BundleArtefacts {
    bundle_path: String,
    bundle_hash: String,
    export_root: std::path::PathBuf,
    files: Vec<std::path::PathBuf>,
}

struct UploadOutcome {
    stats: CommitStats,
    /// Bundle paths for which all artefacts uploaded successfully.
    succeeded_bundles: HashSet<String>,
}

/// Upload every export product per bundle over HIP.
/// Each upload carries the *bundle*'s fingerprint (crc32) so the gateway can
/// group the artefacts by their source bundle version.
async fn upload_bundle_artefacts(
    region_name: &str,
    session: &Arc<crate::core::hip::HipSession>,
    bundles: &[BundleArtefacts],
) -> Result<UploadOutcome, PollError> {
    let mut stats = CommitStats::default();
    let mut succeeded_bundles = HashSet::new();
    for bundle in bundles {
        let mut bundle_ok = true;
        for file in &bundle.files {
            if !file.exists() || !file.is_file() {
                continue;
            }
            let asset_path =
                hip_asset_path_for_file(file, &bundle.export_root, &bundle.bundle_path);
            let ack = session
                .upload_file(
                    bundle.bundle_path.as_str(),
                    asset_path.as_str(),
                    bundle.bundle_hash.as_str(),
                    file,
                )
                .await
                .map_err(|err| {
                    bundle_ok = false;
                    PollError::Hip(err.to_string())
                })?;
            match ack.placement {
                Some(crate::core::hip::codec::Placement::Shared) => stats.uploaded_shared += 1,
                Some(crate::core::hip::codec::Placement::Override) => stats.uploaded_override += 1,
                None => stats.uploaded_shared += 1,
            }
            debug!(
                region = %region_name,
                bundle = %bundle.bundle_path,
                asset = %asset_path,
                "hip upload OK"
            );
        }
        if bundle_ok {
            succeeded_bundles.insert(bundle.bundle_path.clone());
        }
    }
    Ok(UploadOutcome {
        stats,
        succeeded_bundles,
    })
}

fn hip_asset_path_for_file(
    file: &std::path::Path,
    export_root: &std::path::Path,
    bundle_path: &str,
) -> String {
    let relative = match file.strip_prefix(export_root) {
        Ok(rel) => rel.to_string_lossy().replace('\\', "/"),
        Err(_) => file.to_string_lossy().replace('\\', "/"),
    };
    let relative = relative.trim_start_matches('/').to_string();
    let bundle_path = bundle_path.replace('\\', "/");
    let bundle_path = bundle_path.trim_matches('/');
    if bundle_path.is_empty() {
        return relative;
    }
    relative
        .strip_prefix(bundle_path)
        .and_then(|value| value.strip_prefix('/'))
        .unwrap_or(&relative)
        .to_string()
}

#[derive(Debug, Clone, thiserror::Error)]
enum PollError {
    #[error("config error: {0}")]
    Config(String),
    #[error("execution error: {0}")]
    Execution(String),
    #[error("hip error: {0}")]
    Hip(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn prune_empty_parent_dirs_stops_at_export_root() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("root");
        let nested = root.join("a/b/c");
        fs::create_dir_all(&nested).unwrap();
        let file = nested.join("asset.bin");
        fs::write(&file, b"x").unwrap();
        fs::remove_file(&file).unwrap();

        let removed = prune_empty_parent_dirs(&file, &root).unwrap();

        assert_eq!(removed, 3);
        assert!(root.exists());
        assert!(!root.join("a").exists());
    }

    #[test]
    fn prune_empty_parent_dirs_preserves_non_empty_siblings() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("root");
        let nested = root.join("a/b/c");
        fs::create_dir_all(&nested).unwrap();
        fs::write(root.join("a/sibling.bin"), b"keep").unwrap();
        let file = nested.join("asset.bin");
        fs::write(&file, b"x").unwrap();
        fs::remove_file(&file).unwrap();

        let removed = prune_empty_parent_dirs(&file, &root).unwrap();

        assert_eq!(removed, 2);
        assert!(root.join("a").exists());
        assert!(root.join("a/sibling.bin").exists());
    }

    #[test]
    fn prune_empty_parent_dirs_ignores_paths_outside_export_root() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("root");
        let other = temp.path().join("other/a");
        fs::create_dir_all(&root).unwrap();
        fs::create_dir_all(&other).unwrap();
        let file = other.join("asset.bin");
        fs::write(&file, b"x").unwrap();
        fs::remove_file(&file).unwrap();

        let removed = prune_empty_parent_dirs(&file, &root).unwrap();

        assert_eq!(removed, 0);
        assert!(other.exists());
    }

    #[test]
    fn hip_asset_path_strips_bundle_prefix_from_region_export_root() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();

        let sound_file = root.join("sound/scenario/bgm/bgm001/bgm001.mp3");
        assert_eq!(
            hip_asset_path_for_file(&sound_file, root, "sound/scenario/bgm/bgm001"),
            "bgm001.mp3"
        );

        let mesh_file = root.join("mysekai/fixture/mdl_foo/mdl_foo.obj");
        assert_eq!(
            hip_asset_path_for_file(&mesh_file, root, "mysekai/fixture/mdl_foo"),
            "mdl_foo.obj"
        );
    }

    #[test]
    fn hip_asset_path_keeps_paths_already_relative_to_bundle_root() {
        let temp = tempfile::tempdir().unwrap();
        let bundle_root = temp.path().join("mysekai/fixture/mdl_foo");
        let file = bundle_root.join("mdl_foo.obj");

        assert_eq!(
            hip_asset_path_for_file(&file, &bundle_root, "mysekai/fixture/mdl_foo"),
            "mdl_foo.obj"
        );
    }

    #[test]
    fn uploader_error_is_returned_to_poller() {
        let mut processed = HashSet::new();

        let result = handle_uploader_result(
            "tw",
            Ok(Err(PollError::Hip("already present".to_string()))),
            &mut processed,
        );

        assert!(matches!(result, Err(PollError::Hip(message)) if message == "already present"));
        assert!(processed.is_empty());
    }

    #[test]
    fn uploader_success_extends_processed_bundles() {
        let mut processed = HashSet::from(["skipped".to_string()]);
        let uploaded = HashSet::from(["uploaded".to_string()]);

        let result = handle_uploader_result("tw", Ok(Ok(uploaded)), &mut processed);

        assert!(result.is_ok());
        assert!(processed.contains("skipped"));
        assert!(processed.contains("uploaded"));
    }

    #[test]
    fn resolve_region_cleanup_dir_accepts_region_scoped_dir() {
        let temp = tempfile::tempdir().unwrap();
        let dir = temp.path().join("tw-assets");
        fs::create_dir_all(&dir).unwrap();

        let resolved = resolve_region_cleanup_dir("tw", &dir).unwrap().unwrap();

        assert_eq!(resolved, fs::canonicalize(dir).unwrap());
    }

    #[test]
    fn resolve_region_cleanup_dir_rejects_non_region_scoped_dir() {
        let temp = tempfile::tempdir().unwrap();
        let dir = temp.path().join("Data");
        fs::create_dir_all(&dir).unwrap();

        let err = resolve_region_cleanup_dir("tw", &dir).unwrap_err();

        assert!(err.to_string().contains("refusing cleanup"));
    }
}
