use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tokio::io::BufReader;
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::sync::{Mutex as TokioMutex, OwnedSemaphorePermit, Semaphore};
use tracing::{debug, info, warn};

use crate::frame::{read_worker_frame, write_worker_frame};
use crate::types::*;

#[derive(Debug, Serialize, Deserialize)]
pub struct WorkerServerRequest {
    pub id: u64,
    pub request: AssetStudioFfiRequest,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WorkerServerResponse {
    pub id: u64,
    pub status: Option<i32>,
    pub response: Option<AssetStudioFfiResponse>,
    #[serde(default)]
    pub payload_len: usize,
    pub payload_file: Option<String>,
    pub error: Option<String>,
}

#[derive(Debug)]
pub struct WorkerOutput {
    pub status: String,
    pub status_success: bool,
    pub response: AssetStudioFfiResponse,
    pub stderr: String,
    pub payload: Vec<u8>,
    pub payload_file: Option<PathBuf>,
}

/// Extra tuning knobs for spawned FFI worker processes. Each worker is an
/// independent .NET NativeAOT process; by default the .NET GC sizes its
/// generation budgets against the *whole* container's available memory, so
/// running several of these concurrently (per `process_concurrency`) can
/// multiply the effective footprint. These knobs let the caller put a hard
/// ceiling on each worker's heap and make idle workers exit instead of
/// sitting around holding onto committed memory indefinitely.
#[derive(Debug, Clone, Copy, Default)]
pub struct WorkerPoolTuning {
    /// 0 disables idle reaping (workers only recycle via `max_calls_per_worker`).
    pub idle_timeout: Duration,
    /// Per-worker `DOTNET_GCHeapHardLimit`, in megabytes. 0 leaves the .NET
    /// default (which sizes itself off total container memory) untouched.
    pub gc_heap_hard_limit_mb: u64,
    /// `DOTNET_GCConserveMemory` (0-9). Higher values trade GC/allocation
    /// throughput for returning memory to the OS more aggressively. `None`
    /// leaves the .NET default untouched.
    pub gc_conserve_memory: Option<u8>,
}

pub struct AssetStudioWorkerPool {
    worker_path: PathBuf,
    native_library_path: String,
    process_concurrency: usize,
    max_calls_per_worker: usize,
    tuning: WorkerPoolTuning,
    semaphore: Arc<Semaphore>,
    available: TokioMutex<Vec<PooledWorker>>,
    next_id: AtomicU64,
    next_worker_id: AtomicU64,
    stats: Arc<WorkerPoolStats>,
}

#[derive(Debug, Clone, Default)]
pub struct WorkerPoolStatsSnapshot {
    pub spawned: u64,
    pub recycled: u64,
    pub killed: u64,
    pub idle_reaped: u64,
    pub protocol_errors: u64,
    pub completed_calls: u64,
    pub max_call_ms: u64,
}

#[derive(Debug, Clone)]
pub struct WorkerLeaseStats {
    pub worker_id: u64,
    pub worker_completed_calls: u64,
    pub pool: WorkerPoolStatsSnapshot,
}

#[derive(Default)]
struct WorkerPoolStats {
    spawned: AtomicUsize,
    recycled: AtomicUsize,
    killed: AtomicUsize,
    idle_reaped: AtomicUsize,
    protocol_errors: AtomicUsize,
    completed_calls: AtomicUsize,
    max_call_ms: AtomicU64,
}

impl WorkerPoolStats {
    fn record_call(&self, elapsed_ms: u64) {
        self.completed_calls.fetch_add(1, Ordering::Relaxed);
        record_atomic_max(&self.max_call_ms, elapsed_ms);
    }

    fn snapshot(&self) -> WorkerPoolStatsSnapshot {
        WorkerPoolStatsSnapshot {
            spawned: self.spawned.load(Ordering::Relaxed) as u64,
            recycled: self.recycled.load(Ordering::Relaxed) as u64,
            killed: self.killed.load(Ordering::Relaxed) as u64,
            idle_reaped: self.idle_reaped.load(Ordering::Relaxed) as u64,
            protocol_errors: self.protocol_errors.load(Ordering::Relaxed) as u64,
            completed_calls: self.completed_calls.load(Ordering::Relaxed) as u64,
            max_call_ms: self.max_call_ms.load(Ordering::Relaxed),
        }
    }
}

fn record_atomic_max(target: &AtomicU64, value: u64) {
    let mut current = target.load(Ordering::Relaxed);
    while value > current {
        match target.compare_exchange_weak(current, value, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(next) => current = next,
        }
    }
}

impl AssetStudioWorkerPool {
    pub fn shared(
        worker_path: &Path,
        native_library_path: &str,
        process_concurrency: usize,
        max_calls_per_worker: usize,
    ) -> Arc<Self> {
        Self::shared_with_tuning(
            worker_path,
            native_library_path,
            process_concurrency,
            max_calls_per_worker,
            WorkerPoolTuning::default(),
        )
    }

    pub fn shared_with_tuning(
        worker_path: &Path,
        native_library_path: &str,
        process_concurrency: usize,
        max_calls_per_worker: usize,
        tuning: WorkerPoolTuning,
    ) -> Arc<Self> {
        let process_concurrency = process_concurrency.max(1);
        let key = format!(
            "{}\0{}\0{}\0{}\0{}\0{}\0{:?}",
            process_concurrency,
            max_calls_per_worker,
            worker_path.display(),
            native_library_path,
            tuning.idle_timeout.as_millis(),
            tuning.gc_heap_hard_limit_mb,
            tuning.gc_conserve_memory,
        );
        static POOLS: OnceLock<Mutex<HashMap<String, Arc<AssetStudioWorkerPool>>>> =
            OnceLock::new();
        let mut pools = POOLS
            .get_or_init(|| Mutex::new(HashMap::new()))
            .lock()
            .unwrap();
        let (pool, freshly_created) = match pools.get(&key) {
            Some(pool) => (pool.clone(), false),
            None => {
                let pool = Arc::new(AssetStudioWorkerPool {
                    worker_path: worker_path.to_path_buf(),
                    native_library_path: native_library_path.to_string(),
                    process_concurrency,
                    max_calls_per_worker,
                    tuning,
                    semaphore: Arc::new(Semaphore::new(process_concurrency)),
                    available: TokioMutex::new(Vec::with_capacity(process_concurrency)),
                    next_id: AtomicU64::new(1),
                    next_worker_id: AtomicU64::new(1),
                    stats: Arc::new(WorkerPoolStats::default()),
                });
                pools.insert(key, pool.clone());
                (pool, true)
            }
        };
        drop(pools);
        if freshly_created && !tuning.idle_timeout.is_zero() {
            spawn_idle_reaper(pool.clone());
        }
        pool
    }

    pub async fn acquire(self: &Arc<Self>) -> Result<WorkerLease, AssetStudioFfiError> {
        let permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|source| {
                AssetStudioFfiError::message(format!("ffi worker pool limiter closed: {source}"))
            })?;
        let worker = match self.available.lock().await.pop() {
            Some(mut worker) => {
                worker.idle_since = None;
                worker
            }
            None => self.spawn_worker().await?,
        };
        Ok(WorkerLease {
            pool: self.clone(),
            worker: Some(worker),
            _permit: permit,
        })
    }

    pub async fn acquire_exclusive(self: &Arc<Self>) -> Result<WorkerLease, AssetStudioFfiError> {
        let permit = self
            .semaphore
            .clone()
            .acquire_many_owned(self.process_concurrency as u32)
            .await
            .map_err(|source| {
                AssetStudioFfiError::message(format!(
                    "ffi worker pool exclusive limiter closed: {source}"
                ))
            })?;
        let worker = self.spawn_worker().await?;
        Ok(WorkerLease {
            pool: self.clone(),
            worker: Some(worker),
            _permit: permit,
        })
    }

    async fn spawn_worker(&self) -> Result<PooledWorker, AssetStudioFfiError> {
        let worker_program = absolute_command_path(&self.worker_path);
        let mut command = Command::new(&worker_program);
        command
            .arg("--server")
            .arg("--ffi-library")
            .arg(&self.native_library_path)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        if let Some(native_library_dir) = native_library_working_dir(&self.native_library_path) {
            command.current_dir(native_library_dir);
        }
        // Each worker is a standalone .NET NativeAOT process. Left to its
        // defaults, its GC sizes generation budgets against the *entire*
        // container's memory — fine for one process, but with
        // `process_concurrency` workers running side by side those
        // per-process assumptions stack up fast. Apply explicit per-worker
        // caps so N workers don't each behave as if they alone own the box.
        if self.tuning.gc_heap_hard_limit_mb > 0 {
            command.env(
                "DOTNET_GCHeapHardLimit",
                format!(
                    "{:x}",
                    self.tuning
                        .gc_heap_hard_limit_mb
                        .saturating_mul(1024 * 1024)
                ),
            );
        }
        if let Some(conserve) = self.tuning.gc_conserve_memory {
            command.env("DOTNET_GCConserveMemory", conserve.min(9).to_string());
        }
        let mut child = command
            .spawn()
            .map_err(|source| AssetStudioFfiError::Spawn {
                program: worker_program.display().to_string(),
                source,
            })?;
        let pid = child.id();
        let stdin = child.stdin.take().ok_or_else(|| {
            AssetStudioFfiError::message(format!(
                "failed to open stdin for native pooled worker `{}`",
                self.worker_path.display()
            ))
        })?;
        let stdout = child.stdout.take().ok_or_else(|| {
            AssetStudioFfiError::message(format!(
                "failed to open stdout for native pooled worker `{}`",
                self.worker_path.display()
            ))
        })?;

        let worker_id = self.next_worker_id.fetch_add(1, Ordering::Relaxed);
        let spawned = self.stats.spawned.fetch_add(1, Ordering::Relaxed) + 1;
        info!(
            worker_id,
            pid = pid.unwrap_or_default(),
            spawned_workers = spawned,
            process_concurrency = self.process_concurrency,
            worker_max_calls = self.max_calls_per_worker,
            worker_idle_timeout_secs = self.tuning.idle_timeout.as_secs(),
            worker_gc_heap_hard_limit_mb = self.tuning.gc_heap_hard_limit_mb,
            worker_gc_conserve_memory = self
                .tuning
                .gc_conserve_memory
                .map(u64::from)
                .unwrap_or_default(),
            "spawned assetstudio ffi worker"
        );

        Ok(PooledWorker {
            worker_id,
            pid,
            program: self.worker_path.display().to_string(),
            child,
            stdin,
            stdout: BufReader::new(stdout),
            completed_calls: 0,
            idle_since: None,
            stats: self.stats.clone(),
        })
    }

    async fn return_or_recycle_worker(&self, mut worker: PooledWorker) {
        if self.max_calls_per_worker > 0 && worker.completed_calls >= self.max_calls_per_worker {
            let recycled = self.stats.recycled.fetch_add(1, Ordering::Relaxed) + 1;
            info!(
                worker_id = worker.worker_id,
                completed_calls = worker.completed_calls,
                max_calls = self.max_calls_per_worker,
                recycled_workers = recycled,
                "recycling assetstudio ffi worker after configured call limit"
            );
            worker.kill().await;
            return;
        }
        worker.idle_since = Some(Instant::now());
        self.available.lock().await.push(worker);
    }

    /// Kill any pooled worker that has been sitting idle in `available`
    /// longer than `tuning.idle_timeout`. Called periodically by the
    /// background reaper task spawned in `shared_with_tuning`. Workers
    /// currently leased out (mid-call) are untouched since they aren't in
    /// `available`.
    async fn reap_idle_workers(&self) {
        if self.tuning.idle_timeout.is_zero() {
            return;
        }
        let mut expired = Vec::new();
        {
            let mut available = self.available.lock().await;
            let mut index = 0;
            while index < available.len() {
                let is_idle_expired = available[index]
                    .idle_since
                    .is_some_and(|since| since.elapsed() >= self.tuning.idle_timeout);
                if is_idle_expired {
                    expired.push(available.remove(index));
                } else {
                    index += 1;
                }
            }
        }
        if expired.is_empty() {
            return;
        }
        let idle_reaped = self
            .stats
            .idle_reaped
            .fetch_add(expired.len(), Ordering::Relaxed)
            + expired.len();
        info!(
            reaped = expired.len(),
            idle_reaped_total = idle_reaped,
            idle_timeout_secs = self.tuning.idle_timeout.as_secs(),
            "reaping idle assetstudio ffi workers to release memory"
        );
        for mut worker in expired {
            worker.kill().await;
        }
    }
}

/// Periodically sweeps `pool.available` for workers that have exceeded the
/// configured idle timeout and kills them. Runs for the process lifetime;
/// holds only a `Weak` reference so it doesn't itself keep the pool (and its
/// live worker processes) artificially alive.
fn spawn_idle_reaper(pool: Arc<AssetStudioWorkerPool>) {
    let sweep_interval = (pool.tuning.idle_timeout / 4).max(Duration::from_secs(5));
    let weak_pool = Arc::downgrade(&pool);
    drop(pool);
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(sweep_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            let Some(pool) = Weak::upgrade(&weak_pool) else {
                break;
            };
            pool.reap_idle_workers().await;
        }
    });
}

pub struct WorkerLease {
    pool: Arc<AssetStudioWorkerPool>,
    worker: Option<PooledWorker>,
    _permit: OwnedSemaphorePermit,
}

impl WorkerLease {
    pub async fn call(
        &mut self,
        request: &AssetStudioFfiRequest,
    ) -> Result<WorkerOutput, AssetStudioFfiError> {
        let id = self.pool.next_id.fetch_add(1, Ordering::Relaxed);
        let worker = self
            .worker
            .as_mut()
            .ok_or_else(|| AssetStudioFfiError::message("ffi worker lease has no worker"))?;
        worker.call(id, request).await
    }

    pub async fn finish_success(mut self) -> WorkerLeaseStats {
        let worker = self.worker.take().expect("worker lease already consumed");
        let stats = WorkerLeaseStats {
            worker_id: worker.worker_id,
            worker_completed_calls: worker.completed_calls as u64,
            pool: self.pool.stats.snapshot(),
        };
        self.pool.return_or_recycle_worker(worker).await;
        stats
    }

    pub async fn kill(mut self) {
        if let Some(mut worker) = self.worker.take() {
            worker.kill().await;
        }
    }
}

struct PooledWorker {
    worker_id: u64,
    pid: Option<u32>,
    program: String,
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    completed_calls: usize,
    /// Set while sitting in `available`; cleared as soon as it's leased out.
    /// Used by `reap_idle_workers` to find long-idle workers to kill.
    idle_since: Option<Instant>,
    stats: Arc<WorkerPoolStats>,
}

impl PooledWorker {
    async fn call(
        &mut self,
        id: u64,
        request: &AssetStudioFfiRequest,
    ) -> Result<WorkerOutput, AssetStudioFfiError> {
        let started = Instant::now();
        let operation = request.operation();
        let request = WorkerServerRequest {
            id,
            request: request.clone(),
        };
        let request_bytes = sonic_rs::to_vec(&request)
            .map_err(|source| AssetStudioFfiError::FfiSerialize { source })?;
        if let Err(source) = write_worker_frame(&mut self.stdin, &request_bytes).await {
            return Err(self.protocol_error(source));
        }

        let response_bytes = match read_worker_frame(&mut self.stdout).await {
            Ok(bytes) => bytes,
            Err(source) => return Err(self.protocol_error(source)),
        };
        let response: WorkerServerResponse =
            sonic_rs::from_slice(&response_bytes).map_err(|source| {
                AssetStudioFfiError::message(format!(
                    "failed to parse ffi worker pool response: {source}"
                ))
            })?;
        if response.id != id {
            return Err(AssetStudioFfiError::message(format!(
                "ffi worker pool response id mismatch: expected {id}, got {}",
                response.id
            )));
        }
        if let Some(error) = response.error {
            return Err(AssetStudioFfiError::message(error));
        }
        let status = response.status.unwrap_or(100);
        let typed_response = response.response.ok_or_else(|| {
            AssetStudioFfiError::message("ffi worker pool response is missing typed response")
        })?;
        let payload_file = response.payload_file.as_ref().map(PathBuf::from);
        let payload = if let Some(payload_file) = payload_file.as_ref() {
            let metadata =
                std::fs::metadata(payload_file).map_err(|source| AssetStudioFfiError::Io {
                    path: payload_file.clone(),
                    source,
                })?;
            if metadata.len() != response.payload_len as u64 {
                return Err(AssetStudioFfiError::message(format!(
                    "ffi worker payload file length mismatch: expected {}, got {} at {}",
                    response.payload_len,
                    metadata.len(),
                    payload_file.display()
                )));
            }
            Vec::new()
        } else if response.payload_len > 0 {
            let payload = match read_worker_frame(&mut self.stdout).await {
                Ok(bytes) => bytes,
                Err(source) => return Err(self.protocol_error(source)),
            };
            if payload.len() != response.payload_len {
                return Err(AssetStudioFfiError::message(format!(
                    "ffi worker payload length mismatch: expected {}, got {}",
                    response.payload_len,
                    payload.len()
                )));
            }
            payload
        } else {
            Vec::new()
        };

        self.completed_calls = self.completed_calls.saturating_add(1);
        let elapsed_ms = started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
        self.stats.record_call(elapsed_ms);
        info!(
            worker_id = self.worker_id,
            pid = self.pid.unwrap_or_default(),
            request_id = id,
            operation = operation.as_str(),
            status,
            completed_calls = self.completed_calls,
            elapsed_ms,
            payload_len = response.payload_len,
            payload_in_memory_len = payload.len(),
            payload_file = payload_file
                .as_ref()
                .map(|path| path.display().to_string())
                .unwrap_or_default(),
            "assetstudio ffi worker call completed"
        );

        Ok(WorkerOutput {
            status: status.to_string(),
            status_success: status == 0,
            response: typed_response,
            stderr: String::new(),
            payload,
            payload_file,
        })
    }

    fn protocol_error(&mut self, source: io::Error) -> AssetStudioFfiError {
        let protocol_errors = self.stats.protocol_errors.fetch_add(1, Ordering::Relaxed) + 1;
        let status = self
            .child
            .try_wait()
            .ok()
            .flatten()
            .map(|status| status.to_string())
            .unwrap_or_else(|| "protocol error".to_string());
        debug!(worker_id = self.worker_id, completed_calls = self.completed_calls, status = %status, protocol_errors, error = %source, "assetstudio ffi worker protocol error");
        AssetStudioFfiError::CommandFailed {
            program: format!("{} --server", self.program),
            status,
            stderr: source.to_string(),
        }
    }

    async fn kill(&mut self) {
        let killed = self.stats.killed.fetch_add(1, Ordering::Relaxed) + 1;
        debug!(
            worker_id = self.worker_id,
            completed_calls = self.completed_calls,
            killed_workers = killed,
            "killing assetstudio ffi worker"
        );
        if let Err(source) = self.child.start_kill() {
            debug!(
                worker_id = self.worker_id,
                error = %source,
                "assetstudio ffi worker kill signal failed"
            );
            return;
        }

        match tokio::time::timeout(Duration::from_secs(5), self.child.wait()).await {
            Ok(Ok(status)) => {
                debug!(
                    worker_id = self.worker_id,
                    status = %status,
                    "assetstudio ffi worker exited after kill"
                );
            }
            Ok(Err(source)) => {
                warn!(
                    worker_id = self.worker_id,
                    error = %source,
                    "failed to wait for killed assetstudio ffi worker"
                );
            }
            Err(_) => {
                warn!(
                    worker_id = self.worker_id,
                    "timed out waiting for killed assetstudio ffi worker to exit"
                );
            }
        }
    }
}

pub fn configured_worker_path(
    configured_path: Option<&str>,
) -> Result<PathBuf, AssetStudioFfiError> {
    if let Some(path) = configured_path
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return Ok(PathBuf::from(path));
    }
    if let Ok(path) = std::env::var("HARUKI_ASSET_STUDIO_FFI_WORKER_PATH") {
        let path = path.trim();
        if !path.is_empty() {
            return Ok(PathBuf::from(path));
        }
    }
    let current_exe = std::env::current_exe().map_err(|source| AssetStudioFfiError::Spawn {
        program: "current_exe".to_string(),
        source,
    })?;
    let Some(dir) = current_exe.parent() else {
        return Err(AssetStudioFfiError::message(format!(
            "failed to infer ffi worker path from current executable `{}`",
            current_exe.display()
        )));
    };
    Ok(dir.join(worker_executable_name()))
}

pub fn worker_executable_name() -> &'static str {
    if cfg!(windows) {
        "assetstudio_ffi_worker.exe"
    } else {
        "assetstudio_ffi_worker"
    }
}

fn native_library_working_dir(native_library_path: &str) -> Option<&Path> {
    Path::new(native_library_path).parent()
}

fn absolute_command_path(path: &Path) -> PathBuf {
    path.canonicalize().unwrap_or_else(|_| path.to_path_buf())
}

pub async fn with_worker_lease<T, E, F, Fut>(lease: &mut WorkerLease, f: F) -> Result<T, E>
where
    F: FnOnce(&mut WorkerLease) -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    f(lease).await
}
