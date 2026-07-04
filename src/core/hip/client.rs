//! HIP/1 client session implementation.
//!
//! The session owns a single TCP (optionally TLS) connection. A background
//! writer serializes all outbound frames; a background reader dispatches
//! inbound frames to per-request oneshot channels keyed by batch_id / stream_id.

use std::collections::HashMap;
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot, Mutex, Semaphore};
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_rustls::rustls::pki_types::ServerName;
use tokio_rustls::rustls::{ClientConfig as RustlsClientConfig, RootCertStore};
use tokio_rustls::TlsConnector;
use tracing::{debug, warn};

use super::codec::{
    self, CheckAckItem, CheckBatch, CheckBatchItem, CheckResult, Commit, CommitAck, CommitStats,
    Hello, HelloAck, HipErrorPayload, Placement, UploadAck, UploadBegin, UploadEnd, Window,
};
use super::errors::HipError;
use super::frame::{read_frame, write_frame, Frame, FrameType, MAX_DEFAULT_FRAME_BYTES};

/// Client-facing hint about where the server intends to place a bundle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlacementHint {
    Shared,
    Override,
}

impl From<Placement> for PlacementHint {
    fn from(value: Placement) -> Self {
        match value {
            Placement::Shared => Self::Shared,
            Placement::Override => Self::Override,
        }
    }
}

#[derive(Debug, Clone)]
pub struct HipClientConfig {
    pub endpoint: String,
    pub bearer_token: String,
    pub tls_enabled: bool,
    pub tls_ca_file: Option<String>,
    pub handshake_timeout: Duration,
    pub request_timeout: Duration,
    pub max_frame_bytes: u64,
    pub chunk_size_bytes: usize,
    pub heartbeat_interval: Duration,
    pub unpacker_version: String,
}

impl Default for HipClientConfig {
    fn default() -> Self {
        Self {
            endpoint: "127.0.0.1:7420".to_string(),
            bearer_token: String::new(),
            tls_enabled: false,
            tls_ca_file: None,
            handshake_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(30),
            max_frame_bytes: MAX_DEFAULT_FRAME_BYTES,
            chunk_size_bytes: 1024 * 1024,
            heartbeat_interval: Duration::from_secs(30),
            unpacker_version: env!("CARGO_PKG_VERSION").to_string(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct HelloParams {
    pub region: String,
    pub app_version: String,
    pub asset_version: String,
    pub asset_hash: String,
    pub run_id: String,
}

pub struct HipClient;

impl HipClient {
    pub async fn connect(
        config: HipClientConfig,
        hello: HelloParams,
    ) -> Result<HipSession, HipError> {
        let stream = TcpStream::connect(&config.endpoint).await?;
        stream.set_nodelay(true)?;

        if config.tls_enabled {
            let (host, _port) = split_host_port(&config.endpoint)?;
            let server_name = ServerName::try_from(host.to_string())
                .map_err(|err| HipError::Tls(format!("invalid dns name `{host}`: {err}")))?;
            let tls_config = build_client_tls_config(config.tls_ca_file.as_deref())?;
            let connector = TlsConnector::from(Arc::new(tls_config));
            let tls_stream = connector
                .connect(server_name, stream)
                .await
                .map_err(|err| HipError::Tls(err.to_string()))?;
            let (read_half, write_half) = tokio::io::split(tls_stream);
            HipSession::start(config, hello, Box::pin(read_half), Box::pin(write_half)).await
        } else {
            let (read_half, write_half) = stream.into_split();
            HipSession::start(config, hello, Box::pin(read_half), Box::pin(write_half)).await
        }
    }
}

fn split_host_port(endpoint: &str) -> Result<(&str, &str), HipError> {
    let (host, port) = endpoint
        .rsplit_once(':')
        .ok_or_else(|| HipError::Config(format!("hip endpoint `{endpoint}` missing :port")))?;
    Ok((host, port))
}

fn install_default_crypto_provider_once() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        let _ = rustls::crypto::CryptoProvider::install_default(
            rustls::crypto::ring::default_provider(),
        );
    });
}

fn build_client_tls_config(ca_file: Option<&str>) -> Result<RustlsClientConfig, HipError> {
    install_default_crypto_provider_once();
    let mut roots = RootCertStore::empty();
    if let Some(ca) = ca_file.filter(|value| !value.trim().is_empty()) {
        let mut reader = std::io::BufReader::new(
            std::fs::File::open(ca)
                .map_err(|err| HipError::Tls(format!("open ca_file {ca}: {err}")))?,
        );
        for cert in rustls_pemfile::certs(&mut reader) {
            let cert = cert.map_err(|err| HipError::Tls(format!("read ca_file {ca}: {err}")))?;
            roots
                .add(cert)
                .map_err(|err| HipError::Tls(format!("add ca cert: {err}")))?;
        }
    } else {
        for cert in rustls_native_certs::load_native_certs().certs {
            let _ = roots.add(cert);
        }
    }
    Ok(RustlsClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth())
}

/// One HIP session. Cheap to keep as `Arc<HipSession>` across upload tasks.
pub struct HipSession {
    inner: Arc<HipSessionInner>,
}

impl HipSession {
    pub fn hello_ack(&self) -> &HelloAck {
        &self.inner.hello_ack
    }

    pub async fn check_batch(
        &self,
        items: Vec<CheckBatchItem>,
    ) -> Result<Vec<CheckAckItem>, HipError> {
        if items.is_empty() {
            return Ok(Vec::new());
        }
        let batch_id = self.inner.next_batch_id();
        let (tx, rx) = oneshot::channel();
        {
            let mut waiters = self.inner.check_waiters.lock().await;
            waiters.insert(batch_id, tx);
        }
        let payload = codec::encode(&CheckBatch { batch_id, items })?;
        self.inner
            .send_frame(FrameType::CheckBatch, payload)
            .await?;
        let ack = timeout(self.inner.config.request_timeout, rx)
            .await
            .map_err(|_| HipError::Timeout(self.inner.config.request_timeout.as_millis() as u64))?
            .map_err(|_| HipError::SessionClosed("check_batch reply dropped".into()))?;
        Ok(ack.results)
    }

    /// Upload the bytes at `file_path` as one HIP upload stream.
    ///
    /// The client streams the file, computing sha256 as it goes, and sends
    /// UPLOAD_BEGIN, UPLOAD_CHUNK*, UPLOAD_END. The server acknowledges via
    /// UPLOAD_ACK.
    pub async fn upload_file(
        &self,
        bundle_path: &str,
        asset_path: &str,
        fingerprint: &str,
        file_path: &Path,
    ) -> Result<UploadAck, HipError> {
        let file = tokio::fs::File::open(file_path).await?;
        let metadata = file.metadata().await?;
        self.upload_stream(bundle_path, asset_path, fingerprint, metadata.len(), file)
            .await
    }

    pub async fn upload_stream<R>(
        &self,
        bundle_path: &str,
        asset_path: &str,
        fingerprint: &str,
        size: u64,
        mut reader: R,
    ) -> Result<UploadAck, HipError>
    where
        R: AsyncRead + Unpin,
    {
        let _permit = self
            .inner
            .upload_semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| HipError::SessionClosed("upload semaphore closed".into()))?;

        let stream_id = self.inner.next_stream_id();
        let (tx, mut rx) = oneshot::channel();
        {
            let mut waiters = self.inner.upload_waiters.lock().await;
            waiters.insert(stream_id, tx);
        }

        let begin = UploadBegin {
            stream_id,
            bundle_path: bundle_path.to_string(),
            path: asset_path.to_string(),
            fingerprint: fingerprint.to_string(),
            size,
            content_type: Some("application/octet-stream".to_string()),
        };
        self.inner
            .send_frame(FrameType::UploadBegin, codec::encode(&begin)?)
            .await?;

        // The server can reject at UPLOAD_BEGIN when this exact asset already
        // exists. Treat that as a successful no-op and do not stream chunks;
        // otherwise the server will see chunks for an unknown stream.
        match timeout(Duration::from_millis(10), &mut rx).await {
            Ok(Ok(ack)) => return validate_upload_ack(ack),
            Ok(Err(_)) => return Err(HipError::SessionClosed("upload reply dropped".into())),
            Err(_) => {}
        }

        let mut hasher = Sha256::new();
        let mut total_bytes: u64 = 0;
        let mut chunk = vec![0u8; self.inner.config.chunk_size_bytes];
        let stream_prefix = stream_id.to_be_bytes();
        loop {
            let n = reader.read(&mut chunk).await?;
            if n == 0 {
                break;
            }
            hasher.update(&chunk[..n]);
            total_bytes += n as u64;

            let mut payload = Vec::with_capacity(4 + n);
            payload.extend_from_slice(&stream_prefix);
            payload.extend_from_slice(&chunk[..n]);
            self.inner
                .send_frame(FrameType::UploadChunk, payload)
                .await?;
        }

        if total_bytes != size {
            return Err(HipError::Protocol(format!(
                "upload size mismatch: declared {size} but sent {total_bytes} bytes"
            )));
        }

        let end = UploadEnd {
            stream_id,
            total_bytes,
            sha256: hex::encode(hasher.finalize()),
        };
        self.inner
            .send_frame(FrameType::UploadEnd, codec::encode(&end)?)
            .await?;

        let ack = timeout(self.inner.config.request_timeout, rx)
            .await
            .map_err(|_| HipError::Timeout(self.inner.config.request_timeout.as_millis() as u64))?
            .map_err(|_| HipError::SessionClosed("upload reply dropped".into()))?;

        validate_upload_ack(ack)
    }

    pub async fn commit(
        &self,
        bundle_count: u64,
        stats: CommitStats,
    ) -> Result<CommitAck, HipError> {
        let rx = {
            let mut slot = self.inner.commit_waiter.lock().await;
            if slot.is_some() {
                return Err(HipError::Protocol("commit already in flight".into()));
            }
            let (tx, rx) = oneshot::channel();
            *slot = Some(tx);
            rx
        };

        let payload = codec::encode(&Commit {
            bundle_count,
            stats,
        })?;
        self.inner.send_frame(FrameType::Commit, payload).await?;

        let ack = timeout(self.inner.config.request_timeout, rx)
            .await
            .map_err(|_| HipError::Timeout(self.inner.config.request_timeout.as_millis() as u64))?
            .map_err(|_| HipError::SessionClosed("commit reply dropped".into()))?;
        Ok(ack)
    }

    pub async fn close(self) -> Result<(), HipError> {
        // Send BYE and let the writer drain the mpsc queue before we abort
        // the background tasks. We do this by dropping the writer sender
        // via `HipSessionInner::close_writer`, which lets the writer loop
        // observe the mpsc close and flush.
        let _ = self.inner.send_frame(FrameType::Bye, Vec::new()).await;
        self.inner.close_writer().await;
        // Give the writer up to 2s to drain.
        let writer_task = self.inner.writer_task.lock().await.take();
        if let Some(handle) = writer_task {
            let _ = tokio::time::timeout(Duration::from_secs(2), handle).await;
        }
        // Reader must go too — it's parked on read_frame.
        if let Some(handle) = self.inner.reader_task.lock().await.take() {
            handle.abort();
        }
        Ok(())
    }
}

impl HipSession {
    async fn start(
        config: HipClientConfig,
        hello_params: HelloParams,
        read_stream: Pin<Box<dyn AsyncRead + Send>>,
        mut write_stream: Pin<Box<dyn AsyncWrite + Send>>,
    ) -> Result<Self, HipError> {
        // Send HELLO.
        let hello = Hello {
            proto: "hip".to_string(),
            version: 1,
            bearer_token: config.bearer_token.clone(),
            region: hello_params.region.clone(),
            app_version: hello_params.app_version.clone(),
            asset_version: hello_params.asset_version.clone(),
            asset_hash: hello_params.asset_hash.clone(),
            run_id: hello_params.run_id.clone(),
            unpacker_version: config.unpacker_version.clone(),
            expected_max_frame: config.max_frame_bytes,
        };
        let hello_frame = Frame::new(FrameType::Hello, codec::encode(&hello)?);
        write_frame(&mut write_stream, &hello_frame, config.max_frame_bytes).await?;

        // Read HELLO_ACK.
        let mut read_stream = read_stream;
        let ack_frame = timeout(
            config.handshake_timeout,
            read_frame(&mut read_stream, config.max_frame_bytes),
        )
        .await
        .map_err(|_| HipError::Timeout(config.handshake_timeout.as_millis() as u64))??;

        let hello_ack: HelloAck = match ack_frame.frame_type {
            FrameType::HelloAck => codec::decode(&ack_frame.payload)?,
            FrameType::Error => {
                let payload: HipErrorPayload = codec::decode(&ack_frame.payload)?;
                return Err(HipError::Handshake(format!(
                    "server rejected HELLO: {} ({})",
                    payload.message, payload.code
                )));
            }
            other => {
                return Err(HipError::Handshake(format!(
                    "unexpected frame type {other:?} during handshake"
                )));
            }
        };

        let max_in_flight = hello_ack.max_in_flight_uploads.max(1) as usize;
        let effective_max_frame = hello_ack.max_frame.min(config.max_frame_bytes);
        let mut effective_config = config;
        effective_config.max_frame_bytes = effective_max_frame;

        let (writer_tx, writer_rx) = mpsc::channel::<Frame>(64);
        let writer_handle = spawn_writer(
            writer_rx,
            write_stream,
            effective_config.max_frame_bytes,
            effective_config.heartbeat_interval,
        );

        let inner = Arc::new(HipSessionInner {
            hello_ack: hello_ack.clone(),
            config: effective_config.clone(),
            writer_tx: Mutex::new(Some(writer_tx)),
            batch_id: AtomicU64::new(1),
            stream_id: AtomicU32::new(1),
            upload_semaphore: Arc::new(Semaphore::new(max_in_flight)),
            check_waiters: Mutex::new(HashMap::new()),
            upload_waiters: Mutex::new(HashMap::new()),
            commit_waiter: Mutex::new(None),
            writer_task: Mutex::new(Some(writer_handle)),
            reader_task: Mutex::new(None),
        });
        let reader_handle = spawn_reader(inner.clone(), read_stream);
        *inner.reader_task.lock().await = Some(reader_handle);

        Ok(Self { inner })
    }
}

struct HipSessionInner {
    hello_ack: HelloAck,
    config: HipClientConfig,
    writer_tx: Mutex<Option<mpsc::Sender<Frame>>>,
    batch_id: AtomicU64,
    stream_id: AtomicU32,
    upload_semaphore: Arc<Semaphore>,
    check_waiters: Mutex<HashMap<u64, oneshot::Sender<CheckResult>>>,
    upload_waiters: Mutex<HashMap<u32, oneshot::Sender<UploadAck>>>,
    commit_waiter: Mutex<Option<oneshot::Sender<CommitAck>>>,
    writer_task: Mutex<Option<JoinHandle<()>>>,
    reader_task: Mutex<Option<JoinHandle<()>>>,
}

impl HipSessionInner {
    fn next_batch_id(&self) -> u64 {
        self.batch_id.fetch_add(1, Ordering::AcqRel)
    }

    fn next_stream_id(&self) -> u32 {
        self.stream_id.fetch_add(1, Ordering::AcqRel)
    }

    async fn send_frame(&self, frame_type: FrameType, payload: Vec<u8>) -> Result<(), HipError> {
        let tx = {
            let guard = self.writer_tx.lock().await;
            guard.clone()
        };
        match tx {
            Some(tx) => tx
                .send(Frame::new(frame_type, payload))
                .await
                .map_err(|_| HipError::SessionClosed("writer task terminated".into())),
            None => Err(HipError::SessionClosed("session closed".into())),
        }
    }

    /// Drop the writer's mpsc sender, allowing the writer task to flush and
    /// exit naturally once its receiver observes the channel close.
    async fn close_writer(&self) {
        self.writer_tx.lock().await.take();
    }
}

fn spawn_writer(
    mut rx: mpsc::Receiver<Frame>,
    mut writer: Pin<Box<dyn AsyncWrite + Send>>,
    max_frame_bytes: u64,
    heartbeat_interval: Duration,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(heartbeat_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                maybe_frame = rx.recv() => {
                    match maybe_frame {
                        Some(frame) => {
                            if let Err(err) = write_frame(&mut writer, &frame, max_frame_bytes).await {
                                warn!(error = %err, "hip writer failed; closing session");
                                break;
                            }
                            if writer.flush().await.is_err() {
                                break;
                            }
                        }
                        None => break,
                    }
                }
                _ = ticker.tick() => {
                    let ping = Frame::new(FrameType::Ping, Vec::new());
                    if let Err(err) = write_frame(&mut writer, &ping, max_frame_bytes).await {
                        warn!(error = %err, "hip heartbeat ping failed");
                        break;
                    }
                    if writer.flush().await.is_err() {
                        break;
                    }
                }
            }
        }
    })
}

fn spawn_reader(
    inner: Arc<HipSessionInner>,
    mut reader: Pin<Box<dyn AsyncRead + Send>>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let frame = match read_frame(&mut reader, inner.config.max_frame_bytes).await {
                Ok(frame) => frame,
                Err(err) => {
                    debug!(error = %err, "hip reader exiting");
                    fail_all_waiters(&inner, err).await;
                    break;
                }
            };
            if let Err(err) = dispatch_frame(&inner, frame).await {
                warn!(error = %err, "hip reader dispatch failed; closing session");
                fail_all_waiters(&inner, err).await;
                break;
            }
        }
    })
}

async fn dispatch_frame(inner: &HipSessionInner, frame: Frame) -> Result<(), HipError> {
    match frame.frame_type {
        FrameType::CheckAck => {
            let ack: CheckResult = codec::decode(&frame.payload)?;
            let waiter = inner.check_waiters.lock().await.remove(&ack.batch_id);
            if let Some(tx) = waiter {
                let _ = tx.send(ack);
            } else {
                warn!(batch_id = ack.batch_id, "check ack without waiter");
            }
        }
        FrameType::UploadAck => {
            let ack: UploadAck = codec::decode(&frame.payload)?;
            let waiter = inner.upload_waiters.lock().await.remove(&ack.stream_id);
            if let Some(tx) = waiter {
                let _ = tx.send(ack);
            } else {
                warn!(stream_id = ack.stream_id, "upload ack without waiter");
            }
        }
        FrameType::CommitAck => {
            let ack: CommitAck = codec::decode(&frame.payload)?;
            let waiter = inner.commit_waiter.lock().await.take();
            if let Some(tx) = waiter {
                let _ = tx.send(ack);
            }
        }
        FrameType::Window => {
            let _window: Window = codec::decode(&frame.payload)?;
            // TODO: dynamically adjust semaphore; MVP ignores window updates.
        }
        FrameType::Ping => {
            inner.send_frame(FrameType::Pong, Vec::new()).await?;
        }
        FrameType::Pong => {}
        FrameType::Error => {
            let payload: HipErrorPayload = codec::decode(&frame.payload)?;
            let err = HipError::Server {
                code: payload.code,
                message: payload.message,
                fatal: payload.fatal,
            };
            fail_all_waiters(inner, err.clone_for_dispatch()).await;
            if payload.fatal {
                return Err(err);
            }
        }
        other => {
            return Err(HipError::Protocol(format!(
                "unexpected server frame {other:?}"
            )));
        }
    }
    Ok(())
}

fn validate_upload_ack(ack: UploadAck) -> Result<UploadAck, HipError> {
    if ack.status == "OK" {
        return Ok(ack);
    }

    let message = ack.message.clone().unwrap_or_default();
    if ack.status.eq_ignore_ascii_case("REJECTED")
        && message.to_ascii_lowercase().contains("already present")
    {
        debug!(
            stream_id = ack.stream_id,
            status = %ack.status,
            message = %message,
            "hip upload already present; treating as successful no-op"
        );
        return Ok(ack);
    }

    Err(HipError::Server {
        code: ack.status.clone(),
        message,
        fatal: false,
    })
}

async fn fail_all_waiters(inner: &HipSessionInner, err: HipError) {
    let mut check = inner.check_waiters.lock().await;
    check.drain().for_each(|(_, tx)| {
        // best-effort: only the receiver side needs to observe the failure via
        // its own timeout / dropped-sender error path
        drop(tx);
    });
    drop(check);
    let mut upload = inner.upload_waiters.lock().await;
    upload.drain().for_each(|(_, tx)| {
        drop(tx);
    });
    drop(upload);
    if let Some(tx) = inner.commit_waiter.lock().await.take() {
        drop(tx);
    }
    debug!(error = %err, "hip session terminated; all waiters cleared");
}

impl HipError {
    fn clone_for_dispatch(&self) -> HipError {
        match self {
            HipError::Server {
                code,
                message,
                fatal,
            } => HipError::Server {
                code: code.clone(),
                message: message.clone(),
                fatal: *fatal,
            },
            other => HipError::Protocol(format!("{other}")),
        }
    }
}

// Re-export CheckAction for convenient matches.
pub use codec::CheckAction as ServerCheckAction;
// Compile-time sanity: encode/decode used across the module.
#[allow(dead_code)]
fn _codec_use<T: Serialize>(_: &T) {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upload_ack_already_present_is_successful_noop() {
        let ack = UploadAck {
            stream_id: 7,
            status: "REJECTED".to_string(),
            placement: None,
            server_sha256: None,
            storage_key: None,
            message: Some("already present".to_string()),
        };

        let accepted = validate_upload_ack(ack).unwrap();

        assert_eq!(accepted.stream_id, 7);
        assert_eq!(accepted.status, "REJECTED");
    }

    #[test]
    fn upload_ack_other_rejection_is_error() {
        let ack = UploadAck {
            stream_id: 8,
            status: "REJECTED".to_string(),
            placement: None,
            server_sha256: None,
            storage_key: None,
            message: Some("quota exceeded".to_string()),
        };

        assert!(validate_upload_ack(ack).is_err());
    }
}
