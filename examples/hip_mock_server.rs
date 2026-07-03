use std::collections::HashMap;
use std::error::Error;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use haruki_sekai_asset_updater::core::hip::codec::{
    self, CheckAckItem, CheckResult, Commit, CommitAck, Hello, HelloAck, Placement, UploadAck,
    UploadBegin, UploadEnd,
};
use haruki_sekai_asset_updater::core::hip::frame::{
    read_frame, write_frame, MAX_DEFAULT_FRAME_BYTES,
};
use haruki_sekai_asset_updater::core::hip::{CheckAction, Frame, FrameType};
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tracing::{error, info, warn};

#[derive(Default)]
struct Stats {
    sessions: AtomicU64,
    checks: AtomicU64,
    check_items: AtomicU64,
    upload_begins: AtomicU64,
    upload_chunks: AtomicU64,
    upload_bytes: AtomicU64,
    uploads_ok: AtomicU64,
    commits: AtomicU64,
}

struct InflightUpload {
    path: String,
    expected_size: u64,
    received: u64,
    hasher: Sha256,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "info,hyper=warn".to_string()),
        )
        .init();

    let bind = std::env::var("HARUKI_HIP_MOCK_BIND").unwrap_or_else(|_| "127.0.0.1:7420".into());
    let max_in_flight = std::env::var("HARUKI_HIP_MOCK_MAX_IN_FLIGHT")
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(2);
    let listener = TcpListener::bind(&bind).await?;
    let stats = Arc::new(Stats::default());
    info!(%bind, max_in_flight, "hip mock server listening");

    loop {
        let (socket, peer) = listener.accept().await?;
        let stats = stats.clone();
        info!(%peer, "accepted hip mock session");
        tokio::spawn(async move {
            if let Err(err) = serve_session(socket, stats, max_in_flight).await {
                error!(%peer, error = %err, "hip mock session failed");
            }
        });
    }
}

async fn serve_session(
    socket: TcpStream,
    stats: Arc<Stats>,
    max_in_flight: u32,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    stats.sessions.fetch_add(1, Ordering::Relaxed);
    let (mut reader, mut writer) = socket.into_split();

    let hello_frame = read_frame(&mut reader, MAX_DEFAULT_FRAME_BYTES).await?;
    if hello_frame.frame_type != FrameType::Hello {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "expected HELLO").into());
    }
    let hello: Hello = codec::decode(&hello_frame.payload)?;
    info!(
        region = %hello.region,
        asset_version = %hello.asset_version,
        asset_hash = %hello.asset_hash,
        run_id = %hello.run_id,
        "hip mock hello"
    );

    let ack = HelloAck {
        session_id: format!("mock-{}", stats.sessions.load(Ordering::Relaxed)),
        server_version: "hip-mock/local".into(),
        max_frame: MAX_DEFAULT_FRAME_BYTES,
        max_in_flight_uploads: max_in_flight,
        sha256_required: true,
        known_version: false,
    };
    write_frame(
        &mut writer,
        &Frame::new(FrameType::HelloAck, codec::encode(&ack)?),
        MAX_DEFAULT_FRAME_BYTES,
    )
    .await?;
    writer.flush().await?;

    let mut uploads: HashMap<u32, InflightUpload> = HashMap::new();

    loop {
        let frame = match read_frame(&mut reader, MAX_DEFAULT_FRAME_BYTES).await {
            Ok(frame) => frame,
            Err(err) => {
                warn!(error = %err, "hip mock read ended");
                return Ok(());
            }
        };

        match frame.frame_type {
            FrameType::CheckBatch => {
                let batch: codec::CheckBatch = codec::decode(&frame.payload)?;
                stats.checks.fetch_add(1, Ordering::Relaxed);
                stats
                    .check_items
                    .fetch_add(batch.items.len() as u64, Ordering::Relaxed);
                let results = batch
                    .items
                    .into_iter()
                    .map(|item| CheckAckItem {
                        path: item.path,
                        action: CheckAction::Upload,
                        placement: Some(Placement::Shared),
                    })
                    .collect();
                let out = CheckResult {
                    batch_id: batch.batch_id,
                    results,
                };
                write_frame(
                    &mut writer,
                    &Frame::new(FrameType::CheckAck, codec::encode(&out)?),
                    MAX_DEFAULT_FRAME_BYTES,
                )
                .await?;
                writer.flush().await?;
            }
            FrameType::UploadBegin => {
                let begin: UploadBegin = codec::decode(&frame.payload)?;
                stats.upload_begins.fetch_add(1, Ordering::Relaxed);
                uploads.insert(
                    begin.stream_id,
                    InflightUpload {
                        path: begin.path,
                        expected_size: begin.size,
                        received: 0,
                        hasher: Sha256::new(),
                    },
                );
            }
            FrameType::UploadChunk => {
                if frame.payload.len() < 4 {
                    return Err(
                        io::Error::new(io::ErrorKind::InvalidData, "short upload chunk").into(),
                    );
                }
                let stream_id = u32::from_be_bytes(frame.payload[0..4].try_into()?);
                let body = &frame.payload[4..];
                let upload = uploads.get_mut(&stream_id).ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("chunk before begin for stream {stream_id}"),
                    )
                })?;
                upload.hasher.update(body);
                upload.received += body.len() as u64;
                stats.upload_chunks.fetch_add(1, Ordering::Relaxed);
                stats
                    .upload_bytes
                    .fetch_add(body.len() as u64, Ordering::Relaxed);
            }
            FrameType::UploadEnd => {
                let end: UploadEnd = codec::decode(&frame.payload)?;
                let upload = uploads.remove(&end.stream_id).ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("end before begin for stream {}", end.stream_id),
                    )
                })?;
                let server_sha = hex::encode(upload.hasher.finalize());
                let status = if server_sha == end.sha256 && upload.received == upload.expected_size
                {
                    stats.uploads_ok.fetch_add(1, Ordering::Relaxed);
                    "OK"
                } else {
                    warn!(
                        stream_id = end.stream_id,
                        path = %upload.path,
                        expected_size = upload.expected_size,
                        received = upload.received,
                        client_sha = %end.sha256,
                        server_sha = %server_sha,
                        "hip mock upload verification failed"
                    );
                    "SHA_MISMATCH"
                };
                let ack = UploadAck {
                    stream_id: end.stream_id,
                    status: status.into(),
                    placement: Some(Placement::Shared),
                    server_sha256: Some(server_sha),
                    storage_key: Some(format!("/mock/{}", upload.path)),
                    message: None,
                };
                write_frame(
                    &mut writer,
                    &Frame::new(FrameType::UploadAck, codec::encode(&ack)?),
                    MAX_DEFAULT_FRAME_BYTES,
                )
                .await?;
                writer.flush().await?;
            }
            FrameType::Commit => {
                let commit: Commit = codec::decode(&frame.payload)?;
                stats.commits.fetch_add(1, Ordering::Relaxed);
                info!(
                    bundle_count = commit.bundle_count,
                    skipped_by_layer1 = commit.stats.skipped_by_layer1,
                    skipped_by_check = commit.stats.skipped_by_check,
                    uploaded_shared = commit.stats.uploaded_shared,
                    uploaded_override = commit.stats.uploaded_override,
                    sessions = stats.sessions.load(Ordering::Relaxed),
                    checks = stats.checks.load(Ordering::Relaxed),
                    check_items = stats.check_items.load(Ordering::Relaxed),
                    upload_begins = stats.upload_begins.load(Ordering::Relaxed),
                    upload_chunks = stats.upload_chunks.load(Ordering::Relaxed),
                    upload_mb = stats.upload_bytes.load(Ordering::Relaxed) as f64 / 1024.0 / 1024.0,
                    uploads_ok = stats.uploads_ok.load(Ordering::Relaxed),
                    commits = stats.commits.load(Ordering::Relaxed),
                    "hip mock commit"
                );
                let ack = CommitAck {
                    version_id: stats.commits.load(Ordering::Relaxed),
                    override_index_rebuilt: true,
                };
                write_frame(
                    &mut writer,
                    &Frame::new(FrameType::CommitAck, codec::encode(&ack)?),
                    MAX_DEFAULT_FRAME_BYTES,
                )
                .await?;
                writer.flush().await?;
            }
            FrameType::Ping => {
                write_frame(
                    &mut writer,
                    &Frame::new(FrameType::Pong, Vec::new()),
                    MAX_DEFAULT_FRAME_BYTES,
                )
                .await?;
                writer.flush().await?;
            }
            FrameType::Pong => {}
            FrameType::Bye => return Ok(()),
            other => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("unexpected frame {other:?}"),
                )
                .into())
            }
        }
    }
}
