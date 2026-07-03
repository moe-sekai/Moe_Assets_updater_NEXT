use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use haruki_sekai_asset_updater::core::hip::client::{HelloParams, HipClientConfig};
use haruki_sekai_asset_updater::core::hip::{CommitStats, HipClient};
use tokio::io::{AsyncRead, ReadBuf};
use tracing::info;

struct PatternReader {
    remaining: u64,
    byte: u8,
}

impl AsyncRead for PatternReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if self.remaining == 0 || buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        let n = buf.remaining().min(self.remaining as usize);
        let start = buf.filled().len();
        buf.initialize_unfilled_to(n).fill(self.byte);
        buf.advance(n);
        self.remaining -= n as u64;
        self.byte = self.byte.wrapping_add((buf.filled().len() - start) as u8);
        Poll::Ready(Ok(()))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(std::env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string()))
        .init();

    let endpoint =
        std::env::var("HARUKI_HIP_STRESS_ENDPOINT").unwrap_or_else(|_| "127.0.0.1:7420".into());
    let total_mb = std::env::var("HARUKI_HIP_STRESS_MB")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(1024);
    let streams = std::env::var("HARUKI_HIP_STRESS_STREAMS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(8)
        .max(1);
    let chunk_size = std::env::var("HARUKI_HIP_STRESS_CHUNK_BYTES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(512 * 1024)
        .max(64 * 1024);

    let cfg = HipClientConfig {
        endpoint,
        bearer_token: "stress".into(),
        tls_enabled: false,
        tls_ca_file: None,
        handshake_timeout: Duration::from_secs(5),
        request_timeout: Duration::from_secs(120),
        max_frame_bytes: 16 * 1024 * 1024,
        chunk_size_bytes: chunk_size,
        heartbeat_interval: Duration::from_secs(30),
        unpacker_version: env!("CARGO_PKG_VERSION").into(),
    };
    let session = HipClient::connect(
        cfg,
        HelloParams {
            region: "tw".into(),
            app_version: "stress".into(),
            asset_version: "stress".into(),
            asset_hash: "stress".into(),
            run_id: uuid::Uuid::new_v4().to_string(),
        },
    )
    .await?;

    let bytes_per_stream = total_mb * 1024 * 1024 / streams;
    info!(
        total_mb,
        streams,
        bytes_per_stream,
        chunk_size,
        server_max_in_flight = session.hello_ack().max_in_flight_uploads,
        "starting hip upload stress"
    );

    let started = Instant::now();
    for stream in 0..streams {
        let reader = PatternReader {
            remaining: bytes_per_stream,
            byte: stream as u8,
        };
        let ack = session
            .upload_stream(
                &format!("stress/bundle-{stream}"),
                &format!("stress/asset-{stream}.bin"),
                &format!("{stream}"),
                bytes_per_stream,
                reader,
            )
            .await?;
        info!(stream, status = %ack.status, "hip stress upload complete");
    }

    let commit = session
        .commit(
            streams,
            CommitStats {
                skipped_by_layer1: 0,
                skipped_by_check: 0,
                uploaded_shared: streams,
                uploaded_override: 0,
            },
        )
        .await?;
    session.close().await?;
    info!(
        elapsed_secs = started.elapsed().as_secs_f64(),
        version_id = commit.version_id,
        "hip upload stress done"
    );
    Ok(())
}
