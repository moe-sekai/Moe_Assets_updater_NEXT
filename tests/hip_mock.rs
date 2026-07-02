//! Integration test: HIP mock server + real HipClient over loopback TCP.
//!
//! Exercises the full frame codec, HELLO handshake, CHECK batch, UPLOAD
//! (chunked + sha256 verification), and COMMIT.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use haruki_sekai_asset_updater::core::hip::client::HipClientConfig;
use haruki_sekai_asset_updater::core::hip::codec::{
    self, CheckAckItem, CheckBatch, CheckResult, Commit, CommitAck, Hello, HelloAck, Placement,
    UploadAck, UploadBegin, UploadEnd,
};
use haruki_sekai_asset_updater::core::hip::frame::{read_frame, write_frame};
use haruki_sekai_asset_updater::core::hip::{
    CheckAction, CheckBatchItem, CommitStats, Frame, FrameType, HelloParams, HipClient,
};
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;

const MAX_FRAME: u64 = 16 * 1024 * 1024;

struct MockConfig {
    /// Bundle paths the server pretends to already have (returns SKIP).
    skip_paths: Vec<String>,
}

async fn spawn_mock(cfg: MockConfig) -> (String, Arc<MockStats>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let stats = Arc::new(MockStats::default());
    let stats_child = stats.clone();
    tokio::spawn(async move {
        loop {
            let (socket, _) = match listener.accept().await {
                Ok(v) => v,
                Err(_) => return,
            };
            let stats = stats_child.clone();
            let cfg_child = MockConfig {
                skip_paths: cfg.skip_paths.clone(),
            };
            tokio::spawn(async move {
                let _ = serve_one_session(socket, cfg_child, stats).await;
            });
        }
    });
    (addr.to_string(), stats)
}

#[derive(Default)]
struct MockStats {
    hellos: AtomicUsize,
    checks: AtomicUsize,
    uploads_ok: AtomicUsize,
    commits: AtomicUsize,
    sha_mismatch: AtomicUsize,
}

async fn serve_one_session(
    socket: tokio::net::TcpStream,
    cfg: MockConfig,
    stats: Arc<MockStats>,
) -> std::io::Result<()> {
    let (mut r, mut w) = socket.into_split();
    // Expect HELLO
    let hello_frame = read_frame(&mut r, MAX_FRAME).await.unwrap();
    assert_eq!(hello_frame.frame_type, FrameType::Hello);
    let _hello: Hello = codec::decode(&hello_frame.payload).unwrap();
    stats.hellos.fetch_add(1, Ordering::Relaxed);

    let ack = HelloAck {
        session_id: "test-session".into(),
        server_version: "hip-mock/1.0".into(),
        max_frame: MAX_FRAME,
        max_in_flight_uploads: 4,
        sha256_required: true,
        known_version: false,
    };
    let frame = Frame::new(FrameType::HelloAck, codec::encode(&ack).unwrap());
    write_frame(&mut w, &frame, MAX_FRAME).await.unwrap();
    w.flush().await.unwrap();

    // Active upload streams: stream_id -> (path, expected size, hasher, running total)
    let mut uploads: std::collections::HashMap<u32, InflightUpload> =
        std::collections::HashMap::new();

    loop {
        let frame = match read_frame(&mut r, MAX_FRAME).await {
            Ok(f) => f,
            Err(_) => return Ok(()),
        };
        match frame.frame_type {
            FrameType::CheckBatch => {
                let batch: CheckBatch = codec::decode(&frame.payload).unwrap();
                stats.checks.fetch_add(1, Ordering::Relaxed);
                let results = batch
                    .items
                    .into_iter()
                    .map(|item| {
                        if cfg.skip_paths.iter().any(|p| p == &item.path) {
                            CheckAckItem {
                                path: item.path,
                                action: CheckAction::Skip,
                                placement: None,
                            }
                        } else {
                            CheckAckItem {
                                path: item.path,
                                action: CheckAction::Upload,
                                placement: Some(Placement::Shared),
                            }
                        }
                    })
                    .collect();
                let out = CheckResult {
                    batch_id: batch.batch_id,
                    results,
                };
                let fr = Frame::new(FrameType::CheckAck, codec::encode(&out).unwrap());
                write_frame(&mut w, &fr, MAX_FRAME).await.unwrap();
                w.flush().await.unwrap();
            }
            FrameType::UploadBegin => {
                let begin: UploadBegin = codec::decode(&frame.payload).unwrap();
                uploads.insert(
                    begin.stream_id,
                    InflightUpload {
                        path: begin.path.clone(),
                        expected_size: begin.size,
                        received: 0,
                        hasher: Sha256::new(),
                    },
                );
            }
            FrameType::UploadChunk => {
                assert!(frame.payload.len() >= 4);
                let stream_id = u32::from_be_bytes([
                    frame.payload[0],
                    frame.payload[1],
                    frame.payload[2],
                    frame.payload[3],
                ]);
                let up = uploads.get_mut(&stream_id).expect("chunk before begin");
                let body = &frame.payload[4..];
                up.hasher.update(body);
                up.received += body.len() as u64;
            }
            FrameType::UploadEnd => {
                let end: UploadEnd = codec::decode(&frame.payload).unwrap();
                let up = uploads.remove(&end.stream_id).expect("end without begin");
                let server_sha = hex::encode(up.hasher.finalize());
                let status = if server_sha != end.sha256 {
                    stats.sha_mismatch.fetch_add(1, Ordering::Relaxed);
                    "SHA_MISMATCH".to_string()
                } else if end.total_bytes != up.expected_size {
                    "SIZE_MISMATCH".to_string()
                } else {
                    stats.uploads_ok.fetch_add(1, Ordering::Relaxed);
                    "OK".to_string()
                };
                let ack = UploadAck {
                    stream_id: end.stream_id,
                    status,
                    placement: Some(Placement::Shared),
                    server_sha256: Some(server_sha),
                    storage_key: Some(format!("/shared-assets/{}", up.path)),
                    message: None,
                };
                let fr = Frame::new(FrameType::UploadAck, codec::encode(&ack).unwrap());
                write_frame(&mut w, &fr, MAX_FRAME).await.unwrap();
                w.flush().await.unwrap();
            }
            FrameType::Commit => {
                let _c: Commit = codec::decode(&frame.payload).unwrap();
                stats.commits.fetch_add(1, Ordering::Relaxed);
                let ack = CommitAck {
                    version_id: 1,
                    override_index_rebuilt: true,
                };
                let fr = Frame::new(FrameType::CommitAck, codec::encode(&ack).unwrap());
                write_frame(&mut w, &fr, MAX_FRAME).await.unwrap();
                w.flush().await.unwrap();
            }
            FrameType::Ping => {
                let fr = Frame::new(FrameType::Pong, Vec::new());
                write_frame(&mut w, &fr, MAX_FRAME).await.unwrap();
                w.flush().await.unwrap();
            }
            FrameType::Pong => {}
            FrameType::Bye => return Ok(()),
            other => {
                panic!("unexpected client frame {other:?}");
            }
        }
    }
}

struct InflightUpload {
    path: String,
    expected_size: u64,
    received: u64,
    hasher: Sha256,
}

fn client_config(endpoint: String) -> HipClientConfig {
    HipClientConfig {
        endpoint,
        bearer_token: "test".into(),
        tls_enabled: false,
        tls_ca_file: None,
        handshake_timeout: Duration::from_secs(2),
        request_timeout: Duration::from_secs(5),
        max_frame_bytes: MAX_FRAME,
        chunk_size_bytes: 32 * 1024,
        heartbeat_interval: Duration::from_secs(60),
        unpacker_version: "test".into(),
    }
}

fn hello() -> HelloParams {
    HelloParams {
        region: "jp".into(),
        app_version: "6.6.0".into(),
        asset_version: "6.6.0.20".into(),
        asset_hash: "e1f2ec17".into(),
        run_id: "run-test-1".into(),
    }
}

#[tokio::test]
async fn hip_check_batch_and_commit() {
    let (addr, stats) = spawn_mock(MockConfig {
        skip_paths: vec!["music/foo".into()],
    })
    .await;
    let session = HipClient::connect(client_config(addr), hello())
        .await
        .unwrap();

    // CHECK
    let results = session
        .check_batch(vec![
            CheckBatchItem {
                path: "music/foo".into(),
                fingerprint: "100".into(),
                size: 10,
                provider: "jp".into(),
            },
            CheckBatchItem {
                path: "music/bar".into(),
                fingerprint: "200".into(),
                size: 20,
                provider: "jp".into(),
            },
        ])
        .await
        .unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].action, CheckAction::Skip);
    assert_eq!(results[1].action, CheckAction::Upload);

    // COMMIT
    let commit = session.commit(2, CommitStats::default()).await.unwrap();
    assert_eq!(commit.version_id, 1);
    let _ = session.close().await;

    assert_eq!(stats.hellos.load(Ordering::Relaxed), 1);
    assert_eq!(stats.checks.load(Ordering::Relaxed), 1);
    assert_eq!(stats.commits.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn hip_upload_stream_sha256_ok() {
    let (addr, stats) = spawn_mock(MockConfig { skip_paths: vec![] }).await;
    let session = HipClient::connect(client_config(addr), hello())
        .await
        .unwrap();

    let data = b"hello hip upload".to_vec();
    let cursor = std::io::Cursor::new(data.clone());
    let ack = session
        .upload_stream(
            "bundle/abc",
            "asset/abc.png",
            "12345",
            data.len() as u64,
            cursor,
        )
        .await
        .unwrap();
    assert_eq!(ack.status, "OK");
    let _ = session.close().await;

    assert_eq!(stats.uploads_ok.load(Ordering::Relaxed), 1);
    assert_eq!(stats.sha_mismatch.load(Ordering::Relaxed), 0);
}
