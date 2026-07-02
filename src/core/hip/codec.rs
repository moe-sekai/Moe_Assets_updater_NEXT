//! HIP/1 message schemas (msgpack).
//!
//! All messages except `UPLOAD_CHUNK` are msgpack-encoded structs. `UPLOAD_CHUNK`
//! is a raw `[u32 stream_id][bytes]` layout, encoded/decoded in `client.rs`.

use serde::{Deserialize, Serialize};

use super::errors::HipError;

pub fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, HipError> {
    let mut buf = Vec::new();
    rmp_serde::encode::write_named(&mut buf, value)?;
    Ok(buf)
}

pub fn decode<'de, T: Deserialize<'de>>(bytes: &'de [u8]) -> Result<T, HipError> {
    Ok(rmp_serde::from_slice(bytes)?)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Hello {
    pub proto: String,
    pub version: u32,
    pub bearer_token: String,
    pub region: String,
    pub app_version: String,
    pub asset_version: String,
    pub asset_hash: String,
    pub run_id: String,
    pub unpacker_version: String,
    pub expected_max_frame: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HelloAck {
    pub session_id: String,
    pub server_version: String,
    pub max_frame: u64,
    pub max_in_flight_uploads: u32,
    pub sha256_required: bool,
    #[serde(default)]
    pub known_version: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckBatchItem {
    pub path: String,
    pub fingerprint: String,
    pub size: u64,
    pub provider: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckBatch {
    pub batch_id: u64,
    pub items: Vec<CheckBatchItem>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CheckAction {
    Skip,
    Upload,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Placement {
    Shared,
    Override,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckAckItem {
    pub path: String,
    pub action: CheckAction,
    #[serde(default)]
    pub placement: Option<Placement>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckResult {
    pub batch_id: u64,
    pub results: Vec<CheckAckItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadBegin {
    pub stream_id: u32,
    pub bundle_path: String,
    pub path: String,
    pub fingerprint: String,
    pub size: u64,
    #[serde(default)]
    pub content_type: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadEnd {
    pub stream_id: u32,
    pub total_bytes: u64,
    pub sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadAck {
    pub stream_id: u32,
    pub status: String,
    #[serde(default)]
    pub placement: Option<Placement>,
    #[serde(default)]
    pub server_sha256: Option<String>,
    #[serde(default)]
    pub storage_key: Option<String>,
    #[serde(default)]
    pub message: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CommitStats {
    pub skipped_by_layer1: u64,
    pub skipped_by_check: u64,
    pub uploaded_shared: u64,
    pub uploaded_override: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Commit {
    pub bundle_count: u64,
    pub stats: CommitStats,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitAck {
    pub version_id: u64,
    #[serde(default)]
    pub override_index_rebuilt: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HipErrorPayload {
    pub code: String,
    pub message: String,
    #[serde(default)]
    pub fatal: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Window {
    pub max_in_flight_uploads: u32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hello_round_trip() {
        let hello = Hello {
            proto: "hip".into(),
            version: 1,
            bearer_token: "tok".into(),
            region: "jp".into(),
            app_version: "6.6.0".into(),
            asset_version: "6.6.0.20".into(),
            asset_hash: "e1f2ec17".into(),
            run_id: "01J8".into(),
            unpacker_version: "6.0.5".into(),
            expected_max_frame: MAX_DEFAULT_FRAME_BYTES_CONST,
        };
        let bytes = encode(&hello).unwrap();
        let back: Hello = decode(&bytes).unwrap();
        assert_eq!(back.region, "jp");
        assert_eq!(back.asset_version, "6.6.0.20");
    }

    #[test]
    fn check_ack_round_trip() {
        let ack = CheckResult {
            batch_id: 1,
            results: vec![
                CheckAckItem {
                    path: "music/foo".into(),
                    action: CheckAction::Skip,
                    placement: None,
                },
                CheckAckItem {
                    path: "music/bar".into(),
                    action: CheckAction::Upload,
                    placement: Some(Placement::Shared),
                },
            ],
        };
        let bytes = encode(&ack).unwrap();
        let back: CheckResult = decode(&bytes).unwrap();
        assert_eq!(back.results.len(), 2);
        assert_eq!(back.results[0].action, CheckAction::Skip);
        assert_eq!(back.results[1].placement, Some(Placement::Shared));
    }

    const MAX_DEFAULT_FRAME_BYTES_CONST: u64 = 16 * 1024 * 1024;
}
