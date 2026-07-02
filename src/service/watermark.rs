//! Persist per-region "last successfully committed version" watermarks.
//!
//! One JSON file, mapping region name → RegionWatermark. Reads/writes are
//! serialized via a Mutex; on-disk atomicity uses write-to-temp + rename.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::core::errors::AssetExecutionError;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RegionWatermark {
    pub asset_version: String,
    pub asset_hash: String,
    pub app_version: String,
    pub bundle_count: u64,
    pub committed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WatermarkFile {
    #[serde(default)]
    pub regions: BTreeMap<String, RegionWatermark>,
}

#[derive(Clone)]
pub struct WatermarkStore {
    path: PathBuf,
    inner: Arc<Mutex<WatermarkFile>>,
}

impl WatermarkStore {
    pub async fn open(path: impl AsRef<Path>) -> Result<Self, AssetExecutionError> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await.map_err(|err| {
                AssetExecutionError::BlockingTask(format!("create watermark parent dir: {err}"))
            })?;
        }
        let file = match tokio::fs::read(&path).await {
            Ok(bytes) if !bytes.is_empty() => sonic_rs::from_slice::<WatermarkFile>(&bytes)
                .map_err(|err| AssetExecutionError::AssetInfoDecode(err.to_string()))?,
            Ok(_) => WatermarkFile::default(),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => WatermarkFile::default(),
            Err(err) => {
                return Err(AssetExecutionError::BlockingTask(format!(
                    "read watermark file {path:?}: {err}"
                )))
            }
        };
        Ok(Self {
            path,
            inner: Arc::new(Mutex::new(file)),
        })
    }

    pub async fn get(&self, region: &str) -> Option<RegionWatermark> {
        self.inner.lock().await.regions.get(region).cloned()
    }

    pub async fn set(
        &self,
        region: &str,
        watermark: RegionWatermark,
    ) -> Result<(), AssetExecutionError> {
        let mut guard = self.inner.lock().await;
        guard.regions.insert(region.to_string(), watermark);
        write_atomic(&self.path, &*guard).await
    }

    pub async fn snapshot(&self) -> WatermarkFile {
        self.inner.lock().await.clone()
    }
}

async fn write_atomic(path: &Path, file: &WatermarkFile) -> Result<(), AssetExecutionError> {
    let json = sonic_rs::to_vec_pretty(file)
        .map_err(|err| AssetExecutionError::AssetInfoDecode(err.to_string()))?;
    let tmp = path.with_extension("json.tmp");
    tokio::fs::write(&tmp, json).await.map_err(|err| {
        AssetExecutionError::BlockingTask(format!("write watermark tmp {tmp:?}: {err}"))
    })?;
    tokio::fs::rename(&tmp, path).await.map_err(|err| {
        AssetExecutionError::BlockingTask(format!("rename watermark {path:?}: {err}"))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[tokio::test]
    async fn round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("watermarks.json");
        let store = WatermarkStore::open(&path).await.unwrap();
        assert!(store.get("jp").await.is_none());

        let wm = RegionWatermark {
            asset_version: "6.6.0.20".into(),
            asset_hash: "e1f2ec17".into(),
            app_version: "6.6.0".into(),
            bundle_count: 100,
            committed_at: Utc.timestamp_opt(1_700_000_000, 0).single().unwrap(),
        };
        store.set("jp", wm.clone()).await.unwrap();

        let reopened = WatermarkStore::open(&path).await.unwrap();
        let got = reopened.get("jp").await.unwrap();
        assert_eq!(got.asset_version, "6.6.0.20");
    }
}
