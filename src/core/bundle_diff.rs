//! Layer-1 pruning: diff a freshly fetched AssetBundleInfo against the last
//! successfully committed snapshot to keep only the bundles whose fingerprint
//! (crc32) has changed or which are entirely new.

use std::collections::HashMap;
use std::io::Read;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::core::asset_execution::AssetBundleInfo;
use crate::core::errors::AssetExecutionError;

/// A bundle path whose fingerprint has changed or which is newly added.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangedBundle {
    pub bundle_name: String,
    pub fingerprint: String,
    pub size: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DiffStats {
    pub added: u64,
    pub changed: u64,
    pub unchanged: u64,
    pub removed: u64,
}

/// Result of a diff run.
#[derive(Debug, Clone)]
pub struct BundleDiff {
    pub changed: Vec<ChangedBundle>,
    pub stats: DiffStats,
}

/// Return `(fingerprint_string, size)` for a given detail.
///
/// * ColorfulPalette (JP/EN) : we still use `crc` here so both providers share
///   the same fingerprint dimension.
/// * Nuverse (TW/KR/CN)      : `crc` is authoritative.
///
/// The value is stringified base-10 to match the HIP wire format.
fn fingerprint_for(detail: &crate::core::asset_execution::AssetBundleDetail) -> (String, u64) {
    (detail.crc.to_string(), detail.file_size.max(0) as u64)
}

pub fn diff(old: Option<&AssetBundleInfo>, new: &AssetBundleInfo) -> BundleDiff {
    let old_map: HashMap<&str, (&str, u64)> = old
        .map(|info| {
            info.bundles
                .iter()
                .map(|(name, detail)| {
                    (
                        name.as_str(),
                        (
                            // avoid re-stringifying by looking at crc directly
                            "",
                            detail.file_size.max(0) as u64,
                        ),
                    )
                })
                .collect()
        })
        .unwrap_or_default();

    // Second pass to materialise fingerprint strings for the old side.
    let mut old_fp: HashMap<String, String> = HashMap::new();
    if let Some(info) = old {
        for (name, detail) in &info.bundles {
            let (fp, _) = fingerprint_for(detail);
            old_fp.insert(name.clone(), fp);
        }
    }
    let _ = old_map;

    let mut changed = Vec::new();
    let mut stats = DiffStats::default();
    for (name, detail) in &new.bundles {
        let (fp, size) = fingerprint_for(detail);
        match old_fp.get(name) {
            Some(prev) if prev == &fp => {
                stats.unchanged += 1;
            }
            Some(_) => {
                stats.changed += 1;
                changed.push(ChangedBundle {
                    bundle_name: name.clone(),
                    fingerprint: fp,
                    size,
                });
            }
            None => {
                stats.added += 1;
                changed.push(ChangedBundle {
                    bundle_name: name.clone(),
                    fingerprint: fp,
                    size,
                });
            }
        }
    }

    if let Some(old_info) = old {
        for name in old_info.bundles.keys() {
            if !new.bundles.contains_key(name) {
                stats.removed += 1;
            }
        }
    }

    BundleDiff { changed, stats }
}

/// Persist a bundle info snapshot to `path` as zstd-compressed msgpack.
pub fn save_snapshot(path: &Path, info: &AssetBundleInfo) -> Result<(), AssetExecutionError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|source| {
            AssetExecutionError::BlockingTask(format!("create last_info parent: {source}"))
        })?;
    }
    let mut packed = Vec::new();
    rmp_serde::encode::write_named(&mut packed, info)
        .map_err(|err| AssetExecutionError::AssetInfoDecode(err.to_string()))?;
    let compressed = zstd::stream::encode_all(packed.as_slice(), 6)
        .map_err(|source| AssetExecutionError::BlockingTask(format!("zstd encode: {source}")))?;
    std::fs::write(path, compressed).map_err(|source| {
        AssetExecutionError::BlockingTask(format!("write last_info {path:?}: {source}"))
    })
}

pub fn load_snapshot(path: &Path) -> Result<Option<AssetBundleInfo>, AssetExecutionError> {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(source) => {
            return Err(AssetExecutionError::BlockingTask(format!(
                "read last_info {path:?}: {source}"
            )))
        }
    };
    let mut decoded = Vec::new();
    zstd::stream::Decoder::new(bytes.as_slice())
        .map_err(|source| AssetExecutionError::BlockingTask(format!("zstd decoder: {source}")))?
        .read_to_end(&mut decoded)
        .map_err(|source| AssetExecutionError::BlockingTask(format!("zstd decode: {source}")))?;
    let info: AssetBundleInfo = rmp_serde::from_slice(&decoded)
        .map_err(|err| AssetExecutionError::AssetInfoDecode(err.to_string()))?;
    Ok(Some(info))
}

pub fn snapshot_path(dir: &str, region: &str) -> PathBuf {
    Path::new(dir).join(format!("{region}.msgpack.zst"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::asset_execution::{AssetBundleDetail, AssetCategory};

    fn detail(crc: i64, size: i64) -> AssetBundleDetail {
        AssetBundleDetail {
            bundle_name: "".into(),
            cache_file_name: "".into(),
            cache_directory_name: "".into(),
            hash: "".into(),
            category: AssetCategory::StartApp,
            crc,
            file_size: size,
            dependencies: vec![],
            paths: vec![],
            is_builtin: false,
            is_relocate: None,
            md5_hash: None,
            download_path: None,
        }
    }

    #[test]
    fn diff_detects_added_and_changed() {
        let mut old = AssetBundleInfo {
            version: None,
            os: None,
            bundles: Default::default(),
        };
        old.bundles.insert("music/foo".into(), detail(100, 10));
        old.bundles.insert("music/bar".into(), detail(200, 20));

        let mut new = AssetBundleInfo {
            version: None,
            os: None,
            bundles: Default::default(),
        };
        new.bundles.insert("music/foo".into(), detail(100, 10)); // unchanged
        new.bundles.insert("music/bar".into(), detail(999, 20)); // changed
        new.bundles.insert("music/baz".into(), detail(300, 30)); // added

        let result = diff(Some(&old), &new);
        assert_eq!(result.stats.added, 1);
        assert_eq!(result.stats.changed, 1);
        assert_eq!(result.stats.unchanged, 1);
        assert_eq!(result.changed.len(), 2);
    }

    #[test]
    fn diff_full_when_snapshot_missing() {
        let mut new = AssetBundleInfo {
            version: None,
            os: None,
            bundles: Default::default(),
        };
        new.bundles.insert("music/foo".into(), detail(100, 10));
        let result = diff(None, &new);
        assert_eq!(result.stats.added, 1);
        assert_eq!(result.changed.len(), 1);
    }

    #[test]
    fn snapshot_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let path = snapshot_path(dir.path().to_str().unwrap(), "jp");
        let mut info = AssetBundleInfo {
            version: Some("1".into()),
            os: None,
            bundles: Default::default(),
        };
        info.bundles.insert("music/foo".into(), detail(100, 10));
        save_snapshot(&path, &info).unwrap();
        let loaded = load_snapshot(&path).unwrap().unwrap();
        assert_eq!(loaded.bundles.len(), 1);
    }
}
