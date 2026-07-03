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
///
/// Writes atomically via `path + ".tmp"` + `rename`, so a crash mid-write can
/// only leave the stale-but-valid previous snapshot in place, never a
/// half-written file that would fail zstd decode on next load.
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
    let tmp_path = {
        let mut tmp = path.as_os_str().to_owned();
        tmp.push(".tmp");
        PathBuf::from(tmp)
    };
    std::fs::write(&tmp_path, &compressed).map_err(|source| {
        AssetExecutionError::BlockingTask(format!("write last_info tmp {tmp_path:?}: {source}"))
    })?;
    std::fs::rename(&tmp_path, path).map_err(|source| {
        // best-effort cleanup so a failed rename doesn't leak the tmp
        let _ = std::fs::remove_file(&tmp_path);
        AssetExecutionError::BlockingTask(format!(
            "rename last_info tmp → target ({tmp_path:?} → {path:?}): {source}"
        ))
    })
}

/// Merge `entries` into whatever snapshot currently lives at `path` and write
/// atomically. Semantics:
///
/// * If `path` does not exist yet, or the existing snapshot's
///   `(version, os)` does not match the caller-provided `(new_version,
///   new_os)`, the existing snapshot is *discarded* (it belonged to a stale
///   asset_version and would poison next-tick's layer-1 diff). A fresh
///   snapshot containing only `entries` is written.
///
/// * Otherwise `entries` are merged in on top (same key → overwritten with
///   the fresh detail), preserving everything already committed in prior
///   batches.
///
/// This is the incremental-commit hook: called after every successful
/// per-batch HIP commit so a crash between batches only loses the last
/// un-committed batch, not the entire region.
pub fn merge_snapshot(
    path: &Path,
    new_version: Option<&str>,
    new_os: Option<&str>,
    entries: &[(String, crate::core::asset_execution::AssetBundleDetail)],
) -> Result<(), AssetExecutionError> {
    let existing = load_snapshot(path)?;
    let mut merged = match existing {
        Some(prev) if prev.version.as_deref() == new_version && prev.os.as_deref() == new_os => {
            prev
        }
        _ => AssetBundleInfo {
            version: new_version.map(str::to_string),
            os: new_os.map(str::to_string),
            bundles: HashMap::new(),
        },
    };
    for (name, detail) in entries {
        merged.bundles.insert(name.clone(), detail.clone());
    }
    save_snapshot(path, &merged)
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

    #[test]
    fn save_snapshot_writes_atomically_without_leaking_tmp() {
        let dir = tempfile::tempdir().unwrap();
        let path = snapshot_path(dir.path().to_str().unwrap(), "jp");
        let mut info = AssetBundleInfo {
            version: Some("1".into()),
            os: None,
            bundles: Default::default(),
        };
        info.bundles.insert("music/foo".into(), detail(100, 10));
        save_snapshot(&path, &info).unwrap();

        // Target file exists and decodes.
        assert!(path.exists(), "snapshot target file should exist");
        let _ = load_snapshot(&path).unwrap().unwrap();

        // The `.tmp` sibling must not linger — save_snapshot must have
        // rename()d it into place, not left it behind.
        let tmp_path = {
            let mut tmp = path.as_os_str().to_owned();
            tmp.push(".tmp");
            PathBuf::from(tmp)
        };
        assert!(
            !tmp_path.exists(),
            "atomic-write tmp sibling should not be left behind after successful save"
        );
    }

    #[test]
    fn merge_snapshot_creates_when_absent() {
        let dir = tempfile::tempdir().unwrap();
        let path = snapshot_path(dir.path().to_str().unwrap(), "jp");
        // Sanity: nothing to load yet.
        assert!(load_snapshot(&path).unwrap().is_none());

        merge_snapshot(
            &path,
            Some("6.6.0.20"),
            Some("ios"),
            &[
                ("music/foo".into(), detail(100, 10)),
                ("music/bar".into(), detail(200, 20)),
            ],
        )
        .unwrap();

        let loaded = load_snapshot(&path).unwrap().unwrap();
        assert_eq!(loaded.version.as_deref(), Some("6.6.0.20"));
        assert_eq!(loaded.os.as_deref(), Some("ios"));
        assert_eq!(loaded.bundles.len(), 2);
        assert_eq!(loaded.bundles["music/foo"].crc, 100);
        assert_eq!(loaded.bundles["music/bar"].crc, 200);
    }

    #[test]
    fn merge_snapshot_accumulates_across_batches_within_same_version() {
        let dir = tempfile::tempdir().unwrap();
        let path = snapshot_path(dir.path().to_str().unwrap(), "jp");

        // Batch 1
        merge_snapshot(
            &path,
            Some("6.6.0.20"),
            Some("ios"),
            &[("music/foo".into(), detail(100, 10))],
        )
        .unwrap();
        // Batch 2 (same version): must accumulate on top of batch 1.
        merge_snapshot(
            &path,
            Some("6.6.0.20"),
            Some("ios"),
            &[
                ("music/bar".into(), detail(200, 20)),
                ("music/baz".into(), detail(300, 30)),
            ],
        )
        .unwrap();

        let loaded = load_snapshot(&path).unwrap().unwrap();
        assert_eq!(loaded.bundles.len(), 3);
        assert_eq!(loaded.bundles["music/foo"].crc, 100);
        assert_eq!(loaded.bundles["music/bar"].crc, 200);
        assert_eq!(loaded.bundles["music/baz"].crc, 300);
    }

    #[test]
    fn merge_snapshot_overwrites_existing_key_within_same_version() {
        let dir = tempfile::tempdir().unwrap();
        let path = snapshot_path(dir.path().to_str().unwrap(), "jp");

        merge_snapshot(
            &path,
            Some("6.6.0.20"),
            None,
            &[("music/foo".into(), detail(100, 10))],
        )
        .unwrap();
        // Same key with a different fingerprint → the newer one wins.
        merge_snapshot(
            &path,
            Some("6.6.0.20"),
            None,
            &[("music/foo".into(), detail(999, 11))],
        )
        .unwrap();

        let loaded = load_snapshot(&path).unwrap().unwrap();
        assert_eq!(loaded.bundles.len(), 1);
        assert_eq!(loaded.bundles["music/foo"].crc, 999);
        assert_eq!(loaded.bundles["music/foo"].file_size, 11);
    }

    #[test]
    fn merge_snapshot_discards_stale_snapshot_when_asset_version_changes() {
        let dir = tempfile::tempdir().unwrap();
        let path = snapshot_path(dir.path().to_str().unwrap(), "jp");

        // Old version snapshot.
        merge_snapshot(
            &path,
            Some("6.6.0.20"),
            Some("ios"),
            &[
                ("music/foo".into(), detail(100, 10)),
                ("music/bar".into(), detail(200, 20)),
            ],
        )
        .unwrap();

        // New asset_version: everything old must be dropped, only the new
        // entries survive.
        merge_snapshot(
            &path,
            Some("6.7.0.0"),
            Some("ios"),
            &[("music/baz".into(), detail(300, 30))],
        )
        .unwrap();

        let loaded = load_snapshot(&path).unwrap().unwrap();
        assert_eq!(loaded.version.as_deref(), Some("6.7.0.0"));
        assert_eq!(loaded.bundles.len(), 1);
        assert!(loaded.bundles.contains_key("music/baz"));
    }
}
