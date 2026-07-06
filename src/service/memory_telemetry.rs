#![cfg_attr(not(target_os = "linux"), allow(dead_code, unused_imports))]

use std::collections::{HashMap, HashSet, VecDeque};
use std::fs;
use std::path::Path;
use std::time::Duration;

use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

const DEFAULT_INTERVAL_SECONDS: u64 = 60;

pub fn spawn_memory_telemetry(cancel: CancellationToken) {
    #[cfg(target_os = "linux")]
    spawn_linux_memory_telemetry(cancel);

    #[cfg(not(target_os = "linux"))]
    {
        let _ = cancel;
        debug!("process memory telemetry is only enabled on Linux");
    }
}

#[cfg(target_os = "linux")]
fn spawn_linux_memory_telemetry(cancel: CancellationToken) {
    let interval = telemetry_interval();
    if interval.is_zero() {
        debug!("process memory telemetry disabled");
        return;
    }

    tokio::spawn(async move {
        let root_pid = std::process::id();
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = ticker.tick() => {
                    match sample_process_tree(root_pid) {
                        Ok(sample) => log_sample(root_pid, &sample),
                        Err(err) => tracing::warn!(error = %err, "failed to sample process memory telemetry"),
                    }
                }
            }
        }
    });
}

fn telemetry_interval() -> Duration {
    let seconds = std::env::var("HARUKI_MEMORY_TELEMETRY_INTERVAL_SECONDS")
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .unwrap_or(DEFAULT_INTERVAL_SECONDS);
    Duration::from_secs(seconds)
}

fn log_sample(root_pid: u32, processes: &[ProcessMemorySample]) {
    if processes.is_empty() {
        return;
    }
    let main_rss_kb = processes
        .iter()
        .find(|process| process.pid == root_pid)
        .map(|process| process.rss_kb)
        .unwrap_or_default();
    let total_rss_kb: u64 = processes.iter().map(|process| process.rss_kb).sum();
    let total_pss_kb: u64 = processes
        .iter()
        .map(|process| process.pss_kb.unwrap_or(0))
        .sum();
    let total_anon_kb: u64 = processes.iter().map(|process| process.rss_anon_kb).sum();
    let child_rss_kb = total_rss_kb.saturating_sub(main_rss_kb);
    let worker_rss_kb: u64 = processes
        .iter()
        .filter(|process| is_assetstudio_worker(&process.name))
        .map(|process| process.rss_kb)
        .sum();
    let worker_count = processes
        .iter()
        .filter(|process| is_assetstudio_worker(&process.name))
        .count();
    let cgroup = read_cgroup_memory();
    let top_processes = format_top_processes(processes, 6);

    info!(
        root_pid,
        process_count = processes.len(),
        worker_count,
        total_rss_mb = kb_to_mb(total_rss_kb),
        total_pss_mb = kb_to_mb(total_pss_kb),
        total_anon_mb = kb_to_mb(total_anon_kb),
        main_rss_mb = kb_to_mb(main_rss_kb),
        child_rss_mb = kb_to_mb(child_rss_kb),
        assetstudio_worker_rss_mb = kb_to_mb(worker_rss_kb),
        cgroup_current_mb = cgroup.current_bytes.map(bytes_to_mb).unwrap_or_default(),
        cgroup_limit_mb = cgroup.limit_bytes.map(bytes_to_mb).unwrap_or_default(),
        top_processes = %top_processes,
        "process tree memory telemetry"
    );
}

fn is_assetstudio_worker(name: &str) -> bool {
    name.contains("assetstudio_ffi_worker") || name.contains("assetstudio")
}

fn format_top_processes(processes: &[ProcessMemorySample], limit: usize) -> String {
    let mut sorted = processes.to_vec();
    sorted.sort_by_key(|process| std::cmp::Reverse(process.rss_kb));
    sorted
        .into_iter()
        .take(limit)
        .map(|process| {
            format!(
                "{}:{}:{}MiB anon={}MiB pss={}MiB",
                process.pid,
                process.name,
                kb_to_mb(process.rss_kb),
                kb_to_mb(process.rss_anon_kb),
                process.pss_kb.map(kb_to_mb).unwrap_or_default()
            )
        })
        .collect::<Vec<_>>()
        .join(",")
}

#[derive(Debug, Clone)]
struct ProcessMemorySample {
    pid: u32,
    ppid: u32,
    name: String,
    rss_kb: u64,
    rss_anon_kb: u64,
    pss_kb: Option<u64>,
}

fn sample_process_tree(root_pid: u32) -> std::io::Result<Vec<ProcessMemorySample>> {
    let mut all = Vec::new();
    for entry in fs::read_dir("/proc")? {
        let entry = entry?;
        let file_name = entry.file_name();
        let Some(pid) = file_name
            .to_str()
            .and_then(|value| value.parse::<u32>().ok())
        else {
            continue;
        };
        if let Some(sample) = read_process_sample(pid) {
            all.push(sample);
        }
    }

    let children = descendants(root_pid, &all);
    Ok(all
        .into_iter()
        .filter(|sample| sample.pid == root_pid || children.contains(&sample.pid))
        .collect())
}

fn descendants(root_pid: u32, processes: &[ProcessMemorySample]) -> HashSet<u32> {
    let mut by_parent: HashMap<u32, Vec<u32>> = HashMap::new();
    for process in processes {
        by_parent.entry(process.ppid).or_default().push(process.pid);
    }

    let mut out = HashSet::new();
    let mut queue = VecDeque::from([root_pid]);
    while let Some(parent) = queue.pop_front() {
        if let Some(children) = by_parent.get(&parent) {
            for &child in children {
                if out.insert(child) {
                    queue.push_back(child);
                }
            }
        }
    }
    out
}

fn read_process_sample(pid: u32) -> Option<ProcessMemorySample> {
    let status = fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    let mut name = String::new();
    let mut ppid = 0;
    let mut rss_kb = 0;
    let mut rss_anon_kb = 0;

    for line in status.lines() {
        if let Some(value) = line.strip_prefix("Name:") {
            name = value.trim().to_string();
        } else if let Some(value) = line.strip_prefix("PPid:") {
            ppid = parse_first_u64(value) as u32;
        } else if let Some(value) = line.strip_prefix("VmRSS:") {
            rss_kb = parse_first_u64(value);
        } else if let Some(value) = line.strip_prefix("RssAnon:") {
            rss_anon_kb = parse_first_u64(value);
        }
    }

    Some(ProcessMemorySample {
        pid,
        ppid,
        name,
        rss_kb,
        rss_anon_kb,
        pss_kb: read_smaps_rollup_pss(pid),
    })
}

fn read_smaps_rollup_pss(pid: u32) -> Option<u64> {
    let rollup = fs::read_to_string(format!("/proc/{pid}/smaps_rollup")).ok()?;
    for line in rollup.lines() {
        if let Some(value) = line.strip_prefix("Pss:") {
            return Some(parse_first_u64(value));
        }
    }
    None
}

#[derive(Default)]
struct CgroupMemory {
    current_bytes: Option<u64>,
    limit_bytes: Option<u64>,
}

fn read_cgroup_memory() -> CgroupMemory {
    let current_bytes = read_u64_file("/sys/fs/cgroup/memory.current")
        .or_else(|| read_u64_file("/sys/fs/cgroup/memory/memory.usage_in_bytes"));
    let limit_bytes = read_cgroup_limit("/sys/fs/cgroup/memory.max")
        .or_else(|| read_cgroup_limit("/sys/fs/cgroup/memory/memory.limit_in_bytes"));
    CgroupMemory {
        current_bytes,
        limit_bytes,
    }
}

fn read_cgroup_limit(path: impl AsRef<Path>) -> Option<u64> {
    let raw = fs::read_to_string(path).ok()?;
    let trimmed = raw.trim();
    if trimmed == "max" {
        None
    } else {
        trimmed.parse::<u64>().ok()
    }
}

fn read_u64_file(path: impl AsRef<Path>) -> Option<u64> {
    fs::read_to_string(path).ok()?.trim().parse::<u64>().ok()
}

fn parse_first_u64(value: &str) -> u64 {
    value
        .split_whitespace()
        .next()
        .and_then(|part| part.parse::<u64>().ok())
        .unwrap_or_default()
}

fn kb_to_mb(kb: u64) -> u64 {
    bytes_to_mb(kb.saturating_mul(1024))
}

fn bytes_to_mb(bytes: u64) -> u64 {
    bytes / 1024 / 1024
}
