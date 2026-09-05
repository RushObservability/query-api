//! Portable process and Tokio runtime gauges.
//!
//! Linux exposes the richest process view through `/proc`; Unix `getrusage`
//! supplies CPU time and a peak-memory fallback on macOS. Missing OS counters
//! are reported as zero rather than making the metrics endpoint fail.

use crate::self_metrics::SelfMetrics;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

pub fn spawn(metrics: Arc<SelfMetrics>) {
    tokio::spawn(async move {
        let interval_secs = std::env::var("RUSH_RUNTIME_METRICS_INTERVAL_SECS")
            .ok()
            .and_then(|value| value.trim().parse::<u64>().ok())
            .filter(|value| *value >= 1)
            .unwrap_or(15);
        let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));
        loop {
            interval.tick().await;
            sample(&metrics);
        }
    });
}

pub fn sample(metrics: &SelfMetrics) {
    let process = ProcessSnapshot::read();
    metrics.set_gauge(
        "rush_process_resident_memory_bytes",
        &[],
        process.resident_memory_bytes as f64,
    );
    metrics.set_gauge(
        "rush_process_max_resident_memory_bytes",
        &[],
        process.max_resident_memory_bytes as f64,
    );
    metrics.set_gauge(
        "rush_process_memory_limit_bytes",
        &[],
        process.memory_limit_bytes as f64,
    );
    metrics.set_gauge("rush_process_cpu_seconds_total", &[], process.cpu_seconds);
    metrics.set_gauge(
        "rush_process_cpu_utilization_ratio",
        &[],
        process.cpu_utilization_ratio,
    );
    metrics.set_gauge("rush_process_open_fds", &[], process.open_fds as f64);
    metrics.set_gauge(
        "rush_process_open_fds_limit",
        &[],
        process.open_fds_limit as f64,
    );
    metrics.set_gauge("rush_process_threads", &[], process.threads as f64);
    metrics.set_gauge(
        "rush_process_start_time_seconds",
        &[],
        process.start_time_seconds,
    );

    let runtime = tokio::runtime::Handle::current().metrics();
    metrics.set_gauge("rush_runtime_workers", &[], runtime.num_workers() as f64);
    metrics.set_gauge(
        "rush_runtime_alive_tasks",
        &[],
        runtime.num_alive_tasks() as f64,
    );
}

struct ProcessSnapshot {
    resident_memory_bytes: u64,
    max_resident_memory_bytes: u64,
    memory_limit_bytes: u64,
    cpu_seconds: f64,
    cpu_utilization_ratio: f64,
    open_fds: u64,
    open_fds_limit: u64,
    threads: u64,
    start_time_seconds: f64,
}

impl ProcessSnapshot {
    fn read() -> Self {
        let (cpu_seconds, max_resident_memory_bytes) = resource_usage();
        let cpu_utilization_ratio = cpu_utilization_ratio(cpu_seconds);
        Self {
            resident_memory_bytes: current_resident_memory(max_resident_memory_bytes),
            max_resident_memory_bytes,
            memory_limit_bytes: process_memory_limit_bytes(),
            cpu_seconds,
            cpu_utilization_ratio,
            open_fds: proc_count("/proc/self/fd"),
            open_fds_limit: open_file_limit(),
            threads: proc_threads(),
            start_time_seconds: process_start_time_seconds(),
        }
    }
}

fn process_memory_limit_bytes() -> u64 {
    [
        "/sys/fs/cgroup/memory.max",
        "/sys/fs/cgroup/memory/memory.limit_in_bytes",
    ]
    .iter()
    .find_map(|path| {
        std::fs::read_to_string(path)
            .ok()
            .map(|value| parse_cgroup_memory_limit(&value))
            .filter(|value| *value > 0)
    })
    .unwrap_or_else(physical_memory_bytes)
}

fn parse_cgroup_memory_limit(value: &str) -> u64 {
    let value = value.trim();
    if value.eq_ignore_ascii_case("max") {
        return 0;
    }
    value
        .parse::<u64>()
        .ok()
        // cgroup v1 represents an unlimited value as a number close to i64::MAX.
        .filter(|limit| *limit < (1_u64 << 60))
        .unwrap_or(0)
}

fn cpu_utilization_ratio(cpu_seconds: f64) -> f64 {
    static PREVIOUS: OnceLock<Mutex<Option<(Instant, f64)>>> = OnceLock::new();
    let now = Instant::now();
    let Ok(mut previous) = PREVIOUS.get_or_init(|| Mutex::new(None)).lock() else {
        return 0.0;
    };
    let ratio = previous
        .map(|(sampled_at, sampled_cpu)| {
            let wall_seconds = now.duration_since(sampled_at).as_secs_f64();
            let cpu_delta = (cpu_seconds - sampled_cpu).max(0.0);
            if wall_seconds > 0.0 {
                cpu_delta / wall_seconds / cpu_capacity_cores()
            } else {
                0.0
            }
        })
        .unwrap_or(0.0);
    *previous = Some((now, cpu_seconds));
    ratio.max(0.0)
}

fn cpu_capacity_cores() -> f64 {
    if let Ok(value) = std::fs::read_to_string("/sys/fs/cgroup/cpu.max") {
        if let Some(cores) = parse_cpu_max(&value) {
            return cores;
        }
    }
    if let (Ok(quota), Ok(period)) = (
        std::fs::read_to_string("/sys/fs/cgroup/cpu/cpu.cfs_quota_us"),
        std::fs::read_to_string("/sys/fs/cgroup/cpu/cpu.cfs_period_us"),
    ) {
        if let (Ok(quota), Ok(period)) = (quota.trim().parse::<f64>(), period.trim().parse::<f64>())
        {
            if quota > 0.0 && period > 0.0 {
                return (quota / period).max(0.01);
            }
        }
    }
    logical_cpu_count()
}

fn parse_cpu_max(value: &str) -> Option<f64> {
    let mut fields = value.split_whitespace();
    let quota = fields.next()?;
    let period = fields.next()?.parse::<f64>().ok()?;
    if quota == "max" || period <= 0.0 {
        return None;
    }
    let quota = quota.parse::<f64>().ok()?;
    (quota > 0.0).then(|| (quota / period).max(0.01))
}

fn logical_cpu_count() -> f64 {
    #[cfg(unix)]
    {
        let count = unsafe { libc::sysconf(libc::_SC_NPROCESSORS_ONLN) };
        if count > 0 {
            return count as f64;
        }
    }
    1.0
}

fn physical_memory_bytes() -> u64 {
    #[cfg(unix)]
    {
        let pages = unsafe { libc::sysconf(libc::_SC_PHYS_PAGES) };
        let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        if pages > 0 && page_size > 0 {
            return (pages as u64).saturating_mul(page_size as u64);
        }
    }
    0
}

#[cfg(unix)]
fn open_file_limit() -> u64 {
    let mut limit = unsafe { std::mem::zeroed::<libc::rlimit>() };
    if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) } != 0
        || limit.rlim_cur == libc::RLIM_INFINITY
    {
        return 0;
    }
    limit.rlim_cur as u64
}

#[cfg(not(unix))]
fn open_file_limit() -> u64 {
    0
}

fn process_start_time_seconds() -> f64 {
    static START: std::sync::OnceLock<f64> = std::sync::OnceLock::new();
    *START.get_or_init(|| {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_secs_f64())
            .unwrap_or(0.0)
    })
}

fn current_resident_memory(fallback: u64) -> u64 {
    #[cfg(target_os = "linux")]
    {
        let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        if page_size > 0 {
            if let Ok(statm) = std::fs::read_to_string("/proc/self/statm") {
                if let Some(pages) = statm
                    .split_whitespace()
                    .nth(1)
                    .and_then(|v| v.parse::<u64>().ok())
                {
                    return pages.saturating_mul(page_size as u64);
                }
            }
        }
    }
    fallback
}

fn proc_count(path: &str) -> u64 {
    std::fs::read_dir(path)
        .map(|entries| entries.filter_map(Result::ok).count() as u64)
        .unwrap_or(0)
}

fn proc_threads() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|status| {
            status.lines().find_map(|line| {
                line.strip_prefix("Threads:")
                    .and_then(|value| value.trim().parse::<u64>().ok())
            })
        })
        .unwrap_or(0)
}

#[cfg(unix)]
fn resource_usage() -> (f64, u64) {
    let mut usage = unsafe { std::mem::zeroed::<libc::rusage>() };
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) } != 0 {
        return (0.0, 0);
    }
    let cpu = timeval_seconds(usage.ru_utime) + timeval_seconds(usage.ru_stime);
    let peak = if cfg!(target_os = "macos") {
        usage.ru_maxrss.max(0) as u64
    } else {
        (usage.ru_maxrss.max(0) as u64).saturating_mul(1024)
    };
    (cpu, peak)
}

#[cfg(not(unix))]
fn resource_usage() -> (f64, u64) {
    (0.0, 0)
}

#[cfg(unix)]
fn timeval_seconds(value: libc::timeval) -> f64 {
    value.tv_sec as f64 + value.tv_usec as f64 / 1_000_000.0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_is_safe_without_procfs() {
        let snapshot = ProcessSnapshot::read();
        assert!(snapshot.cpu_seconds.is_finite());
        assert!(snapshot.cpu_utilization_ratio.is_finite());
        assert!(snapshot.start_time_seconds > 0.0);
    }

    #[test]
    fn cgroup_memory_limit_parser_rejects_unlimited_values() {
        assert_eq!(parse_cgroup_memory_limit("max"), 0);
        assert_eq!(parse_cgroup_memory_limit("9223372036854771712"), 0);
        assert_eq!(parse_cgroup_memory_limit("1073741824"), 1_073_741_824);
    }

    #[test]
    fn cpu_quota_parser_returns_available_cores() {
        assert_eq!(parse_cpu_max("200000 100000"), Some(2.0));
        assert_eq!(parse_cpu_max("50000 100000"), Some(0.5));
        assert_eq!(parse_cpu_max("max 100000"), None);
    }
}
