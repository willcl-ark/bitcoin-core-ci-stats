use anyhow::Result;
use chrono::{DateTime, Utc};
use regex::Regex;
use std::io::BufRead;
use std::sync::OnceLock;

use crate::models::{Command, TaskRuntimeStats};

/// Returns a compiled regex for extracting ccache hit rate percentages.
/// Pattern matches both "75.69%" and "100%" formats.
fn ccache_hitrate_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r"\(\s*(\d+(?:\.\d+)?%)\)").expect("valid ccache hitrate regex")
    })
}

/// Returns a compiled regex for parsing GitHub Actions log command lines.
fn command_pattern_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z) .*?\+ (.+)")
            .expect("valid command pattern regex")
    })
}

struct PendingCommand {
    cmd: String,
    start: DateTime<Utc>,
    line: usize,
    docker_cached: bool,
    ccache_hitrate: Option<f64>,
}

fn finalize_command(
    prev: PendingCommand,
    end: DateTime<Utc>,
    commands: &mut Vec<Command>,
    stats: &mut TaskRuntimeStats,
) {
    let duration = (end - prev.start).num_seconds();
    stats.process_command(&prev.cmd, duration, prev.docker_cached, prev.ccache_hitrate);
    if duration >= 1 {
        commands.push(Command {
            cmd: prev.cmd,
            line: prev.line,
            duration,
        });
    }
}

pub fn parse_job_log<R: BufRead>(
    reader: R,
    job_completed_at: i64,
) -> Result<(usize, Vec<Command>, TaskRuntimeStats)> {
    let mut commands = Vec::new();
    let mut runtime_stats = TaskRuntimeStats::default();
    let mut pending: Option<PendingCommand> = None;
    let mut line_count: usize = 0;

    for (line_num, line_result) in reader.lines().enumerate() {
        let line = line_result?;
        line_count = line_num + 1;

        if let Some(caps) = command_pattern_regex().captures(&line) {
            if let Ok(timestamp) = caps[1].parse::<DateTime<Utc>>() {
                if let Some(prev) = pending.take() {
                    finalize_command(prev, timestamp, &mut commands, &mut runtime_stats);
                }
                pending = Some(PendingCommand {
                    cmd: caps[2].to_string(),
                    start: timestamp,
                    line: line_num,
                    docker_cached: false,
                    ccache_hitrate: None,
                });
            }
        } else if let Some(ref mut p) = pending {
            if p.cmd.contains("docker build") && line.contains(" CACHED") {
                p.docker_cached = true;
            }
            if p.cmd.contains("ccache --show-stats")
                && line.contains("Hits:")
                && let Some(caps) = ccache_hitrate_regex().captures(&line)
            {
                p.ccache_hitrate = caps[1].trim_end_matches('%').parse::<f64>().ok();
            }
        }
    }

    if let Some(prev) = pending {
        let end = DateTime::from_timestamp(job_completed_at, 0).unwrap_or(prev.start);
        finalize_command(prev, end, &mut commands, &mut runtime_stats);
    }

    Ok((line_count, commands, runtime_stats))
}

#[cfg(test)]
mod tests {
    use super::parse_job_log;
    use chrono::DateTime;
    use std::fs;
    use std::io::Cursor;

    fn fixture_content() -> String {
        let path = format!(
            "{}/tests/fixtures/bitcoin-job-65254626824-excerpt.log",
            env!("CARGO_MANIFEST_DIR")
        );
        fs::read_to_string(path).expect("read fixture log")
    }

    #[test]
    fn parses_real_github_job_excerpt_fixture() {
        let completed_at = DateTime::parse_from_rfc3339("2026-02-28T17:04:37Z")
            .expect("valid timestamp")
            .timestamp();
        let content = fixture_content();
        let (line_count, commands, stats) =
            parse_job_log(Cursor::new(content), completed_at).expect("parse fixture");

        assert!(line_count >= 10);
        assert!(!commands.is_empty());
        assert!(commands.iter().any(|c| c.cmd.contains("docker buildx build")));
        assert!(commands.iter().any(|c| c.cmd.contains("cmake -S ")));
        assert!(commands.iter().any(|c| c.cmd.contains("cmake --build ")));
        assert!(commands.iter().any(|c| c.cmd.contains("ctest ")));

        assert!(stats.docker_build_cached);
        assert_eq!(stats.ccache_hitrate, Some(0.9));
        assert!(stats.build_duration.is_some());
        assert!(stats.unit_test_duration.is_some());
    }

    #[test]
    fn parses_truncated_log_without_failing() {
        let log = "\
2026-02-28T16:48:38.2495150Z + cmake -S /tmp -B /tmp/build\n\
2026-02-28T16:48:48.7307863Z + cmake --build /tmp/build -j4\n";
        let completed_at = DateTime::parse_from_rfc3339("2026-02-28T16:48:50Z")
            .expect("valid timestamp")
            .timestamp();
        let (_line_count, commands, stats) =
            parse_job_log(Cursor::new(log), completed_at).expect("parse truncated log");

        assert_eq!(commands.len(), 2);
        assert!(stats.configure_duration.is_some());
        assert!(stats.build_duration.is_some());
    }
}
