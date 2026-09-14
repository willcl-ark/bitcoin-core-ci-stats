use anyhow::Result;
use chrono::{DateTime, Utc};
use regex::Regex;
use std::io::BufRead;
use std::sync::OnceLock;

use crate::models::{Command, TaskRuntimeStats, TestKind, TestTiming};

/// Returns a compiled regex for extracting ccache hit rate percentages.
/// Pattern matches both "75.69%" and "100%" formats.
fn ccache_hitrate_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"\(\s*(\d+(?:\.\d+)?%)\)").expect("valid ccache hitrate regex"))
}

/// Returns a compiled regex for parsing GitHub Actions log command lines.
fn command_pattern_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z) .*?\+ (.+)")
            .expect("valid command pattern regex")
    })
}

fn unit_test_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r"\b\d+/\d+ Test #\d+: (.+?) \.{2,}\s+\*{0,3}(Passed|Failed|Not Run|Timeout)\s+([\d.]+) sec")
            .expect("valid unit test regex")
    })
}

fn functional_test_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r"Z\s+(.+?\.py(?:\s+[^|]*?)?)\s+\|\s+(.+?)\s+\|\s+(\d+) s(?:\s|$)")
            .expect("valid functional test regex")
    })
}

fn ansi_escape_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"\x1b\[[0-9;]*[A-Za-z]").expect("valid ANSI escape regex"))
}

fn parse_test_timing(line: &str) -> Option<TestTiming> {
    let stripped = line
        .contains('\x1b')
        .then(|| ansi_escape_regex().replace_all(line, ""));
    let line = stripped.as_deref().unwrap_or(line);
    if let Some(caps) = unit_test_regex().captures(line) {
        let duration_ms = (caps[3].parse::<f64>().ok()? * 1000.0).round() as u64;
        return Some(TestTiming {
            kind: TestKind::Unit,
            name: caps[1].to_string(),
            duration_ms,
            status: caps[2].to_string(),
        });
    }
    let caps = functional_test_regex().captures(line)?;
    Some(TestTiming {
        kind: TestKind::Functional,
        name: caps[1].trim().to_string(),
        duration_ms: caps[3].parse::<u64>().ok()? * 1000,
        status: caps[2].trim().to_string(),
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
) -> Result<(usize, Vec<Command>, TaskRuntimeStats, Vec<TestTiming>)> {
    let mut commands = Vec::new();
    let mut runtime_stats = TaskRuntimeStats::default();
    let mut test_timings = Vec::new();
    let mut pending: Option<PendingCommand> = None;
    let mut line_count: usize = 0;

    for (line_num, line_result) in reader.lines().enumerate() {
        let line = line_result?;
        line_count = line_num + 1;

        if let Some(timing) = parse_test_timing(&line) {
            test_timings.push(timing);
        }

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

    Ok((line_count, commands, runtime_stats, test_timings))
}

#[cfg(test)]
mod tests {
    use super::parse_job_log;
    use crate::models::TestKind;
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
        let (line_count, commands, stats, _) =
            parse_job_log(Cursor::new(content), completed_at).expect("parse fixture");

        assert!(line_count >= 10);
        assert!(!commands.is_empty());
        assert!(
            commands
                .iter()
                .any(|c| c.cmd.contains("docker buildx build"))
        );
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
        let (_line_count, commands, stats, _) =
            parse_job_log(Cursor::new(log), completed_at).expect("parse truncated log");

        assert_eq!(commands.len(), 2);
        assert!(stats.configure_duration.is_some());
        assert!(stats.build_duration.is_some());
    }

    #[test]
    fn parses_windows_test_summaries() {
        let log = "\
2026-09-13T23:43:04.3645655Z  97/376 Test #100: secp256k1.noverify_tests.test_recipient_sort ........   Passed    0.03 sec\n\
2026-09-13T23:44:01.5174361Z 220/376 Test #222: test_bitcoin-qt ........   Passed    5.84 sec\n\
2026-09-14T00:01:52.0330890Z example_test.py                                         | ✓ Passed  | 2 s\n\
2026-09-14T00:01:52.0341414Z feature_bip68_sequence.py                            | ✓ Passed  | 27 s\n\
2026-09-14T00:01:52.0341414Z feature_failed.py                                    | ✖ Failed  | 4 s\n\
2026-09-13T23:44:01.5174361Z 221/376 Test #223: failed_unit ........   ***Failed    1.25 sec\n\
2026-09-14T00:01:52.0341414Z \x1b[0m\x1b[0;32minterface_http.py\x1b[0m                       | ✓ Passed  | 12 s\n\
2026-09-14T00:01:52.0848207Z ALL                                  | ✓ Passed  | 3661 s (accumulated)\n";
        let (_, _, _, timings) = parse_job_log(Cursor::new(log), 0).expect("parse test summaries");
        assert_eq!(timings.len(), 7);
        assert_eq!(timings[0].kind, TestKind::Unit);
        assert_eq!(
            timings[0].name,
            "secp256k1.noverify_tests.test_recipient_sort"
        );
        assert_eq!(timings[0].duration_ms, 30);
        assert_eq!(timings[1].duration_ms, 5840);
        assert_eq!(timings[2].kind, TestKind::Functional);
        assert_eq!(timings[2].name, "example_test.py");
        assert_eq!(timings[2].duration_ms, 2000);
        assert_eq!(timings[3].duration_ms, 27000);
        assert!(timings.iter().any(|timing| timing.name == "failed_unit"
            && timing.status == "Failed"
            && timing.duration_ms == 1250));
        assert!(
            timings
                .iter()
                .any(|timing| timing.name == "feature_failed.py"
                    && timing.status == "✖ Failed"
                    && timing.duration_ms == 4000)
        );
        assert!(
            timings
                .iter()
                .any(|timing| timing.name == "interface_http.py" && timing.duration_ms == 12000)
        );
    }
}
