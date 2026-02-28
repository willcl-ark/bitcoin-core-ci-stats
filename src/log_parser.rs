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
    RE.get_or_init(|| Regex::new(r"\((\d+(?:\.\d+)?%)\)").expect("valid ccache hitrate regex"))
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
