use anyhow::Result;
use chrono::{DateTime, Utc};
use clap::Parser;
use http_body_util::BodyExt;
use octocrab::Octocrab;
use regex::Regex;
use serde::de::Deserializer;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fs;
use std::io::{BufRead, BufReader, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::OnceLock;
use thiserror::Error;
use tracing::{info, warn};

/// Application-specific errors with structured variants.
#[derive(Debug, Error)]
enum AppError {
    #[error("GITHUB_TOKEN environment variable is required")]
    MissingGitHubToken,

    #[error("missing field '{0}' in GitHub API response")]
    MissingField(&'static str),

}

/// Returns a compiled regex for extracting ccache hit rate percentages.
/// Pattern matches both "75.69%" and "100%" formats.
fn ccache_hitrate_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"\((\d+(?:\.\d+)?%)\)").unwrap())
}

/// Returns a compiled regex for parsing GitHub Actions log command lines.
fn command_pattern_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z) .*?\+ (.+)").unwrap()
    })
}

fn deserialize_optional_f64<'de, D: Deserializer<'de>>(d: D) -> Result<Option<f64>, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum StringOrFloat {
        Float(f64),
        Str(String),
    }
    match Option::<StringOrFloat>::deserialize(d)? {
        Some(StringOrFloat::Float(v)) => Ok(Some(v)),
        Some(StringOrFloat::Str(s)) => Ok(s.trim_end_matches('%').parse::<f64>().ok()),
        None => Ok(None),
    }
}

const DEFAULT_RUNS_TO_QUERY: usize = 400;
const MAX_LOG_BYTES: usize = 512 * 1024 * 1024;
const TASKS_FILENAME: &str = "tasks.json";
const GRAPH_FILENAME: &str = "graph.json";
const CHECKPOINT_FILENAME: &str = ".checkpoint.json";

/// Status of a CI task or build.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum TaskStatus {
    Completed,
    Failed,
    Aborted,
}

fn is_terminal_conclusion(conclusion: Option<&str>) -> bool {
    matches!(
        conclusion,
        Some("success" | "failure" | "cancelled" | "skipped" | "timed_out" | "neutral")
    )
}

fn status_from_conclusion(conclusion: Option<&str>) -> TaskStatus {
    match conclusion {
        Some("success") | Some("neutral") => TaskStatus::Completed,
        Some("failure") | Some("timed_out") => TaskStatus::Failed,
        Some("cancelled") | Some("skipped") => TaskStatus::Aborted,
        _ => TaskStatus::Completed,
    }
}

fn required_u64(value: &serde_json::Value, field: &'static str) -> Result<u64> {
    value[field]
        .as_u64()
        .ok_or(AppError::MissingField(field).into())
}

fn required_str<'a>(value: &'a serde_json::Value, field: &'static str) -> Result<&'a str> {
    value[field]
        .as_str()
        .ok_or(AppError::MissingField(field).into())
}

#[derive(Parser, Debug)]
#[command(name = "fetch-tasks-github")]
#[command(about = "Fetch GitHub Actions workflow run data for Bitcoin Core CI stats")]
struct Args {
    #[arg(long, default_value = "bitcoin")]
    owner: String,

    #[arg(long, default_value = "bitcoin")]
    repository: String,

    #[arg(long, default_value_t = DEFAULT_RUNS_TO_QUERY)]
    runs: usize,

    #[arg(long, help = "Start fetching from this specific run ID")]
    since_run_id: Option<u64>,

    #[arg(long, help = "Enable checkpoint-based incremental fetching")]
    checkpoint: bool,

    #[arg(long, help = "Reset the checkpoint file")]
    reset_checkpoint: bool,

    #[arg(long, help = "Set backfill target run ID")]
    backfill_to: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Checkpoint {
    last_run_id: u64,
    last_fetched_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    backfill_cursor: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    backfill_target: Option<u64>,
}

impl Checkpoint {
    fn load() -> Result<Option<Self>> {
        if !std::path::Path::new(CHECKPOINT_FILENAME).exists() {
            return Ok(None);
        }

        let content = fs::read_to_string(CHECKPOINT_FILENAME)?;
        let checkpoint: Self = serde_json::from_str(&content)?;
        Ok(Some(checkpoint))
    }

    fn save(&self) -> Result<()> {
        let content = serde_json::to_string_pretty(self)?;
        fs::write(CHECKPOINT_FILENAME, content)?;
        info!(
            "Saved checkpoint: last_run_id={}, backfill_cursor={:?}, backfill_target={:?}",
            self.last_run_id, self.backfill_cursor, self.backfill_target
        );
        Ok(())
    }

    fn reset() -> Result<()> {
        if std::path::Path::new(CHECKPOINT_FILENAME).exists() {
            fs::remove_file(CHECKPOINT_FILENAME)?;
            info!("Checkpoint file deleted");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Build {
    id: u64,
    status: TaskStatus,
    branch: String,
    #[serde(rename = "changeIdInRepo")]
    change_id_in_repo: String,
    #[serde(rename = "changeMessageTitle")]
    change_message_title: String,
    #[serde(rename = "buildCreatedTimestamp")]
    build_created_timestamp: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Task {
    id: u64,
    status: TaskStatus,
    name: String,
    #[serde(rename = "creationTimestamp")]
    creation_timestamp: i64,
    #[serde(rename = "scheduledTimestamp")]
    scheduled_timestamp: i64,
    #[serde(rename = "executingTimestamp")]
    executing_timestamp: i64,
    duration: i64,
    #[serde(rename = "finalStatusTimestamp")]
    final_status_timestamp: i64,
    #[serde(rename = "executionInfoLabels")]
    execution_info_labels: Vec<String>,
    build: Build,
    log: String,
    #[serde(rename = "log_status_code")]
    log_status_code: u16,
    commands: Vec<Command>,
    #[serde(rename = "runtime_stats")]
    runtime_stats: TaskRuntimeStats,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Command {
    cmd: String,
    line: usize,
    duration: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TaskRuntimeStats {
    /// Compiler cache hit rate as a percentage (0.0-100.0)
    #[serde(
        rename = "ccache_hitrate",
        default,
        deserialize_with = "deserialize_optional_f64",
        skip_serializing_if = "Option::is_none"
    )]
    ccache_hitrate: Option<f64>,
    #[serde(rename = "docker_build_cached")]
    docker_build_cached: bool,
    #[serde(
        rename = "docker_build_duration",
        skip_serializing_if = "Option::is_none"
    )]
    docker_build_duration: Option<i64>,
    #[serde(
        rename = "ccache_zerostats_duration",
        skip_serializing_if = "Option::is_none"
    )]
    ccache_zerostats_duration: Option<i64>,
    #[serde(rename = "configure_duration", skip_serializing_if = "Option::is_none")]
    configure_duration: Option<i64>,
    #[serde(rename = "build_duration", skip_serializing_if = "Option::is_none")]
    build_duration: Option<i64>,
    #[serde(rename = "unit_test_duration", skip_serializing_if = "Option::is_none")]
    unit_test_duration: Option<i64>,
    #[serde(
        rename = "functional_test_duration",
        skip_serializing_if = "Option::is_none"
    )]
    functional_test_duration: Option<i64>,
    #[serde(
        rename = "depends_build_duration",
        skip_serializing_if = "Option::is_none"
    )]
    depends_build_duration: Option<i64>,
}

impl Default for TaskRuntimeStats {
    fn default() -> Self {
        Self {
            ccache_hitrate: None,
            docker_build_cached: false,
            docker_build_duration: None,
            ccache_zerostats_duration: None,
            configure_duration: None,
            build_duration: None,
            unit_test_duration: None,
            functional_test_duration: None,
            depends_build_duration: None,
        }
    }
}

impl TaskRuntimeStats {
    fn process_command(
        &mut self,
        cmd: &str,
        duration_secs: i64,
        docker_cached: bool,
        ccache_hitrate: Option<f64>,
    ) {
        if cmd.contains("docker build") {
            self.docker_build_duration = Some(duration_secs);
            self.docker_build_cached = docker_cached || duration_secs < 10;
        } else if cmd == "ccache --zero-stats" {
            self.ccache_zerostats_duration = Some(duration_secs);
        } else if cmd.contains("cmake -S ") {
            self.configure_duration = Some(duration_secs);
        } else if cmd.contains(" make ") && cmd.contains(" -C depends ") {
            self.depends_build_duration = Some(duration_secs);
        } else if cmd.contains("cmake --build ") {
            self.build_duration = Some(duration_secs);
        } else if cmd.contains("ccache --show-stats") {
            self.ccache_hitrate = ccache_hitrate;
        } else if cmd.contains("ctest ") {
            self.unit_test_duration = Some(duration_secs);
        } else if cmd.contains("test/functional/test_runner.py ") {
            self.functional_test_duration = Some(duration_secs);
        }
    }
}

struct PendingCommand {
    cmd: String,
    start: DateTime<Utc>,
    line: usize,
    docker_cached: bool,
    ccache_hitrate: Option<f64>,
}

struct TempFileGuard {
    path: PathBuf,
}

impl Drop for TempFileGuard {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct GraphStats {
    id: u64,
    name: String,
    duration: i64,
    #[serde(rename = "scheduleDuration")]
    schedule_duration: i64,
    #[serde(rename = "unitTestDuration")]
    unit_test_duration: i64,
    #[serde(rename = "functionalTestDuration")]
    functional_test_duration: i64,
    #[serde(rename = "buildDuration")]
    build_duration: i64,
    #[serde(rename = "ccacheHitrate")]
    ccache_hitrate: f64,
    created: i64,
}

impl From<&Task> for GraphStats {
    fn from(task: &Task) -> Self {
        Self {
            id: task.id,
            name: task.name.clone(),
            duration: task.duration,
            schedule_duration: task.executing_timestamp - task.creation_timestamp,
            unit_test_duration: task.runtime_stats.unit_test_duration.unwrap_or(-1),
            functional_test_duration: task.runtime_stats.functional_test_duration.unwrap_or(-1),
            build_duration: task.runtime_stats.build_duration.unwrap_or(-1),
            ccache_hitrate: task.runtime_stats.ccache_hitrate.unwrap_or(-1.0),
            created: task.creation_timestamp,
        }
    }
}

struct GitHubActionsFetcher {
    octocrab: Octocrab,
    owner: String,
    repo: String,
}

impl GitHubActionsFetcher {
    fn new(owner: String, repo: String) -> Result<Self> {
        let token = std::env::var("GITHUB_TOKEN").map_err(|_| AppError::MissingGitHubToken)?;

        let octocrab = Octocrab::builder().personal_token(token).build()?;

        Ok(Self {
            octocrab,
            owner,
            repo,
        })
    }

    async fn fetch_workflow_runs(
        &self,
        max_runs: usize,
        since_run_id: Option<u64>,
        existing_run_ids: &HashSet<u64>,
        backfill_cursor: Option<u64>,
        backfill_target: Option<u64>,
    ) -> Result<Vec<Task>> {
        let is_backfill = backfill_target.is_some();

        if is_backfill {
            info!(
                "Backfill mode: cursor={:?}, target={:?}",
                backfill_cursor, backfill_target
            );
        } else if let Some(since_id) = since_run_id {
            info!(
                "Fetching workflow runs since run ID {} from GitHub API...",
                since_id
            );
        } else {
            info!("Fetching workflow runs from GitHub API...");
        }

        let mut all_tasks = Vec::new();
        let mut page = 1;
        let per_page = 100;
        let mut new_job_count = 0;

        while new_job_count < max_runs {
            let url = format!(
                "/repos/{}/{}/actions/runs?per_page={}&page={}",
                self.owner, self.repo, per_page, page
            );

            let response: serde_json::Value = self.octocrab.get(&url, None::<&()>).await?;
            let runs = response["workflow_runs"]
                .as_array()
                .ok_or(AppError::MissingField("workflow_runs"))?;

            if runs.is_empty() {
                break;
            }

            for run_value in runs {
                if new_job_count >= max_runs {
                    break;
                }

                let conclusion = run_value["conclusion"].as_str();
                if !is_terminal_conclusion(conclusion) {
                    continue;
                }

                let run_id = run_value["id"]
                    .as_u64()
                    .ok_or(AppError::MissingField("id"))?;

                if is_backfill {
                    // Skip runs above the backfill cursor (already processed)
                    if let Some(cursor) = backfill_cursor {
                        if run_id >= cursor {
                            continue;
                        }
                    }

                    // Stop when we reach the backfill target
                    if let Some(target) = backfill_target {
                        if run_id <= target {
                            info!("Reached backfill target {}, stopping", target);
                            return Ok(all_tasks);
                        }
                    }

                    // Skip runs we already have
                    if existing_run_ids.contains(&run_id) {
                        continue;
                    }
                } else {
                    // Normal mode: stop at checkpoint
                    if let Some(since_id) = since_run_id {
                        if run_id <= since_id {
                            info!("Reached checkpoint run ID {}, stopping fetch", since_id);
                            return Ok(all_tasks);
                        }
                    }
                }

                // Fetch jobs for this workflow run
                let jobs_url = format!(
                    "/repos/{}/{}/actions/runs/{}/jobs",
                    self.owner, self.repo, run_id
                );
                let jobs_response: serde_json::Value =
                    self.octocrab.get(&jobs_url, None::<&()>).await?;
                let jobs = jobs_response["jobs"]
                    .as_array()
                    .ok_or(AppError::MissingField("jobs"))?;

                for job_value in jobs {
                    let job_name = job_value["name"].as_str().unwrap_or("");
                    if job_name.to_lowercase().contains("lint") {
                        continue;
                    }

                    let job_conclusion = job_value["conclusion"].as_str();
                    if !is_terminal_conclusion(job_conclusion) {
                        continue;
                    }

                    let task = self.convert_json_to_task(run_value, job_value).await?;
                    all_tasks.push(task);
                    new_job_count += 1;

                    if new_job_count >= max_runs {
                        break;
                    }
                }
            }

            page += 1;
        }

        info!("Fetched {} jobs from GitHub Actions", all_tasks.len());
        Ok(all_tasks)
    }

    async fn convert_json_to_task(
        &self,
        run_value: &serde_json::Value,
        job_value: &serde_json::Value,
    ) -> Result<Task> {
        let run_created_at_str = run_value["created_at"]
            .as_str()
            .ok_or(AppError::MissingField("created_at"))?;
        let run_created_at = DateTime::parse_from_rfc3339(run_created_at_str)?.timestamp();

        let job_started_at = if let Some(started_str) = job_value["started_at"].as_str() {
            DateTime::parse_from_rfc3339(started_str)?.timestamp()
        } else {
            run_created_at
        };

        let job_completed_at = if let Some(completed_str) = job_value["completed_at"].as_str() {
            DateTime::parse_from_rfc3339(completed_str)?.timestamp()
        } else {
            0
        };

        let duration = if job_completed_at > 0 {
            job_completed_at - job_started_at
        } else {
            0
        };

        // Map GitHub conclusion to task status
        let status = status_from_conclusion(Some(required_str(job_value, "conclusion")?));
        let build_status = status_from_conclusion(Some(required_str(run_value, "conclusion")?));

        let build = Build {
            id: required_u64(run_value, "id")?,
            status: build_status,
            branch: required_str(run_value, "head_branch")?.to_string(),
            change_id_in_repo: required_str(run_value, "head_sha")?.to_string(),
            change_message_title: required_str(run_value, "display_title")?.to_string(),
            build_created_timestamp: run_created_at,
        };

        // Fetch and parse job log
        let job_id = required_u64(job_value, "id")?;
        let (log_status_code, commands, runtime_stats) =
            self.fetch_and_parse_job_log(job_id, job_completed_at).await;

        Ok(Task {
            id: job_id,
            status,
            name: required_str(job_value, "name")?.to_string(),
            creation_timestamp: run_created_at,
            scheduled_timestamp: job_started_at,
            executing_timestamp: job_started_at,
            duration,
            final_status_timestamp: job_completed_at,
            execution_info_labels: vec![],
            build,
            log: "<cleared>".to_string(), // Clear log content to save space
            log_status_code,
            commands,
            runtime_stats,
        })
    }

    async fn fetch_and_parse_job_log(
        &self,
        job_id: u64,
        job_completed_at: i64,
    ) -> (u16, Vec<Command>, TaskRuntimeStats) {
        match self.download_and_parse_log(job_id, job_completed_at).await {
            Ok((line_count, commands, stats)) => {
                info!("Fetched log for job {}: {} lines", job_id, line_count);
                (200, commands, stats)
            }
            Err(e) => {
                warn!("Failed to process log for job {}: {}", job_id, e);
                (500, vec![], TaskRuntimeStats::default())
            }
        }
    }

    async fn download_and_parse_log(
        &self,
        job_id: u64,
        job_completed_at: i64,
    ) -> Result<(usize, Vec<Command>, TaskRuntimeStats)> {
        let url = format!(
            "https://api.github.com/repos/{}/{}/actions/jobs/{}/logs",
            self.owner, self.repo, job_id
        );

        let response = self.octocrab._get(url).await?;
        if !response.status().is_success() {
            anyhow::bail!("HTTP {}", response.status().as_u16());
        }

        let mut attempt = 0u32;
        let file_path = loop {
            let path = std::env::temp_dir().join(format!(
                "fetch-tasks-github-{}-{}-{}.log",
                job_id,
                std::process::id(),
                attempt
            ));
            match fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(&path)
            {
                Ok(_) => break path,
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                    attempt += 1;
                }
                Err(e) => return Err(e.into()),
            }
        };

        let _tmp_guard = TempFileGuard {
            path: file_path.clone(),
        };
        let mut file = fs::OpenOptions::new().read(true).write(true).open(&file_path)?;

        let mut body = Box::pin(http_body_util::Limited::new(
            response.into_body(),
            MAX_LOG_BYTES,
        ));
        while let Some(frame) = body.frame().await {
            match frame {
                Ok(f) => {
                    if let Some(data) = f.data_ref() {
                        file.write_all(data)?;
                    }
                }
                Err(e) => anyhow::bail!("{e}"),
            }
        }

        file.seek(SeekFrom::Start(0))?;
        let reader = BufReader::new(file);
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

fn load_existing_task_ids() -> Result<HashSet<u64>> {
    if !std::path::Path::new(TASKS_FILENAME).exists() {
        return Ok(HashSet::new());
    }

    let content = fs::read_to_string(TASKS_FILENAME)?;
    let tasks: Vec<Task> = serde_json::from_str(&content)?;
    Ok(tasks.into_iter().map(|task| task.id).collect())
}

fn load_existing_run_ids() -> Result<HashSet<u64>> {
    if !std::path::Path::new(TASKS_FILENAME).exists() {
        return Ok(HashSet::new());
    }

    let content = fs::read_to_string(TASKS_FILENAME)?;
    let tasks: Vec<Task> = serde_json::from_str(&content)?;
    Ok(tasks.into_iter().map(|task| task.build.id).collect())
}

fn save_tasks(tasks: &[Task]) -> Result<()> {
    // Load existing tasks
    let mut all_tasks = if std::path::Path::new(TASKS_FILENAME).exists() {
        let content = fs::read_to_string(TASKS_FILENAME)?;
        serde_json::from_str::<Vec<Task>>(&content)?
    } else {
        Vec::new()
    };

    // Add new tasks
    all_tasks.extend_from_slice(tasks);

    // Sort by creation timestamp
    all_tasks.sort_by_key(|task| task.creation_timestamp);

    // Save tasks
    let tasks_json = serde_json::to_string_pretty(&all_tasks)?;
    fs::write(TASKS_FILENAME, tasks_json)?;

    // Create graph stats for completed tasks
    let graph_stats: Vec<GraphStats> = all_tasks
        .iter()
        .filter(|task| task.status == TaskStatus::Completed)
        .map(GraphStats::from)
        .collect();

    let graph_json = serde_json::to_string_pretty(&graph_stats)?;
    fs::write(GRAPH_FILENAME, graph_json)?;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    info!(
        "Starting GitHub Actions fetcher for {}/{}",
        args.owner, args.repository
    );

    // Handle checkpoint reset
    if args.reset_checkpoint {
        Checkpoint::reset()?;
        return Ok(());
    }

    // Load checkpoint if enabled
    let mut checkpoint = if args.checkpoint {
        Checkpoint::load()?
    } else {
        None
    };

    // Initialize backfill if --backfill-to is provided
    if let Some(target) = args.backfill_to {
        let cp = checkpoint.get_or_insert(Checkpoint {
            last_run_id: 0,
            last_fetched_at: chrono::Utc::now(),
            backfill_cursor: None,
            backfill_target: None,
        });
        cp.backfill_target = Some(target);
        if cp.backfill_cursor.is_none() && cp.last_run_id > target {
            cp.backfill_cursor = Some(cp.last_run_id);
        }
        info!(
            "Backfill initialized: cursor={:?}, target={}",
            cp.backfill_cursor, target
        );
    }

    let is_backfill = checkpoint
        .as_ref()
        .is_some_and(|c| c.backfill_target.is_some());

    // Determine since_run_id from checkpoint or CLI argument (normal mode only)
    let since_run_id = if is_backfill {
        None
    } else {
        args.since_run_id
            .or_else(|| checkpoint.as_ref().map(|c| c.last_run_id))
    };

    if let Some(since_id) = since_run_id {
        info!("Using incremental fetch from run ID: {}", since_id);
    }

    // Load existing IDs to avoid duplicates
    let existing_task_ids = load_existing_task_ids()?;
    let existing_run_ids = load_existing_run_ids()?;
    info!(
        "Found {} existing tasks, {} existing runs",
        existing_task_ids.len(),
        existing_run_ids.len()
    );

    let fetcher = GitHubActionsFetcher::new(args.owner, args.repository)?;

    let backfill_cursor = checkpoint.as_ref().and_then(|c| c.backfill_cursor);
    let backfill_target = checkpoint.as_ref().and_then(|c| c.backfill_target);

    let tasks = fetcher
        .fetch_workflow_runs(
            args.runs,
            since_run_id,
            &existing_run_ids,
            backfill_cursor,
            backfill_target,
        )
        .await?;

    // Filter out existing tasks (by job ID)
    let new_tasks: Vec<Task> = tasks
        .into_iter()
        .filter(|task| !existing_task_ids.contains(&task.id))
        .collect();

    info!("Found {} new tasks to process", new_tasks.len());

    if new_tasks.is_empty() {
        info!("No new tasks to process");
        // Still update backfill cursor if in backfill mode with no new tasks
        // (all runs in this page range were already fetched)
        if args.checkpoint && is_backfill {
            if let Some(ref mut cp) = checkpoint {
                // Backfill made no progress — likely complete or stuck
                info!("Backfill produced no new tasks; clearing backfill fields");
                cp.backfill_cursor = None;
                cp.backfill_target = None;
                cp.last_fetched_at = chrono::Utc::now();
                cp.save()?;
            }
        }
        return Ok(());
    }

    save_tasks(&new_tasks)?;
    info!("Successfully saved {} new tasks", new_tasks.len());

    // Update checkpoint
    if args.checkpoint {
        if is_backfill {
            if let Some(ref mut cp) = checkpoint {
                let min_run_id = new_tasks.iter().map(|t| t.build.id).min().unwrap();
                cp.backfill_cursor = Some(min_run_id);
                cp.last_fetched_at = chrono::Utc::now();

                if min_run_id <= cp.backfill_target.unwrap() {
                    info!("Backfill complete! Clearing backfill fields.");
                    cp.backfill_cursor = None;
                    cp.backfill_target = None;
                }
                cp.save()?;
            }
        } else if let Some(max_run_id) = new_tasks.iter().map(|t| t.build.id).max() {
            let last_run_id = checkpoint
                .as_ref()
                .map(|c| c.last_run_id.max(max_run_id))
                .unwrap_or(max_run_id);
            let new_checkpoint = Checkpoint {
                last_run_id,
                last_fetched_at: chrono::Utc::now(),
                backfill_cursor: None,
                backfill_target: None,
            };
            new_checkpoint.save()?;
        }
    }

    Ok(())
}
