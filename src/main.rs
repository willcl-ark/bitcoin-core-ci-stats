use anyhow::{Result, anyhow};
use chrono::{DateTime, Utc};
use clap::Parser;
use http_body_util::BodyExt;
use octocrab::Octocrab;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::fs;
use tracing::{info, warn};

const MIN_COMMAND_DURATION_SEC: u64 = 1;
const DEFAULT_RUNS_TO_QUERY: usize = 400;
const TASKS_FILENAME: &str = "tasks.json";
const GRAPH_FILENAME: &str = "graph.json";
const CHECKPOINT_FILENAME: &str = ".checkpoint.json";

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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Checkpoint {
    last_run_id: u64,
    last_fetched_at: DateTime<Utc>,
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
        info!("Saved checkpoint: last_run_id={}", self.last_run_id);
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
    status: String,
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
    status: String,
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
    #[serde(rename = "ccache_hitrate")]
    ccache_hitrate: String,
    #[serde(rename = "docker_build_cached")]
    docker_build_cached: bool,
    #[serde(rename = "docker_build_duration")]
    docker_build_duration: i64,
    #[serde(rename = "ccache_zerostats_duration")]
    ccache_zerostats_duration: i64,
    #[serde(rename = "configure_duration")]
    configure_duration: i64,
    #[serde(rename = "build_duration")]
    build_duration: i64,
    #[serde(rename = "unit_test_duration")]
    unit_test_duration: i64,
    #[serde(rename = "functional_test_duration")]
    functional_test_duration: i64,
    #[serde(rename = "depends_build_duration")]
    depends_build_duration: i64,
}

impl Default for TaskRuntimeStats {
    fn default() -> Self {
        Self {
            ccache_hitrate: String::new(),
            docker_build_cached: false,
            docker_build_duration: -1,
            ccache_zerostats_duration: -1,
            configure_duration: -1,
            build_duration: -1,
            unit_test_duration: -1,
            functional_test_duration: -1,
            depends_build_duration: -1,
        }
    }
}

impl TaskRuntimeStats {
    fn process_command(&mut self, cmd: &str, duration_secs: i64, output_lines: &[String]) {
        if cmd.contains("docker build") {
            self.docker_build_duration = duration_secs;

            // Check if docker build was cached
            for line in output_lines {
                if line.contains(" CACHED") || duration_secs < 10 {
                    self.docker_build_cached = true;
                    break;
                }
            }
        } else if cmd == "ccache --zero-stats" {
            self.ccache_zerostats_duration = duration_secs;
        } else if cmd.contains("cmake -S ") {
            self.configure_duration = duration_secs;
        } else if cmd.contains(" make ") && cmd.contains(" -C depends ") {
            self.depends_build_duration = duration_secs;
        } else if cmd.contains("cmake --build ") {
            self.build_duration = duration_secs;
        } else if cmd.contains("ccache --show-stats") {
            // Extract ccache hit rate
            let re = Regex::new(r"\((\d+(?:\.\d+)?%)\)").unwrap();
            for line in output_lines {
                if line.contains("Hits:")
                    && let Some(caps) = re.captures(line)
                {
                    self.ccache_hitrate = caps[1].to_string();
                    break;
                }
            }
        } else if cmd.contains("ctest ") {
            self.unit_test_duration = duration_secs;
        } else if cmd.contains("test/functional/test_runner.py ") {
            self.functional_test_duration = duration_secs;
        }
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
        let ccache_hitrate = if task.runtime_stats.ccache_hitrate.is_empty() {
            -1.0
        } else {
            task.runtime_stats
                .ccache_hitrate
                .replace("%", "")
                .parse::<f64>()
                .unwrap_or(-1.0)
        };

        Self {
            id: task.id,
            name: task.name.clone(),
            duration: task.duration,
            schedule_duration: task.executing_timestamp - task.creation_timestamp,
            unit_test_duration: task.runtime_stats.unit_test_duration,
            functional_test_duration: task.runtime_stats.functional_test_duration,
            build_duration: task.runtime_stats.build_duration,
            ccache_hitrate,
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
        let token = std::env::var("GITHUB_TOKEN")
            .map_err(|_| anyhow!("GITHUB_TOKEN environment variable is required"))?;

        let octocrab = Octocrab::builder().personal_token(token).build()?;

        Ok(Self {
            octocrab,
            owner,
            repo,
        })
    }

    async fn fetch_workflow_runs(&self, max_runs: usize, since_run_id: Option<u64>) -> Result<Vec<Task>> {
        if let Some(since_id) = since_run_id {
            info!("Fetching workflow runs since run ID {} from GitHub API...", since_id);
        } else {
            info!("Fetching workflow runs from GitHub API...");
        }

        let mut all_tasks = Vec::new();
        let mut page = 1;
        let per_page = 100; // GitHub's max per page
        let mut total_fetched = 0;

        while total_fetched < max_runs {
            // Use low-level HTTP API to fetch workflow runs
            let url = format!(
                "/repos/{}/{}/actions/runs?per_page={}&page={}",
                self.owner, self.repo, per_page, page
            );

            let response: serde_json::Value = self.octocrab.get(&url, None::<&()>).await?;
            let runs = response["workflow_runs"]
                .as_array()
                .ok_or_else(|| anyhow!("No workflow_runs in response"))?;

            if runs.is_empty() {
                break;
            }

            for run_value in runs {
                if total_fetched >= max_runs {
                    break;
                }

                let conclusion = run_value["conclusion"].as_str();
                if !matches!(
                    conclusion,
                    Some("success" | "failure" | "cancelled" | "skipped" | "timed_out" | "neutral")
                ) {
                    continue;
                }

                let run_id = run_value["id"]
                    .as_u64()
                    .ok_or_else(|| anyhow!("Missing run id"))?;

                // Check if we've reached the checkpoint run ID (early termination)
                if let Some(since_id) = since_run_id {
                    if run_id <= since_id {
                        info!("Reached checkpoint run ID {}, stopping fetch", since_id);
                        return Ok(all_tasks);
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
                    .ok_or_else(|| anyhow!("No jobs in response"))?;

                for job_value in jobs {
                    let job_name = job_value["name"].as_str().unwrap_or("");
                    if job_name.to_lowercase().contains("lint") {
                        continue;
                    }

                    let job_conclusion = job_value["conclusion"].as_str();
                    if !matches!(
                        job_conclusion,
                        Some(
                            "success"
                                | "failure"
                                | "cancelled"
                                | "skipped"
                                | "timed_out"
                                | "neutral"
                        )
                    ) {
                        continue;
                    }

                    let task = self.convert_json_to_task(run_value, job_value).await?;
                    all_tasks.push(task);
                    total_fetched += 1;

                    if total_fetched >= max_runs {
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
            .ok_or_else(|| anyhow!("Missing run created_at"))?;
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

        // Map GitHub conclusion to Cirrus status for compatibility
        let status = match job_value["conclusion"].as_str() {
            Some("success") => "COMPLETED",
            Some("failure") | Some("timed_out") => "FAILED",
            Some("cancelled") | Some("skipped") => "ABORTED",
            Some("neutral") => "COMPLETED",
            _ => "COMPLETED",
        }
        .to_string();

        let build_status = match run_value["conclusion"].as_str() {
            Some("success") => "COMPLETED",
            Some("failure") | Some("timed_out") => "FAILED",
            Some("cancelled") | Some("skipped") => "ABORTED",
            Some("neutral") => "COMPLETED",
            _ => "COMPLETED",
        }
        .to_string();

        let build = Build {
            id: run_value["id"].as_u64().unwrap_or(0),
            status: build_status,
            branch: run_value["head_branch"]
                .as_str()
                .unwrap_or("unknown")
                .to_string(),
            change_id_in_repo: run_value["head_sha"].as_str().unwrap_or("").to_string(),
            change_message_title: run_value["display_title"]
                .as_str()
                .unwrap_or("")
                .to_string(),
            build_created_timestamp: run_created_at,
        };

        // Fetch and parse job log
        let job_id = job_value["id"].as_u64().unwrap_or(0);
        let (log_status_code, commands, runtime_stats) = self.fetch_and_parse_job_log(job_id).await;

        Ok(Task {
            id: job_id,
            status,
            name: job_value["name"].as_str().unwrap_or("").to_string(),
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

    async fn fetch_and_parse_job_log(&self, job_id: u64) -> (u16, Vec<Command>, TaskRuntimeStats) {
        match self.fetch_job_log(job_id).await {
            Ok(log_content) => {
                info!(
                    "Fetched log for job {}: {} lines",
                    job_id,
                    log_content.lines().count()
                );
                let (commands, stats) = self.parse_log(&log_content);
                (200, commands, stats)
            }
            Err(e) => {
                warn!("Failed to fetch log for job {}: {}", job_id, e);
                (500, vec![], TaskRuntimeStats::default())
            }
        }
    }

    async fn fetch_job_log(&self, job_id: u64) -> Result<String> {
        // GitHub job logs are returned as plain text, not JSON
        // Use the raw HTTP method to avoid JSON parsing
        let url = format!(
            "https://api.github.com/repos/{}/{}/actions/jobs/{}/logs",
            self.owner, self.repo, job_id
        );

        let response = self.octocrab._get(url).await?;

        if response.status().is_success() {
            let body = response.into_body();
            let bytes = body
                .collect()
                .await
                .map_err(|e| anyhow!("Failed to read response body: {}", e))?
                .to_bytes();
            let text = String::from_utf8_lossy(&bytes);
            Ok(text.to_string())
        } else {
            Err(anyhow!("Failed to fetch log: HTTP {}", response.status()))
        }
    }

    fn parse_log(&self, log_content: &str) -> (Vec<Command>, TaskRuntimeStats) {
        let mut commands = Vec::new();
        let mut runtime_stats = TaskRuntimeStats::default();

        // GitHub Actions log format: timestamp + content
        let command_pattern =
            Regex::new(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z) .*?\+ (.+)").unwrap();
        let _timestamp_pattern =
            Regex::new(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z)").unwrap();

        let lines: Vec<&str> = log_content.lines().collect();
        let mut current_command: Option<(String, DateTime<Utc>, usize, Vec<String>)> = None;

        for (line_num, line) in lines.iter().enumerate() {
            if let Some(caps) = command_pattern.captures(line) {
                let timestamp_str = &caps[1];
                let command = caps[2].to_string();

                if let Ok(timestamp) = timestamp_str.parse::<DateTime<Utc>>() {
                    // Process previous command if exists
                    if let Some((prev_cmd, prev_start, prev_line, prev_output)) = current_command {
                        let duration = (timestamp - prev_start).num_seconds();

                        if duration >= MIN_COMMAND_DURATION_SEC as i64 {
                            commands.push(Command {
                                cmd: prev_cmd.clone(),
                                line: prev_line,
                                duration,
                            });

                            runtime_stats.process_command(&prev_cmd, duration, &prev_output);
                        }
                    }

                    // Start new command
                    current_command = Some((command, timestamp, line_num, Vec::new()));
                }
            } else if let Some((_, _, _, ref mut output)) = current_command {
                // Add line to current command output
                output.push(line.to_string());
            }
        }

        // Process final command
        if let Some((cmd, _start_time, line, output)) = current_command {
            // Use a reasonable default duration for the last command
            let duration = 1; // 1 second default

            if duration >= MIN_COMMAND_DURATION_SEC as i64 {
                commands.push(Command {
                    cmd: cmd.clone(),
                    line,
                    duration,
                });

                runtime_stats.process_command(&cmd, duration, &output);
            }
        }

        (commands, runtime_stats)
    }
}

fn load_existing_task_ids() -> Result<Vec<u64>> {
    if !std::path::Path::new(TASKS_FILENAME).exists() {
        return Ok(vec![]);
    }

    let content = fs::read_to_string(TASKS_FILENAME)?;
    let tasks: Vec<Task> = serde_json::from_str(&content)?;
    Ok(tasks.into_iter().map(|task| task.id).collect())
}

fn save_tasks(tasks: &[Task]) -> Result<()> {
    // Load existing tasks
    let mut all_tasks = if std::path::Path::new(TASKS_FILENAME).exists() {
        let content = fs::read_to_string(TASKS_FILENAME)?;
        serde_json::from_str::<Vec<Task>>(&content).unwrap_or_default()
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
        .filter(|task| task.status == "COMPLETED")
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
    let checkpoint = if args.checkpoint {
        Checkpoint::load()?
    } else {
        None
    };

    // Determine since_run_id from checkpoint or CLI argument
    let since_run_id = args.since_run_id.or_else(|| {
        checkpoint.as_ref().map(|c| c.last_run_id)
    });

    if let Some(since_id) = since_run_id {
        info!("Using incremental fetch from run ID: {}", since_id);
    }

    // Load existing task IDs to avoid duplicates
    let existing_task_ids = load_existing_task_ids()?;
    info!("Found {} existing tasks", existing_task_ids.len());

    // Create GitHub client
    let fetcher = GitHubActionsFetcher::new(args.owner, args.repository)?;

    // Fetch workflow runs
    let tasks = fetcher.fetch_workflow_runs(args.runs, since_run_id).await?;

    // Filter out existing tasks
    let new_tasks: Vec<Task> = tasks
        .into_iter()
        .filter(|task| !existing_task_ids.contains(&task.id))
        .collect();

    info!("Found {} new tasks to process", new_tasks.len());

    if new_tasks.is_empty() {
        info!("No new tasks to process");
        return Ok(());
    }

    // Save tasks
    save_tasks(&new_tasks)?;

    info!("Successfully saved {} new tasks", new_tasks.len());

    // Update checkpoint with the highest run ID from new tasks if checkpointing is enabled
    if args.checkpoint && !new_tasks.is_empty() {
        if let Some(max_run_id) = new_tasks.iter().map(|t| t.build.id).max() {
            let new_checkpoint = Checkpoint {
                last_run_id: max_run_id,
                last_fetched_at: chrono::Utc::now(),
            };
            new_checkpoint.save()?;
        }
    }

    Ok(())
}
