use anyhow::{Context, Result, bail};
use chrono::{Duration, Utc};
use clap::Parser;
use fetch_tasks_github::github::GitHubActionsFetcher;
use fetch_tasks_github::models::Task;
use octocrab::Octocrab;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

const OWNER: &str = "bitcoin";
const REPO: &str = "bitcoin";
const WORKFLOW_NAME: &str = "CI";

#[derive(Parser, Debug)]
#[command(name = "backfill-test-timings")]
#[command(about = "Backfill per-test timings into an existing task archive")]
struct Args {
    input_json: PathBuf,
    output_json: PathBuf,

    #[arg(long, default_value_t = 60)]
    days: u32,

    #[arg(long, default_value_t = 3)]
    max_runs: usize,

    #[arg(long, default_value_t = 1, help = "Process every Nth eligible run")]
    run_stride: usize,

    #[arg(long = "job", help = "Exact job name to backfill; may be repeated")]
    jobs: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct WorkflowRunsResponse {
    workflow_runs: Vec<WorkflowRun>,
}

#[derive(Debug, Deserialize)]
struct WorkflowRun {
    id: u64,
    name: Option<String>,
    event: Option<String>,
    head_branch: Option<String>,
    conclusion: Option<String>,
}

impl WorkflowRun {
    fn is_completed_master_ci_push(&self) -> bool {
        self.name.as_deref() == Some(WORKFLOW_NAME)
            && self.event.as_deref() == Some("push")
            && self.head_branch.as_deref() == Some("master")
            && is_terminal_conclusion(self.conclusion.as_deref())
    }
}

fn is_terminal_conclusion(conclusion: Option<&str>) -> bool {
    matches!(
        conclusion,
        Some("success" | "failure" | "cancelled" | "skipped" | "timed_out" | "neutral")
    )
}

fn ensure_output_path(input: &Path, output: &Path) -> Result<()> {
    let input_path = std::fs::canonicalize(input)
        .with_context(|| format!("canonicalize input path {}", input.display()))?;

    if output.exists() {
        bail!("output path already exists: {}", output.display());
    }

    let output_file_name = output
        .file_name()
        .with_context(|| format!("output path has no file name: {}", output.display()))?;
    let output_parent = output.parent().unwrap_or_else(|| Path::new("."));
    let output_path = std::fs::canonicalize(output_parent)
        .with_context(|| format!("canonicalize output parent {}", output_parent.display()))?
        .join(output_file_name);

    if input_path == output_path {
        bail!("output path must be different from input path");
    }

    Ok(())
}

fn build_tasks_by_run(tasks: &[Task], selected_jobs: &HashSet<String>) -> HashMap<u64, Vec<usize>> {
    let mut tasks_by_run: HashMap<u64, Vec<usize>> = HashMap::new();

    for (index, task) in tasks.iter().enumerate() {
        if !task.test_timings.is_empty() || task.final_status_timestamp <= 0 {
            continue;
        }
        if !selected_jobs.is_empty() && !selected_jobs.contains(&task.name) {
            continue;
        }
        tasks_by_run.entry(task.build.id).or_default().push(index);
    }

    tasks_by_run
}

fn github_client() -> Result<Octocrab> {
    let token =
        std::env::var("GITHUB_TOKEN").context("GITHUB_TOKEN environment variable is required")?;
    Ok(Octocrab::builder().personal_token(token).build()?)
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    if args.days == 0 || args.days > 89 {
        bail!("--days must be between 1 and 89");
    }
    if args.max_runs == 0 {
        bail!("--max-runs must be greater than 0");
    }
    if args.run_stride == 0 {
        bail!("--run-stride must be greater than 0");
    }

    ensure_output_path(&args.input_json, &args.output_json)?;

    let input = std::fs::read_to_string(&args.input_json)
        .with_context(|| format!("read input JSON {}", args.input_json.display()))?;
    let mut tasks: Vec<Task> = serde_json::from_str(&input)
        .with_context(|| format!("parse input JSON {}", args.input_json.display()))?;

    let selected_jobs: HashSet<String> = args.jobs.into_iter().collect();
    let tasks_by_run = build_tasks_by_run(&tasks, &selected_jobs);
    let archived_run_ids: HashSet<u64> = tasks_by_run.keys().copied().collect();

    println!("loaded_tasks={}", tasks.len());
    println!(
        "eligible_tasks={}",
        tasks_by_run.values().map(Vec::len).sum::<usize>()
    );
    println!("eligible_runs={}", archived_run_ids.len());

    let octocrab = github_client()?;
    let fetcher = GitHubActionsFetcher::new(OWNER.to_string(), REPO.to_string())?;
    let since = (Utc::now() - Duration::days(i64::from(args.days)))
        .date_naive()
        .format("%Y-%m-%d");

    let mut page = 1usize;
    let mut api_runs_seen = 0usize;
    let mut ci_push_runs_seen = 0usize;
    let mut archived_runs_processed = 0usize;
    let mut eligible_runs_seen = 0usize;
    let mut jobs_attempted = 0usize;
    let mut jobs_updated = 0usize;
    let mut timing_records = 0usize;
    let mut log_failures = 0usize;
    let mut empty_timing_logs = 0usize;

    while archived_runs_processed < args.max_runs {
        let url = format!(
            "/repos/{OWNER}/{REPO}/actions/workflows/ci.yml/runs?event=push&branch=master&created=%3E%3D{since}&status=completed&per_page=100&page={page}"
        );
        let response: WorkflowRunsResponse = octocrab.get(&url, None::<&()>).await?;
        if response.workflow_runs.is_empty() {
            break;
        }

        for run in response.workflow_runs {
            api_runs_seen += 1;
            if !run.is_completed_master_ci_push() {
                continue;
            }
            ci_push_runs_seen += 1;

            let Some(task_indexes) = tasks_by_run.get(&run.id) else {
                continue;
            };
            let selected = eligible_runs_seen.is_multiple_of(args.run_stride);
            eligible_runs_seen += 1;
            if !selected {
                continue;
            }
            archived_runs_processed += 1;
            let job_names: Vec<&str> = task_indexes
                .iter()
                .map(|task_index| tasks[*task_index].name.as_str())
                .collect();
            println!("processing_run_id={} jobs={}", run.id, job_names.join(","));

            for task_index in task_indexes {
                let task = &tasks[*task_index];
                jobs_attempted += 1;
                match fetcher
                    .fetch_job_test_timings(task.id, task.final_status_timestamp)
                    .await
                {
                    Ok(timings) if timings.is_empty() => {
                        empty_timing_logs += 1;
                    }
                    Ok(timings) => {
                        timing_records += timings.len();
                        tasks[*task_index].test_timings = timings;
                        jobs_updated += 1;
                    }
                    Err(e) => {
                        log_failures += 1;
                        eprintln!(
                            "failed to fetch timings for job {} in run {} ({}): {e}",
                            task.id, task.build.id, task.name
                        );
                    }
                }
            }

            if archived_runs_processed >= args.max_runs {
                break;
            }
        }

        page += 1;
    }

    println!("api_runs_seen={api_runs_seen}");
    println!("ci_push_runs_seen={ci_push_runs_seen}");
    println!("archived_runs_processed={archived_runs_processed}");
    println!("eligible_runs_seen={eligible_runs_seen}");
    println!("jobs_attempted={jobs_attempted}");
    println!("jobs_updated={jobs_updated}");
    println!("timing_records={timing_records}");
    println!("empty_timing_logs={empty_timing_logs}");
    println!("log_failures={log_failures}");

    if jobs_updated == 0 {
        println!("no jobs gained timing data; output not written");
        return Ok(());
    }

    let output = serde_json::to_string_pretty(&tasks)?;
    std::fs::write(&args.output_json, output)
        .with_context(|| format!("write output JSON {}", args.output_json.display()))?;
    println!("wrote_output={}", args.output_json.display());

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{WorkflowRun, ensure_output_path};

    #[test]
    fn filters_completed_master_ci_push_runs() {
        let run = WorkflowRun {
            id: 1,
            name: Some("CI".to_string()),
            event: Some("push".to_string()),
            head_branch: Some("master".to_string()),
            conclusion: Some("success".to_string()),
        };
        assert!(run.is_completed_master_ci_push());

        let pull_request = WorkflowRun {
            event: Some("pull_request".to_string()),
            ..run
        };
        assert!(!pull_request.is_completed_master_ci_push());

        let in_progress = WorkflowRun {
            event: Some("push".to_string()),
            conclusion: None,
            ..pull_request
        };
        assert!(!in_progress.is_completed_master_ci_push());
    }

    #[test]
    fn refuses_existing_output_path() -> anyhow::Result<()> {
        let dir =
            std::env::temp_dir().join(format!("backfill-test-timings-{}", std::process::id()));
        std::fs::create_dir_all(&dir)?;
        let input = dir.join("tasks.json");
        let output = dir.join("tasks-with-timings.json");
        std::fs::write(&input, "[]")?;
        std::fs::write(&output, "[]")?;

        let error = ensure_output_path(&input, &output)
            .expect_err("existing output path should be rejected")
            .to_string();
        assert!(error.contains("output path already exists"));

        std::fs::remove_file(input)?;
        std::fs::remove_file(output)?;
        std::fs::remove_dir(dir)?;

        Ok(())
    }
}
