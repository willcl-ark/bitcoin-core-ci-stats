use anyhow::Result;
use chrono::DateTime;
use http_body_util::BodyExt;
use octocrab::Octocrab;
use std::collections::HashSet;
use std::io::{BufReader, Seek, SeekFrom, Write};
use std::path::PathBuf;
use tracing::{info, warn};

use crate::constants::MAX_LOG_BYTES;
use crate::errors::AppError;
use crate::log_parser::parse_job_log;
use crate::models::{Build, Task, TaskRuntimeStats};

struct TempFileGuard {
    path: PathBuf,
}

impl Drop for TempFileGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

fn is_terminal_conclusion(conclusion: Option<&str>) -> bool {
    matches!(
        conclusion,
        Some("success" | "failure" | "cancelled" | "skipped" | "timed_out" | "neutral")
    )
}

fn status_from_conclusion(conclusion: Option<&str>) -> crate::models::TaskStatus {
    match conclusion {
        Some("success") | Some("neutral") => crate::models::TaskStatus::Completed,
        Some("failure") | Some("timed_out") => crate::models::TaskStatus::Failed,
        Some("cancelled") | Some("skipped") => crate::models::TaskStatus::Aborted,
        _ => crate::models::TaskStatus::Completed,
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

fn emit_github_actions_warning(message: &str) {
    println!("::warning title=fetch-tasks-github::{message}");
}

pub struct GitHubActionsFetcher {
    octocrab: Octocrab,
    owner: String,
    repo: String,
}

impl GitHubActionsFetcher {
    pub fn new(owner: String, repo: String) -> Result<Self> {
        let token = std::env::var("GITHUB_TOKEN").map_err(|_| AppError::MissingGitHubToken)?;

        let octocrab = Octocrab::builder().personal_token(token).build()?;

        Ok(Self {
            octocrab,
            owner,
            repo,
        })
    }

    pub async fn fetch_workflow_runs(
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
        let mut skipped_runs = 0usize;
        let mut skipped_jobs = 0usize;
        let mut log_processing_failures = 0usize;

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

                let Some(run_id) = run_value["id"].as_u64() else {
                    skipped_runs += 1;
                    warn!("Skipping run with missing id field");
                    continue;
                };

                if is_backfill {
                    if let Some(cursor) = backfill_cursor
                        && run_id >= cursor
                    {
                        continue;
                    }

                    if let Some(target) = backfill_target
                        && run_id <= target
                    {
                        info!("Reached backfill target {}, stopping", target);
                        return Ok(all_tasks);
                    }

                    if existing_run_ids.contains(&run_id) {
                        continue;
                    }
                } else if let Some(since_id) = since_run_id
                    && run_id <= since_id
                {
                    info!("Reached checkpoint run ID {}, stopping fetch", since_id);
                    return Ok(all_tasks);
                }

                let jobs_url = format!(
                    "/repos/{}/{}/actions/runs/{}/jobs",
                    self.owner, self.repo, run_id
                );
                let jobs_response: serde_json::Value =
                    match self.octocrab.get(&jobs_url, None::<&()>).await {
                        Ok(v) => v,
                        Err(e) => {
                            skipped_runs += 1;
                            warn!("Skipping run {} after jobs fetch error: {}", run_id, e);
                            continue;
                        }
                    };
                let Some(jobs) = jobs_response["jobs"].as_array() else {
                    skipped_runs += 1;
                    warn!("Skipping run {} with malformed jobs payload", run_id);
                    continue;
                };

                for job_value in jobs {
                    let job_name = job_value["name"].as_str().unwrap_or("");
                    if job_name.to_lowercase().contains("lint") {
                        continue;
                    }

                    let job_conclusion = job_value["conclusion"].as_str();
                    if !is_terminal_conclusion(job_conclusion) {
                        continue;
                    }

                    let job_id_hint = job_value["id"].as_u64().unwrap_or(0);
                    let task = match self.convert_json_to_task(run_value, job_value).await {
                        Ok(task) => task,
                        Err(e) => {
                            skipped_jobs += 1;
                            warn!(
                                "Skipping job {} in run {} after conversion error: {}",
                                job_id_hint, run_id, e
                            );
                            continue;
                        }
                    };
                    if task.log_status_code != 200 {
                        log_processing_failures += 1;
                    }
                    all_tasks.push(task);
                    new_job_count += 1;

                    if new_job_count >= max_runs {
                        break;
                    }
                }
            }

            page += 1;
        }

        if skipped_runs > 0 || skipped_jobs > 0 || log_processing_failures > 0 {
            let summary = format!(
                "completed with partial failures: skipped_runs={}, skipped_jobs={}, log_processing_failures={}",
                skipped_runs, skipped_jobs, log_processing_failures
            );
            warn!("{}", summary);
            emit_github_actions_warning(&summary);
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
            log: "<cleared>".to_string(),
            log_status_code,
            commands,
            runtime_stats,
        })
    }

    async fn fetch_and_parse_job_log(
        &self,
        job_id: u64,
        job_completed_at: i64,
    ) -> (u16, Vec<crate::models::Command>, TaskRuntimeStats) {
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
    ) -> Result<(usize, Vec<crate::models::Command>, TaskRuntimeStats)> {
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
            match std::fs::OpenOptions::new()
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
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&file_path)?;

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
        parse_job_log(reader, job_completed_at)
    }
}
