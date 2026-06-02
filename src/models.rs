use chrono::{DateTime, Datelike, Utc};
use serde::de::Deserializer;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap};

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

/// Status of a CI task or build.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TaskStatus {
    Completed,
    Failed,
    Aborted,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Checkpoint {
    pub last_run_id: u64,
    pub last_fetched_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub backfill_cursor: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub backfill_target: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Build {
    pub id: u64,
    pub status: TaskStatus,
    pub branch: String,
    #[serde(rename = "changeIdInRepo")]
    pub change_id_in_repo: String,
    #[serde(rename = "changeMessageTitle")]
    pub change_message_title: String,
    #[serde(rename = "buildCreatedTimestamp")]
    pub build_created_timestamp: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Task {
    pub id: u64,
    pub status: TaskStatus,
    pub name: String,
    #[serde(rename = "creationTimestamp")]
    pub creation_timestamp: i64,
    #[serde(rename = "scheduledTimestamp")]
    pub scheduled_timestamp: i64,
    #[serde(rename = "executingTimestamp")]
    pub executing_timestamp: i64,
    pub duration: i64,
    #[serde(rename = "finalStatusTimestamp")]
    pub final_status_timestamp: i64,
    #[serde(rename = "executionInfoLabels")]
    pub execution_info_labels: Vec<String>,
    pub build: Build,
    pub log: String,
    #[serde(rename = "log_status_code")]
    pub log_status_code: u16,
    pub commands: Vec<Command>,
    #[serde(rename = "runtime_stats")]
    pub runtime_stats: TaskRuntimeStats,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Command {
    pub cmd: String,
    pub line: usize,
    pub duration: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TaskRuntimeStats {
    /// Compiler cache hit rate as a percentage (0.0-100.0)
    #[serde(
        rename = "ccache_hitrate",
        default,
        deserialize_with = "deserialize_optional_f64",
        skip_serializing_if = "Option::is_none"
    )]
    pub ccache_hitrate: Option<f64>,
    #[serde(rename = "docker_build_cached")]
    pub docker_build_cached: bool,
    #[serde(
        rename = "docker_build_duration",
        skip_serializing_if = "Option::is_none"
    )]
    pub docker_build_duration: Option<i64>,
    #[serde(
        rename = "ccache_zerostats_duration",
        skip_serializing_if = "Option::is_none"
    )]
    pub ccache_zerostats_duration: Option<i64>,
    #[serde(rename = "configure_duration", skip_serializing_if = "Option::is_none")]
    pub configure_duration: Option<i64>,
    #[serde(rename = "build_duration", skip_serializing_if = "Option::is_none")]
    pub build_duration: Option<i64>,
    #[serde(rename = "unit_test_duration", skip_serializing_if = "Option::is_none")]
    pub unit_test_duration: Option<i64>,
    #[serde(
        rename = "functional_test_duration",
        skip_serializing_if = "Option::is_none"
    )]
    pub functional_test_duration: Option<i64>,
    #[serde(
        rename = "depends_build_duration",
        skip_serializing_if = "Option::is_none"
    )]
    pub depends_build_duration: Option<i64>,
}

impl TaskRuntimeStats {
    pub fn process_command(
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
            if ccache_hitrate.is_some() {
                self.ccache_hitrate = ccache_hitrate;
            }
        } else if cmd.contains("ctest ") {
            self.unit_test_duration = Some(duration_secs);
        } else if cmd.contains("test/functional/test_runner.py ") {
            self.functional_test_duration = Some(duration_secs);
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphStats {
    pub id: u64,
    pub name: String,
    pub duration: i64,
    #[serde(rename = "scheduleDuration")]
    pub schedule_duration: i64,
    #[serde(rename = "unitTestDuration")]
    pub unit_test_duration: i64,
    #[serde(rename = "functionalTestDuration")]
    pub functional_test_duration: i64,
    #[serde(rename = "buildDuration")]
    pub build_duration: i64,
    #[serde(rename = "ccacheHitrate")]
    pub ccache_hitrate: f64,
    pub created: i64,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Stats {
    pub generated_from_tasks: usize,
    pub totals: StatsTotals,
    pub series: StatsSeries,
    pub top_jobs_by_tasks: Vec<NamedCount>,
    pub top_jobs_by_failures: Vec<NamedCount>,
    pub slowest_jobs: Vec<NamedDuration>,
    pub top_branches: Vec<NamedCount>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StatsTotals {
    pub runs: usize,
    pub tasks: usize,
    pub completed: usize,
    pub failed: usize,
    pub aborted: usize,
    pub median_queue_duration: i64,
    pub p90_queue_duration: i64,
    pub p99_queue_duration: i64,
    pub median_execution_duration: i64,
    pub p90_execution_duration: i64,
    pub p99_execution_duration: i64,
    pub wasted_runner_duration: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatsSeries {
    pub daily: Vec<StatsBucket>,
    pub weekly: Vec<StatsBucket>,
    pub monthly: Vec<StatsBucket>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StatsBucket {
    pub period: String,
    pub runs: usize,
    pub tasks: usize,
    pub completed: usize,
    pub failed: usize,
    pub aborted: usize,
    pub median_queue_duration: i64,
    pub p90_queue_duration: i64,
    pub p99_queue_duration: i64,
    pub median_execution_duration: i64,
    pub p90_execution_duration: i64,
    pub p99_execution_duration: i64,
    pub wasted_runner_duration: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NamedCount {
    pub name: String,
    pub count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NamedDuration {
    pub name: String,
    pub median_duration: i64,
    pub samples: usize,
}

#[derive(Default)]
struct BucketAccumulator {
    runs: BTreeSet<u64>,
    tasks: usize,
    completed: usize,
    failed: usize,
    aborted: usize,
    queue_durations: Vec<i64>,
    execution_durations: Vec<i64>,
    wasted_runner_duration: i64,
}

impl BucketAccumulator {
    fn add_task(&mut self, task: &Task) {
        let queue_duration = (task.executing_timestamp - task.creation_timestamp).max(0);
        let execution_duration = task.duration.max(0);
        self.runs.insert(task.build.id);
        self.tasks += 1;
        match task.status {
            TaskStatus::Completed => self.completed += 1,
            TaskStatus::Failed => {
                self.failed += 1;
                self.wasted_runner_duration += execution_duration;
            }
            TaskStatus::Aborted => {
                self.aborted += 1;
                self.wasted_runner_duration += execution_duration;
            }
        }
        self.queue_durations.push(queue_duration);
        self.execution_durations.push(execution_duration);
    }

    fn into_bucket(self, period: String) -> StatsBucket {
        StatsBucket {
            period,
            runs: self.runs.len(),
            tasks: self.tasks,
            completed: self.completed,
            failed: self.failed,
            aborted: self.aborted,
            median_queue_duration: percentile(&self.queue_durations, 50),
            p90_queue_duration: percentile(&self.queue_durations, 90),
            p99_queue_duration: percentile(&self.queue_durations, 99),
            median_execution_duration: percentile(&self.execution_durations, 50),
            p90_execution_duration: percentile(&self.execution_durations, 90),
            p99_execution_duration: percentile(&self.execution_durations, 99),
            wasted_runner_duration: self.wasted_runner_duration,
        }
    }
}

impl From<&[Task]> for Stats {
    fn from(tasks: &[Task]) -> Self {
        let mut totals = BucketAccumulator::default();
        let mut daily = BTreeMap::new();
        let mut weekly = BTreeMap::new();
        let mut monthly = BTreeMap::new();
        let mut job_counts = HashMap::new();
        let mut job_failures = HashMap::new();
        let mut job_durations: HashMap<String, Vec<i64>> = HashMap::new();
        let mut branch_counts = HashMap::new();

        for task in tasks {
            totals.add_task(task);

            let timestamp = DateTime::from_timestamp(task.creation_timestamp, 0)
                .unwrap_or(DateTime::<Utc>::UNIX_EPOCH);
            let date = timestamp.date_naive();
            let iso_week = date.iso_week();

            daily
                .entry(date.format("%Y-%m-%d").to_string())
                .or_insert_with(BucketAccumulator::default)
                .add_task(task);
            weekly
                .entry(format!("{}-W{:02}", iso_week.year(), iso_week.week()))
                .or_insert_with(BucketAccumulator::default)
                .add_task(task);
            monthly
                .entry(date.format("%Y-%m").to_string())
                .or_insert_with(BucketAccumulator::default)
                .add_task(task);

            *job_counts.entry(task.name.clone()).or_insert(0) += 1;
            if task.status == TaskStatus::Failed {
                *job_failures.entry(task.name.clone()).or_insert(0) += 1;
            }
            job_durations
                .entry(task.name.clone())
                .or_default()
                .push(task.duration);
            *branch_counts.entry(task.build.branch.clone()).or_insert(0) += 1;
        }

        let totals = StatsTotals {
            runs: totals.runs.len(),
            tasks: totals.tasks,
            completed: totals.completed,
            failed: totals.failed,
            aborted: totals.aborted,
            median_queue_duration: percentile(&totals.queue_durations, 50),
            p90_queue_duration: percentile(&totals.queue_durations, 90),
            p99_queue_duration: percentile(&totals.queue_durations, 99),
            median_execution_duration: percentile(&totals.execution_durations, 50),
            p90_execution_duration: percentile(&totals.execution_durations, 90),
            p99_execution_duration: percentile(&totals.execution_durations, 99),
            wasted_runner_duration: totals.wasted_runner_duration,
        };

        Self {
            generated_from_tasks: tasks.len(),
            totals,
            series: StatsSeries {
                daily: buckets_from_map(daily),
                weekly: buckets_from_map(weekly),
                monthly: buckets_from_map(monthly),
            },
            top_jobs_by_tasks: top_counts(job_counts, 10),
            top_jobs_by_failures: top_counts(job_failures, 10),
            slowest_jobs: slowest_jobs(job_durations, 10),
            top_branches: top_counts(branch_counts, 10),
        }
    }
}

fn buckets_from_map(map: BTreeMap<String, BucketAccumulator>) -> Vec<StatsBucket> {
    map.into_iter()
        .map(|(period, bucket)| bucket.into_bucket(period))
        .collect()
}

fn median(values: Vec<i64>) -> i64 {
    percentile(&values, 50)
}

fn percentile(values: &[i64], percentile: usize) -> i64 {
    if values.is_empty() {
        return 0;
    }
    let mut values = values.to_vec();
    values.sort_unstable();
    let index = (values.len() - 1) * percentile / 100;
    values[index]
}

fn top_counts(counts: HashMap<String, usize>, limit: usize) -> Vec<NamedCount> {
    let mut counts: Vec<_> = counts
        .into_iter()
        .map(|(name, count)| NamedCount { name, count })
        .collect();
    counts.sort_by(|a, b| b.count.cmp(&a.count).then_with(|| a.name.cmp(&b.name)));
    counts.truncate(limit);
    counts
}

fn slowest_jobs(durations: HashMap<String, Vec<i64>>, limit: usize) -> Vec<NamedDuration> {
    let mut durations: Vec<_> = durations
        .into_iter()
        .map(|(name, values)| {
            let samples = values.len();
            NamedDuration {
                name,
                median_duration: median(values),
                samples,
            }
        })
        .collect();
    durations.sort_by(|a, b| {
        b.median_duration
            .cmp(&a.median_duration)
            .then_with(|| b.samples.cmp(&a.samples))
            .then_with(|| a.name.cmp(&b.name))
    });
    durations.truncate(limit);
    durations
}
