use chrono::{DateTime, Utc};
use serde::de::Deserializer;
use serde::{Deserialize, Serialize};

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
