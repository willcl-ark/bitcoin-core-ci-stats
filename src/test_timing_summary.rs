use chrono::{DateTime, Datelike, Duration, Utc};
use serde::Serialize;
use std::collections::{BTreeMap, HashMap};

use crate::models::{EXCLUDED_TEST_TIMING_JOB, Task, TestKind};

const RECENT_DAYS: i64 = 7;
const BASELINE_DAYS: i64 = 28;
const TREND_WEEKS: i64 = 12;
const MIN_RECENT_SAMPLES: usize = 3;
const MIN_BASELINE_SAMPLES: usize = 3;

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TestTimingSummary {
    pub generated_at: DateTime<Utc>,
    pub rows: Vec<TestTimingRow>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TestTimingRow {
    pub job: String,
    pub kind: TestKind,
    pub test: String,
    pub recent_median_ms: f64,
    pub baseline_median_ms: f64,
    pub recent_samples: usize,
    pub baseline_samples: usize,
    pub change_percent: f64,
    pub weeks: Vec<TestTimingWeek>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TestTimingWeek {
    pub week: String,
    pub median_ms: f64,
    pub samples: usize,
}

#[derive(Debug, Default)]
struct Samples {
    recent: Vec<u64>,
    baseline: Vec<u64>,
    weeks: BTreeMap<String, Vec<u64>>,
}

#[derive(Debug, Hash, PartialEq, Eq)]
struct TestKey {
    job: String,
    kind: TestKind,
    test: String,
}

impl TestTimingSummary {
    pub fn from_tasks(tasks: &[Task]) -> Self {
        Self {
            generated_at: latest_task_timestamp(tasks).unwrap_or(DateTime::<Utc>::UNIX_EPOCH),
            rows: rows_from_tasks(tasks),
        }
    }
}

fn latest_task_timestamp(tasks: &[Task]) -> Option<DateTime<Utc>> {
    DateTime::from_timestamp(
        tasks
            .iter()
            .filter(|task| task.build.branch == "master")
            .map(|task| task.creation_timestamp)
            .max()?,
        0,
    )
}

fn rows_from_tasks(tasks: &[Task]) -> Vec<TestTimingRow> {
    let Some(anchor) = latest_task_timestamp(tasks) else {
        return Vec::new();
    };

    // Windows are anchored to the newest task in the archive so regenerated
    // files remain stable for historical snapshots.
    let recent_start = anchor - Duration::days(RECENT_DAYS);
    let baseline_start = recent_start - Duration::days(BASELINE_DAYS);
    let trend_start = anchor - Duration::weeks(TREND_WEEKS);
    let mut samples = HashMap::<TestKey, Samples>::new();

    for task in tasks {
        if task.build.branch != "master"
            || task.test_timings.is_empty()
            || task.name == EXCLUDED_TEST_TIMING_JOB
        {
            continue;
        }

        let Some(created) = DateTime::from_timestamp(task.creation_timestamp, 0) else {
            continue;
        };
        if created < baseline_start && created < trend_start {
            continue;
        }

        let week = if created >= trend_start {
            Some(iso_week(created))
        } else {
            None
        };

        for timing in task
            .test_timings
            .iter()
            .filter(|timing| is_success_status(&timing.status))
        {
            let entry = samples
                .entry(TestKey {
                    job: task.name.clone(),
                    kind: timing.kind,
                    test: timing.name.clone(),
                })
                .or_default();

            if created >= recent_start && created <= anchor {
                entry.recent.push(timing.duration_ms);
            } else if created >= baseline_start && created < recent_start {
                entry.baseline.push(timing.duration_ms);
            }

            if let Some(week) = &week {
                entry
                    .weeks
                    .entry(week.clone())
                    .or_default()
                    .push(timing.duration_ms);
            }
        }
    }

    let mut rows: Vec<_> = samples
        .into_iter()
        .filter(|(_, samples)| {
            samples.recent.len() >= MIN_RECENT_SAMPLES
                && (samples.baseline.is_empty() || samples.baseline.len() >= MIN_BASELINE_SAMPLES)
        })
        .map(|(key, samples)| {
            let recent_median_ms = median_ms(&samples.recent);
            let baseline_median_ms = median_ms(&samples.baseline);
            let change_percent = if baseline_median_ms > 0.0 {
                (recent_median_ms - baseline_median_ms) * 100.0 / baseline_median_ms
            } else {
                0.0
            };
            let weeks = samples
                .weeks
                .into_iter()
                .map(|(week, values)| TestTimingWeek {
                    week,
                    median_ms: median_ms(&values),
                    samples: values.len(),
                })
                .collect();

            TestTimingRow {
                job: key.job,
                kind: key.kind,
                test: key.test,
                recent_median_ms,
                baseline_median_ms,
                recent_samples: samples.recent.len(),
                baseline_samples: samples.baseline.len(),
                change_percent,
                weeks,
            }
        })
        .collect();

    rows.sort_by(|a, b| {
        b.change_percent
            .total_cmp(&a.change_percent)
            .then_with(|| b.recent_median_ms.total_cmp(&a.recent_median_ms))
            .then_with(|| a.job.cmp(&b.job))
            .then_with(|| kind_sort_key(a.kind).cmp(kind_sort_key(b.kind)))
            .then_with(|| a.test.cmp(&b.test))
    });
    rows
}

fn is_success_status(status: &str) -> bool {
    let normalized: String = status
        .trim()
        .to_ascii_lowercase()
        .chars()
        .filter(|ch| ch.is_ascii_alphabetic())
        .collect();
    matches!(
        normalized.as_str(),
        "pass" | "passed" | "success" | "successful" | "ok"
    )
}

fn iso_week(timestamp: DateTime<Utc>) -> String {
    let week = timestamp.date_naive().iso_week();
    format!("{}-W{:02}", week.year(), week.week())
}

fn kind_sort_key(kind: TestKind) -> &'static str {
    match kind {
        TestKind::Unit => "unit",
        TestKind::Functional => "functional",
    }
}

fn median_ms(values: &[u64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }

    let mut values = values.to_vec();
    values.sort_unstable();
    let mid = values.len() / 2;
    if values.len() % 2 == 0 {
        (values[mid - 1] + values[mid]) as f64 / 2.0
    } else {
        values[mid] as f64
    }
}

#[cfg(test)]
mod tests {
    use super::{TestTimingSummary, median_ms};
    use crate::models::TaskStatus;
    use crate::models::{Build, Task, TaskRuntimeStats, TestKind, TestTiming};

    const DAY: i64 = 24 * 60 * 60;
    const ANCHOR: i64 = 1_700_000_000;

    #[test]
    fn median_averages_even_sample_counts() {
        assert_eq!(median_ms(&[400, 100, 300, 200]), 250.0);
    }

    #[test]
    fn compares_recent_window_to_preceding_baseline() {
        let mut tasks = Vec::new();
        for duration_ms in [900, 1000, 1100] {
            tasks.push(task(
                ANCHOR - 20 * DAY,
                "job-a",
                duration_ms,
                "Passed",
                TaskStatus::Completed,
            ));
        }
        for duration_ms in [1900, 2000, 2100] {
            tasks.push(task(
                ANCHOR - 2 * DAY,
                "job-a",
                duration_ms,
                "Passed",
                TaskStatus::Completed,
            ));
        }
        tasks.push(task(
            ANCHOR - 2 * DAY,
            "job-a",
            99_000,
            "Failed",
            TaskStatus::Completed,
        ));
        tasks.push(task(
            ANCHOR - 2 * DAY,
            "job-a",
            2100,
            "✓ Passed",
            TaskStatus::Failed,
        ));
        tasks.push(task(
            ANCHOR - 80 * DAY,
            "job-a",
            99_000,
            "Passed",
            TaskStatus::Completed,
        ));

        let summary = TestTimingSummary::from_tasks(&tasks);

        assert_eq!(summary.rows.len(), 1);
        let row = &summary.rows[0];
        assert_eq!(row.job, "job-a");
        assert_eq!(row.kind, TestKind::Unit);
        assert_eq!(row.test, "validation_tests");
        assert_eq!(row.baseline_median_ms, 1000.0);
        assert_eq!(row.recent_median_ms, 2050.0);
        assert_eq!(row.baseline_samples, 3);
        assert_eq!(row.recent_samples, 4);
        assert_eq!(row.change_percent, 105.0);
        assert!(row.weeks.iter().all(|week| week.samples > 0));
    }

    #[test]
    fn requires_enough_samples_in_both_windows() {
        let tasks = vec![
            task(
                ANCHOR - 20 * DAY,
                "job-a",
                1000,
                "Passed",
                TaskStatus::Completed,
            ),
            task(
                ANCHOR - 19 * DAY,
                "job-a",
                1100,
                "Passed",
                TaskStatus::Completed,
            ),
            task(
                ANCHOR - 2 * DAY,
                "job-a",
                2000,
                "Passed",
                TaskStatus::Completed,
            ),
            task(
                ANCHOR - 1 * DAY,
                "job-a",
                2100,
                "Passed",
                TaskStatus::Completed,
            ),
            task(ANCHOR, "job-a", 2200, "Passed", TaskStatus::Completed),
        ];

        let summary = TestTimingSummary::from_tasks(&tasks);

        assert!(summary.rows.is_empty());
    }

    #[test]
    fn includes_new_tests_after_three_recent_samples() {
        let tasks = vec![
            task(
                ANCHOR - 2 * DAY,
                "job-a",
                2000,
                "Passed",
                TaskStatus::Completed,
            ),
            task(
                ANCHOR - 1 * DAY,
                "job-a",
                2100,
                "Passed",
                TaskStatus::Completed,
            ),
            task(ANCHOR, "job-a", 2200, "Passed", TaskStatus::Completed),
        ];

        let summary = TestTimingSummary::from_tasks(&tasks);

        assert_eq!(summary.rows.len(), 1);
        let row = &summary.rows[0];
        assert_eq!(row.baseline_samples, 0);
        assert_eq!(row.baseline_median_ms, 0.0);
        assert_eq!(row.change_percent, 0.0);
    }

    #[test]
    fn excludes_repeated_ancestor_test_job() {
        let tasks = vec![task(
            ANCHOR,
            "test ancestor commits",
            1000,
            "Passed",
            TaskStatus::Completed,
        )];

        assert!(TestTimingSummary::from_tasks(&tasks).rows.is_empty());
    }

    #[test]
    fn ignores_pr_samples_and_timestamps() {
        let mut tasks: Vec<_> = (0..3)
            .map(|_| task(ANCHOR, "job-a", 1000, "Passed", TaskStatus::Completed))
            .collect();
        let mut pr = task(
            ANCHOR + 100 * DAY,
            "job-a",
            99000,
            "Passed",
            TaskStatus::Completed,
        );
        pr.build.branch = "feature".into();
        tasks.push(pr);
        let summary = TestTimingSummary::from_tasks(&tasks);
        assert_eq!(summary.rows[0].recent_samples, 3);
        assert_eq!(summary.rows[0].recent_median_ms, 1000.0);
        assert_eq!(summary.generated_at.timestamp(), ANCHOR);
    }

    fn task(
        creation_timestamp: i64,
        name: &str,
        duration_ms: u64,
        status: &str,
        task_status: TaskStatus,
    ) -> Task {
        Task {
            id: creation_timestamp as u64,
            status: task_status,
            name: name.to_string(),
            creation_timestamp,
            scheduled_timestamp: creation_timestamp,
            executing_timestamp: creation_timestamp,
            duration: 1,
            final_status_timestamp: creation_timestamp + 1,
            execution_info_labels: Vec::new(),
            build: Build {
                id: creation_timestamp as u64,
                status: task_status,
                branch: "master".to_string(),
                change_id_in_repo: "abc".to_string(),
                change_message_title: "message".to_string(),
                build_created_timestamp: creation_timestamp,
            },
            log: String::new(),
            log_status_code: 200,
            commands: Vec::new(),
            runtime_stats: TaskRuntimeStats::default(),
            test_timings: vec![TestTiming {
                kind: TestKind::Unit,
                name: "validation_tests".to_string(),
                duration_ms,
                status: status.to_string(),
            }],
        }
    }
}
