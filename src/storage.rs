use anyhow::Result;
use std::collections::HashSet;

use crate::constants::{GRAPH_FILENAME, TASKS_FILENAME};
use crate::models::{GraphStats, Task, TaskStatus};

pub fn load_existing_task_ids() -> Result<HashSet<u64>> {
    if !std::path::Path::new(TASKS_FILENAME).exists() {
        return Ok(HashSet::new());
    }

    let content = std::fs::read_to_string(TASKS_FILENAME)?;
    let tasks: Vec<Task> = serde_json::from_str(&content)?;
    Ok(tasks.into_iter().map(|task| task.id).collect())
}

pub fn load_existing_run_ids() -> Result<HashSet<u64>> {
    if !std::path::Path::new(TASKS_FILENAME).exists() {
        return Ok(HashSet::new());
    }

    let content = std::fs::read_to_string(TASKS_FILENAME)?;
    let tasks: Vec<Task> = serde_json::from_str(&content)?;
    Ok(tasks.into_iter().map(|task| task.build.id).collect())
}

pub fn save_tasks(tasks: &[Task]) -> Result<()> {
    let mut all_tasks = if std::path::Path::new(TASKS_FILENAME).exists() {
        let content = std::fs::read_to_string(TASKS_FILENAME)?;
        serde_json::from_str::<Vec<Task>>(&content)?
    } else {
        Vec::new()
    };

    all_tasks.extend_from_slice(tasks);
    all_tasks.sort_by_key(|task| task.creation_timestamp);

    let tasks_json = serde_json::to_string_pretty(&all_tasks)?;
    std::fs::write(TASKS_FILENAME, tasks_json)?;

    let graph_stats: Vec<GraphStats> = all_tasks
        .iter()
        .filter(|task| task.status == TaskStatus::Completed)
        .map(GraphStats::from)
        .collect();

    let graph_json = serde_json::to_string_pretty(&graph_stats)?;
    std::fs::write(GRAPH_FILENAME, graph_json)?;

    Ok(())
}
