use anyhow::Result;
use clap::Parser;
use fetch_tasks_github::constants::DEFAULT_RUNS_TO_QUERY;
use fetch_tasks_github::github::GitHubActionsFetcher;
use fetch_tasks_github::models::Checkpoint;
use fetch_tasks_github::storage::{load_existing_run_ids, load_existing_task_ids, save_tasks};
use tracing::info;

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

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    info!(
        "Starting GitHub Actions fetcher for {}/{}",
        args.owner, args.repository
    );

    if args.reset_checkpoint {
        Checkpoint::reset()?;
        return Ok(());
    }

    let mut checkpoint = if args.checkpoint {
        Checkpoint::load()?
    } else {
        None
    };

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

    let since_run_id = if is_backfill {
        None
    } else {
        args.since_run_id
            .or_else(|| checkpoint.as_ref().map(|c| c.last_run_id))
    };

    if let Some(since_id) = since_run_id {
        info!("Using incremental fetch from run ID: {}", since_id);
    }

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

    let new_tasks: Vec<_> = tasks
        .into_iter()
        .filter(|task| !existing_task_ids.contains(&task.id))
        .collect();

    info!("Found {} new tasks to process", new_tasks.len());

    if new_tasks.is_empty() {
        info!("No new tasks to process");
        if args.checkpoint && is_backfill
            && let Some(ref mut cp) = checkpoint
        {
            info!("Backfill produced no new tasks; clearing backfill fields");
            cp.backfill_cursor = None;
            cp.backfill_target = None;
            cp.last_fetched_at = chrono::Utc::now();
            cp.save()?;
        }
        return Ok(());
    }

    save_tasks(&new_tasks)?;
    info!("Successfully saved {} new tasks", new_tasks.len());

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
