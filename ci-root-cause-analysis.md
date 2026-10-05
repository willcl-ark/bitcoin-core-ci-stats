# CI publication failures, 5 October 2026

The reported recurrence is the same size failure running code that has not
received the fix. There is also a design defect: a calendar interval is not a
bound on compressed file size.

## Evidence and timeline

- The earlier monolithic task archive reached 104.80 MB, prompting commit
  `d79c39c` to introduce monthly gzip shards with a 50 MiB guard.
- [Run 37268092418](https://github.com/willcl-ark/bitcoin-core-ci-stats/actions/runs/37268092418)
  fetched data successfully, then failed packing `2026-09.json.gz`.
- [Run 37295898269](https://github.com/willcl-ark/bitcoin-core-ci-stats/actions/runs/37295898269)
  ran daily-shard commit `43a248f` on `fix/daily-data-shards`, successfully
  published 4,500 tasks and checkpoint `37285211048` in data commit `ce0566a`.
- [Pages run 37299155628](https://github.com/willcl-ark/bitcoin-core-ci-stats/actions/runs/37299155628)
  successfully deployed that data.
- [Run 37324914722](https://github.com/willcl-ark/bitcoin-core-ci-stats/actions/runs/37324914722)
  ran old default-branch commit `d79c39c`. It read the updated checkpoint,
  fetched 416 tasks, and regrouped the daily archive into monthly files.
  September again exceeded 50 MiB. Commit and push were skipped.

The feature run changed data, not the default branch's workflow code. Scheduled
runs continue using the old packer until the fix is merged. Triggering another
feature run alone cannot resolve the recurring scheduled failure.

## Failure mechanism

Archive growth exceeds the fixed period's file-size assumption. Packing rejects
an oversized file before publication. Data and checkpoint are committed together,
so the failed run's local checkpoint is not published. The next run retries the
same interval. Pages deliberately skips failed fetch runs and retains the last
successful site.

The guard is useful. Raising or removing it would merely move failure to GitHub's
file-size limit. Switching from months to days gives headroom but cannot itself
bound file size.

## Implemented correction

Keep UTC daily grouping to avoid rewriting a whole month's data on each update.
Measure the compressed output. If a file exceeds the limit, split its rows in
half and repeat until each part fits. Binary filename suffixes preserve row order
under the existing sorted assembly. Unchanged inputs produce identical gzip
bytes. Repacking replaces the old shard set, preventing duplicate rows.

A single row larger than the limit remains an explicit error; silently truncating
it would corrupt the archive. Temporary packing preserves existing published
shards on this error. This correction bounds individual task and graph shard
files, not the total archive or unrelated summary files.

A dedicated Actions workflow runs the Python regression suite on pushes and PRs
that touch packing code, tests, or that workflow. Tests cover repeated splitting,
byte limits, exact row round trips, stable output, obsolete-part removal, date
filtering of split files, and preserving existing shards on rejection.

Local validation passed all four regression tests. Forced a 1 MiB limit on
September 14's published data: 1,702 rows round-tripped exactly across eight
parts, with the largest at 849,528 bytes. This exercises automatic splitting
rather than relying on current production headroom.

## Deployment and acceptance

1. Merge the fix branch into `master`. A feature-only dispatch is insufficient.
2. Dispatch a fresh fetch run on `master`, or wait for the schedule. Rerunning an
   old failed run uses its old commit.
3. Confirm packing and data commit/push succeed, then confirm the resulting
   Pages workflow succeeds.
4. Confirm a subsequent scheduled run uses the fixed default-branch commit.

Merging or pushing to `master` requires explicit user authorization. This task
prepares and validates the change without performing that write.

## Separate reliability findings

These do not explain the reported pack failure and require their own changes.

- Checkpoint completeness: `src/github.rs` fetches newest-first and stops at
  4,500 jobs; `src/main.rs` advances to the maximum fetched run ID. A capped
  fetch can skip the unfinished older interval. The successful feature run hit
  exactly 4,500 tasks without logging that it reached the previous checkpoint.
  An archive/source comparison is needed to quantify omissions. A fix must
  persist continuation state for unfinished intervals and finish whole runs.
- Failed or active runs: job-request/conversion errors are skipped, and active
  runs are ignored. A newer checkpoint can exclude them permanently. Backfill
  also skips a run if any task from it already exists. Retry and completeness
  rules need tests with capped, partial, and overlapping runs; changing the
  shard packer cannot recover already omitted data.
- Snapshot consistency: Pages clones one data revision, then downloads summary
  files from the moving branch head. Copying all inputs from the existing clone
  would make a deployment use one revision and avoid empty fallback summaries.
- Total archive growth: the Rust fetcher repeatedly parses all history and the
  workflow repacks all history. Daily size-bounded files do not bound memory,
  processing time, Git history, or total storage. Retention or external archive
  storage needs a separate decision about how much history to preserve. No
  memory, disk, or total-storage failure was shown in this run.
