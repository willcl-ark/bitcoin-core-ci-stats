use anyhow::Result;
use tracing::info;

use crate::constants::CHECKPOINT_FILENAME;
use crate::models::Checkpoint;

impl Checkpoint {
    pub fn load() -> Result<Option<Self>> {
        if !std::path::Path::new(CHECKPOINT_FILENAME).exists() {
            return Ok(None);
        }

        let content = std::fs::read_to_string(CHECKPOINT_FILENAME)?;
        let checkpoint: Self = serde_json::from_str(&content)?;
        Ok(Some(checkpoint))
    }

    pub fn save(&self) -> Result<()> {
        let content = serde_json::to_string_pretty(self)?;
        std::fs::write(CHECKPOINT_FILENAME, content)?;
        info!(
            "Saved checkpoint: last_run_id={}, backfill_cursor={:?}, backfill_target={:?}",
            self.last_run_id, self.backfill_cursor, self.backfill_target
        );
        Ok(())
    }

    pub fn reset() -> Result<()> {
        if std::path::Path::new(CHECKPOINT_FILENAME).exists() {
            std::fs::remove_file(CHECKPOINT_FILENAME)?;
            info!("Checkpoint file deleted");
        }
        Ok(())
    }
}
