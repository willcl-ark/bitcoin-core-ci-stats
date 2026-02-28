use thiserror::Error;

/// Application-specific errors with structured variants.
#[derive(Debug, Error)]
pub enum AppError {
    #[error("GITHUB_TOKEN environment variable is required")]
    MissingGitHubToken,

    #[error("missing field '{0}' in GitHub API response")]
    MissingField(&'static str),
}
