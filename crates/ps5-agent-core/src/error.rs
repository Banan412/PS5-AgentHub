use thiserror::Error;

/// Unified error type across ps5-agent crates.
#[derive(Debug, Error)]
pub enum AgentError {
    #[error("not found: {0}")]
    NotFound(String),

    #[error("invalid input: {0}")]
    InvalidInput(String),

    #[error("permission denied: {0}")]
    PermissionDenied(String),

    #[error("protected resource: {0}")]
    Protected(String),

    #[error("limit exceeded: {0}")]
    LimitExceeded(String),

    #[error("conflict: {0}")]
    Conflict(String),

    #[error("approval required ({approval_id}): {reason}")]
    ApprovalRequired { approval_id: String, reason: String },

    #[error("inference error: {0}")]
    Inference(String),

    #[error("inference unavailable: {0}")]
    InferenceUnavailable(String),

    #[error("timeout: {0}")]
    Timeout(String),

    #[error("cancelled: {0}")]
    Cancelled(String),

    #[error("storage error: {0}")]
    Storage(String),

    #[error("tool error ({tool_id}): {message}")]
    Tool { tool_id: String, message: String },

    #[error("internal error: {0}")]
    Internal(String),
}

pub type Result<T> = std::result::Result<T, AgentError>;
