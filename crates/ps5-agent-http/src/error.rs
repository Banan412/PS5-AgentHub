use axum::http::{header, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use ps5_agent_core::AgentError;
use serde::Serialize;

#[derive(Debug)]
pub enum HttpError {
    BadRequest(String),
    Unauthorized(&'static str),
    Forbidden(&'static str),
    NotFound(String),
    Conflict(String),
    PayloadTooLarge,
    TooManyRequests {
        retry_after_secs: u64,
        message: &'static str,
    },
    ApprovalRequired {
        approval_id: String,
        reason: String,
    },
    BadGateway(String),
    ServiceUnavailable(String),
    GatewayTimeout(String),
    Internal(String),
}

#[derive(Serialize)]
struct ErrorEnvelope<'a> {
    error: ErrorBody<'a>,
}

#[derive(Serialize)]
struct ErrorBody<'a> {
    code: &'a str,
    message: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    approval_id: Option<&'a str>,
}

impl HttpError {
    pub fn from_agent(err: AgentError) -> Self {
        match err {
            AgentError::NotFound(m) => Self::NotFound(m),
            AgentError::InvalidInput(m) => Self::BadRequest(m),
            AgentError::PermissionDenied(_) => Self::Forbidden("Permission denied"),
            AgentError::Protected(_) => Self::Forbidden("Resource is protected"),
            AgentError::LimitExceeded(_) => Self::TooManyRequests {
                retry_after_secs: 5,
                message: "Runtime limit exceeded",
            },
            AgentError::Conflict(m) => Self::Conflict(m),
            AgentError::ApprovalRequired {
                approval_id,
                reason,
            } => Self::ApprovalRequired {
                approval_id,
                reason,
            },
            AgentError::InferenceUnavailable(m) => Self::ServiceUnavailable(m),
            AgentError::Inference(m) => Self::BadGateway(m),
            AgentError::Timeout(m) => Self::GatewayTimeout(m),
            AgentError::Cancelled(m) => Self::Conflict(m),
            AgentError::Storage(m) => {
                ps5_agent_logging::log_error("http.error", format!("Storage operation failed: {m}"));
                Self::Internal("Storage operation failed".to_string())
            }
            AgentError::Tool { tool_id, message } => {
                Self::BadRequest(format!("Tool '{tool_id}' failed: {message}"))
            }
            AgentError::Internal(m) => {
                ps5_agent_logging::log_error("http.error", format!("Internal agent error: {m}"));
                Self::Internal("Internal server error".to_string())
            }
        }
    }
}

impl From<AgentError> for HttpError {
    fn from(value: AgentError) -> Self {
        Self::from_agent(value)
    }
}

impl IntoResponse for HttpError {
    fn into_response(self) -> Response {
        match self {
            Self::BadRequest(msg) => (
                StatusCode::BAD_REQUEST,
                Json(ErrorEnvelope {
                    error: ErrorBody {
                        code: "bad_request",
                        message: &msg,
                        approval_id: None,
                    },
                }),
            )
                .into_response(),
            Self::Unauthorized(msg) => (
                StatusCode::UNAUTHORIZED,
                Json(ErrorEnvelope {
                    error: ErrorBody {
                        code: "unauthorized",
                        message: msg,
                        approval_id: None,
                    },
                }),
            )
                .into_response(),
            Self::Forbidden(msg) => (
                StatusCode::FORBIDDEN,
                Json(ErrorEnvelope {
                    error: ErrorBody {
                        code: "forbidden",
                        message: msg,
                        approval_id: None,
                    },
                }),
            )
                .into_response(),
            Self::NotFound(msg) => (
                StatusCode::NOT_FOUND,
                Json(ErrorEnvelope {
                    error: ErrorBody {
                        code: "not_found",
                        message: &msg,
                        approval_id: None,
                    },
                }),
            )
                .into_response(),
            Self::Conflict(msg) => (
                StatusCode::CONFLICT,
                Json(ErrorEnvelope {
                    error: ErrorBody {
                        code: "conflict",
                        message: &msg,
                        approval_id: None,
                    },
                }),
            )
                .into_response(),
            Self::PayloadTooLarge => (
                StatusCode::PAYLOAD_TOO_LARGE,
                Json(ErrorEnvelope {
                    error: ErrorBody {
                        code: "payload_too_large",
                        message: "Request body exceeds maximum allowed size",
                        approval_id: None,
                    },
                }),
            )
                .into_response(),
            Self::TooManyRequests {
                retry_after_secs,
                message,
            } => {
                let mut resp = (
                    StatusCode::TOO_MANY_REQUESTS,
                    Json(ErrorEnvelope {
                        error: ErrorBody {
                            code: "rate_limited",
                            message,
                            approval_id: None,
                        },
                    }),
                )
                    .into_response();
                if let Ok(val) = HeaderValue::from_str(&retry_after_secs.max(1).to_string()) {
                    resp.headers_mut().insert(header::RETRY_AFTER, val);
                }
                resp
            }
            Self::ApprovalRequired {
                approval_id,
                reason,
            } => (
                StatusCode::CONFLICT,
                Json(ErrorEnvelope {
                    error: ErrorBody {
                        code: "approval_required",
                        message: &reason,
                        approval_id: Some(&approval_id),
                    },
                }),
            )
                .into_response(),
            Self::BadGateway(msg) => {
                ps5_agent_logging::log_error("http.error", format!("502 Bad Gateway: {msg}"));
                (
                    StatusCode::BAD_GATEWAY,
                    Json(ErrorEnvelope {
                        error: ErrorBody {
                            code: "bad_gateway",
                            message: &msg,
                            approval_id: None,
                        },
                    }),
                )
                    .into_response()
            }
            Self::ServiceUnavailable(msg) => {
                ps5_agent_logging::log_error("http.error", format!("503 Service Unavailable: {msg}"));
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(ErrorEnvelope {
                        error: ErrorBody {
                            code: "service_unavailable",
                            message: &msg,
                            approval_id: None,
                        },
                    }),
                )
                    .into_response()
            }
            Self::GatewayTimeout(msg) => {
                ps5_agent_logging::log_error("http.error", format!("504 Gateway Timeout: {msg}"));
                (
                    StatusCode::GATEWAY_TIMEOUT,
                    Json(ErrorEnvelope {
                        error: ErrorBody {
                            code: "gateway_timeout",
                            message: &msg,
                            approval_id: None,
                        },
                    }),
                )
                    .into_response()
            }
            Self::Internal(msg) => {
                if !matches!(
                    msg.as_str(),
                    "Storage operation failed" | "Internal server error"
                ) {
                    ps5_agent_logging::log_error("http.error", format!("500 Internal Error: {msg}"));
                }
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorEnvelope {
                        error: ErrorBody {
                            code: "internal_error",
                            message: &msg,
                            approval_id: None,
                        },
                    }),
                )
                    .into_response()
            }
        }
    }
}

pub type HttpResult<T> = std::result::Result<T, HttpError>;
