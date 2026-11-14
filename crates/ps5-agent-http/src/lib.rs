pub mod auth;
pub mod backend;
pub mod error;
pub mod routes;
pub mod security;
pub mod server;

pub use auth::{
    ensure_bootstrap_credentials, ensure_bootstrap_credentials_with_report, hash_password,
    sha256_hex, verify_password,
};
pub use backend::{
    CancelConversationTasksRequest, CreateAgentRequest, CreateTaskRequest, CreateWebhookRequest,
    CreateWebhookResponse, DashboardAssets, ExecuteToolRequest, ExtensionsOverviewResponse,
    ForkConversationRequest, HttpBackend, HttpCreateSchedule, HttpScheduleAiDraft,
    HttpScheduleAiRequest, HttpScheduleAiResponse, HttpToggleScheduleRequest, HttpUpdateSchedule,
    ImportMcpRequest, ImportMemoriesRequest, ImportPluginRequest, ImportSkillRequest,
    McpServerDto, McpToolDto, MessageActionRequest, MkdirRequest, PluginInfoDto,
    ReorderConversationsRequest, SandboxStateDto, SendAgentMessageRequest, SendMessageRequest,
    SendMessageResponse, SkillDetailResponse, SkillInfoDto, SlashCommandRequest,
    ToggleExtensionRequest, UpdateAgentRequest, UpdateSandboxRequest, UpdateToolPolicyRequest,
    UploadWorkspaceFileRequest, WebhookTriggerResponse, WorkspaceFileEntry,
    WorkspaceFilesResponse, WriteBrainFileRequest,
};
pub use error::{HttpError, HttpResult};
pub use security::{is_allowed_lan_ip, SlidingWindowRateLimiter, BROWSER_PREVIEW_CSP_VALUE};
pub use server::{serve, serve_with_listener, RecoveringTcpListener};

use axum::extract::DefaultBodyLimit;
use axum::http::StatusCode;
use axum::middleware;
use axum::response::IntoResponse;
use axum::routing::{delete, get, patch, post};
use axum::Router;
use ps5_agent_config::AppConfig;
use ps5_agent_core::{StorageBackend, StreamEvent};
use ps5_agent_logging::AppLogger;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;
use tower_http::limit::RequestBodyLimitLayer;

pub struct HttpState {
    pub config: AppConfig,
    pub store: Arc<dyn StorageBackend>,
    pub backend: Arc<dyn HttpBackend>,
    pub logger: AppLogger,
    pub events_tx: broadcast::Sender<StreamEvent>,
    pub shutdown: CancellationToken,
    pub assets: DashboardAssets,
    pub general_limiter: SlidingWindowRateLimiter,
    pub login_limiter: SlidingWindowRateLimiter,
    network_degraded: AtomicBool,
    http_requests_total: AtomicU64,
    http_total_ms: AtomicU64,
    pub(crate) active_sse_connections: AtomicUsize,
}

impl HttpState {
    pub fn new(
        config: AppConfig,
        store: Arc<dyn StorageBackend>,
        backend: Arc<dyn HttpBackend>,
        logger: AppLogger,
        events_tx: broadcast::Sender<StreamEvent>,
        shutdown: CancellationToken,
        assets: DashboardAssets,
    ) -> Arc<Self> {
        Arc::new(Self {
            config,
            store,
            backend,
            logger,
            events_tx,
            shutdown,
            assets,
            general_limiter: SlidingWindowRateLimiter::new(),
            login_limiter: SlidingWindowRateLimiter::new(),
            network_degraded: AtomicBool::new(false),
            http_requests_total: AtomicU64::new(0),
            http_total_ms: AtomicU64::new(0),
            active_sse_connections: AtomicUsize::new(0),
        })
    }

    pub fn effective_config(&self) -> AppConfig {
        self.backend
            .current_config()
            .unwrap_or_else(|| self.config.clone())
    }

    pub fn is_network_degraded(&self) -> bool {
        self.network_degraded.load(Ordering::Relaxed)
    }

    /// Sets the `network_degraded` flag and returns the previous value.
    pub fn set_network_degraded(&self, degraded: bool) -> bool {
        self.network_degraded.swap(degraded, Ordering::Relaxed)
    }

    pub fn record_http_request(&self, elapsed_ms: u64) {
        self.http_requests_total.fetch_add(1, Ordering::Relaxed);
        self.http_total_ms.fetch_add(elapsed_ms, Ordering::Relaxed);
    }

    pub fn http_metrics(&self) -> (u64, u64) {
        let total = self.http_requests_total.load(Ordering::Relaxed);
        let total_ms = self.http_total_ms.load(Ordering::Relaxed);
        let avg = if total == 0 { 0 } else { total_ms / total };
        (total, avg)
    }

    pub fn active_sse_connections(&self) -> usize {
        self.active_sse_connections.load(Ordering::Relaxed)
    }
}

pub fn build_router(state: Arc<HttpState>) -> Router {
    let cfg = state.effective_config();
    let max_body_effective = if cfg.server.max_body_bytes == 0 {
        256 * 1024 * 1024
    } else {
        cfg.server.max_body_bytes
    };
    let upload_max = if cfg.server.upload_max_body_bytes == 0 {
        256 * 1024 * 1024
    } else {
        cfg.server.upload_max_body_bytes.max(max_body_effective)
    };

    let standard_api_routes = Router::new()
        // Health, metrics, SSE
        .route("/health", get(routes::health_handler))
        .route("/metrics", get(routes::metrics_handler))
        .route("/events", get(routes::events_sse_handler))
        .route("/events/stream", get(routes::events_sse_handler))
        // Auth
        .route("/auth/login", post(auth::login_handler))
        .route("/auth/logout", post(auth::logout_handler))
        .route("/auth/session", get(auth::session_handler))
        .route("/auth/password", post(auth::change_password_handler))
        // Conversations
        .route(
            "/conversations",
            get(routes::list_conversations).post(routes::create_conversation),
        )
        .route("/conversations/search", get(routes::search_conversations))
        .route("/conversations/reorder", post(routes::reorder_conversations))
        .route(
            "/conversations/{id}",
            get(routes::get_conversation).delete(routes::delete_conversation),
        )
        .route("/conversations/{id}/fork", post(routes::fork_conversation))
        .route(
            "/conversations/{id}/cancel",
            post(routes::cancel_conversation_tasks),
        )
        .route(
            "/conversations/{id}/command",
            post(routes::execute_conversation_command),
        )
        .route(
            "/conversations/{id}/messages/{msg_id}",
            delete(routes::delete_chat_message),
        )
        .route(
            "/conversations/{id}/messages/{msg_id}/retry",
            post(routes::retry_chat_message),
        )
        .route(
            "/conversations/{id}/messages/{msg_id}/regenerate",
            post(routes::regenerate_chat_message),
        )
        .route(
            "/conversations/{id}/messages/{msg_id}/continue",
            post(routes::continue_chat_message),
        )
        // Agents
        .route(
            "/agents",
            get(routes::list_agents).post(routes::create_agent),
        )
        .route("/agents/enable-all", post(routes::enable_all_agents))
        .route(
            "/agents/{id}",
            get(routes::get_agent)
                .put(routes::update_agent)
                .delete(routes::delete_agent),
        )
        .route("/agents/{id}/enable", post(routes::enable_agent))
        .route("/agents/{id}/disable", post(routes::disable_agent))
        .route("/agents/{id}/memory", get(routes::get_agent_memory))
        // Agent messages
        .route(
            "/agent-messages",
            get(routes::list_agent_messages).post(routes::send_agent_message),
        )
        // Tasks
        .route("/tasks", get(routes::list_tasks).post(routes::create_task))
        .route("/tasks/{id}", get(routes::get_task))
        .route("/tasks/{id}/cancel", post(routes::cancel_task))
        .route("/tasks/{id}/expedite", post(routes::expedite_task))
        .route("/tasks/{id}/events", get(routes::get_task_events))
        // Schedules
        .route(
            "/schedules",
            get(routes::list_schedules).post(routes::create_schedule),
        )
        .route("/schedules/ai", post(routes::parse_schedule_ai))
        .route(
            "/schedules/{id}",
            get(routes::get_schedule)
                .put(routes::update_schedule)
                .patch(routes::update_schedule)
                .delete(routes::delete_schedule),
        )
        .route("/schedules/{id}/toggle", post(routes::toggle_schedule))
        .route("/schedules/{id}/run-now", post(routes::run_schedule_now))
        // Approvals
        .route("/approvals", get(routes::list_approvals))
        .route("/approvals/{id}/approve", post(routes::approve_action))
        .route("/approvals/{id}/deny", post(routes::deny_action))
        // Memories
        .route(
            "/memories",
            get(routes::list_memories).post(routes::create_memory),
        )
        .route("/memories/export", get(routes::export_memories))
        .route("/memories/{id}", delete(routes::delete_memory))
        // Tools
        .route(
            "/tools/{id}",
            patch(routes::update_tool_policy).delete(routes::delete_custom_tool),
        )
        .route("/tools/{id}/toggle", post(routes::toggle_tool))
        .route("/tools/{id}/execute", post(routes::execute_tool))
        .route("/tool-executions", get(routes::list_tool_executions))
        // Workspace Files
        .route(
            "/files",
            get(routes::list_workspace_files).delete(routes::delete_workspace_file),
        )
        .route("/files/content", get(routes::get_workspace_file_content))
        .route("/files/download", get(routes::download_workspace_file))
        .route("/files/preview", get(routes::preview_workspace_file))
        .route("/files/mkdir", post(routes::create_workspace_folder))
        // Extensions
        .route("/extensions", get(routes::list_extensions))
        .route("/extensions/skills/{id}", get(routes::get_skill_detail))
        .route("/extensions/mcp/{id}/sync", post(routes::sync_mcp_server))
        .route(
            "/extensions/{kind}/{id}",
            patch(routes::toggle_extension).delete(routes::delete_extension),
        )
        // Logs & Audit logs
        .route("/logs", get(routes::list_logs))
        .route("/audit-logs", get(routes::list_audit_logs))
        // Settings, Inference & System
        .route("/settings", get(routes::get_settings).put(routes::update_settings))
        .route("/doctor", get(routes::run_doctor))
        .route("/doctor/fix", post(routes::fix_doctor))
        .route(
            "/sandbox",
            get(routes::get_sandbox_state).put(routes::update_sandbox_policy),
        )
        .route(
            "/sandbox/browser/sessions",
            get(routes::list_browser_sessions),
        )
        .route(
            "/sandbox/browser/preview",
            get(routes::preview_browser_session),
        )
        .route(
            "/sandbox/browser/action",
            post(routes::execute_browser_action),
        )
        .route(
            "/sandbox/browser/sessions/{id}",
            delete(routes::close_browser_session),
        )
        .route(
            "/brain",
            get(routes::list_or_read_brain).put(routes::write_brain),
        )
        .route(
            "/brain/{*name}",
            get(routes::get_brain_file).put(routes::put_brain_file),
        )
        .route(
            "/webhooks",
            get(routes::list_webhooks).post(routes::create_webhook),
        )
        .route(
            "/webhooks/inbound/{id}",
            post(routes::trigger_inbound_webhook),
        )
        .route("/webhooks/{id}", delete(routes::delete_webhook))
        .route("/inference/models", get(routes::list_inference_models))
        .route(
            "/inference/capabilities",
            get(routes::get_inference_capabilities),
        )
        .route(
            "/inference/sync",
            post(routes::sync_inference_capabilities),
        )
        .route("/inference/import-kv", post(routes::import_inference_kv))
        .route("/inference/systemone", post(routes::evaluate_systemone))
        .route("/system/checkpoint", post(routes::system_checkpoint))
        .route("/system/shutdown", post(routes::system_shutdown))
        .layer(DefaultBodyLimit::max(max_body_effective))
        .layer(RequestBodyLimitLayer::new(max_body_effective));

    let upload_api_routes = Router::new()
        .route(
            "/conversations/{id}/messages",
            get(routes::list_conversation_messages).post(routes::post_conversation_message),
        )
        .route(
            "/tools",
            get(routes::list_tools).post(routes::create_or_import_tools),
        )
        .route("/memories/import", post(routes::import_memories))
        .route("/files/upload", post(routes::upload_workspace_file))
        .route("/extensions/skills/import", post(routes::import_skill))
        .route("/extensions/plugins/import", post(routes::import_plugin))
        .route("/extensions/mcp", post(routes::import_mcp_servers))
        .layer(DefaultBodyLimit::max(upload_max))
        .layer(RequestBodyLimitLayer::new(upload_max));

    let api_routes = standard_api_routes
        .merge(upload_api_routes)
        .layer(middleware::from_fn_with_state(
            state.clone(),
            security::auth_csrf_middleware,
        ));

    let public_routes = Router::new()
        .route("/", get(routes::index_html))
        .route("/styles.css", get(routes::styles_css))
        .route("/app.js", get(routes::app_js))
        .layer(DefaultBodyLimit::max(max_body_effective))
        .layer(RequestBodyLimitLayer::new(max_body_effective));

    Router::new()
        .merge(public_routes)
        .nest("/api/v1", api_routes)
        .fallback(|| async {
            (
                StatusCode::NOT_FOUND,
                axum::Json(serde_json::json!({
                    "error": { "code": "not_found", "message": "Route not found" }
                })),
            )
                .into_response()
        })
        .layer(middleware::from_fn_with_state(
            state.clone(),
            security::security_network_middleware,
        ))
        .with_state(state)
}
