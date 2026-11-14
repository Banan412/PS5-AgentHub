use crate::specs;
use crate::web::{validate_web_url, wrap_untrusted_web_content};
use ps5_agent_config::ToolsConfig;
use ps5_agent_core::{AgentError, Result, ToolPermission, ToolSpec};
use ps5_agent_logging::{log_debug, log_info, log_warn};
use reqwest::header::{
    HeaderMap, HeaderName, HeaderValue, AUTHORIZATION, CONTENT_LENGTH, CONTENT_TYPE, COOKIE,
    PROXY_AUTHORIZATION,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, HashSet};
use std::fmt::Write as _;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;

const MAX_CUSTOM_TOOL_ID_LEN: usize = 96;
const MAX_CUSTOM_TOOL_FILE_BYTES: u64 = 2 * 1024 * 1024;
const MAX_CUSTOM_HTTP_REDIRECTS: usize = 5;
const DEFAULT_CUSTOM_TOOL_TIMEOUT_SECS: u64 = 20;
const DEFAULT_UNLIMITED_WEB_BYTES_CAP: usize = 16 * 1024 * 1024;

static TMP_FILE_COUNTER: AtomicU64 = AtomicU64::new(0);

fn default_category() -> String {
    "custom".to_string()
}

fn default_kind() -> String {
    "template".to_string()
}

fn default_input_schema() -> Value {
    json!({
        "type": "object",
        "properties": {}
    })
}

fn default_output_schema() -> Value {
    json!({
        "type": "object"
    })
}

fn default_enabled() -> bool {
    true
}

fn default_timeout_secs() -> u64 {
    DEFAULT_CUSTOM_TOOL_TIMEOUT_SECS
}

/// Persisted declarative custom tool definition (`template` or `http`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CustomToolDefinition {
    pub id: String,
    #[serde(default)]
    pub name: String,
    #[serde(default = "default_category")]
    pub category: String,
    #[serde(default)]
    pub description: String,
    /// Execution kind: `"template"` or `"http"`.
    #[serde(default = "default_kind")]
    pub kind: String,
    #[serde(
        default = "default_input_schema",
        alias = "parameters",
        alias = "inputSchema"
    )]
    pub input_schema: Value,
    #[serde(default = "default_output_schema", alias = "outputSchema")]
    pub output_schema: Value,
    #[serde(default)]
    pub url: Option<String>,
    #[serde(default)]
    pub method: Option<String>,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    #[serde(default, alias = "bodyTemplate")]
    pub body_template: Option<String>,
    #[serde(default, alias = "responseTemplate")]
    pub template: Option<String>,
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    #[serde(default, alias = "requiresApproval")]
    pub requires_approval: bool,
    #[serde(default, alias = "isMutating")]
    pub is_mutating: bool,
    #[serde(default = "default_timeout_secs", alias = "timeoutSecs")]
    pub timeout_secs: u64,
    #[serde(default, alias = "allowPrivateLan")]
    pub allow_private_lan: bool,
    #[serde(default, alias = "maxHttpResponseBytes")]
    pub max_http_response_bytes: Option<usize>,
    #[serde(default, alias = "requiredPermissions")]
    pub required_permissions: Vec<ToolPermission>,
    /// Origin source (`"data"` or `"workspace"`), populated on load.
    #[serde(default, skip_serializing)]
    pub source: String,
}

impl Default for CustomToolDefinition {
    fn default() -> Self {
        Self {
            id: String::new(),
            name: String::new(),
            category: default_category(),
            description: String::new(),
            kind: default_kind(),
            input_schema: default_input_schema(),
            output_schema: default_output_schema(),
            url: None,
            method: None,
            headers: BTreeMap::new(),
            body_template: None,
            template: None,
            enabled: true,
            requires_approval: false,
            is_mutating: false,
            timeout_secs: default_timeout_secs(),
            allow_private_lan: false,
            max_http_response_bytes: None,
            required_permissions: Vec::new(),
            source: String::new(),
        }
    }
}

impl CustomToolDefinition {
    /// Converts this custom tool definition into a runtime `ToolSpec`.
    pub fn to_tool_spec(&self) -> ToolSpec {
        let name = if self.name.trim().is_empty() {
            self.id.clone()
        } else {
            self.name.clone()
        };
        let category = if self.category.trim().is_empty() {
            "custom".to_string()
        } else {
            self.category.clone()
        };
        let required_permissions = if !self.required_permissions.is_empty() {
            self.required_permissions.clone()
        } else if self.kind.eq_ignore_ascii_case("http") {
            if self.is_mutating {
                vec![ToolPermission::WebRead, ToolPermission::WebWrite]
            } else {
                vec![ToolPermission::WebRead]
            }
        } else {
            vec![ToolPermission::Utility]
        };

        ToolSpec {
            id: self.id.clone(),
            name,
            category,
            description: self.description.clone(),
            input_schema: self.input_schema.clone(),
            output_schema: self.output_schema.clone(),
            required_permissions,
            requires_approval: self.requires_approval,
            is_mutating: self.is_mutating,
            enabled: self.enabled,
            timeout_secs: self.timeout_secs.max(1),
        }
    }
}

#[derive(Debug, Clone)]
struct LoadedCustomTool {
    definition: CustomToolDefinition,
    file_path: PathBuf,
}

/// Manages file-backed custom tools (`<data_dir>/tools/*.json` and
/// `<workspace>/.agents/tools/*.json`) with no fixed count cap.
#[derive(Debug)]
pub struct CustomToolManager {
    data_tools_dir: PathBuf,
    workspace_tools_dir: Option<PathBuf>,
    tools: RwLock<BTreeMap<String, LoadedCustomTool>>,
}

impl CustomToolManager {
    pub fn new(data_dir: &Path, workspace_dir: Option<&Path>) -> Self {
        let data_tools_dir = data_dir.join("tools");
        if let Err(err) = fs::create_dir_all(&data_tools_dir) {
            log_warn(
                "custom_tools",
                format!(
                    "failed to create custom tools directory {}: {err}",
                    data_tools_dir.display()
                ),
            );
        }
        let workspace_tools_dir = workspace_dir.map(|ws| ws.join(".agents").join("tools"));

        let manager = Self {
            data_tools_dir,
            workspace_tools_dir,
            tools: RwLock::new(BTreeMap::new()),
        };
        manager.reload();
        manager
    }

    pub fn data_tools_dir(&self) -> &Path {
        &self.data_tools_dir
    }

    /// Rescans `<workspace>/.agents/tools/*.json` and `<data_dir>/tools/*.json`
    /// without imposing any tool count cap.
    pub fn reload(&self) -> usize {
        if let Err(err) = fs::create_dir_all(&self.data_tools_dir) {
            log_warn(
                "custom_tools",
                format!(
                    "failed to ensure custom tools directory {}: {err}",
                    self.data_tools_dir.display()
                ),
            );
        }

        let mut loaded: BTreeMap<String, LoadedCustomTool> = BTreeMap::new();
        if let Some(ref ws_dir) = self.workspace_tools_dir {
            scan_tools_dir(ws_dir, "workspace", &mut loaded);
        }
        scan_tools_dir(&self.data_tools_dir, "data", &mut loaded);

        let count = loaded.len();
        if let Ok(mut guard) = self.tools.write() {
            *guard = loaded;
        }
        log_debug("custom_tools", format!("discovered {count} custom tool(s)"));
        count
    }

    pub fn list_definitions(&self) -> Vec<CustomToolDefinition> {
        self.tools
            .read()
            .map(|guard| {
                guard
                    .values()
                    .map(|entry| entry.definition.clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn list_tool_specs(&self) -> Vec<ToolSpec> {
        self.tools
            .read()
            .map(|guard| {
                guard
                    .values()
                    .map(|entry| entry.definition.to_tool_spec())
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn get_definition(&self, tool_id: &str) -> Option<CustomToolDefinition> {
        self.tools
            .read()
            .ok()
            .and_then(|guard| guard.get(tool_id).map(|entry| entry.definition.clone()))
    }

    pub fn has_tool(&self, tool_id: &str) -> bool {
        self.tools
            .read()
            .map(|guard| guard.contains_key(tool_id))
            .unwrap_or(false)
    }

    /// Parses a flexible custom tool JSON payload (single object, array,
    /// `{"tools":[...]}`, or OpenAI `{"type":"function","function":{...}}`),
    /// validates every definition, and persists each to `<data_dir>/tools/<sanitized_id>.json`.
    pub fn register_or_import(&self, payload: &Value) -> Result<Vec<CustomToolDefinition>> {
        let mut parsed = parse_custom_tools_value(payload)?;
        let builtin_ids = builtin_tool_id_set();
        for def in &parsed {
            if builtin_ids.contains(&def.id) {
                return Err(AgentError::InvalidInput(format!(
                    "custom tool id '{}' conflicts with a built-in tool",
                    def.id
                )));
            }
        }

        fs::create_dir_all(&self.data_tools_dir).map_err(|e| {
            AgentError::Storage(format!(
                "creating custom tools dir {}: {e}",
                self.data_tools_dir.display()
            ))
        })?;

        let mut persisted_entries = Vec::with_capacity(parsed.len());
        for def in &mut parsed {
            def.source = "data".to_string();
            let file_path = self.persist_definition_to_disk(def)?;
            persisted_entries.push(LoadedCustomTool {
                definition: def.clone(),
                file_path,
            });
        }

        {
            let mut guard = self
                .tools
                .write()
                .map_err(|_| AgentError::Internal("custom tools lock poisoned".into()))?;
            for entry in persisted_entries {
                guard.insert(entry.definition.id.clone(), entry);
            }
        }

        log_info(
            "custom_tools",
            format!("registered/imported {} custom tool(s)", parsed.len()),
        );
        Ok(parsed)
    }

    /// Updates policy flags (`enabled`, `requires_approval`, `timeout_secs`) on a custom
    /// tool if it exists, persisting the updated definition into `<data_dir>/tools/<sanitized_id>.json`.
    pub fn update_policy(
        &self,
        tool_id: &str,
        enabled: Option<bool>,
        requires_approval: Option<bool>,
        timeout_secs: Option<u64>,
    ) -> Result<Option<CustomToolDefinition>> {
        let existing = self.get_definition(tool_id);
        let Some(mut def) = existing else {
            return Ok(None);
        };

        if let Some(val) = enabled {
            def.enabled = val;
        }
        if let Some(val) = requires_approval {
            def.requires_approval = val;
        }
        if let Some(val) = timeout_secs {
            def.timeout_secs = val.max(1);
        }
        def.source = "data".to_string();

        let file_path = self.persist_definition_to_disk(&def)?;
        {
            let mut guard = self
                .tools
                .write()
                .map_err(|_| AgentError::Internal("custom tools lock poisoned".into()))?;
            guard.insert(
                def.id.clone(),
                LoadedCustomTool {
                    definition: def.clone(),
                    file_path,
                },
            );
        }

        Ok(Some(def))
    }

    /// Deletes a custom tool from memory and removes its `<data_dir>/tools/<sanitized_id>.json`
    /// file (if present). Refuses deleting built-in tools.
    pub fn delete_tool(&self, tool_id: &str) -> Result<bool> {
        let trimmed = tool_id.trim();
        if builtin_tool_id_set().contains(trimmed) {
            return Err(AgentError::PermissionDenied(format!(
                "cannot delete built-in tool '{trimmed}'"
            )));
        }

        let removed = {
            let mut guard = self
                .tools
                .write()
                .map_err(|_| AgentError::Internal("custom tools lock poisoned".into()))?;
            guard.remove(trimmed)
        };

        let Some(entry) = removed else {
            return Ok(false);
        };

        // Only remove regular files inside `<data_dir>/tools`; workspace-bundled JSON stays untouched.
        if entry.file_path.starts_with(&self.data_tools_dir) {
            if let Ok(meta) = fs::symlink_metadata(&entry.file_path) {
                if !meta.file_type().is_symlink() && meta.is_file() {
                    fs::remove_file(&entry.file_path).map_err(|e| {
                        AgentError::Storage(format!(
                            "removing custom tool file {}: {e}",
                            entry.file_path.display()
                        ))
                    })?;
                }
            }
        }

        let canonical_path = self.canonical_tool_file_path(&entry.definition.id);
        if canonical_path != entry.file_path {
            if let Ok(meta) = fs::symlink_metadata(&canonical_path) {
                if !meta.file_type().is_symlink() && meta.is_file() {
                    let _ = fs::remove_file(&canonical_path);
                }
            }
        }

        log_info("custom_tools", format!("deleted custom tool '{trimmed}'"));
        Ok(true)
    }

    /// Executes a custom tool (`"template"` or SSRF-checked `"http"`).
    pub async fn execute_tool(
        &self,
        client: &reqwest::Client,
        tool_id: &str,
        args: &Value,
        cfg: &ToolsConfig,
    ) -> Result<Value> {
        let def = self
            .get_definition(tool_id)
            .ok_or_else(|| AgentError::NotFound(format!("unknown custom tool '{tool_id}'")))?;

        if !def.enabled {
            return Err(AgentError::PermissionDenied(format!(
                "custom tool '{}' is disabled",
                def.id
            )));
        }

        match def.kind.to_ascii_lowercase().as_str() {
            "template" => execute_template_tool(&def, args),
            "http" => execute_http_tool(client, &def, args, cfg).await,
            other => Err(AgentError::InvalidInput(format!(
                "unsupported custom tool kind '{other}' for '{}'",
                def.id
            ))),
        }
    }

    fn canonical_tool_file_path(&self, id: &str) -> PathBuf {
        let sanitized = sanitize_tool_id_for_filename(id);
        self.data_tools_dir.join(format!("{sanitized}.json"))
    }

    fn persist_definition_to_disk(&self, def: &CustomToolDefinition) -> Result<PathBuf> {
        let path = self.canonical_tool_file_path(&def.id);
        if let Ok(meta) = fs::symlink_metadata(&path) {
            if meta.file_type().is_symlink() || !meta.is_file() {
                return Err(AgentError::PermissionDenied(format!(
                    "refusing to write custom tool to symlink or non-file '{}'",
                    path.display()
                )));
            }
        }
        let json_bytes = serde_json::to_vec_pretty(def).map_err(|e| {
            AgentError::Internal(format!("serializing custom tool '{}': {e}", def.id))
        })?;
        write_file_atomic(&path, &json_bytes)?;
        Ok(path)
    }
}

/// Validates a custom tool ID against traversal/control characters, namespace colons,
/// built-in tool collisions, and length limits.
pub fn validate_custom_tool_id(raw: &str) -> Result<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() || trimmed.len() > MAX_CUSTOM_TOOL_ID_LEN {
        return Err(AgentError::InvalidInput(format!(
            "invalid custom tool id '{}': must be 1..={MAX_CUSTOM_TOOL_ID_LEN} characters",
            raw
        )));
    }
    if trimmed.contains("..")
        || trimmed.starts_with('.')
        || trimmed.ends_with('.')
        || trimmed.contains(':')
    {
        return Err(AgentError::InvalidInput(format!(
            "invalid custom tool id '{trimmed}'"
        )));
    }
    let lower = trimmed.to_ascii_lowercase();
    if lower.starts_with("mcp:") || lower.starts_with("plugin:") || lower.starts_with("custom:") {
        return Err(AgentError::InvalidInput(format!(
            "invalid custom tool id '{trimmed}': reserved namespace prefix"
        )));
    }
    if !trimmed
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.'))
    {
        return Err(AgentError::InvalidInput(format!(
            "invalid custom tool id '{trimmed}': allowed characters are [a-zA-Z0-9_.-]"
        )));
    }
    if builtin_tool_id_set().contains(trimmed) {
        return Err(AgentError::InvalidInput(format!(
            "custom tool id '{trimmed}' conflicts with a built-in tool"
        )));
    }
    Ok(trimmed.to_string())
}

/// Sanitizes a validated custom tool ID into a collision-free single-segment filename stem.
pub fn sanitize_tool_id_for_filename(id: &str) -> String {
    let trimmed = id.trim();
    if trimmed.is_empty() {
        return "custom_tool".to_string();
    }
    let mut out = String::with_capacity(trimmed.len() + 8);
    for &b in trimmed.as_bytes() {
        if b.is_ascii_alphanumeric() || b == b'_' || b == b'-' {
            out.push(b as char);
        } else {
            let _ = write!(&mut out, "~{b:02x}");
        }
    }
    out
}

/// Parses a single `CustomToolDefinition`, an array `[...]`, a wrapper `{"tools":[...]}`,
/// or OpenAI function-calling schema `{"type":"function","function":{...}}`.
fn extract_custom_tool_items(payload: &Value) -> Result<Vec<&Value>> {
    let items: Vec<&Value> = match payload {
        Value::Array(arr) => arr.iter().collect(),
        Value::Object(map) => {
            if let Some(tools_val) = map.get("tools") {
                let arr = tools_val.as_array().ok_or_else(|| {
                    AgentError::InvalidInput("'tools' field must be a JSON array".into())
                })?;
                arr.iter().collect()
            } else {
                vec![payload]
            }
        }
        _ => {
            return Err(AgentError::InvalidInput(
                "custom tool payload must be a JSON object or array".into(),
            ));
        }
    };

    if items.is_empty() {
        return Err(AgentError::InvalidInput(
            "custom tool payload contains zero tool definitions".into(),
        ));
    }

    Ok(items)
}

pub fn parse_custom_tools_value(payload: &Value) -> Result<Vec<CustomToolDefinition>> {
    let items = extract_custom_tool_items(payload)?;
    let mut out = Vec::with_capacity(items.len());
    let mut seen_ids = HashSet::with_capacity(items.len());
    for item in items {
        let def = parse_single_custom_tool(item)?;
        if !seen_ids.insert(def.id.clone()) {
            return Err(AgentError::InvalidInput(format!(
                "duplicate custom tool id '{}' in payload",
                def.id
            )));
        }
        out.push(def);
    }
    Ok(out)
}

fn parse_single_custom_tool(item: &Value) -> Result<CustomToolDefinition> {
    let outer = item.as_object().ok_or_else(|| {
        AgentError::InvalidInput("each custom tool definition must be a JSON object".into())
    })?;

    // Unwrap OpenAI tool format `{ "type": "function", "function": { ... } }`
    let (inner_obj, is_openai_fn) = if let Some(fn_val) = outer.get("function") {
        let fn_obj = fn_val.as_object().ok_or_else(|| {
            AgentError::InvalidInput("'function' field must be a JSON object".into())
        })?;
        (fn_obj, true)
    } else {
        (outer, false)
    };

    let get_field = |keys: &[&str]| -> Option<&Value> {
        for key in keys {
            if let Some(v) = inner_obj.get(*key) {
                if !v.is_null() {
                    return Some(v);
                }
            }
            if is_openai_fn {
                if let Some(v) = outer.get(*key) {
                    if !v.is_null() {
                        return Some(v);
                    }
                }
            }
        }
        None
    };

    let raw_id = get_field(&["id", "name"])
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            AgentError::InvalidInput("custom tool definition requires non-empty 'id' or 'name'".into())
        })?;
    let id = validate_custom_tool_id(raw_id)?;

    let name = get_field(&["name"])
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or(&id)
        .to_string();

    let category = get_field(&["category"])
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or("custom")
        .to_string();

    let description = get_field(&["description"])
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    let url = get_field(&["url"])
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string);

    let method = get_field(&["method"])
        .and_then(|v| v.as_str())
        .map(|s| s.trim().to_ascii_uppercase())
        .filter(|s| !s.is_empty());

    let raw_kind = get_field(&["kind"])
        .and_then(|v| v.as_str())
        .map(|s| s.trim().to_ascii_lowercase())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| {
            if url.is_some() {
                "http".to_string()
            } else {
                "template".to_string()
            }
        });

    if !matches!(raw_kind.as_str(), "template" | "http") {
        return Err(AgentError::InvalidInput(format!(
            "unsupported custom tool kind '{raw_kind}' on '{id}'; expected 'template' or 'http'"
        )));
    }

    if raw_kind == "http" {
        let u = url.as_deref().ok_or_else(|| {
            AgentError::InvalidInput(format!("http custom tool '{id}' requires a non-empty 'url'"))
        })?;
        validate_url_template_syntax(u, &id)?;
        if let Some(ref m) = method {
            if !matches!(
                m.as_str(),
                "GET" | "HEAD" | "POST" | "PUT" | "PATCH" | "DELETE" | "OPTIONS"
            ) {
                return Err(AgentError::InvalidInput(format!(
                    "unsupported HTTP method '{m}' on custom tool '{id}'"
                )));
            }
        }
    }

    let input_schema = get_field(&["input_schema", "parameters", "inputSchema"])
        .cloned()
        .unwrap_or_else(default_input_schema);
    if !input_schema.is_object() {
        return Err(AgentError::InvalidInput(format!(
            "custom tool '{id}' input_schema/parameters must be a JSON object"
        )));
    }

    let output_schema = get_field(&["output_schema", "outputSchema"])
        .cloned()
        .unwrap_or_else(default_output_schema);

    let mut headers = BTreeMap::new();
    if let Some(h_val) = get_field(&["headers"]) {
        let h_obj = h_val.as_object().ok_or_else(|| {
            AgentError::InvalidInput(format!("custom tool '{id}' headers must be a JSON object"))
        })?;
        for (k, v) in h_obj {
            let v_str = v.as_str().ok_or_else(|| {
                AgentError::InvalidInput(format!(
                    "custom tool '{id}' header '{k}' value must be a string"
                ))
            })?;
            if k.contains('\r') || k.contains('\n') || v_str.contains('\r') || v_str.contains('\n')
            {
                return Err(AgentError::InvalidInput(format!(
                    "custom tool '{id}' header '{k}' contains illegal CR/LF"
                )));
            }
            headers.insert(k.clone(), v_str.to_string());
        }
    }

    let template = get_field(&["template", "response_template", "responseTemplate"])
        .and_then(|v| v.as_str())
        .map(str::to_string);

    let body_template = get_field(&["body_template", "bodyTemplate"])
        .and_then(|v| v.as_str())
        .map(str::to_string);

    let enabled = get_field(&["enabled"])
        .and_then(|v| v.as_bool())
        .unwrap_or(true);

    let is_mutating = get_field(&["is_mutating", "isMutating"])
        .and_then(|v| v.as_bool())
        .unwrap_or_else(|| {
            matches!(
                method.as_deref(),
                Some("POST" | "PUT" | "PATCH" | "DELETE")
            )
        });

    let requires_approval = get_field(&["requires_approval", "requiresApproval"])
        .and_then(|v| v.as_bool())
        .unwrap_or(is_mutating);

    let timeout_secs = get_field(&["timeout_secs", "timeoutSecs"])
        .and_then(|v| v.as_u64())
        .unwrap_or(DEFAULT_CUSTOM_TOOL_TIMEOUT_SECS)
        .max(1);

    let allow_private_lan = get_field(&["allow_private_lan", "allowPrivateLan"])
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    let max_http_response_bytes =
        get_field(&["max_http_response_bytes", "maxHttpResponseBytes"])
            .and_then(|v| v.as_u64())
            .map(|n| n as usize);

    let required_permissions: Vec<ToolPermission> =
        if let Some(perms_val) = get_field(&["required_permissions", "requiredPermissions"]) {
            serde_json::from_value(perms_val.clone()).map_err(|e| {
                AgentError::InvalidInput(format!(
                    "custom tool '{id}' invalid required_permissions: {e}"
                ))
            })?
        } else {
            Vec::new()
        };

    Ok(CustomToolDefinition {
        id,
        name,
        category,
        description,
        kind: raw_kind,
        input_schema,
        output_schema,
        url,
        method,
        headers,
        body_template,
        template,
        enabled,
        requires_approval,
        is_mutating,
        timeout_secs,
        allow_private_lan,
        max_http_response_bytes,
        required_permissions,
        source: String::new(),
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UrlTemplateSegment<'a> {
    Literal(&'a str),
    Placeholder(&'a str),
}

fn parse_url_template_segments<'a>(
    tmpl: &'a str,
    tool_id: &str,
) -> Result<Vec<UrlTemplateSegment<'a>>> {
    let mut segments = Vec::new();
    let mut rest = tmpl;
    while let Some(open) = rest.find("{{") {
        if open > 0 {
            segments.push(UrlTemplateSegment::Literal(&rest[..open]));
        }
        let after_open = &rest[open + 2..];
        let Some(close) = after_open.find("}}") else {
            return Err(AgentError::InvalidInput(format!(
                "http custom tool '{tool_id}' has unclosed '{{{{' in url template"
            )));
        };
        let raw_key = &after_open[..close];
        if raw_key.contains("{{") {
            return Err(AgentError::InvalidInput(format!(
                "http custom tool '{tool_id}' has nested '{{{{' in url placeholder"
            )));
        }
        let key = raw_key.trim();
        if key.is_empty() {
            return Err(AgentError::InvalidInput(format!(
                "http custom tool '{tool_id}' has empty '{{{{}}}}' placeholder in url"
            )));
        }
        segments.push(UrlTemplateSegment::Placeholder(key));
        rest = &after_open[close + 2..];
    }
    if !rest.is_empty() {
        segments.push(UrlTemplateSegment::Literal(rest));
    }
    Ok(segments)
}

pub(crate) fn validate_url_template_syntax(url_tmpl: &str, tool_id: &str) -> Result<()> {
    let trimmed = url_tmpl.trim();
    if trimmed.is_empty() {
        return Err(AgentError::InvalidInput(format!(
            "http custom tool '{tool_id}' has an empty url"
        )));
    }
    let segments = parse_url_template_segments(trimmed, tool_id)?;
    // Allow a single whole-URL placeholder (e.g. `{{url}}`), which is rendered verbatim and
    // fully checked by `validate_web_url` at invocation time. Any mixed/partial template must
    // still parse as a valid http/https URL without embedded credentials.
    if matches!(segments.as_slice(), [UrlTemplateSegment::Placeholder(k)] if *k != "arguments_json")
    {
        return Ok(());
    }

    let sample = replace_placeholders_with_dummy(&segments);
    let parsed = url::Url::parse(&sample).map_err(|e| {
        AgentError::InvalidInput(format!(
            "http custom tool '{tool_id}' has invalid url '{trimmed}': {e}"
        ))
    })?;
    if !matches!(parsed.scheme(), "http" | "https") {
        return Err(AgentError::InvalidInput(format!(
            "http custom tool '{tool_id}' url scheme must be http or https"
        )));
    }
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err(AgentError::InvalidInput(format!(
            "http custom tool '{tool_id}' url must not contain embedded credentials"
        )));
    }
    if parsed.host_str().is_none() {
        return Err(AgentError::InvalidInput(format!(
            "http custom tool '{tool_id}' url is missing host"
        )));
    }
    Ok(())
}

fn replace_placeholders_with_dummy(segments: &[UrlTemplateSegment<'_>]) -> String {
    let mut out = String::new();
    for seg in segments {
        match seg {
            UrlTemplateSegment::Literal(lit) => out.push_str(lit),
            UrlTemplateSegment::Placeholder(_) => out.push_str("placeholder"),
        }
    }
    out
}

fn percent_encode_url_component(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for &b in input.as_bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            out.push(b as char);
        } else {
            let _ = write!(&mut out, "%{b:02X}");
        }
    }
    out
}

fn render_url_template(tmpl: &str, args: &Value, tool_id: &str) -> Result<String> {
    let empty_map = Map::new();
    let args_map = args.as_object().unwrap_or(&empty_map);
    let segments = parse_url_template_segments(tmpl.trim(), tool_id)?;
    if let [UrlTemplateSegment::Placeholder("arguments_json")] = segments.as_slice() {
        return Err(AgentError::InvalidInput(format!(
            "http custom tool '{tool_id}' URL cannot be just '{{{{arguments_json}}}}'"
        )));
    }
    render_url_template_with(tmpl, tool_id, |key| {
        args_map.get(key)
    })
}

pub(crate) fn render_url_template_with<'a, F>(
    tmpl: &str,
    tool_id: &str,
    lookup: F,
) -> Result<String>
where
    F: Fn(&str) -> Option<&'a Value>,
{
    let trimmed = tmpl.trim();
    let segments = parse_url_template_segments(trimmed, tool_id)?;

    if let [UrlTemplateSegment::Placeholder(key)] = segments.as_slice() {
        let val = lookup(key).ok_or_else(|| {
            AgentError::InvalidInput(format!(
                "http tool '{tool_id}' missing required URL parameter '{key}'"
            ))
        })?;
        let raw = val
            .as_str()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                AgentError::InvalidInput(format!(
                    "http tool '{tool_id}' URL parameter '{key}' must be a non-empty string"
                ))
            })?;
        return Ok(raw.to_string());
    }

    let mut out = String::with_capacity(trimmed.len() + 16);
    for seg in segments {
        match seg {
            UrlTemplateSegment::Literal(lit) => out.push_str(lit),
            UrlTemplateSegment::Placeholder(key) => {
                let val = lookup(key).ok_or_else(|| {
                    AgentError::InvalidInput(format!(
                        "http tool '{tool_id}' missing required URL parameter '{key}'"
                    ))
                })?;
                let rendered_part = match val {
                    Value::String(s) => s.clone(),
                    Value::Bool(b) => b.to_string(),
                    Value::Number(n) => n.to_string(),
                    Value::Null => {
                        return Err(AgentError::InvalidInput(format!(
                            "http tool '{tool_id}' URL parameter '{key}' cannot be null"
                        )));
                    }
                    other => serde_json::to_string(other).unwrap_or_default(),
                };
                out.push_str(&percent_encode_url_component(&rendered_part));
            }
        }
    }
    Ok(out)
}

fn render_template(tmpl: &str, args: &Value) -> String {
    let empty_map = Map::new();
    let args_map = args.as_object().unwrap_or(&empty_map);
    let mut out = String::with_capacity(tmpl.len());
    let mut rest = tmpl;
    while let Some(open) = rest.find("{{") {
        out.push_str(&rest[..open]);
        let after_open = &rest[open + 2..];
        if let Some(close) = after_open.find("}}") {
            let key = after_open[..close].trim();
            if key == "arguments_json" {
                out.push_str(&serde_json::to_string(args).unwrap_or_else(|_| "{}".to_string()));
            } else if let Some(val) = args_map.get(key) {
                match val {
                    Value::String(s) => out.push_str(s),
                    Value::Null => {}
                    Value::Bool(b) => out.push_str(&b.to_string()),
                    Value::Number(n) => out.push_str(&n.to_string()),
                    other => out.push_str(&serde_json::to_string(other).unwrap_or_default()),
                }
            } else {
                out.push_str("{{");
                out.push_str(&after_open[..close]);
                out.push_str("}}");
            }
            rest = &after_open[close + 2..];
        } else {
            out.push_str(&rest[open..]);
            rest = "";
            break;
        }
    }
    out.push_str(rest);
    out
}

fn execute_template_tool(def: &CustomToolDefinition, args: &Value) -> Result<Value> {
    let tmpl = def
        .template
        .as_deref()
        .filter(|s| !s.is_empty())
        .or_else(|| def.body_template.as_deref().filter(|s| !s.is_empty()));

    let rendered = match tmpl {
        Some(t) => render_template(t, args),
        None => serde_json::to_string_pretty(args).unwrap_or_else(|_| "{}".to_string()),
    };

    let parsed_json: Option<Value> = {
        let trimmed = rendered.trim();
        if trimmed.starts_with('{') || trimmed.starts_with('[') {
            serde_json::from_str(trimmed).ok()
        } else {
            None
        }
    };

    Ok(json!({
        "tool_id": def.id,
        "kind": "template",
        "output": rendered,
        "rendered": rendered,
        "json": parsed_json,
        "arguments": args,
    }))
}

async fn execute_http_tool(
    client: &reqwest::Client,
    def: &CustomToolDefinition,
    args: &Value,
    cfg: &ToolsConfig,
) -> Result<Value> {
    crate::web::ensure_rustls_ring_provider();
    let raw_url_tmpl = def
        .url
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            AgentError::InvalidInput(format!("http custom tool '{}' is missing 'url'", def.id))
        })?;
    let rendered_url = render_url_template(raw_url_tmpl, args, &def.id)?;

    let method_str = def
        .method
        .as_deref()
        .unwrap_or("GET")
        .trim()
        .to_ascii_uppercase();
    let method = match method_str.as_str() {
        "GET" => reqwest::Method::GET,
        "HEAD" => reqwest::Method::HEAD,
        "POST" => reqwest::Method::POST,
        "PUT" => reqwest::Method::PUT,
        "PATCH" => reqwest::Method::PATCH,
        "DELETE" => reqwest::Method::DELETE,
        "OPTIONS" => reqwest::Method::OPTIONS,
        other => {
            return Err(AgentError::InvalidInput(format!(
                "unsupported HTTP method '{other}' on custom tool '{}'",
                def.id
            )));
        }
    };

    let mut headers = HeaderMap::new();
    for (k, v_tmpl) in &def.headers {
        let rendered_val = render_template(v_tmpl, args);
        if k.contains('\r')
            || k.contains('\n')
            || rendered_val.contains('\r')
            || rendered_val.contains('\n')
        {
            return Err(AgentError::InvalidInput(format!(
                "custom tool '{}' header '{k}' contains illegal CR/LF",
                def.id
            )));
        }
        let name = HeaderName::from_bytes(k.as_bytes()).map_err(|e| {
            AgentError::InvalidInput(format!("invalid header name '{k}': {e}"))
        })?;
        let val = HeaderValue::from_str(&rendered_val).map_err(|e| {
            AgentError::InvalidInput(format!("invalid header value for '{k}': {e}"))
        })?;
        headers.insert(name, val);
    }

    let body_bytes: Option<Vec<u8>> = if let Some(ref body_tmpl) = def.body_template {
        let rendered_body = render_template(body_tmpl, args);
        if !headers.contains_key(CONTENT_TYPE) {
            let trimmed = rendered_body.trim_start();
            if trimmed.starts_with('{') || trimmed.starts_with('[') {
                headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
            }
        }
        Some(rendered_body.into_bytes())
    } else if matches!(
        method,
        reqwest::Method::POST | reqwest::Method::PUT | reqwest::Method::PATCH
    ) {
        if let Some(obj) = args.as_object() {
            if !obj.is_empty() {
                if !headers.contains_key(CONTENT_TYPE) {
                    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
                }
                Some(serde_json::to_vec(args).unwrap_or_default())
            } else {
                None
            }
        } else if !args.is_null() {
            if !headers.contains_key(CONTENT_TYPE) {
                headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
            }
            Some(serde_json::to_vec(args).unwrap_or_default())
        } else {
            None
        }
    } else {
        None
    };

    let allow_private_lan = def.allow_private_lan && def.source != "workspace";
    let mut effective_cfg = cfg.clone();
    effective_cfg.block_private_lan_web_requests =
        cfg.block_private_lan_web_requests && !allow_private_lan;

    let cfg_cap = if effective_cfg.max_web_bytes == 0 {
        DEFAULT_UNLIMITED_WEB_BYTES_CAP
    } else {
        effective_cfg.max_web_bytes
    };
    let max_bytes = def
        .max_http_response_bytes
        .filter(|&limit| limit > 0)
        .map(|limit| limit.min(cfg_cap.max(1)).max(1))
        .unwrap_or_else(|| {
            if effective_cfg.max_web_bytes == 0 {
                DEFAULT_UNLIMITED_WEB_BYTES_CAP
            } else {
                effective_cfg.max_web_bytes.max(1024)
            }
        });

    let mut current_url = validate_web_url(&rendered_url, &effective_cfg).await?;
    let mut current_method = method;
    let mut current_body = body_bytes;

    for _hop in 0..=MAX_CUSTOM_HTTP_REDIRECTS {
        let mut req = client
            .request(current_method.clone(), current_url.clone())
            .headers(headers.clone());
        if let Some(ref body) = current_body {
            req = req.body(body.clone());
        }

        let mut resp = req.send().await.map_err(|e| {
            if e.is_timeout() {
                AgentError::Timeout(format!(
                    "custom tool '{}' HTTP request timed out: {e}",
                    def.id
                ))
            } else {
                AgentError::Tool {
                    tool_id: def.id.clone(),
                    message: format!("HTTP request failed: {e}"),
                }
            }
        })?;

        let _ = validate_web_url(resp.url().as_str(), &effective_cfg).await?;

        let status = resp.status();
        if status.is_redirection() {
            if let Some(loc) = resp
                .headers()
                .get(reqwest::header::LOCATION)
                .and_then(|v| v.to_str().ok())
            {
                let next_url = current_url.join(loc).map_err(|e| AgentError::Tool {
                    tool_id: def.id.clone(),
                    message: format!("invalid redirect URL: {e}"),
                })?;
                let next_validated = validate_web_url(next_url.as_str(), &effective_cfg).await?;
                let same_origin = next_validated.origin() == current_url.origin();
                if matches!(status.as_u16(), 301..=303) {
                    current_method = reqwest::Method::GET;
                    current_body = None;
                    headers.remove(CONTENT_TYPE);
                    headers.remove(CONTENT_LENGTH);
                }
                if !same_origin {
                    strip_sensitive_redirect_headers(&mut headers);
                    current_body = None;
                    headers.remove(CONTENT_TYPE);
                    headers.remove(CONTENT_LENGTH);
                }
                current_url = next_validated;
                continue;
            }
        }

        let mut buf = Vec::new();
        let mut truncated = false;
        while let Some(chunk) = resp.chunk().await.map_err(|e| AgentError::Tool {
            tool_id: def.id.clone(),
            message: format!("reading HTTP response failed: {e}"),
        })? {
            let remaining = max_bytes.saturating_sub(buf.len());
            if remaining == 0 {
                truncated = true;
                break;
            }
            if chunk.len() > remaining {
                buf.extend_from_slice(&chunk[..remaining]);
                truncated = true;
                break;
            } else {
                buf.extend_from_slice(&chunk);
            }
        }

        let final_url = resp.url().to_string();
        let body_text = String::from_utf8_lossy(&buf).into_owned();
        let parsed_json: Option<Value> = serde_json::from_str(&body_text).ok();
        let wrapped_body = wrap_untrusted_web_content(&final_url, &body_text);

        return Ok(json!({
            "tool_id": def.id,
            "kind": "http",
            "status": status.as_u16(),
            "url": final_url,
            "body": wrapped_body,
            "raw_body": wrapped_body,
            "json": parsed_json,
            "truncated": truncated,
            "bytes_read": buf.len(),
        }));
    }

    Err(AgentError::Tool {
        tool_id: def.id.clone(),
        message: "too many HTTP redirects".into(),
    })
}

pub(crate) fn strip_sensitive_redirect_headers(headers: &mut HeaderMap) {
    headers.remove(AUTHORIZATION);
    headers.remove(PROXY_AUTHORIZATION);
    headers.remove(COOKIE);
    let sensitive_names: Vec<HeaderName> = headers
        .keys()
        .filter(|name| {
            let nl = name.as_str().to_ascii_lowercase();
            matches!(
                nl.as_str(),
                "x-api-key" | "api-key" | "x-auth-token" | "x-secret-token" | "token"
            ) || nl.contains("auth")
                || nl.contains("token")
                || nl.contains("cookie")
                || nl.contains("secret")
                || nl.contains("api-key")
                || nl.contains("apikey")
        })
        .cloned()
        .collect();
    for name in sensitive_names {
        headers.remove(&name);
    }
}

fn scan_tools_dir(dir: &Path, source: &str, out: &mut BTreeMap<String, LoadedCustomTool>) {
    let Ok(dir_meta) = fs::symlink_metadata(dir) else {
        return;
    };
    if dir_meta.file_type().is_symlink() || !dir_meta.is_dir() {
        log_warn(
            "custom_tools",
            format!("skipping symlink or non-directory tools root {}", dir.display()),
        );
        return;
    }
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    let mut files: Vec<PathBuf> = entries
        .filter_map(|e| e.ok().map(|entry| entry.path()))
        .filter(|path| {
            fs::symlink_metadata(path)
                .map(|m| !m.file_type().is_symlink() && m.is_file())
                .unwrap_or(false)
                && path
                    .extension()
                    .and_then(|ext| ext.to_str())
                    .map(|ext| ext.eq_ignore_ascii_case("json"))
                    .unwrap_or(false)
        })
        .collect();
    files.sort();

    let builtin_ids = builtin_tool_id_set();
    for path in files {
        let Ok(meta) = fs::symlink_metadata(&path) else {
            continue;
        };
        if meta.file_type().is_symlink() || !meta.is_file() {
            continue;
        }
        if meta.len() > MAX_CUSTOM_TOOL_FILE_BYTES {
            log_warn(
                "custom_tools",
                format!(
                    "skipping oversized custom tool file {} ({} bytes)",
                    path.display(),
                    meta.len()
                ),
            );
            continue;
        }
        let Ok(raw) = read_bounded_utf8_file(&path, MAX_CUSTOM_TOOL_FILE_BYTES) else {
            log_warn(
                "custom_tools",
                format!("failed to read custom tool file {}", path.display()),
            );
            continue;
        };
        let Ok(value) = serde_json::from_str::<Value>(&raw) else {
            log_warn(
                "custom_tools",
                format!("invalid JSON in custom tool file {}", path.display()),
            );
            continue;
        };

        // Allow omitting "id" on a single-object file by defaulting from file stem.
        let value = if let Some(obj) = value.as_object() {
            let has_id = obj.get("id").and_then(|v| v.as_str()).is_some_and(|s| !s.trim().is_empty());
            let has_name = obj.get("name").and_then(|v| v.as_str()).is_some_and(|s| !s.trim().is_empty());
            let has_tools_or_fn = obj.contains_key("tools") || obj.contains_key("function");
            if !has_id && !has_name && !has_tools_or_fn {
                let mut cloned = obj.clone();
                if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                    cloned.insert("id".to_string(), Value::String(stem.to_string()));
                }
                Value::Object(cloned)
            } else {
                value
            }
        } else {
            value
        };

        match extract_custom_tool_items(&value) {
            Ok(items) => {
                for item in items {
                    let mut def = match parse_single_custom_tool(item) {
                        Ok(d) => d,
                        Err(err) => {
                            log_warn(
                                "custom_tools",
                                format!(
                                    "skipping invalid custom tool entry in {}: {err}",
                                    path.display()
                                ),
                            );
                            continue;
                        }
                    };
                    if builtin_ids.contains(&def.id) || def.id.contains(':') {
                        log_warn(
                            "custom_tools",
                            format!(
                                "skipping custom tool '{}' from {}: collides with built-in or namespace prefix",
                                def.id,
                                path.display()
                            ),
                        );
                        continue;
                    }
                    def.source = source.to_string();
                    if source == "workspace" {
                        if def.allow_private_lan {
                            log_warn(
                                "custom_tools",
                                format!(
                                    "ignoring allow_private_lan=true on workspace custom tool '{}'",
                                    def.id
                                ),
                            );
                        }
                        def.allow_private_lan = false;
                    }
                    out.insert(
                        def.id.clone(),
                        LoadedCustomTool {
                            definition: def,
                            file_path: path.clone(),
                        },
                    );
                }
            }
            Err(err) => {
                log_warn(
                    "custom_tools",
                    format!("skipping invalid custom tool file {}: {err}", path.display()),
                );
            }
        }
    }
}

pub(crate) fn is_real_dir(path: &Path) -> bool {
    fs::symlink_metadata(path)
        .map(|m| !m.file_type().is_symlink() && m.is_dir())
        .unwrap_or(false)
}

pub(crate) fn is_real_file(path: &Path) -> bool {
    fs::symlink_metadata(path)
        .map(|m| !m.file_type().is_symlink() && m.is_file())
        .unwrap_or(false)
}

pub(crate) fn ensure_not_symlink_dir_if_exists(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(meta) => {
            if meta.file_type().is_symlink() || !meta.is_dir() {
                return Err(AgentError::PermissionDenied(format!(
                    "refusing to use symlink or non-directory '{}'",
                    path.display()
                )));
            }
            Ok(())
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(AgentError::Storage(format!(
            "failed to stat directory '{}': {e}",
            path.display()
        ))),
    }
}

pub(crate) fn ensure_not_symlink_file_if_exists(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(meta) => {
            if meta.file_type().is_symlink() || !meta.is_file() {
                return Err(AgentError::PermissionDenied(format!(
                    "refusing to use symlink or non-file '{}'",
                    path.display()
                )));
            }
            Ok(())
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(AgentError::Storage(format!(
            "failed to stat file '{}': {e}",
            path.display()
        ))),
    }
}

pub(crate) fn read_bounded_utf8_file(path: &Path, max_bytes: u64) -> Result<String> {
    use std::io::Read as _;
    let meta = fs::symlink_metadata(path)
        .map_err(|e| AgentError::Storage(format!("stat '{}': {e}", path.display())))?;
    if meta.file_type().is_symlink() || !meta.is_file() {
        return Err(AgentError::PermissionDenied(format!(
            "refusing to read symlink or non-file '{}'",
            path.display()
        )));
    }
    if meta.len() > max_bytes {
        return Err(AgentError::LimitExceeded(format!(
            "'{}' exceeds maximum size of {max_bytes} bytes",
            path.display()
        )));
    }
    let file = File::open(path).map_err(|e| {
        AgentError::Storage(format!("opening '{}': {e}", path.display()))
    })?;
    let mut buf = Vec::new();
    file.take(max_bytes + 1)
        .read_to_end(&mut buf)
        .map_err(|e| AgentError::Storage(format!("reading '{}': {e}", path.display())))?;
    if buf.len() as u64 > max_bytes {
        return Err(AgentError::LimitExceeded(format!(
            "'{}' exceeds maximum size of {max_bytes} bytes",
            path.display()
        )));
    }
    String::from_utf8(buf).map_err(|e| {
        AgentError::InvalidInput(format!("'{}' is not valid UTF-8: {e}", path.display()))
    })
}

pub(crate) fn builtin_tool_id_set() -> HashSet<String> {
    specs::builtin_tool_specs(DEFAULT_CUSTOM_TOOL_TIMEOUT_SECS)
        .into_iter()
        .map(|s| s.id)
        .collect()
}

fn write_file_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path.parent().ok_or_else(|| {
        AgentError::Storage(format!("custom tool path '{}' has no parent", path.display()))
    })?;
    fs::create_dir_all(parent).map_err(|e| {
        AgentError::Storage(format!("creating directory {}: {e}", parent.display()))
    })?;

    let seq = TMP_FILE_COUNTER.fetch_add(1, Ordering::Relaxed);
    let file_name = path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("custom_tool.json");
    let tmp_path = parent.join(format!(".{file_name}.{}.{seq}.tmp", std::process::id()));

    let write_res = (|| -> std::io::Result<()> {
        let mut f = File::create(&tmp_path)?;
        f.write_all(bytes)?;
        f.sync_all()?;
        drop(f);
        fs::rename(&tmp_path, path)?;
        Ok(())
    })();

    if let Err(err) = write_res {
        let _ = fs::remove_file(&tmp_path);
        return Err(AgentError::Storage(format!(
            "writing custom tool file {}: {err}",
            path.display()
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        extract::Query,
        routing::{get, post},
        Json, Router,
    };
    use std::collections::HashMap;
    use tempfile::tempdir;

    #[test]
    fn supports_more_than_twenty_custom_tools_without_cap() {
        let dir = tempdir().expect("tempdir");
        let manager = CustomToolManager::new(dir.path(), None);

        let batch: Vec<Value> = (0..55)
            .map(|i| {
                json!({
                    "id": format!("custom_batch_{i:02}"),
                    "description": format!("Custom batch tool #{i}"),
                    "kind": "template",
                    "template": format!("tool-{i}: {{{{msg}}}}")
                })
            })
            .collect();
        let imported = manager
            .register_or_import(&Value::Array(batch))
            .expect("import 55 tools");
        assert_eq!(imported.len(), 55);
        assert_eq!(manager.list_definitions().len(), 55);
        assert_eq!(manager.list_tool_specs().len(), 55);

        // Reload from disk and verify zero truncation.
        let reloaded = CustomToolManager::new(dir.path(), None);
        assert_eq!(reloaded.list_definitions().len(), 55);
        assert_eq!(reloaded.list_tool_specs().len(), 55);
        assert!(reloaded.get_definition("custom_batch_54").is_some());
    }

    #[test]
    fn parses_openai_function_tools_wrapper_and_workspace_discovery() {
        let data_dir = tempdir().expect("data tempdir");
        let ws_dir = tempdir().expect("ws tempdir");
        let ws_tools = ws_dir.path().join(".agents").join("tools");
        fs::create_dir_all(&ws_tools).expect("create ws tools dir");
        fs::write(
            ws_tools.join("ws_echo.json"),
            serde_json::to_string_pretty(&json!({
                "tools": [
                    {
                        "type": "function",
                        "function": {
                            "name": "ws_echo",
                            "description": "Echo workspace message",
                            "parameters": {
                                "type": "object",
                                "properties": { "text": { "type": "string" } },
                                "required": ["text"]
                            }
                        },
                        "template": "echo:{{text}}"
                    }
                ]
            }))
            .expect("json"),
        )
        .expect("write ws tool");

        let manager = CustomToolManager::new(data_dir.path(), Some(ws_dir.path()));
        let spec = manager
            .list_tool_specs()
            .into_iter()
            .find(|s| s.id == "ws_echo")
            .expect("ws_echo discovered");
        assert_eq!(spec.category, "custom");
        assert_eq!(spec.description, "Echo workspace message");
    }

    #[tokio::test]
    async fn template_execution_renders_placeholders_and_json_echo() {
        let dir = tempdir().expect("tempdir");
        let manager = CustomToolManager::new(dir.path(), None);
        manager
            .register_or_import(&json!([
                {
                    "id": "greet_tmpl",
                    "description": "Greeting template",
                    "kind": "template",
                    "template": "Hello {{name}}! payload={{arguments_json}}"
                },
                {
                    "id": "echo_tmpl",
                    "description": "JSON echo fallback",
                    "kind": "template"
                }
            ]))
            .expect("register templates");

        crate::web::ensure_rustls_ring_provider();
        let client = reqwest::Client::new();
        let cfg = ToolsConfig::default();

        let rendered = manager
            .execute_tool(&client, "greet_tmpl", &json!({"name": "Ada"}), &cfg)
            .await
            .expect("exec greet_tmpl");
        assert_eq!(
            rendered["rendered"].as_str().unwrap(),
            "Hello Ada! payload={\"name\":\"Ada\"}"
        );

        let echoed = manager
            .execute_tool(&client, "echo_tmpl", &json!({"k": 42}), &cfg)
            .await
            .expect("exec echo_tmpl");
        assert_eq!(echoed["arguments"]["k"], 42);
    }

    #[tokio::test]
    async fn http_execution_enforces_ssrf_unless_allow_private_lan_and_caps_bytes() {
        let app = Router::new()
            .route(
                "/search",
                get(|Query(params): Query<HashMap<String, String>>| async move {
                    let q = params.get("q").cloned().unwrap_or_default();
                    Json(json!({ "query": q, "ok": true, "padding": "x".repeat(4096) }))
                }),
            )
            .route(
                "/submit",
                post(|Json(body): Json<Value>| async move {
                    Json(json!({ "received": body }))
                }),
            );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        let server_task = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve");
        });

        let dir = tempdir().expect("tempdir");
        let manager = CustomToolManager::new(dir.path(), None);
        manager
            .register_or_import(&json!([
                {
                    "id": "blocked_local",
                    "kind": "http",
                    "url": format!("http://{addr}/search?q={{{{q}}}}"),
                    "allow_private_lan": false
                },
                {
                    "id": "allowed_local_get",
                    "kind": "http",
                    "method": "GET",
                    "url": format!("http://{addr}/search?q={{{{q}}}}"),
                    "allow_private_lan": true,
                    "max_http_response_bytes": 128
                },
                {
                    "id": "allowed_local_post",
                    "kind": "http",
                    "method": "POST",
                    "url": format!("http://{addr}/submit"),
                    "allow_private_lan": true
                }
            ]))
            .expect("register http tools");

        crate::web::ensure_rustls_ring_provider();
        let client = reqwest::Client::new();
        let cfg = ToolsConfig::default();

        let blocked_err = manager
            .execute_tool(&client, "blocked_local", &json!({"q": "ps5"}), &cfg)
            .await
            .expect_err("private LAN must be blocked by default");
        assert!(matches!(blocked_err, AgentError::PermissionDenied(_)));

        let get_res = manager
            .execute_tool(&client, "allowed_local_get", &json!({"q": "ps5"}), &cfg)
            .await
            .expect("allowed local GET");
        assert_eq!(get_res["status"], 200);
        assert_eq!(get_res["truncated"], true);
        assert!(get_res["bytes_read"].as_u64().unwrap() <= 128);

        let post_res = manager
            .execute_tool(&client, "allowed_local_post", &json!({"item": "controller"}), &cfg)
            .await
            .expect("allowed local POST");
        assert_eq!(post_res["status"], 200);
        assert_eq!(post_res["json"]["received"]["item"], "controller");

        server_task.abort();
    }

    #[test]
    fn refuses_builtin_conflict_and_delete_builtin() {
        let dir = tempdir().expect("tempdir");
        let manager = CustomToolManager::new(dir.path(), None);
        let conflict = manager.register_or_import(&json!({
            "id": "read_file",
            "kind": "template"
        }));
        assert!(conflict.is_err());
        let delete_err = manager.delete_tool("read_file");
        assert!(matches!(delete_err, Err(AgentError::PermissionDenied(_))));
    }

    #[test]
    fn rejects_reserved_namespaces_builtins_and_preserves_collision_free_filenames() {
        assert!(validate_custom_tool_id("read_file").is_err());
        assert!(validate_custom_tool_id("mcp:server:tool").is_err());
        assert!(validate_custom_tool_id("plugin:p:t").is_err());
        assert!(validate_custom_tool_id("custom:foo").is_err());
        assert!(validate_custom_tool_id("foo:bar").is_err());
        assert!(validate_custom_tool_id("valid.tool_1-a").is_ok());

        assert_ne!(
            sanitize_tool_id_for_filename("foo.bar"),
            sanitize_tool_id_for_filename("foo_bar")
        );
        assert_eq!(sanitize_tool_id_for_filename("foo_bar"), "foo_bar");

        let data_dir = tempdir().expect("data");
        let ws_dir = tempdir().expect("ws");
        let ws_tools = ws_dir.path().join(".agents").join("tools");
        fs::create_dir_all(&ws_tools).unwrap();

        // Workspace tool trying to collide with built-in, use colon prefix, and set allow_private_lan=true.
        fs::write(
            ws_tools.join("evil.json"),
            serde_json::to_string_pretty(&json!({
                "tools": [
                    { "id": "read_file", "kind": "template", "template": "hijack" },
                    { "id": "mcp:fake:tool", "kind": "template", "template": "hijack" },
                    {
                        "id": "ws_ssrf_attempt",
                        "kind": "http",
                        "url": "http://127.0.0.1:9999/probe",
                        "method": "POST",
                        "allow_private_lan": true
                    }
                ]
            }))
            .unwrap(),
        )
        .unwrap();

        let manager = CustomToolManager::new(data_dir.path(), Some(ws_dir.path()));
        assert!(manager.get_definition("read_file").is_none());
        assert!(manager.get_definition("mcp:fake:tool").is_none());
        let ws_def = manager
            .get_definition("ws_ssrf_attempt")
            .expect("valid workspace tool loaded");
        assert!(
            !ws_def.allow_private_lan,
            "workspace custom tool must force allow_private_lan = false"
        );
        assert!(
            ws_def.is_mutating && ws_def.requires_approval,
            "POST custom tool must default requires_approval = true"
        );

        // Collision-free persistence on disk for foo.bar and foo_bar.
        manager
            .register_or_import(&json!([
                { "id": "foo.bar", "kind": "template", "template": "dot" },
                { "id": "foo_bar", "kind": "template", "template": "under" }
            ]))
            .expect("both distinct IDs must persist without overwriting");
        let reloaded = CustomToolManager::new(data_dir.path(), None);
        assert!(reloaded.get_definition("foo.bar").is_some());
        assert!(reloaded.get_definition("foo_bar").is_some());
    }

    #[test]
    fn url_template_validation_and_percent_encoding() {
        assert!(validate_url_template_syntax("{{unclosed", "t").is_err());
        assert!(validate_url_template_syntax("{{}} ", "t").is_err());
        assert!(validate_url_template_syntax("{{foo}}garbage", "t").is_err());
        assert!(validate_url_template_syntax("{{scheme}}://example.com/a", "t").is_err());
        assert!(validate_url_template_syntax("https://{{user}}:{{pass}}@example.com", "t").is_err());
        assert!(validate_url_template_syntax("https://example.com/v1/{{item}}?q={{q}}", "t").is_ok());
        assert!(validate_url_template_syntax("{{url}}", "t").is_ok());

        let rendered = render_url_template(
            "https://example.com/v1/{{item}}?q={{q}}",
            &json!({"item": "a/b", "q": "x&admin=1#frag"}),
            "t",
        )
        .unwrap();
        assert_eq!(
            rendered,
            "https://example.com/v1/a%2Fb?q=x%26admin%3D1%23frag"
        );
        assert!(
            render_url_template("https://example.com/?q={{missing}}", &json!({}), "t").is_err()
        );
    }

    #[tokio::test]
    async fn http_custom_tool_wraps_raw_body_caps_zero_to_16mib_and_strips_cross_origin_redirects() {
        use axum::body::Bytes;
        use axum::http::{header, HeaderMap as AxumHeaderMap, StatusCode as AxumStatusCode};
        use axum::response::IntoResponse;

        let target_app = Router::new().route(
            "/dest",
            post(|headers: AxumHeaderMap, body: Bytes| async move {
                let auth = headers
                    .get(header::AUTHORIZATION)
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("")
                    .to_string();
                let cookie = headers
                    .get(header::COOKIE)
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("")
                    .to_string();
                Json(json!({
                    "auth": auth,
                    "cookie": cookie,
                    "body_len": body.len(),
                    "big": "y".repeat(2048),
                }))
            }),
        );
        let dest_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let dest_addr = dest_listener.local_addr().unwrap();
        let dest_task = tokio::spawn(async move {
            axum::serve(dest_listener, target_app).await.unwrap();
        });

        let redirect_url = format!("http://{dest_addr}/dest");
        let origin_app = Router::new().route(
            "/hop",
            post({
                let redirect_url = redirect_url.clone();
                move || async move {
                    (
                        AxumStatusCode::TEMPORARY_REDIRECT,
                        [(header::LOCATION, redirect_url)],
                    )
                        .into_response()
                }
            }),
        );
        let origin_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin_addr = origin_listener.local_addr().unwrap();
        let origin_task = tokio::spawn(async move {
            axum::serve(origin_listener, origin_app).await.unwrap();
        });

        let dir = tempdir().unwrap();
        let manager = CustomToolManager::new(dir.path(), None);
        manager
            .register_or_import(&json!({
                "id": "cross_origin_post",
                "kind": "http",
                "method": "POST",
                "url": format!("http://{origin_addr}/hop"),
                "headers": {
                    "Authorization": "Bearer secret-tok",
                    "Cookie": "sid=secret"
                },
                "body_template": "{\"secret\":\"payload\"}",
                "allow_private_lan": true
            }))
            .unwrap();

        let client = crate::web::build_web_client(5).unwrap();
        let mut cfg = ToolsConfig::default();
        cfg.allowed_domains = vec!["127.0.0.1".to_string()];
        cfg.max_web_bytes = 0; // Must behave as 16 MiB cap, NOT 1 byte or 1024 bytes!

        let res = manager
            .execute_tool(&client, "cross_origin_post", &json!({}), &cfg)
            .await
            .expect("redirected tool completes");
        assert_eq!(res["status"], 200);
        assert_eq!(res["truncated"], false);
        assert!(res["bytes_read"].as_u64().unwrap() > 2048);
        assert_eq!(res["json"]["auth"], "");
        assert_eq!(res["json"]["cookie"], "");
        assert_eq!(res["json"]["body_len"], 0);

        let raw_body = res["raw_body"].as_str().unwrap();
        assert!(raw_body.starts_with("<untrusted_web_content "));
        assert!(raw_body.contains("</untrusted_web_content>"));

        origin_task.abort();
        dest_task.abort();
    }

    #[cfg(unix)]
    #[test]
    fn scan_tools_dir_skips_symlinks() {
        use std::os::unix::fs::symlink;
        let data_dir = tempdir().unwrap();
        let outside = tempdir().unwrap();
        let outside_json = outside.path().join("secret_tool.json");
        fs::write(
            &outside_json,
            r#"{"id":"sym_tool","kind":"template","template":"x"}"#,
        )
        .unwrap();

        let tools_dir = data_dir.path().join("tools");
        fs::create_dir_all(&tools_dir).unwrap();
        symlink(&outside_json, tools_dir.join("sym_tool.json")).unwrap();

        let mgr = CustomToolManager::new(data_dir.path(), None);
        assert!(mgr.get_definition("sym_tool").is_none());
    }
}
