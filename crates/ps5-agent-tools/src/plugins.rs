use crate::custom::{
    ensure_not_symlink_dir_if_exists, ensure_not_symlink_file_if_exists, is_real_dir, is_real_file,
    read_bounded_utf8_file, render_url_template_with, strip_sensitive_redirect_headers,
    validate_url_template_syntax,
};
use crate::web::{validate_web_url, wrap_untrusted_web_content};
use ps5_agent_config::ToolsConfig;
use ps5_agent_core::{AgentError, Result, ToolPermission, ToolSpec};
use ps5_agent_logging::{log_debug, log_info, log_warn};
use reqwest::header::{
    HeaderMap, HeaderName, HeaderValue, CONTENT_LENGTH, CONTENT_TYPE,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::RwLock;
use uuid::Uuid;

const MAX_PLUGIN_HTTP_REDIRECTS: usize = 5;
const DEFAULT_PLUGIN_TOOL_TIMEOUT_SECS: u64 = 30;
const DEFAULT_UNLIMITED_WEB_BYTES_CAP: usize = 16 * 1024 * 1024;
const MAX_PLUGIN_MANIFEST_BYTES: u64 = 2 * 1024 * 1024;
const MAX_PLUGIN_RULE_BYTES: u64 = 512 * 1024;
const MAX_PLUGIN_STATE_BYTES: u64 = 1024 * 1024;

fn default_parameters_schema() -> Value {
    json!({
        "type": "object",
        "properties": {}
    })
}

fn default_tool_kind() -> String {
    "template".to_string()
}

/// Declarative custom tool embedded in a `plugin.json` manifest.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PluginCustomToolSpec {
    pub name: String,
    #[serde(default)]
    pub description: String,
    #[serde(
        default = "default_parameters_schema",
        alias = "input_schema",
        alias = "inputSchema"
    )]
    pub parameters: Value,
    #[serde(default = "default_tool_kind")]
    pub kind: String,
    #[serde(default)]
    pub url: Option<String>,
    #[serde(default)]
    pub method: Option<String>,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    #[serde(default, alias = "bodyTemplate")]
    pub body_template: Option<String>,
    #[serde(default, alias = "responseTemplate")]
    pub response_template: Option<String>,
    #[serde(default, alias = "isMutating")]
    pub is_mutating: bool,
}

impl Default for PluginCustomToolSpec {
    fn default() -> Self {
        Self {
            name: String::new(),
            description: String::new(),
            parameters: default_parameters_schema(),
            kind: default_tool_kind(),
            url: None,
            method: None,
            headers: BTreeMap::new(),
            body_template: None,
            response_template: None,
            is_mutating: false,
        }
    }
}

impl PluginCustomToolSpec {
    /// Returns a copy of `headers` with sensitive/non-content header values masked.
    pub fn masked_headers(&self) -> BTreeMap<String, String> {
        mask_headers(&self.headers)
    }
}

/// Antigravity / Claude-compatible plugin manifest (`plugins/<slug>/plugin.json`).
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct PluginManifest {
    pub name: String,
    #[serde(default, alias = "displayName")]
    pub display_name: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub version: Option<String>,
    #[serde(default)]
    pub disabled: bool,
    #[serde(default)]
    pub author: Option<Value>,
    #[serde(default)]
    pub rules: Vec<String>,
    #[serde(default, alias = "mcpServers")]
    pub mcp_servers: Option<Value>,
    #[serde(default)]
    pub tools: Vec<PluginCustomToolSpec>,
}

/// Summary view of a discovered plugin for UI/API responses.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PluginView {
    pub id: String,
    pub name: String,
    pub display_name: String,
    pub description: String,
    pub version: String,
    pub enabled: bool,
    pub rules_count: usize,
    pub skills_count: usize,
    pub tools: Vec<String>,
    pub has_mcp: bool,
}

/// Secret-masked view of a custom tool provided by a plugin.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PluginToolView {
    pub namespaced_id: String,
    pub plugin_id: String,
    pub name: String,
    pub description: String,
    pub parameters: Value,
    pub kind: String,
    pub url: Option<String>,
    pub method: Option<String>,
    pub headers: BTreeMap<String, String>,
    pub is_mutating: bool,
}

/// Metadata for a skill bundled inside `plugins/<slug>/skills/*/SKILL.md`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PluginBundledSkill {
    pub plugin_id: String,
    pub id: String,
    pub dir_path: PathBuf,
    pub skill_md_path: PathBuf,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct PluginsStateFile {
    #[serde(default)]
    overrides: BTreeMap<String, bool>,
}

#[derive(Debug, Clone)]
struct LoadedPlugin {
    id: String,
    source: &'static str,
    manifest: PluginManifest,
    rules: Vec<String>,
    bundled_skills: Vec<PluginBundledSkill>,
    mcp_config: Option<Value>,
}

/// Discovers, manages, and executes declarative plugins from `<data_dir>/plugins`
/// and `<workspace>/.agents/plugins`.
pub struct PluginManager {
    data_plugins_dir: PathBuf,
    workspace_plugins_dir: Option<PathBuf>,
    state_path: PathBuf,
    plugins: RwLock<BTreeMap<String, LoadedPlugin>>,
    state: RwLock<PluginsStateFile>,
}

impl PluginManager {
    /// Scans `<data_dir>/plugins/*/plugin.json` and `<workspace>/.agents/plugins/*/plugin.json`,
    /// loading enable/disable overrides from `<data_dir>/plugins_state.json`.
    pub fn new(data_dir: &Path, workspace_dir: Option<&Path>) -> Self {
        let data_plugins_dir = data_dir.join("plugins");
        if let Err(err) = fs::create_dir_all(&data_plugins_dir) {
            log_warn(
                "plugins",
                format!(
                    "failed to create plugins dir {}: {err}",
                    data_plugins_dir.display()
                ),
            );
        }
        let workspace_plugins_dir = workspace_dir.map(|w| w.join(".agents").join("plugins"));
        let state_path = data_dir.join("plugins_state.json");
        let state = load_state_file(&state_path);

        let manager = Self {
            data_plugins_dir,
            workspace_plugins_dir,
            state_path,
            plugins: RwLock::new(BTreeMap::new()),
            state: RwLock::new(state),
        };
        manager.rescan_internal();
        manager
    }

    /// Re-scans plugin directories and reloads `<data_dir>/plugins_state.json`.
    pub fn reload(&self) -> Result<()> {
        let loaded_state = load_state_file(&self.state_path);
        {
            let mut sg = self
                .state
                .write()
                .map_err(|_| AgentError::Internal("plugins state lock poisoned".into()))?;
            *sg = loaded_state;
        }
        self.rescan_internal();
        Ok(())
    }

    fn rescan_internal(&self) {
        let mut discovered = BTreeMap::new();
        if let Some(ref ws_dir) = self.workspace_plugins_dir {
            scan_plugins_root(ws_dir, "workspace", &mut discovered);
        }
        scan_plugins_root(&self.data_plugins_dir, "data", &mut discovered);

        if let Ok(mut pg) = self.plugins.write() {
            *pg = discovered;
        }
    }

    fn effective_enabled(plugin: &LoadedPlugin, state: &PluginsStateFile) -> bool {
        state
            .overrides
            .get(&plugin.id)
            .copied()
            .unwrap_or(!plugin.manifest.disabled)
    }

    fn to_view(plugin: &LoadedPlugin, state: &PluginsStateFile) -> PluginView {
        let enabled = Self::effective_enabled(plugin, state);
        let display_name = plugin
            .manifest
            .display_name
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .unwrap_or(&plugin.manifest.name)
            .to_string();
        let tools = plugin
            .manifest
            .tools
            .iter()
            .map(|t| namespaced_tool_id(&plugin.id, &t.name))
            .collect();

        PluginView {
            id: plugin.id.clone(),
            name: plugin.manifest.name.clone(),
            display_name,
            description: plugin.manifest.description.clone().unwrap_or_default(),
            version: plugin.manifest.version.clone().unwrap_or_default(),
            enabled,
            rules_count: plugin.rules.len(),
            skills_count: plugin.bundled_skills.len(),
            tools,
            has_mcp: plugin.mcp_config.is_some(),
        }
    }

    /// Lists all discovered plugins deterministically sorted by `id`.
    pub fn list_plugins(&self) -> Vec<PluginView> {
        let Ok(plugins) = self.plugins.read() else {
            return Vec::new();
        };
        let Ok(state) = self.state.read() else {
            return Vec::new();
        };
        plugins
            .values()
            .map(|p| Self::to_view(p, &state))
            .collect()
    }

    /// Looks up a single plugin view by its slug `id`.
    pub fn get_plugin(&self, id: &str) -> Option<PluginView> {
        let plugins = self.plugins.read().ok()?;
        let state = self.state.read().ok()?;
        plugins.get(id).map(|p| Self::to_view(p, &state))
    }

    /// Returns `(plugin_id, combined_rules_markdown)` for every enabled plugin with rules.
    pub fn enabled_rules(&self) -> Vec<(String, String)> {
        let Ok(plugins) = self.plugins.read() else {
            return Vec::new();
        };
        let Ok(state) = self.state.read() else {
            return Vec::new();
        };
        let mut out = Vec::new();
        for plugin in plugins.values() {
            if !Self::effective_enabled(plugin, &state) || plugin.rules.is_empty() {
                continue;
            }
            let combined = plugin.rules.join("\n\n");
            if !combined.trim().is_empty() {
                out.push((plugin.id.clone(), combined));
            }
        }
        out
    }

    /// Returns bundled skill directories (`(skill_id, skill_dir_path)`) from all enabled plugins.
    pub fn enabled_skill_dirs(&self) -> Vec<(String, PathBuf)> {
        let Ok(plugins) = self.plugins.read() else {
            return Vec::new();
        };
        let Ok(state) = self.state.read() else {
            return Vec::new();
        };
        let mut out = Vec::new();
        for plugin in plugins.values() {
            if !Self::effective_enabled(plugin, &state) {
                continue;
            }
            for skill in &plugin.bundled_skills {
                out.push((skill.id.clone(), skill.dir_path.clone()));
            }
        }
        out
    }

    /// Returns full metadata for bundled skills from all enabled plugins.
    pub fn enabled_bundled_skills(&self) -> Vec<PluginBundledSkill> {
        let Ok(plugins) = self.plugins.read() else {
            return Vec::new();
        };
        let Ok(state) = self.state.read() else {
            return Vec::new();
        };
        let mut out = Vec::new();
        for plugin in plugins.values() {
            if !Self::effective_enabled(plugin, &state) {
                continue;
            }
            out.extend(plugin.bundled_skills.iter().cloned());
        }
        out
    }

    /// Returns `(plugin_id, mcp_config_json)` for enabled plugins that declare MCP servers.
    pub fn enabled_mcp_configs(&self) -> Vec<(String, Value)> {
        let Ok(plugins) = self.plugins.read() else {
            return Vec::new();
        };
        let Ok(state) = self.state.read() else {
            return Vec::new();
        };
        let mut out = Vec::new();
        for plugin in plugins.values() {
            if !Self::effective_enabled(plugin, &state) {
                continue;
            }
            if let Some(ref cfg) = plugin.mcp_config {
                out.push((plugin.id.clone(), cfg.clone()));
            }
        }
        out
    }

    /// Returns `ToolSpec` entries for all tools exposed by currently enabled plugins.
    /// Each tool is namespaced as `plugin:<plugin_id>:<tool_name>` and respects `is_mutating`.
    pub fn list_tool_specs(&self) -> Vec<ToolSpec> {
        let Ok(plugins) = self.plugins.read() else {
            return Vec::new();
        };
        let Ok(state) = self.state.read() else {
            return Vec::new();
        };

        let mut specs = Vec::new();
        for plugin in plugins.values() {
            if !Self::effective_enabled(plugin, &state) {
                continue;
            }
            let display_name = plugin
                .manifest
                .display_name
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .unwrap_or(&plugin.manifest.name);

            for tool in &plugin.manifest.tools {
                let ns_id = namespaced_tool_id(&plugin.id, &tool.name);
                let description = if tool.description.trim().is_empty() {
                    format!("Plugin '{display_name}' custom tool '{}'", tool.name)
                } else {
                    tool.description.clone()
                };
                let input_schema = if tool.parameters.is_object() {
                    tool.parameters.clone()
                } else {
                    default_parameters_schema()
                };
                let required_permissions = if tool.kind.trim().eq_ignore_ascii_case("http") {
                    if tool.is_mutating {
                        vec![ToolPermission::WebWrite]
                    } else {
                        vec![ToolPermission::WebRead]
                    }
                } else {
                    vec![ToolPermission::Utility]
                };

                specs.push(ToolSpec {
                    id: ns_id.clone(),
                    name: ns_id,
                    category: "plugin".to_string(),
                    description,
                    input_schema,
                    output_schema: json!({ "type": "object" }),
                    required_permissions,
                    requires_approval: tool.is_mutating,
                    is_mutating: tool.is_mutating,
                    enabled: true,
                    timeout_secs: DEFAULT_PLUGIN_TOOL_TIMEOUT_SECS,
                });
            }
        }
        specs
    }

    /// Returns secret-masked tool views for all enabled plugins.
    pub fn list_tool_views(&self) -> Vec<PluginToolView> {
        let Ok(plugins) = self.plugins.read() else {
            return Vec::new();
        };
        let Ok(state) = self.state.read() else {
            return Vec::new();
        };
        let mut views = Vec::new();
        for plugin in plugins.values() {
            if !Self::effective_enabled(plugin, &state) {
                continue;
            }
            for tool in &plugin.manifest.tools {
                views.push(tool_to_view(&plugin.id, tool));
            }
        }
        views
    }

    /// Returns secret-masked tool views for a specific plugin (regardless of enabled state).
    pub fn plugin_tool_views(&self, id: &str) -> Result<Vec<PluginToolView>> {
        let plugins = self
            .plugins
            .read()
            .map_err(|_| AgentError::Internal("plugins lock poisoned".into()))?;
        let plugin = plugins
            .get(id)
            .ok_or_else(|| AgentError::NotFound(format!("plugin '{id}' not found")))?;
        Ok(plugin
            .manifest
            .tools
            .iter()
            .map(|t| tool_to_view(&plugin.id, t))
            .collect())
    }

    /// Executes a namespaced plugin custom tool (`plugin:<plugin_id>:<tool_name>`).
    /// Supports `"template"` substitution (`{{param}}`) and SSRF-guarded bounded `"http"` calls.
    pub async fn execute_tool(
        &self,
        client: &reqwest::Client,
        namespaced_id: &str,
        args: Value,
        cfg: &ToolsConfig,
    ) -> Result<Value> {
        let (plugin_id, tool_name) = parse_namespaced_tool_id(namespaced_id)?;

        // Copy tool spec without holding std::sync::RwLockReadGuard across .await.
        let tool_spec = {
            let plugins = self
                .plugins
                .read()
                .map_err(|_| AgentError::Internal("plugins lock poisoned".into()))?;
            let state = self
                .state
                .read()
                .map_err(|_| AgentError::Internal("plugins state lock poisoned".into()))?;

            let plugin = plugins
                .get(plugin_id)
                .ok_or_else(|| AgentError::NotFound(format!("plugin '{plugin_id}' not found")))?;

            if !Self::effective_enabled(plugin, &state) {
                return Err(AgentError::PermissionDenied(format!(
                    "plugin '{plugin_id}' is disabled"
                )));
            }

            plugin
                .manifest
                .tools
                .iter()
                .find(|t| t.name == tool_name)
                .cloned()
                .ok_or_else(|| {
                    AgentError::NotFound(format!("plugin tool '{namespaced_id}' not found"))
                })?
        };

        let arg_map = match args {
            Value::Object(map) => map,
            Value::Null => Map::new(),
            _ => {
                return Err(AgentError::InvalidInput(
                    "plugin tool arguments must be a JSON object".into(),
                ));
            }
        };

        let kind = tool_spec.kind.trim().to_ascii_lowercase();
        log_debug(
            "plugins",
            format!("executing plugin tool='{namespaced_id}' kind='{kind}'"),
        );

        match kind.as_str() {
            "template" => execute_template_tool(plugin_id, &tool_spec, &arg_map),
            "http" => execute_http_tool(client, plugin_id, &tool_spec, &arg_map, cfg).await,
            other => Err(AgentError::InvalidInput(format!(
                "unsupported plugin tool kind '{other}' for '{namespaced_id}'"
            ))),
        }
    }

    /// Validates a `plugin.json` manifest (including strict slug check), writes it atomically
    /// to `<data_dir>/plugins/<slug>/plugin.json`, persists state, and returns its `PluginView`.
    pub fn import_plugin_json(&self, manifest_json: &str) -> Result<PluginView> {
        if manifest_json.len() as u64 > MAX_PLUGIN_MANIFEST_BYTES {
            return Err(AgentError::LimitExceeded(format!(
                "plugin manifest exceeds {MAX_PLUGIN_MANIFEST_BYTES} bytes"
            )));
        }
        let manifest: PluginManifest = serde_json::from_str(manifest_json)
            .map_err(|e| AgentError::InvalidInput(format!("invalid plugin.json: {e}")))?;

        let slug = validate_plugin_slug(&manifest.name)?;
        validate_manifest_tools(&manifest.tools)?;

        ensure_not_symlink_dir_if_exists(&self.data_plugins_dir)?;
        let plugin_dir = self.data_plugins_dir.join(&slug);
        ensure_not_symlink_dir_if_exists(&plugin_dir)?;
        let manifest_path = plugin_dir.join("plugin.json");
        ensure_not_symlink_file_if_exists(&manifest_path)?;
        let serialized = serde_json::to_vec_pretty(&manifest)
            .map_err(|e| AgentError::Internal(format!("failed to serialize plugin.json: {e}")))?;
        write_file_atomic(&manifest_path, &serialized)?;

        let loaded = load_plugin_from_dir(&slug, &plugin_dir, "data", manifest)?;
        let enabled = !loaded.manifest.disabled;

        let view = {
            let mut plugins = self
                .plugins
                .write()
                .map_err(|_| AgentError::Internal("plugins lock poisoned".into()))?;
            let mut state = self
                .state
                .write()
                .map_err(|_| AgentError::Internal("plugins state lock poisoned".into()))?;

            state.overrides.insert(slug.clone(), enabled);
            save_state_file_atomic(&self.state_path, &state)?;
            plugins.insert(slug.clone(), loaded);
            Self::to_view(&plugins[&slug], &state)
        };

        log_info("plugins", format!("imported plugin '{slug}'"));
        Ok(view)
    }

    /// Enables or disables a discovered plugin and persists `<data_dir>/plugins_state.json`.
    pub fn set_enabled(&self, id: &str, enabled: bool) -> Result<PluginView> {
        let slug = validate_plugin_slug(id)?;
        let plugins = self
            .plugins
            .read()
            .map_err(|_| AgentError::Internal("plugins lock poisoned".into()))?;
        let plugin = plugins
            .get(&slug)
            .ok_or_else(|| AgentError::NotFound(format!("plugin '{slug}' not found")))?;

        let mut state = self
            .state
            .write()
            .map_err(|_| AgentError::Internal("plugins state lock poisoned".into()))?;
        state.overrides.insert(slug.clone(), enabled);
        save_state_file_atomic(&self.state_path, &state)?;

        log_info("plugins", format!("set plugin '{slug}' enabled={enabled}"));
        Ok(Self::to_view(plugin, &state))
    }

    /// Deletes a plugin installed in `<data_dir>/plugins/<id>` and updates persisted state.
    pub fn delete_plugin(&self, id: &str) -> Result<()> {
        let slug = validate_plugin_slug(id)?;
        let data_dir = self.data_plugins_dir.join(&slug);

        let mut plugins = self
            .plugins
            .write()
            .map_err(|_| AgentError::Internal("plugins lock poisoned".into()))?;
        let mut state = self
            .state
            .write()
            .map_err(|_| AgentError::Internal("plugins state lock poisoned".into()))?;

        match fs::symlink_metadata(&data_dir) {
            Ok(meta) => {
                if meta.file_type().is_symlink() {
                    return Err(AgentError::PermissionDenied(format!(
                        "refusing to delete symlinked plugin path '{}'",
                        data_dir.display()
                    )));
                }
                if !meta.is_dir() {
                    return Err(AgentError::Storage(format!(
                        "plugin path '{}' is not a directory",
                        data_dir.display()
                    )));
                }
                fs::remove_dir_all(&data_dir).map_err(|e| {
                    AgentError::Storage(format!(
                        "failed to delete plugin directory '{}': {e}",
                        data_dir.display()
                    ))
                })?;
                plugins.remove(&slug);
                state.overrides.remove(&slug);

                // If a workspace plugin with the same slug exists, re-expose it cleanly.
                if let Some(ref ws_root) = self.workspace_plugins_dir {
                    let ws_plugin_dir = ws_root.join(&slug);
                    if let Some(ws_loaded) = try_load_plugin_dir(&ws_plugin_dir, &slug, "workspace")
                    {
                        plugins.insert(slug.clone(), ws_loaded);
                    }
                }
                save_state_file_atomic(&self.state_path, &state)?;
                log_info("plugins", format!("deleted plugin '{slug}'"));
                return Ok(());
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                return Err(AgentError::Storage(format!(
                    "failed to stat plugin directory '{}': {e}",
                    data_dir.display()
                )));
            }
        }

        if let Some(existing) = plugins.get(&slug) {
            if existing.source == "workspace" {
                return Err(AgentError::PermissionDenied(format!(
                    "plugin '{slug}' is provided by the workspace and cannot be deleted; disable it instead"
                )));
            }
        }

        Err(AgentError::NotFound(format!("plugin '{slug}' not found")))
    }
}

/// Validates a strict plugin or tool slug: `[a-zA-Z0-9_-]+`, 1..=64 bytes, no `.`/`..`/slashes/NUL.
pub fn validate_plugin_slug(id: &str) -> Result<String> {
    let trimmed = id.trim();
    if trimmed.is_empty() || trimmed.len() > 64 {
        return Err(AgentError::InvalidInput(
            "plugin slug must be 1..=64 characters".into(),
        ));
    }
    if trimmed == "."
        || trimmed == ".."
        || trimmed.contains('/')
        || trimmed.contains('\\')
        || trimmed.contains('\0')
    {
        return Err(AgentError::InvalidInput(format!(
            "invalid plugin slug '{id}': path traversal and separators are forbidden"
        )));
    }
    if !trimmed
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
    {
        return Err(AgentError::InvalidInput(format!(
            "invalid plugin slug '{id}': only [a-zA-Z0-9_-] characters are allowed"
        )));
    }
    Ok(trimmed.to_string())
}

/// Masks sensitive HTTP header values for UI/API views while preserving safe content-type headers.
pub fn mask_headers(headers: &BTreeMap<String, String>) -> BTreeMap<String, String> {
    headers
        .iter()
        .map(|(k, v)| {
            let kl = k.trim().to_ascii_lowercase();
            let masked = if matches!(kl.as_str(), "accept" | "content-type") {
                v.clone()
            } else {
                "***".to_string()
            };
            (k.clone(), masked)
        })
        .collect()
}

fn namespaced_tool_id(plugin_id: &str, tool_name: &str) -> String {
    format!("plugin:{plugin_id}:{tool_name}")
}

fn parse_namespaced_tool_id(namespaced_id: &str) -> Result<(&str, &str)> {
    let rest = namespaced_id.strip_prefix("plugin:").ok_or_else(|| {
        AgentError::InvalidInput(format!(
            "invalid plugin tool id '{namespaced_id}': expected 'plugin:<plugin_id>:<tool_name>'"
        ))
    })?;
    let (plugin_id, tool_name) = rest.split_once(':').ok_or_else(|| {
        AgentError::InvalidInput(format!(
            "invalid plugin tool id '{namespaced_id}': expected 'plugin:<plugin_id>:<tool_name>'"
        ))
    })?;
    if plugin_id.is_empty() || tool_name.is_empty() || tool_name.contains(':') {
        return Err(AgentError::InvalidInput(format!(
            "invalid plugin tool id '{namespaced_id}'"
        )));
    }
    Ok((plugin_id, tool_name))
}

fn tool_to_view(plugin_id: &str, tool: &PluginCustomToolSpec) -> PluginToolView {
    PluginToolView {
        namespaced_id: namespaced_tool_id(plugin_id, &tool.name),
        plugin_id: plugin_id.to_string(),
        name: tool.name.clone(),
        description: tool.description.clone(),
        parameters: tool.parameters.clone(),
        kind: tool.kind.clone(),
        url: tool.url.clone(),
        method: tool.method.clone(),
        headers: tool.masked_headers(),
        is_mutating: tool.is_mutating,
    }
}

fn validate_manifest_tools(tools: &[PluginCustomToolSpec]) -> Result<()> {
    let mut seen = HashSet::with_capacity(tools.len());
    for tool in tools {
        let tool_slug = validate_plugin_slug(&tool.name)?;
        if !seen.insert(tool_slug.clone()) {
            return Err(AgentError::InvalidInput(format!(
                "duplicate plugin tool name '{tool_slug}'"
            )));
        }
        for (k, v) in &tool.headers {
            if k.contains('\r') || k.contains('\n') || v.contains('\r') || v.contains('\n') {
                return Err(AgentError::InvalidInput(format!(
                    "plugin tool '{tool_slug}' header '{k}' contains illegal CR/LF"
                )));
            }
        }
        let kind = tool.kind.trim().to_ascii_lowercase();
        match kind.as_str() {
            "template" => {}
            "http" => {
                let url = tool
                    .url
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| {
                        AgentError::InvalidInput(format!(
                            "http plugin tool '{tool_slug}' requires a non-empty 'url'"
                        ))
                    })?;
                validate_url_template_syntax(url, &tool_slug)?;
                if let Some(ref m) = tool.method {
                    let mu = m.trim().to_ascii_uppercase();
                    if !matches!(mu.as_str(), "GET" | "HEAD" | "POST" | "PUT" | "PATCH" | "DELETE") {
                        return Err(AgentError::InvalidInput(format!(
                            "unsupported HTTP method '{m}' in plugin tool '{tool_slug}'"
                        )));
                    }
                }
            }
            other => {
                return Err(AgentError::InvalidInput(format!(
                    "unsupported plugin tool kind '{other}' on '{tool_slug}'; expected 'http' or 'template'"
                )));
            }
        }
    }
    Ok(())
}

fn scan_plugins_root(root: &Path, source: &'static str, out: &mut BTreeMap<String, LoadedPlugin>) {
    if !is_real_dir(root) {
        return;
    }
    let Ok(entries) = fs::read_dir(root) else {
        return;
    };
    let mut dirs: Vec<(String, PathBuf)> = entries
        .filter_map(|e| e.ok())
        .filter_map(|e| {
            let path = e.path();
            if !is_real_dir(&path) {
                return None;
            }
            let dir_name = e.file_name().to_str()?.to_string();
            let slug = validate_plugin_slug(&dir_name).ok()?;
            Some((slug, path))
        })
        .collect();
    dirs.sort_by(|a, b| a.0.cmp(&b.0));

    for (slug, dir_path) in dirs {
        if let Some(loaded) = try_load_plugin_dir(&dir_path, &slug, source) {
            out.insert(slug, loaded);
        }
    }
}

fn try_load_plugin_dir(
    dir_path: &Path,
    slug: &str,
    source: &'static str,
) -> Option<LoadedPlugin> {
    if !is_real_dir(dir_path) {
        return None;
    }
    let primary = dir_path.join("plugin.json");
    let claude_fallback_dir = dir_path.join(".claude-plugin");
    let claude_fallback = claude_fallback_dir.join("plugin.json");
    let manifest_path = if is_real_file(&primary) {
        primary
    } else if is_real_dir(&claude_fallback_dir) && is_real_file(&claude_fallback) {
        claude_fallback
    } else {
        return None;
    };

    let raw = match read_bounded_utf8_file(&manifest_path, MAX_PLUGIN_MANIFEST_BYTES) {
        Ok(s) => s,
        Err(e) => {
            log_warn(
                "plugins",
                format!("failed to read '{}': {e}", manifest_path.display()),
            );
            return None;
        }
    };

    let manifest: PluginManifest = match serde_json::from_str(&raw) {
        Ok(m) => m,
        Err(e) => {
            log_warn(
                "plugins",
                format!("failed to parse '{}': {e}", manifest_path.display()),
            );
            return None;
        }
    };

    match load_plugin_from_dir(slug, dir_path, source, manifest) {
        Ok(loaded) => Some(loaded),
        Err(e) => {
            log_warn(
                "plugins",
                format!("skipping invalid plugin '{}': {e}", manifest_path.display()),
            );
            None
        }
    }
}

fn load_plugin_from_dir(
    slug: &str,
    dir_path: &Path,
    source: &'static str,
    manifest: PluginManifest,
) -> Result<LoadedPlugin> {
    validate_manifest_tools(&manifest.tools)?;
    let mut rules: Vec<String> = manifest
        .rules
        .iter()
        .filter(|r| !r.trim().is_empty())
        .cloned()
        .collect();
    rules.extend(scan_rule_markdown_files(&dir_path.join("rules")));

    let bundled_skills = scan_bundled_skills(slug, &dir_path.join("skills"));
    let mcp_config = resolve_mcp_config(&manifest, dir_path);

    Ok(LoadedPlugin {
        id: slug.to_string(),
        source,
        manifest,
        rules,
        bundled_skills,
        mcp_config,
    })
}

fn scan_rule_markdown_files(rules_dir: &Path) -> Vec<String> {
    if !is_real_dir(rules_dir) {
        return Vec::new();
    }
    let Ok(entries) = fs::read_dir(rules_dir) else {
        return Vec::new();
    };
    let mut paths: Vec<PathBuf> = entries
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            is_real_file(p)
                && p.extension()
                    .and_then(|ext| ext.to_str())
                    .map_or(false, |ext| ext.eq_ignore_ascii_case("md"))
        })
        .collect();
    paths.sort();

    let mut out = Vec::new();
    for path in paths {
        match read_bounded_utf8_file(&path, MAX_PLUGIN_RULE_BYTES) {
            Ok(content) if !content.trim().is_empty() => out.push(content),
            Ok(_) => {}
            Err(e) => log_warn(
                "plugins",
                format!("failed to read rule file '{}': {e}", path.display()),
            ),
        }
    }
    out
}

fn scan_bundled_skills(plugin_id: &str, skills_dir: &Path) -> Vec<PluginBundledSkill> {
    if !is_real_dir(skills_dir) {
        return Vec::new();
    }
    let Ok(entries) = fs::read_dir(skills_dir) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for entry in entries.filter_map(|e| e.ok()) {
        let dir_path = entry.path();
        if !is_real_dir(&dir_path) {
            continue;
        }
        let Some(name) = entry.file_name().to_str().map(str::to_string) else {
            continue;
        };
        let Ok(skill_id) = validate_plugin_slug(&name) else {
            continue;
        };
        let skill_md_path = dir_path.join("SKILL.md");
        if is_real_file(&skill_md_path) {
            out.push(PluginBundledSkill {
                plugin_id: plugin_id.to_string(),
                id: skill_id,
                dir_path,
                skill_md_path,
            });
        }
    }
    out.sort_by(|a, b| a.id.cmp(&b.id));
    out
}

fn resolve_mcp_config(manifest: &PluginManifest, dir_path: &Path) -> Option<Value> {
    if let Some(ref val) = manifest.mcp_servers {
        if is_non_empty_mcp_value(val) {
            return Some(val.clone());
        }
    }
    for filename in ["mcp_config.json", ".mcp.json"] {
        let candidate = dir_path.join(filename);
        if !is_real_file(&candidate) {
            continue;
        }
        match read_bounded_utf8_file(&candidate, MAX_PLUGIN_MANIFEST_BYTES) {
            Ok(raw) => {
                if let Ok(parsed) = serde_json::from_str::<Value>(&raw) {
                    if is_non_empty_mcp_value(&parsed) {
                        return Some(parsed);
                    }
                }
            }
            Err(e) => log_warn(
                "plugins",
                format!("skipping plugin MCP config '{}': {e}", candidate.display()),
            ),
        }
    }
    None
}

fn is_non_empty_mcp_value(val: &Value) -> bool {
    match val {
        Value::Object(map) => {
            if let Some(inner) = map.get("mcpServers").or_else(|| map.get("mcp_servers")) {
                return is_non_empty_mcp_value(inner);
            }
            !map.is_empty()
        }
        Value::Array(arr) => !arr.is_empty(),
        _ => false,
    }
}

fn execute_template_tool(
    plugin_id: &str,
    tool: &PluginCustomToolSpec,
    args: &Map<String, Value>,
) -> Result<Value> {
    let rendered = match (&tool.body_template, &tool.response_template) {
        (Some(body_tmpl), Some(resp_tmpl)) => {
            let body_out = render_template(body_tmpl, args);
            let mut resp_ctx = args.clone();
            resp_ctx.insert("body".to_string(), Value::String(body_out.clone()));
            resp_ctx.insert("result".to_string(), Value::String(body_out));
            render_template(resp_tmpl, &resp_ctx)
        }
        (Some(body_tmpl), None) => render_template(body_tmpl, args),
        (None, Some(resp_tmpl)) => render_template(resp_tmpl, args),
        (None, None) => serde_json::to_string_pretty(args).unwrap_or_else(|_| "{}".to_string()),
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
        "plugin_id": plugin_id,
        "tool": tool.name,
        "kind": "template",
        "output": rendered,
        "rendered": rendered,
        "json": parsed_json,
    }))
}

async fn execute_http_tool(
    client: &reqwest::Client,
    plugin_id: &str,
    tool: &PluginCustomToolSpec,
    args: &Map<String, Value>,
    cfg: &ToolsConfig,
) -> Result<Value> {
    let raw_url_tmpl = tool
        .url
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            AgentError::InvalidInput(format!(
                "http plugin tool 'plugin:{plugin_id}:{}' is missing 'url'",
                tool.name
            ))
        })?;
    let rendered_url = render_url_template_with(
        raw_url_tmpl,
        &namespaced_tool_id(plugin_id, &tool.name),
        |key| lookup_template_key(args, key),
    )?;

    let method_str = tool
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
        other => {
            return Err(AgentError::InvalidInput(format!(
                "unsupported HTTP method '{other}' on plugin tool 'plugin:{plugin_id}:{}'",
                tool.name
            )));
        }
    };

    let mut headers = HeaderMap::new();
    for (k, v_tmpl) in &tool.headers {
        let rendered_val = render_template(v_tmpl, args);
        if k.contains('\r')
            || k.contains('\n')
            || rendered_val.contains('\r')
            || rendered_val.contains('\n')
        {
            return Err(AgentError::InvalidInput(
                "plugin tool header contains illegal CR/LF".into(),
            ));
        }
        let name = HeaderName::from_bytes(k.as_bytes()).map_err(|e| {
            AgentError::InvalidInput(format!("invalid header name '{k}': {e}"))
        })?;
        let val = HeaderValue::from_str(&rendered_val).map_err(|e| {
            AgentError::InvalidInput(format!("invalid header value for '{k}': {e}"))
        })?;
        headers.insert(name, val);
    }

    let body_bytes: Option<Vec<u8>> = if let Some(ref body_tmpl) = tool.body_template {
        if !headers.contains_key(CONTENT_TYPE) {
            let trimmed = body_tmpl.trim_start();
            if trimmed.starts_with('{') || trimmed.starts_with('[') {
                headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
            }
        }
        Some(render_template(body_tmpl, args).into_bytes())
    } else if matches!(
        method,
        reqwest::Method::POST | reqwest::Method::PUT | reqwest::Method::PATCH
    ) && !args.is_empty()
    {
        if !headers.contains_key(CONTENT_TYPE) {
            headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        }
        Some(serde_json::to_vec(args).unwrap_or_default())
    } else {
        None
    };

    let mut current_url = validate_web_url(&rendered_url, cfg).await?;
    let mut current_method = method;
    let mut current_body = body_bytes;

    for _hop in 0..=MAX_PLUGIN_HTTP_REDIRECTS {
        let mut req = client
            .request(current_method.clone(), current_url.clone())
            .headers(headers.clone());
        if let Some(ref body) = current_body {
            req = req.body(body.clone());
        }

        let mut resp = req.send().await.map_err(|e| {
            if e.is_timeout() {
                AgentError::Timeout(format!(
                    "plugin tool 'plugin:{plugin_id}:{}' HTTP request timed out: {e}",
                    tool.name
                ))
            } else {
                AgentError::Tool {
                    tool_id: namespaced_tool_id(plugin_id, &tool.name),
                    message: format!("HTTP request failed: {e}"),
                }
            }
        })?;

        // Re-validate final URL in case caller passed a client that followed redirects automatically.
        let _ = validate_web_url(resp.url().as_str(), cfg).await?;

        let status = resp.status();
        if status.is_redirection() {
            if let Some(loc) = resp
                .headers()
                .get(reqwest::header::LOCATION)
                .and_then(|v| v.to_str().ok())
            {
                let next_url = current_url.join(loc).map_err(|e| AgentError::Tool {
                    tool_id: namespaced_tool_id(plugin_id, &tool.name),
                    message: format!("invalid redirect URL: {e}"),
                })?;
                let next_validated = validate_web_url(next_url.as_str(), cfg).await?;
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

        let max_bytes = if cfg.max_web_bytes == 0 {
            DEFAULT_UNLIMITED_WEB_BYTES_CAP
        } else {
            cfg.max_web_bytes.max(1024)
        };
        let mut buf = Vec::new();
        let mut truncated = false;
        while let Some(chunk) = resp.chunk().await.map_err(|e| AgentError::Tool {
            tool_id: namespaced_tool_id(plugin_id, &tool.name),
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

        let rendered_response = tool.response_template.as_ref().map(|resp_tmpl| {
            let mut ctx = args.clone();
            ctx.insert("status".to_string(), json!(status.as_u16()));
            ctx.insert("url".to_string(), Value::String(final_url.clone()));
            ctx.insert("body".to_string(), Value::String(body_text.clone()));
            if let Some(ref pj) = parsed_json {
                ctx.insert("json".to_string(), pj.clone());
            }
            let rendered = render_template(resp_tmpl, &ctx);
            wrap_untrusted_web_content(&final_url, &rendered)
        });

        return Ok(json!({
            "plugin_id": plugin_id,
            "tool": tool.name,
            "kind": "http",
            "status": status.as_u16(),
            "url": final_url,
            "body": wrapped_body,
            "json": parsed_json,
            "rendered": rendered_response,
            "truncated": truncated,
        }));
    }

    Err(AgentError::Tool {
        tool_id: namespaced_tool_id(plugin_id, &tool.name),
        message: "too many HTTP redirects".into(),
    })
}

fn render_template(template: &str, args: &Map<String, Value>) -> String {
    let mut out = String::with_capacity(template.len());
    let mut rest = template;
    while let Some(open) = rest.find("{{") {
        out.push_str(&rest[..open]);
        let after_open = &rest[open + 2..];
        if let Some(close) = after_open.find("}}") {
            let key = after_open[..close].trim();
            if let Some(val) = lookup_template_key(args, key) {
                out.push_str(&json_value_to_template_str(val));
            }
            rest = &after_open[close + 2..];
        } else {
            out.push_str("{{");
            rest = after_open;
        }
    }
    out.push_str(rest);
    out
}

fn lookup_template_key<'a>(args: &'a Map<String, Value>, key: &str) -> Option<&'a Value> {
    if key.is_empty() {
        return None;
    }
    if let Some(direct) = args.get(key) {
        return Some(direct);
    }
    let mut parts = key.split('.');
    let first = parts.next()?;
    let mut current = args.get(first)?;
    for part in parts {
        current = current.as_object()?.get(part)?;
    }
    Some(current)
}

fn json_value_to_template_str(val: &Value) -> String {
    match val {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Null => String::new(),
        other => serde_json::to_string(other).unwrap_or_default(),
    }
}

fn load_state_file(path: &Path) -> PluginsStateFile {
    if !is_real_file(path) {
        return PluginsStateFile::default();
    }
    let raw = match read_bounded_utf8_file(path, MAX_PLUGIN_STATE_BYTES) {
        Ok(text) => text,
        Err(e) => {
            log_warn(
                "plugins",
                format!("failed to read '{}': {e}", path.display()),
            );
            return PluginsStateFile::default();
        }
    };
    match serde_json::from_str::<PluginsStateFile>(&raw) {
        Ok(state) => state,
        Err(e) => {
            log_warn(
                "plugins",
                format!("failed to parse '{}': {e}", path.display()),
            );
            PluginsStateFile::default()
        }
    }
}

fn save_state_file_atomic(path: &Path, state: &PluginsStateFile) -> Result<()> {
    let bytes = serde_json::to_vec_pretty(state)
        .map_err(|e| AgentError::Internal(format!("failed to serialize plugins_state.json: {e}")))?;
    write_file_atomic(path, &bytes)
}

fn write_file_atomic(path: &Path, content: &[u8]) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|e| {
            AgentError::Storage(format!(
                "failed to create directory '{}': {e}",
                parent.display()
            ))
        })?;
    }
    let tmp_name = format!(
        ".{}.tmp.{}",
        path.file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("plugin"),
        Uuid::new_v4()
    );
    let tmp_path = path.with_file_name(tmp_name);
    {
        let mut f = fs::File::create(&tmp_path).map_err(|e| {
            AgentError::Storage(format!(
                "failed to create temp file '{}': {e}",
                tmp_path.display()
            ))
        })?;
        f.write_all(content).map_err(|e| {
            AgentError::Storage(format!(
                "failed to write temp file '{}': {e}",
                tmp_path.display()
            ))
        })?;
        f.sync_all().map_err(|e| {
            AgentError::Storage(format!(
                "failed to sync temp file '{}': {e}",
                tmp_path.display()
            ))
        })?;
    }
    fs::rename(&tmp_path, path).map_err(|e| {
        let _ = fs::remove_file(&tmp_path);
        AgentError::Storage(format!(
            "failed to rename '{}' to '{}': {e}",
            tmp_path.display(),
            path.display()
        ))
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::web::build_web_client;
    use axum::{routing::post, Json, Router};
    use tempfile::tempdir;
    use tokio::net::TcpListener;

    #[test]
    fn strict_slug_validation_rejects_traversal_and_invalid_chars() {
        assert_eq!(validate_plugin_slug("my-plugin_01").unwrap(), "my-plugin_01");
        assert!(validate_plugin_slug("").is_err());
        assert!(validate_plugin_slug(".").is_err());
        assert!(validate_plugin_slug("..").is_err());
        assert!(validate_plugin_slug("../evil").is_err());
        assert!(validate_plugin_slug("foo/bar").is_err());
        assert!(validate_plugin_slug("foo\\bar").is_err());
        assert!(validate_plugin_slug("has space").is_err());
        assert!(validate_plugin_slug("bad\0nul").is_err());
        assert!(validate_plugin_slug(&"a".repeat(65)).is_err());
    }

    #[test]
    fn import_plugin_json_persists_and_reloads() {
        let data = tempdir().expect("tempdir");
        let pm = PluginManager::new(data.path(), None);

        let manifest = r#"{
            "name": "weather-helper",
            "displayName": "Weather Helper",
            "description": "Forecast formatting & webhook",
            "version": "1.2.0",
            "rules": ["Always report temperature in Celsius."],
            "mcpServers": {
                "weather-mcp": { "url": "https://mcp.example.com/sse" }
            },
            "tools": [
                {
                    "name": "format_report",
                    "description": "Format weather summary",
                    "parameters": {
                        "type": "object",
                        "properties": { "city": { "type": "string" } },
                        "required": ["city"]
                    },
                    "kind": "template",
                    "body_template": "Weather for {{city}}",
                    "is_mutating": false
                }
            ]
        }"#;

        let view = pm.import_plugin_json(manifest).expect("import succeeds");
        assert_eq!(view.id, "weather-helper");
        assert_eq!(view.display_name, "Weather Helper");
        assert_eq!(view.version, "1.2.0");
        assert!(view.enabled);
        assert_eq!(view.rules_count, 1);
        assert_eq!(view.skills_count, 0);
        assert_eq!(view.tools, vec!["plugin:weather-helper:format_report"]);
        assert!(view.has_mcp);

        assert!(data
            .path()
            .join("plugins")
            .join("weather-helper")
            .join("plugin.json")
            .is_file());
        assert!(data.path().join("plugins_state.json").is_file());

        // Strict slug check via import_plugin_json
        let bad_manifest = r#"{"name": "../escape", "tools": []}"#;
        assert!(matches!(
            pm.import_plugin_json(bad_manifest),
            Err(AgentError::InvalidInput(_))
        ));

        // Fresh manager reloads from disk
        let pm_reloaded = PluginManager::new(data.path(), None);
        let list = pm_reloaded.list_plugins();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0], view);
    }

    #[tokio::test]
    async fn enable_toggle_gates_rules_tool_specs_and_execution() {
        let data = tempdir().expect("tempdir");
        let pm = PluginManager::new(data.path(), None);

        let manifest = r#"{
            "name": "greeter",
            "rules": ["Be polite and concise."],
            "tools": [
                {
                    "name": "greet",
                    "description": "Create greeting",
                    "kind": "template",
                    "body_template": "Hello, {{user}}! Role={{meta.role}}",
                    "is_mutating": true
                }
            ]
        }"#;
        pm.import_plugin_json(manifest).expect("import");

        let specs = pm.list_tool_specs();
        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].id, "plugin:greeter:greet");
        assert!(specs[0].is_mutating);
        assert!(specs[0].requires_approval);
        assert_eq!(pm.enabled_rules().len(), 1);

        let client = build_web_client(5).expect("client");
        let cfg = ToolsConfig::default();
        let res = pm
            .execute_tool(
                &client,
                "plugin:greeter:greet",
                json!({"user": "Daniel", "meta": {"role": "operator"}}),
                &cfg,
            )
            .await
            .expect("template execution succeeds");
        assert_eq!(res["output"], "Hello, Daniel! Role=operator");

        // Disable plugin and verify persistence + gating
        let disabled_view = pm.set_enabled("greeter", false).expect("disable");
        assert!(!disabled_view.enabled);
        assert!(pm.list_tool_specs().is_empty());
        assert!(pm.enabled_rules().is_empty());

        let err = pm
            .execute_tool(&client, "plugin:greeter:greet", json!({"user": "X"}), &cfg)
            .await
            .expect_err("disabled plugin must reject execution");
        assert!(matches!(err, AgentError::PermissionDenied(_)));

        let pm2 = PluginManager::new(data.path(), None);
        assert!(!pm2.get_plugin("greeter").unwrap().enabled);
        assert!(pm2.list_tool_specs().is_empty());

        // Re-enable
        let re_enabled = pm2.set_enabled("greeter", true).expect("re-enable");
        assert!(re_enabled.enabled);
        assert_eq!(pm2.list_tool_specs().len(), 1);
        assert_eq!(pm2.enabled_rules().len(), 1);
    }

    #[test]
    fn rules_markdown_bundled_skills_and_masked_headers_extracted() {
        let data = tempdir().expect("tempdir");
        let workspace = tempdir().expect("workspace");
        let pdir = workspace.path().join(".agents").join("plugins").join("devkit");
        fs::create_dir_all(pdir.join("rules")).unwrap();
        fs::create_dir_all(pdir.join("skills").join("code-review")).unwrap();

        fs::write(
            pdir.join("plugin.json"),
            r#"{
                "name": "devkit",
                "rules": ["Inline rule 1"],
                "tools": [
                    {
                        "name": "notify",
                        "description": "Send webhook",
                        "kind": "http",
                        "url": "https://hooks.example.com/notify",
                        "method": "POST",
                        "headers": {
                            "Authorization": "Bearer top-secret-token",
                            "X-Api-Key": "secret-key-123",
                            "Content-Type": "application/json"
                        },
                        "is_mutating": true
                    }
                ]
            }"#,
        )
        .unwrap();
        fs::write(pdir.join("rules").join("01-first.md"), "File rule A").unwrap();
        fs::write(pdir.join("rules").join("02-second.md"), "File rule B").unwrap();
        fs::write(
            pdir.join("skills").join("code-review").join("SKILL.md"),
            "---\nname: code-review\ndescription: Review diffs\n---\nInspect changes.",
        )
        .unwrap();
        fs::write(
            pdir.join("mcp_config.json"),
            r#"{"mcpServers": {"git": {"url": "http://127.0.0.1:9000/mcp"}}}"#,
        )
        .unwrap();

        let pm = PluginManager::new(data.path(), Some(workspace.path()));
        let view = pm.get_plugin("devkit").expect("workspace plugin discovered");
        assert_eq!(view.rules_count, 3);
        assert_eq!(view.skills_count, 1);
        assert!(view.has_mcp);

        let rules = pm.enabled_rules();
        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0].0, "devkit");
        assert_eq!(rules[0].1, "Inline rule 1\n\nFile rule A\n\nFile rule B");

        let skills = pm.enabled_skill_dirs();
        assert_eq!(skills.len(), 1);
        assert_eq!(skills[0].0, "code-review");

        let tool_views = pm.list_tool_views();
        assert_eq!(tool_views.len(), 1);
        assert_eq!(tool_views[0].headers.get("Authorization").unwrap(), "***");
        assert_eq!(tool_views[0].headers.get("X-Api-Key").unwrap(), "***");
        assert_eq!(
            tool_views[0].headers.get("Content-Type").unwrap(),
            "application/json"
        );
    }

    #[tokio::test]
    async fn http_tool_enforces_validate_web_url_and_executes_bounded_request() {
        let app = Router::new().route(
            "/hook",
            post(|Json(payload): Json<Value>| async move {
                Json(json!({ "ok": true, "echo": payload }))
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let data = tempdir().expect("tempdir");
        let pm = PluginManager::new(data.path(), None);
        let manifest = format!(
            r#"{{
                "name": "webhook",
                "tools": [
                    {{
                        "name": "post_event",
                        "description": "Post event",
                        "kind": "http",
                        "url": "http://{addr}/hook",
                        "method": "POST",
                        "body_template": "{{\"event\":\"{{{{event}}}}\"}}",
                        "response_template": "status={{{{status}}}} ok={{{{json.ok}}}}",
                        "is_mutating": true
                    }}
                ]
            }}"#
        );
        pm.import_plugin_json(&manifest).unwrap();

        let client = build_web_client(5).unwrap();

        // Default ToolsConfig blocks private/loopback & empty allowed_domains via validate_web_url
        let blocked = pm
            .execute_tool(
                &client,
                "plugin:webhook:post_event",
                json!({"event": "deploy"}),
                &ToolsConfig::default(),
            )
            .await
            .expect_err("SSRF guard must block loopback by default");
        assert!(matches!(blocked, AgentError::PermissionDenied(_)));

        // Explicitly allowing 127.0.0.1 succeeds and returns bounded wrapped response
        let mut cfg = ToolsConfig::default();
        cfg.allowed_domains = vec!["127.0.0.1".to_string()];
        cfg.block_private_lan_web_requests = false;

        let ok = pm
            .execute_tool(
                &client,
                "plugin:webhook:post_event",
                json!({"event": "deploy"}),
                &cfg,
            )
            .await
            .expect("explicitly allowed loopback succeeds");
        assert_eq!(ok["status"], 200);
        assert_eq!(ok["json"]["ok"], true);
        assert_eq!(ok["json"]["echo"]["event"], "deploy");
        assert!(ok["rendered"]
            .as_str()
            .unwrap()
            .contains("status=200 ok=true"));

        // Delete removes files and state
        pm.delete_plugin("webhook").unwrap();
        assert!(pm.list_plugins().is_empty());
        assert!(!data.path().join("plugins").join("webhook").exists());
    }

    #[test]
    fn disk_discovery_validates_manifest_tools_and_enforces_size_caps() {
        let data = tempdir().expect("tempdir");
        let plugins_root = data.path().join("plugins");

        // 1. Invalid tool name with ':' on disk must be skipped via validate_manifest_tools in load_plugin_from_dir.
        let bad_tool_dir = plugins_root.join("bad-plugin");
        fs::create_dir_all(&bad_tool_dir).unwrap();
        fs::write(
            bad_tool_dir.join("plugin.json"),
            r#"{
                "name": "bad-plugin",
                "tools": [
                    { "name": "evil:colon", "kind": "template", "body_template": "hi" }
                ]
            }"#,
        )
        .unwrap();

        // 2. Oversized rule file is skipped while valid manifest + rule loads.
        let good_dir = plugins_root.join("good-plugin");
        fs::create_dir_all(good_dir.join("rules")).unwrap();
        fs::write(
            good_dir.join("plugin.json"),
            r#"{"name":"good-plugin","tools":[{"name":"ok_tool","kind":"template","body_template":"ok"}]}"#,
        )
        .unwrap();
        fs::write(good_dir.join("rules").join("01-small.md"), "Valid rule").unwrap();
        fs::write(
            good_dir.join("rules").join("02-huge.md"),
            "x".repeat((MAX_PLUGIN_RULE_BYTES as usize) + 64),
        )
        .unwrap();

        let pm = PluginManager::new(data.path(), None);
        assert!(pm.get_plugin("bad-plugin").is_none());
        let good = pm.get_plugin("good-plugin").expect("valid plugin loads");
        assert_eq!(good.rules_count, 1);
    }

    #[cfg(unix)]
    #[test]
    fn plugin_discovery_skips_symlinks() {
        use std::os::unix::fs::symlink;
        let data = tempdir().unwrap();
        let outside = tempdir().unwrap();
        let secret_md = outside.path().join("secret.md");
        fs::write(&secret_md, "leaked rule").unwrap();
        let secret_mcp = outside.path().join("mcp.json");
        fs::write(&secret_mcp, r#"{"mcpServers":{"s":{"url":"http://127.0.0.1:9999"}}}"#).unwrap();

        let pdir = data.path().join("plugins").join("sym-plugin");
        fs::create_dir_all(pdir.join("rules")).unwrap();
        fs::create_dir_all(pdir.join("skills").join("linked-skill")).unwrap();
        fs::write(pdir.join("plugin.json"), r#"{"name":"sym-plugin"}"#).unwrap();
        symlink(&secret_md, pdir.join("rules").join("01-sym.md")).unwrap();
        symlink(&secret_mcp, pdir.join(".mcp.json")).unwrap();
        symlink(&secret_md, pdir.join("skills").join("linked-skill").join("SKILL.md")).unwrap();

        let pm = PluginManager::new(data.path(), None);
        let view = pm.get_plugin("sym-plugin").expect("plugin loads");
        assert_eq!(view.rules_count, 0);
        assert_eq!(view.skills_count, 0);
        assert!(!view.has_mcp);
    }

    #[tokio::test]
    async fn plugin_http_tool_percent_encodes_urls_caps_zero_to_16mib_and_strips_cross_origin_redirects() {
        use axum::body::Bytes;
        use axum::extract::Query;
        use axum::http::{header, HeaderMap as AxumHeaderMap, StatusCode as AxumStatusCode};
        use axum::response::IntoResponse;
        use axum::routing::get;
        use std::collections::HashMap;

        let dest_app = Router::new().route(
            "/target",
            post(|headers: AxumHeaderMap, body: Bytes| async move {
                let auth = headers
                    .get(header::AUTHORIZATION)
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("")
                    .to_string();
                let secret = headers
                    .get("x-secret-token")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("")
                    .to_string();
                Json(json!({
                    "auth": auth,
                    "secret": secret,
                    "body_len": body.len(),
                    "pad": "z".repeat(2048),
                }))
            }),
        );
        let dest_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let dest_addr = dest_listener.local_addr().unwrap();
        let dest_task = tokio::spawn(async move {
            let _ = axum::serve(dest_listener, dest_app).await;
        });

        let redirect_target = format!("http://{dest_addr}/target");
        let origin_app = Router::new()
            .route(
                "/search",
                get(|Query(params): Query<HashMap<String, String>>| async move {
                    Json(json!({
                        "q": params.get("q").cloned().unwrap_or_default(),
                        "injected": params.contains_key("evil"),
                        "pad": "a".repeat(2048),
                    }))
                }),
            )
            .route(
                "/hop",
                post({
                    let redirect_target = redirect_target.clone();
                    move || async move {
                        (
                            AxumStatusCode::TEMPORARY_REDIRECT,
                            [(header::LOCATION, redirect_target)],
                        )
                            .into_response()
                    }
                }),
            );
        let origin_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin_addr = origin_listener.local_addr().unwrap();
        let origin_task = tokio::spawn(async move {
            let _ = axum::serve(origin_listener, origin_app).await;
        });

        let data = tempdir().unwrap();
        let pm = PluginManager::new(data.path(), None);
        let manifest = format!(
            r#"{{
                "name": "sec-plugin",
                "tools": [
                    {{
                        "name": "query_url",
                        "kind": "http",
                        "method": "GET",
                        "url": "http://{origin_addr}/search?q={{{{q}}}}"
                    }},
                    {{
                        "name": "redir_post",
                        "kind": "http",
                        "method": "POST",
                        "url": "http://{origin_addr}/hop",
                        "headers": {{
                            "Authorization": "Bearer secret",
                            "X-Secret-Token": "tok-123"
                        }},
                        "body_template": "{{\"secret\":1}}"
                    }}
                ]
            }}"#
        );
        pm.import_plugin_json(&manifest).unwrap();

        let client = build_web_client(5).unwrap();
        let mut cfg = ToolsConfig::default();
        cfg.allowed_domains = vec!["127.0.0.1".to_string()];
        cfg.block_private_lan_web_requests = false;
        cfg.max_web_bytes = 0; // Must use 16 MiB safe cap, not 1024B!

        let q_res = pm
            .execute_tool(
                &client,
                "plugin:sec-plugin:query_url",
                json!({"q": "hi&evil=1#frag"}),
                &cfg,
            )
            .await
            .expect("query succeeds");
        assert_eq!(q_res["json"]["q"], "hi&evil=1#frag");
        assert_eq!(q_res["json"]["injected"], false);
        assert_eq!(q_res["truncated"], false);

        let hop_res = pm
            .execute_tool(&client, "plugin:sec-plugin:redir_post", json!({}), &cfg)
            .await
            .expect("redirect succeeds");
        assert_eq!(hop_res["status"], 200);
        assert_eq!(hop_res["json"]["auth"], "");
        assert_eq!(hop_res["json"]["secret"], "");
        assert_eq!(hop_res["json"]["body_len"], 0);
        assert_eq!(hop_res["truncated"], false);

        origin_task.abort();
        dest_task.abort();
    }
}
