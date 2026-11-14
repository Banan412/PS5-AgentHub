//! Embedded Web Dashboard assets for the PS5 Agent Command Center.
//!
//! Serves a self-contained, zero-external-CDN SPA for offline LAN operation on PS5.

pub const INDEX_HTML: &str = include_str!("../assets/index.html");
pub const STYLES_CSS: &str = include_str!("../assets/styles.css");
pub const APP_JS: &str = include_str!("../assets/app.js");

/// Resolves a dashboard asset request path to `(content_type, body)`.
pub fn asset_by_path(path: &str) -> Option<(&'static str, &'static str)> {
    match path {
        "/" | "/index.html" => Some(("text/html; charset=utf-8", INDEX_HTML)),
        "/styles.css" | "/assets/styles.css" => Some(("text/css; charset=utf-8", STYLES_CSS)),
        "/app.js" | "/assets/app.js" => Some(("application/javascript; charset=utf-8", APP_JS)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embedded_assets_are_non_empty() {
        assert!(!INDEX_HTML.trim().is_empty(), "INDEX_HTML must not be empty");
        assert!(!STYLES_CSS.trim().is_empty(), "STYLES_CSS must not be empty");
        assert!(!APP_JS.trim().is_empty(), "APP_JS must not be empty");
    }

    #[test]
    fn html_has_valid_structure_and_zero_external_cdns() {
        assert!(INDEX_HTML.contains("<!DOCTYPE html>"));
        assert!(INDEX_HTML.contains("<html lang=\"en\">"));
        assert!(INDEX_HTML.contains("</html>"));
        assert!(INDEX_HTML.contains("href=\"/styles.css\""));
        assert!(INDEX_HTML.contains("src=\"/app.js\""));
        assert!(!INDEX_HTML.contains("https://"));
        assert!(!INDEX_HTML.contains("href=\"http"));
        assert!(!INDEX_HTML.contains("src=\"http"));

        for section in [
            "section-overview",
            "section-chat",
            "section-files",
            "section-agents",
            "section-a2a",
            "section-tasks",
            "section-schedules",
            "section-memory",
            "section-tools",
            "section-extensions",
            "section-sandbox",
            "section-logs",
            "section-settings",
        ] {
            assert!(
                INDEX_HTML.contains(section),
                "missing dashboard section {section}"
            );
        }
        assert!(INDEX_HTML.contains("data-section=\"files\""));
        assert!(INDEX_HTML.contains("data-section=\"schedules\""));
        assert!(INDEX_HTML.contains("data-section=\"extensions\""));
        assert!(INDEX_HTML.contains("data-section=\"sandbox\""));
        assert!(INDEX_HTML.contains("chat-ephemeral-card"));
        assert!(INDEX_HTML.contains("modal-schedule"));
        assert!(!INDEX_HTML.contains("allow-same-origin"));
        assert!(INDEX_HTML.contains("login-overlay"));
    }

    #[test]
    fn css_contains_theme_accessibility_and_responsive_rules() {
        assert!(STYLES_CSS.contains("#0b0f17"));
        assert!(STYLES_CSS.contains("#131a26"));
        assert!(STYLES_CSS.contains("#1c2638"));
        assert!(STYLES_CSS.contains("#e6edf7"));
        assert!(STYLES_CSS.contains(":focus-visible"));
        assert!(STYLES_CSS.contains("min-height: 44px"));
        assert!(STYLES_CSS.contains("@media (max-width: 768px)"));
    }

    #[test]
    fn js_wires_auth_csrf_sse_and_avoids_inner_html() {
        assert!(APP_JS.contains("/api/v1/auth/login"));
        assert!(APP_JS.contains("X-CSRF-Token"));
        assert!(APP_JS.contains("/api/v1/events"));
        assert!(APP_JS.contains("/api/v1/conversations/search"));
        assert!(APP_JS.contains("/api/v1/system/checkpoint"));
        assert!(APP_JS.contains("/api/v1/system/shutdown"));
        assert!(
            !APP_JS.contains(".innerHTML")
                && !APP_JS.contains(".outerHTML")
                && !APP_JS.contains("insertAdjacentHTML"),
            "app.js must never use raw HTML injection APIs"
        );
    }

    #[test]
    fn asset_by_path_resolves_all_expected_routes() {
        assert_eq!(asset_by_path("/").map(|v| v.0), Some("text/html; charset=utf-8"));
        assert_eq!(asset_by_path("/styles.css").map(|v| v.0), Some("text/css; charset=utf-8"));
        assert_eq!(asset_by_path("/assets/styles.css").map(|v| v.0), Some("text/css; charset=utf-8"));
        assert_eq!(asset_by_path("/app.js").map(|v| v.0), Some("application/javascript; charset=utf-8"));
        assert_eq!(asset_by_path("/assets/app.js").map(|v| v.0), Some("application/javascript; charset=utf-8"));
        assert!(asset_by_path("/nonexistent").is_none());
    }
}
