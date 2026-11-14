use crate::calculator;
use chrono::{DateTime, Utc};
use ps5_agent_core::{AgentError, Result};
use regex::RegexBuilder;
use serde_json::{json, Value};

pub fn exec_calculator(args: &Value) -> Result<Value> {
    let expr = args
        .get("expression")
        .and_then(|v| v.as_str())
        .ok_or_else(|| AgentError::InvalidInput("missing required string 'expression'".into()))?;
    let result = calculator::eval_expression(expr)?;
    Ok(json!({
        "expression": expr,
        "result": result,
    }))
}

pub fn exec_datetime(args: &Value) -> Result<Value> {
    let action = args
        .get("action")
        .and_then(|v| v.as_str())
        .unwrap_or("now")
        .trim()
        .to_ascii_lowercase();

    match action.as_str() {
        "now" => {
            let now = Utc::now();
            Ok(json!({
                "iso8601": now.to_rfc3339(),
                "unix_timestamp": now.timestamp(),
                "unix_millis": now.timestamp_millis(),
                "weekday": now.format("%A").to_string(),
            }))
        }
        "format" => {
            let fmt = args
                .get("format")
                .and_then(|v| v.as_str())
                .unwrap_or("%Y-%m-%d %H:%M:%S UTC");
            let dt = match args.get("timestamp").and_then(|v| v.as_str()) {
                Some(ts) => parse_dt(ts)?,
                None => Utc::now(),
            };
            let mut formatted = String::new();
            std::fmt::Write::write_fmt(&mut formatted, format_args!("{}", dt.format(fmt)))
                .map_err(|_| {
                    AgentError::InvalidInput(format!("invalid datetime format '{fmt}'"))
                })?;
            Ok(json!({
                "formatted": formatted,
                "iso8601": dt.to_rfc3339(),
            }))
        }
        "parse" => {
            let ts = args
                .get("timestamp")
                .and_then(|v| v.as_str())
                .ok_or_else(|| AgentError::InvalidInput("missing 'timestamp'".into()))?;
            let dt = parse_dt(ts)?;
            Ok(json!({
                "iso8601": dt.to_rfc3339(),
                "unix_timestamp": dt.timestamp(),
                "unix_millis": dt.timestamp_millis(),
            }))
        }
        "add_seconds" => {
            let secs = args
                .get("seconds")
                .and_then(|v| v.as_i64())
                .ok_or_else(|| AgentError::InvalidInput("missing integer 'seconds'".into()))?;
            let base = match args.get("timestamp").and_then(|v| v.as_str()) {
                Some(ts) => parse_dt(ts)?,
                None => Utc::now(),
            };
            let delta = chrono::TimeDelta::try_seconds(secs).ok_or_else(|| {
                AgentError::InvalidInput(format!("seconds offset '{secs}' is out of range"))
            })?;
            let target = base.checked_add_signed(delta).ok_or_else(|| {
                AgentError::InvalidInput(format!(
                    "datetime overflow adding {secs} seconds to '{}'",
                    base.to_rfc3339()
                ))
            })?;
            Ok(json!({
                "iso8601": target.to_rfc3339(),
                "unix_timestamp": target.timestamp(),
            }))
        }
        "diff" => {
            let ts1 = args
                .get("timestamp")
                .and_then(|v| v.as_str())
                .ok_or_else(|| AgentError::InvalidInput("missing 'timestamp'".into()))?;
            let ts2 = args
                .get("timestamp2")
                .and_then(|v| v.as_str())
                .ok_or_else(|| AgentError::InvalidInput("missing 'timestamp2'".into()))?;
            let d1 = parse_dt(ts1)?;
            let d2 = parse_dt(ts2)?;
            let diff = d2.signed_duration_since(d1);
            Ok(json!({
                "diff_seconds": diff.num_seconds(),
                "diff_millis": diff.num_milliseconds(),
            }))
        }
        other => Err(AgentError::InvalidInput(format!(
            "unsupported datetime action '{other}'"
        ))),
    }
}

fn parse_dt(s: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(s.trim())
        .map(|d| d.with_timezone(&Utc))
        .map_err(|e| AgentError::InvalidInput(format!("invalid RFC3339 timestamp '{s}': {e}")))
}

pub fn exec_json_process(args: &Value) -> Result<Value> {
    let action = args
        .get("action")
        .and_then(|v| v.as_str())
        .unwrap_or("parse")
        .trim()
        .to_ascii_lowercase();

    let input_val: Value = match args.get("input") {
        Some(Value::String(s)) => serde_json::from_str(s)
            .map_err(|e| AgentError::InvalidInput(format!("invalid JSON input: {e}")))?,
        Some(v) => v.clone(),
        None => {
            return Err(AgentError::InvalidInput("missing 'input'".into()));
        }
    };

    match action.as_str() {
        "parse" | "validate" => Ok(json!({
            "valid": true,
            "value": input_val,
        })),
        "format" => {
            let pretty = serde_json::to_string_pretty(&input_val)
                .map_err(|e| AgentError::InvalidInput(format!("formatting JSON failed: {e}")))?;
            Ok(json!({ "text": pretty }))
        }
        "minify" => {
            let compact = serde_json::to_string(&input_val)
                .map_err(|e| AgentError::InvalidInput(format!("minifying JSON failed: {e}")))?;
            Ok(json!({ "text": compact }))
        }
        "keys" => match input_val {
            Value::Object(map) => {
                let keys: Vec<String> = map.keys().cloned().collect();
                Ok(json!({ "keys": keys, "count": keys.len() }))
            }
            _ => Err(AgentError::InvalidInput("input must be a JSON object for 'keys'".into())),
        },
        "get" => {
            let ptr = args
                .get("pointer")
                .and_then(|v| v.as_str())
                .ok_or_else(|| AgentError::InvalidInput("missing 'pointer'".into()))?;
            let found = if ptr.starts_with('/') {
                input_val.pointer(ptr).cloned()
            } else {
                let normalized = format!("/{}", ptr.replace('.', "/"));
                input_val.pointer(&normalized).cloned()
            };
            Ok(json!({
                "pointer": ptr,
                "found": found.is_some(),
                "value": found.unwrap_or(Value::Null),
            }))
        }
        other => Err(AgentError::InvalidInput(format!(
            "unsupported json_process action '{other}'"
        ))),
    }
}

pub fn exec_text_process(args: &Value) -> Result<Value> {
    let action = args
        .get("action")
        .and_then(|v| v.as_str())
        .ok_or_else(|| AgentError::InvalidInput("missing required string 'action'".into()))?
        .trim()
        .to_ascii_lowercase();
    let text = args
        .get("text")
        .and_then(|v| v.as_str())
        .ok_or_else(|| AgentError::InvalidInput("missing required string 'text'".into()))?;

    match action.as_str() {
        "length" => Ok(json!({
            "bytes": text.len(),
            "chars": text.chars().count(),
            "lines": text.lines().count(),
        })),
        "word_count" => {
            let count = text.split_whitespace().count();
            Ok(json!({ "words": count }))
        }
        "trim" => Ok(json!({ "text": text.trim() })),
        "lowercase" => Ok(json!({ "text": text.to_lowercase() })),
        "uppercase" => Ok(json!({ "text": text.to_uppercase() })),
        "split" => {
            let pat = args.get("pattern").and_then(|v| v.as_str()).unwrap_or("\n");
            let parts: Vec<&str> = text.split(pat).collect();
            Ok(json!({ "parts": parts, "count": parts.len() }))
        }
        "replace" => {
            let pat = args
                .get("pattern")
                .and_then(|v| v.as_str())
                .ok_or_else(|| AgentError::InvalidInput("missing 'pattern'".into()))?;
            let rep = args.get("replacement").and_then(|v| v.as_str()).unwrap_or("");
            Ok(json!({ "text": text.replace(pat, rep) }))
        }
        "truncate" => {
            let max_chars = args
                .get("max_chars")
                .and_then(|v| v.as_u64())
                .ok_or_else(|| AgentError::InvalidInput("missing 'max_chars'".into()))?
                as usize;
            let truncated: String = text.chars().take(max_chars).collect();
            Ok(json!({
                "text": truncated,
                "was_truncated": text.chars().count() > max_chars,
            }))
        }
        "regex_match" => {
            let pat = args
                .get("pattern")
                .and_then(|v| v.as_str())
                .ok_or_else(|| AgentError::InvalidInput("missing 'pattern'".into()))?;
            let re = RegexBuilder::new(pat)
                .size_limit(1 << 20)
                .build()
                .map_err(|e| AgentError::InvalidInput(format!("invalid regex: {e}")))?;
            let matches: Vec<&str> = re.find_iter(text).take(200).map(|m| m.as_str()).collect();
            Ok(json!({
                "matched": !matches.is_empty(),
                "matches": matches,
            }))
        }
        other => Err(AgentError::InvalidInput(format!(
            "unsupported text_process action '{other}'"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn datetime_format_rejects_invalid_specifiers_without_panicking() {
        for bad_fmt in ["%", "%Q", "%Y-%"] {
            let err = exec_datetime(&json!({
                "action": "format",
                "timestamp": "2026-01-15T12:30:00Z",
                "format": bad_fmt,
            }))
            .unwrap_err();
            assert!(
                matches!(err, AgentError::InvalidInput(_)),
                "expected InvalidInput for '{bad_fmt}', got {err:?}"
            );
        }

        let ok = exec_datetime(&json!({
            "action": "format",
            "timestamp": "2026-01-15T12:30:00Z",
            "format": "%Y-%m-%d",
        }))
        .unwrap();
        assert_eq!(ok["formatted"], "2026-01-15");
    }

    #[test]
    fn datetime_add_seconds_rejects_overflow_without_panicking() {
        for extreme in [i64::MAX, i64::MIN, 1_000_000_000_000_000_i64] {
            let err = exec_datetime(&json!({
                "action": "add_seconds",
                "timestamp": "2026-01-15T12:30:00Z",
                "seconds": extreme,
            }))
            .unwrap_err();
            assert!(
                matches!(err, AgentError::InvalidInput(_)),
                "expected InvalidInput for offset {extreme}, got {err:?}"
            );
        }

        let ok = exec_datetime(&json!({
            "action": "add_seconds",
            "timestamp": "2026-01-15T12:30:00Z",
            "seconds": 90,
        }))
        .unwrap();
        assert_eq!(ok["iso8601"], "2026-01-15T12:31:30+00:00");
    }
}
