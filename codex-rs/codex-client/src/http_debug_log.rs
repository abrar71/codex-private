use chrono::SecondsFormat;
use chrono::Utc;
use http::StatusCode;
use serde::Serialize;
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::fs::create_dir_all;
use std::io::Write as _;
use std::path::Path;

#[derive(Serialize)]
pub struct HttpDebugLogEntry {
    pub timestamp: String,
    pub method: String,
    pub url: String,
    pub status: i64,
    pub content_type: String,
    pub request_ids: HashMap<String, String>,
    pub request_headers: Option<Vec<HeaderEntry>>,
    pub request_body: Option<String>,
    pub response_headers: Vec<HeaderEntry>,
    pub response_body: String,
}

#[derive(Serialize)]
pub struct HeaderEntry {
    pub name: String,
    pub value: String,
}

pub fn create_http_debug_entry(
    method: &str,
    url: &str,
    status: StatusCode,
    content_type: &str,
    request_ids: &HashMap<String, String>,
    request_headers: Option<&[(String, String)]>,
    request_body: Option<&str>,
    response_headers: &[(String, String)],
    response_body: &str,
) -> HttpDebugLogEntry {
    let timestamp = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
    let status = i64::from(status.as_u16());
    let request_headers = request_headers.map(|headers| {
        headers
            .iter()
            .map(|(name, value)| HeaderEntry {
                name: name.to_string(),
                value: value.to_string(),
            })
            .collect()
    });
    let response_headers = response_headers
        .iter()
        .map(|(name, value)| HeaderEntry {
            name: name.to_string(),
            value: value.to_string(),
        })
        .collect();
    HttpDebugLogEntry {
        timestamp,
        method: method.to_string(),
        url: url.to_string(),
        status,
        content_type: content_type.to_string(),
        request_ids: request_ids.clone(),
        request_headers,
        request_body: request_body.map(std::string::ToString::to_string),
        response_headers,
        response_body: response_body.to_string(),
    }
}

pub fn encode_http_debug_entry(entry: &HttpDebugLogEntry) -> Option<String> {
    serde_json::to_string(entry).ok()
}

pub fn append_http_debug_entry(entry: &HttpDebugLogEntry, path: &Path) {
    let Some(line) = encode_http_debug_entry(entry) else {
        return;
    };
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        if create_dir_all(parent).is_err() {
            return;
        }
    }
    if let Ok(mut file) = OpenOptions::new().create(true).append(true).open(path) {
        let _ = writeln!(file, "{line}");
    }
}
