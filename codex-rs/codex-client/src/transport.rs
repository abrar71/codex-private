use crate::error::TransportError;
use crate::request::Request;
use crate::request::Response;
use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;
use futures::StreamExt;
use futures::stream::BoxStream;
use http::HeaderMap;
use http::HeaderName;
use http::HeaderValue;
use http::Method;
use http::StatusCode;
use http::header::CONTENT_TYPE;
use serde_json::Value;
use std::collections::HashMap;
use std::fmt;
use std::fmt::Write as _;
use std::fs::File;
use std::fs::OpenOptions;
use std::fs::create_dir_all;
use std::io::Write;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::Context;
use std::task::Poll;
use tracing::Level;
use tracing::enabled;
use tracing::trace;

pub type ByteStream = BoxStream<'static, Result<Bytes, TransportError>>;

pub struct StreamResponse {
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub bytes: ByteStream,
}

#[async_trait]
pub trait HttpTransport: Send + Sync {
    async fn execute(&self, req: Request) -> Result<Response, TransportError>;
    async fn stream(&self, req: Request) -> Result<StreamResponse, TransportError>;
}

#[derive(Clone, Debug)]
pub struct ReqwestTransport {
    client: reqwest::Client,
    debug_sink: Option<DebugSink>,
}

impl ReqwestTransport {
    pub fn new(client: reqwest::Client) -> Self {
        Self {
            client,
            debug_sink: None,
        }
    }

    #[must_use]
    pub fn with_debug(self, _debug: bool) -> Self {
        self
    }

    #[must_use]
    pub fn with_debug_output(mut self, output: Option<PathBuf>) -> Self {
        if let Some(path) = output {
            match DebugSink::new(path.clone()) {
                Ok(sink) => {
                    self.debug_sink = Some(sink);
                }
                Err(err) => {
                    eprintln!("Failed to open debug log file {}: {err}", path.display());
                }
            }
        }
        self
    }

    fn build(&self, req: Request) -> Result<reqwest::RequestBuilder, TransportError> {
        let mut builder = self
            .client
            .request(
                Method::from_bytes(req.method.as_str().as_bytes()).unwrap_or(Method::GET),
                &req.url,
            )
            .headers(req.headers);
        if let Some(timeout) = req.timeout {
            builder = builder.timeout(timeout);
        }
        if let Some(body) = req.body {
            builder = builder.json(&body);
        }
        Ok(builder)
    }

    fn map_error(err: reqwest::Error) -> TransportError {
        if err.is_timeout() {
            TransportError::Timeout
        } else {
            TransportError::Network(err.to_string())
        }
    }
}

#[derive(Clone)]
struct DebugSink {
    path: PathBuf,
    writer: Arc<Mutex<File>>,
}

impl DebugSink {
    fn new(path: PathBuf) -> std::io::Result<Self> {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                create_dir_all(parent)?;
            }
        }

        let writer = OpenOptions::new().create(true).append(true).open(&path)?;

        Ok(Self {
            path,
            writer: Arc::new(Mutex::new(writer)),
        })
    }

    fn write_entry(&self, entry: &str) {
        let mut writer = match self.writer.lock() {
            Ok(writer) => writer,
            Err(err) => err.into_inner(),
        };

        if let Err(err) = writeln!(writer, "{entry}") {
            eprintln!(
                "Failed to write debug log file {}: {err}",
                self.path.display()
            );
        }
    }
}

impl fmt::Debug for DebugSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DebugSink")
            .field("path", &self.path)
            .finish()
    }
}

#[derive(Clone)]
struct RequestLog {
    headers: Vec<(String, String)>,
    body: Option<String>,
}

fn capture_request_log(req: &Request) -> RequestLog {
    RequestLog {
        headers: header_pairs(&req.headers),
        body: body_to_string(&req.body),
    }
}

fn body_to_string(body: &Option<Value>) -> Option<String> {
    let body = body.as_ref()?;
    serde_json::to_string(body)
        .ok()
        .or_else(|| Some(body.to_string()))
}

fn header_value_to_string(value: &HeaderValue) -> Option<String> {
    value
        .to_str()
        .map(str::to_string)
        .ok()
        .or_else(|| Some(String::from_utf8_lossy(value.as_bytes()).into_owned()))
}

fn header_pairs(headers: &HeaderMap) -> Vec<(String, String)> {
    headers
        .iter()
        .filter_map(|(name, value)| header_value_to_string(value).map(|v| (name.to_string(), v)))
        .collect()
}

fn extract_request_ids_from_response_headers(headers: &HeaderMap) -> HashMap<String, String> {
    ["cf-ray", "x-request-id", "x-oai-request-id"]
        .iter()
        .filter_map(|&name| {
            let header_name = HeaderName::from_static(name);
            let value = headers.get(header_name)?;
            let value = header_value_to_string(value)?;
            Some((name.to_string(), value))
        })
        .collect()
}

fn log_http_entry(
    sink: &DebugSink,
    method: &str,
    url: &str,
    status: StatusCode,
    content_type: &str,
    request_log: &RequestLog,
    response_headers: &HeaderMap,
    response_body: &str,
) {
    let request_ids = extract_request_ids_from_response_headers(response_headers);
    let response_headers = header_pairs(response_headers);

    let mut entry = String::new();
    let _ = writeln!(
        &mut entry,
        "ChatGPT HTTP {method} {url} status={} content-type={content_type} request_ids={request_ids:?}",
        status.as_u16()
    );
    if !request_log.headers.is_empty() {
        let _ = writeln!(&mut entry, "request_headers={:?}", request_log.headers);
    }
    if let Some(body) = &request_log.body {
        let _ = writeln!(&mut entry, "request_body={body}");
    }
    let _ = writeln!(&mut entry, "response_headers={response_headers:?}");
    let _ = writeln!(&mut entry, "response_body={response_body}");

    sink.write_entry(entry.trim_end());
}

#[async_trait]
impl HttpTransport for ReqwestTransport {
    async fn execute(&self, req: Request) -> Result<Response, TransportError> {
        let method = req.method.clone();
        let url = req.url.clone();
        let request_log = capture_request_log(&req);

        let builder = self.build(req)?;
        let resp = builder.send().await.map_err(Self::map_error)?;
        let status = resp.status();
        let headers = resp.headers().clone();
        let content_type = headers
            .get(CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();
        let bytes = resp.bytes().await.map_err(Self::map_error)?;
        if let Some(sink) = &self.debug_sink {
            log_http_entry(
                sink,
                method.as_str(),
                &url,
                status,
                &content_type,
                &request_log,
                &headers,
                &String::from_utf8_lossy(&bytes),
            );
        }
        if !status.is_success() {
            let body = String::from_utf8(bytes.to_vec()).ok();
            return Err(TransportError::Http {
                status,
                headers: Some(headers),
                body,
            });
        }
        Ok(Response {
            status,
            headers,
            body: bytes,
        })
    }

    async fn stream(&self, req: Request) -> Result<StreamResponse, TransportError> {
        let method = req.method.clone();
        let url = req.url.clone();
        let request_log = capture_request_log(&req);

        if enabled!(Level::TRACE) {
            trace!(
                "{} to {}: {}",
                req.method,
                req.url,
                req.body.as_ref().unwrap_or_default()
            );
        }

        let builder = self.build(req)?;
        let resp = builder.send().await.map_err(Self::map_error)?;
        let status = resp.status();
        let headers = resp.headers().clone();
        let content_type = headers
            .get(CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();
        if !status.is_success() {
            let body = resp.text().await.ok();
            if let (Some(body), Some(sink)) = (body.as_deref(), &self.debug_sink) {
                log_http_entry(
                    sink,
                    method.as_str(),
                    &url,
                    status,
                    &content_type,
                    &request_log,
                    &headers,
                    body,
                );
            }
            return Err(TransportError::Http {
                status,
                headers: Some(headers),
                body,
            });
        }
        let stream = resp
            .bytes_stream()
            .map(|result| result.map_err(Self::map_error));
        let stream = if let Some(sink) = self.debug_sink.clone() {
            Box::pin(DebugByteStream::new(
                stream,
                method.to_string(),
                url,
                status,
                content_type,
                request_log,
                headers.clone(),
                Some(sink),
            )) as ByteStream
        } else {
            Box::pin(stream) as ByteStream
        };
        Ok(StreamResponse {
            status,
            headers,
            bytes: stream,
        })
    }
}

struct DebugByteStream<S> {
    inner: S,
    method: String,
    url: String,
    status: StatusCode,
    content_type: String,
    request_log: RequestLog,
    response_headers: HeaderMap,
    buffer: Vec<u8>,
    logged: bool,
    debug_sink: Option<DebugSink>,
}

impl<S> DebugByteStream<S> {
    fn new(
        inner: S,
        method: String,
        url: String,
        status: StatusCode,
        content_type: String,
        request_log: RequestLog,
        response_headers: HeaderMap,
        debug_sink: Option<DebugSink>,
    ) -> Self {
        Self {
            inner,
            method,
            url,
            status,
            content_type,
            request_log,
            response_headers,
            buffer: Vec::new(),
            logged: false,
            debug_sink,
        }
    }

    fn log(&mut self) {
        if self.logged || self.debug_sink.is_none() {
            return;
        }
        self.logged = true;
        if let Some(sink) = &self.debug_sink {
            log_http_entry(
                sink,
                &self.method,
                &self.url,
                self.status,
                &self.content_type,
                &self.request_log,
                &self.response_headers,
                &String::from_utf8_lossy(&self.buffer),
            );
        }
    }
}

impl<S> Stream for DebugByteStream<S>
where
    S: Stream<Item = Result<Bytes, TransportError>> + Unpin,
{
    type Item = Result<Bytes, TransportError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(bytes))) => {
                this.buffer.extend_from_slice(&bytes);
                Poll::Ready(Some(Ok(bytes)))
            }
            Poll::Ready(Some(Err(err))) => {
                this.log();
                Poll::Ready(Some(Err(err)))
            }
            Poll::Ready(None) => {
                this.log();
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}
