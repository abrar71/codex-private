use crate::error::TransportError;
use crate::request::Request;
use crate::request::Response;
use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;
use futures::StreamExt;
use futures::stream::BoxStream;
use http::HeaderMap;
use http::Method;
use http::StatusCode;
use serde_json::Value;
use std::fmt;
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

    fn should_log(&self) -> bool {
        self.debug_sink.is_some()
    }

    fn log_lines(&self, lines: &[String]) {
        if let Some(sink) = &self.debug_sink {
            sink.write_lines(lines);
        }
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

    fn log_request(&self, method: &Method, url: &str, body: &Option<Value>) {
        if !self.should_log() {
            return;
        }

        let mut lines = vec![format!("--> {method} {url}")];
        if let Some(body) = body {
            let pretty = serde_json::to_string_pretty(body).unwrap_or_else(|_| body.to_string());
            lines.push(pretty);
        }
        self.log_lines(&lines);
    }

    fn log_response_body(
        &self,
        method: &str,
        url: &str,
        status: StatusCode,
        body: &[u8],
        streaming: bool,
    ) {
        if !self.should_log() {
            return;
        }

        let label = if streaming { "stream" } else { "body" };
        let response = String::from_utf8_lossy(body);
        let lines = vec![
            format!("<-- {status} {method} {url}"),
            format!("response {label}: {response}"),
        ];
        self.log_lines(&lines);
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

    fn write_lines(&self, lines: &[String]) {
        let mut writer = match self.writer.lock() {
            Ok(writer) => writer,
            Err(err) => err.into_inner(),
        };

        for line in lines {
            if let Err(err) = writeln!(writer, "{line}") {
                eprintln!(
                    "Failed to write debug log file {}: {err}",
                    self.path.display()
                );
                break;
            }
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

#[async_trait]
impl HttpTransport for ReqwestTransport {
    async fn execute(&self, req: Request) -> Result<Response, TransportError> {
        let method = req.method.clone();
        let url = req.url.clone();
        let body = req.body.clone();
        self.log_request(&method, &url, &body);

        let builder = self.build(req)?;
        let resp = builder.send().await.map_err(Self::map_error)?;
        let status = resp.status();
        let headers = resp.headers().clone();
        let bytes = resp.bytes().await.map_err(Self::map_error)?;
        self.log_response_body(method.as_str(), &url, status, &bytes, false);
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
        let body = req.body.clone();
        self.log_request(&method, &url, &body);

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
        if !status.is_success() {
            let body = resp.text().await.ok();
            if let Some(body) = body.as_deref() {
                self.log_response_body(method.as_str(), &url, status, body.as_bytes(), true);
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
        let stream = if self.should_log() {
            Box::pin(DebugByteStream::new(
                stream,
                method.to_string(),
                url,
                status,
                self.debug_sink.clone(),
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
        debug_sink: Option<DebugSink>,
    ) -> Self {
        Self {
            inner,
            method,
            url,
            status,
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
        let status = self.status;
        let method = &self.method;
        let url = &self.url;
        let response = String::from_utf8_lossy(&self.buffer);
        let lines = vec![
            format!("<-- {status} {method} {url}"),
            format!("response stream: {response}"),
        ];
        if let Some(sink) = &self.debug_sink {
            sink.write_lines(&lines);
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
