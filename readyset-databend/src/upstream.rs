//! Minimal Databend HTTP Query API client used for bootstrap and change capture.
//! Same protocol loop as `poc/m0.py` `Client`: session carried between statements (so
//! BEGIN/COMMIT work), sticky node when the server asks for it, bounded paging, and a
//! best-effort kill/final for unfinished queries. A statement is NEVER resubmitted.
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail, ensure};
use serde_json::{Map, Value, json};

pub const MAX_RESPONSE: usize = 16 * 1024 * 1024;
const MAX_ROWS: usize = 10_000;
/// Well under Databend's default 60 s result timeout (drivers use half of it).
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(20);

/// Append-only JSON lines journal of SQL statement metadata. Never records credentials or
/// result rows.
pub struct Journal(Mutex<File>);

impl Journal {
    pub fn open(path: &Path) -> Result<Arc<Self>> {
        Ok(Arc::new(Self(Mutex::new(
            OpenOptions::new().create(true).append(true).open(path)?,
        ))))
    }

    pub fn write(&self, value: &Value) {
        if let Ok(mut file) = self.0.lock() {
            let _ = writeln!(file, "{value}");
        }
    }
}

pub fn basic_auth(user: &str, password: &str) -> String {
    use base64::Engine as _;
    format!(
        "Basic {}",
        base64::engine::general_purpose::STANDARD.encode(format!("{user}:{password}"))
    )
}

pub fn http_client() -> Result<reqwest::Client> {
    Ok(reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()?)
}

/// Read a response body, refusing anything larger than `limit` bytes.
pub async fn bounded_body(mut response: reqwest::Response, limit: usize) -> Result<Vec<u8>> {
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        ensure!(
            body.len() + chunk.len() <= limit,
            "upstream response too large; SQL not retried"
        );
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

pub struct DatabendClient {
    http: reqwest::Client,
    endpoint: String,
    auth: String,
    label: String,
    pub session: Map<String, Value>,
    node: Option<String>,
    journal: Option<Arc<Journal>>,
}

impl DatabendClient {
    pub fn new(
        http: reqwest::Client,
        endpoint: &str,
        auth: &str,
        label: &str,
        journal: Option<Arc<Journal>>,
    ) -> Self {
        Self {
            http,
            endpoint: endpoint.trim_end_matches('/').to_owned(),
            auth: auth.to_owned(),
            label: label.to_owned(),
            session: Map::new(),
            node: None,
            journal,
        }
    }

    /// Stream a large result page by page. While the caller holds the cursor the query is kept
    /// alive with `/v1/session/heartbeat`, so slow consumers do not hit the server's result
    /// timeout (`http_handler_result_timeout_secs`). The SQL is never resubmitted.
    pub async fn pages(&mut self, sql: &str, page_rows: usize) -> Result<Pages<'_>> {
        let started = Instant::now();
        let payload = json!({
            "sql": sql,
            "session": self.session,
            "pagination": {"wait_time_secs": 2, "max_rows_per_page": page_rows},
        });
        let event = json!({
            "client": self.label,
            "sql": sql,
            "utc": SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs_f64()).unwrap_or(0.0),
            "streamed": true,
        });
        let response = self
            .call("/v1/query", Some(&payload), Duration::from_secs(60))
            .await;
        let mut pages = Pages {
            client: self,
            event,
            started,
            query_id: None,
            state: String::new(),
            pending: None,
            next_uri: None,
            done: false,
            finished: false,
            rows: 0,
            heartbeat: None,
        };
        match response {
            Ok(response) => {
                pages.query_id = response["id"].as_str().map(str::to_owned);
                pages.pending = Some(response);
                Ok(pages)
            }
            Err(error) => Err(pages.fail(error).await),
        }
    }

    async fn heartbeat_loop(
        http: reqwest::Client,
        endpoint: String,
        auth: String,
        node: String,
        query_id: String,
    ) {
        let body = json!({ "node_to_queries": { node: [query_id] } });
        loop {
            tokio::time::sleep(HEARTBEAT_INTERVAL).await;
            // Best effort: a missed heartbeat surfaces as a page error, never as lost data.
            let _ = http
                .post(format!("{endpoint}/v1/session/heartbeat"))
                .header(reqwest::header::AUTHORIZATION, &auth)
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .body(body.to_string())
                .timeout(Duration::from_secs(10))
                .send()
                .await;
        }
    }

    async fn call(&self, path: &str, payload: Option<&Value>, timeout: Duration) -> Result<Value> {
        ensure!(
            path.starts_with("/v1/query") && !path.contains("://"),
            "invalid query URI: {path}"
        );
        let url = format!("{}{path}", self.endpoint);
        let mut request = match payload {
            Some(payload) => self.http.post(url).body(serde_json::to_vec(payload)?),
            None => self.http.get(url),
        }
        .header(reqwest::header::AUTHORIZATION, &self.auth)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .timeout(timeout);
        if let Some(node) = &self.node
            && self.session.get("need_sticky") == Some(&Value::Bool(true))
        {
            request = request.header("X-DATABEND-STICKY-NODE", node);
        }
        let response = request.send().await?;
        let status = response.status();
        let body = bounded_body(response, MAX_RESPONSE).await?;
        ensure!(
            status.is_success(),
            "Databend HTTP {status}: {}",
            String::from_utf8_lossy(&body[..body.len().min(512)])
        );
        Ok(if body.is_empty() {
            json!({})
        } else {
            serde_json::from_slice(&body)?
        })
    }

    /// Run one statement to completion and return its rows (JSON strings or null).
    pub async fn sql(&mut self, sql: &str) -> Result<Vec<Vec<Value>>> {
        let start = Instant::now();
        let mut event = json!({
            "client": self.label,
            "sql": sql,
            "utc": SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs_f64()).unwrap_or(0.0),
        });
        let mut query_id: Option<String> = None;
        let mut state = String::new();
        let mut finished = false;
        let outcome = self
            .run(sql, start, &mut query_id, &mut state, &mut finished)
            .await;
        if let Some(id) = &query_id
            && !finished
        {
            // Best-effort cleanup only; failures are recorded, never retried.
            if state != "Succeeded" && state != "Failed" {
                let _ = self
                    .call(
                        &format!("/v1/query/{id}/kill"),
                        None,
                        Duration::from_secs(5),
                    )
                    .await;
            }
            if let Err(error) = self
                .call(
                    &format!("/v1/query/{id}/final"),
                    None,
                    Duration::from_secs(5),
                )
                .await
            {
                event["cleanup_error"] = json!(format!("{error:#}"));
            }
        }
        event["query_id"] = json!(query_id);
        event["state"] = json!(state);
        event["txn_state"] = self
            .session
            .get("txn_state")
            .cloned()
            .unwrap_or(Value::Null);
        event["elapsed_ms"] = json!((start.elapsed().as_secs_f64() * 100_000.0).round() / 100.0);
        if let Err(error) = &outcome {
            event["exception"] = json!(format!("{error:#}"));
        }
        if let Some(journal) = &self.journal {
            journal.write(&event);
        }
        outcome
    }

    async fn run(
        &mut self,
        sql: &str,
        start: Instant,
        query_id: &mut Option<String>,
        state: &mut String,
        finished: &mut bool,
    ) -> Result<Vec<Vec<Value>>> {
        let payload = json!({
            "sql": sql,
            "session": self.session,
            "pagination": {"wait_time_secs": 2, "max_rows_per_page": 1000},
        });
        let mut response = self
            .call("/v1/query", Some(&payload), Duration::from_secs(15))
            .await?;
        *query_id = response["id"].as_str().map(str::to_owned);
        let deadline = start + Duration::from_secs(45);
        let mut rows = Vec::new();
        loop {
            if let Some(session) = response["session"].as_object()
                && !session.is_empty()
            {
                self.session = session.clone();
            }
            if let Some(node) = response["node_id"].as_str()
                && !node.is_empty()
            {
                self.node = Some(node.to_owned());
            }
            if let Some(data) = response["data"].as_array() {
                for row in data {
                    rows.push(row.as_array().context("row is not an array")?.clone());
                }
            }
            ensure!(rows.len() <= MAX_ROWS, "result exceeds {MAX_ROWS} rows");
            *state = response["state"].as_str().unwrap_or_default().to_owned();
            if !response["error"].is_null() {
                bail!("Databend SQL error: {}", response["error"]);
            }
            let Some(uri) = response["next_uri"].as_str().map(str::to_owned) else {
                ensure!(*state == "Succeeded", "incomplete query: {state}");
                break;
            };
            let id = query_id
                .as_deref()
                .context("continuation without query ID")?;
            let prefix = format!("/v1/query/{id}/");
            let rest = uri.strip_prefix(&prefix).unwrap_or_default();
            ensure!(
                rest == "final"
                    || rest
                        .strip_prefix("page/")
                        .is_some_and(|n| !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit())),
                "unexpected continuation URI: {uri}"
            );
            let now = Instant::now();
            ensure!(
                now < deadline,
                "query deadline exceeded; SQL not resubmitted"
            );
            if rest == "final" {
                ensure!(
                    *state == "Succeeded",
                    "final URI before successful completion"
                );
                self.call(&uri, None, Duration::from_secs(15)).await?;
                *finished = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
            let remaining = (deadline - now).clamp(Duration::from_secs(1), Duration::from_secs(15));
            response = self.call(&uri, None, remaining).await?;
        }
        Ok(rows)
    }
}

/// Cursor over one streamed statement (see [`DatabendClient::pages`]). Call [`Pages::close`]
/// after the last page, or [`Pages::abort`] on any error. A result is complete only when
/// `close` succeeds: the query reached `Succeeded` and every page was returned.
pub struct Pages<'a> {
    client: &'a mut DatabendClient,
    event: Value,
    started: Instant,
    query_id: Option<String>,
    state: String,
    pending: Option<Value>,
    next_uri: Option<String>,
    done: bool,
    finished: bool,
    rows: usize,
    heartbeat: Option<tokio::task::JoinHandle<()>>,
}

impl Pages<'_> {
    /// The next non-empty page; `None` once the query has `Succeeded` and all rows were read.
    pub async fn next(&mut self) -> Result<Option<Vec<Vec<Value>>>> {
        while !self.done {
            let response = match self.pending.take() {
                Some(response) => response,
                None => {
                    let uri = self.next_uri.take().context("missing continuation URI")?;
                    self.client
                        .call(&uri, None, Duration::from_secs(60))
                        .await?
                }
            };
            let rows = self.absorb(response).await?;
            if !rows.is_empty() {
                self.rows += rows.len();
                return Ok(Some(rows));
            }
        }
        Ok(None)
    }

    /// True once the first response showed there is more than one page to read.
    pub fn has_more(&self) -> bool {
        !self.done
    }

    async fn absorb(&mut self, response: Value) -> Result<Vec<Vec<Value>>> {
        if let Some(session) = response["session"].as_object()
            && !session.is_empty()
        {
            self.client.session = session.clone();
        }
        if let Some(node) = response["node_id"].as_str()
            && !node.is_empty()
        {
            self.client.node = Some(node.to_owned());
        }
        if self.heartbeat.is_none()
            && let (Some(node), Some(id)) = (self.client.node.clone(), self.query_id.clone())
        {
            self.heartbeat = Some(tokio::spawn(DatabendClient::heartbeat_loop(
                self.client.http.clone(),
                self.client.endpoint.clone(),
                self.client.auth.clone(),
                node,
                id,
            )));
        }
        self.state = response["state"].as_str().unwrap_or_default().to_owned();
        if !response["error"].is_null() {
            bail!("Databend SQL error: {}", response["error"]);
        }
        let mut rows = Vec::new();
        if let Some(data) = response["data"].as_array() {
            for row in data {
                rows.push(row.as_array().context("row is not an array")?.clone());
            }
        }
        let Some(uri) = response["next_uri"].as_str().map(str::to_owned) else {
            ensure!(
                self.state == "Succeeded",
                "incomplete query: {}",
                self.state
            );
            self.done = true;
            return Ok(rows);
        };
        let id = self
            .query_id
            .as_deref()
            .context("continuation without query ID")?;
        let rest = uri
            .strip_prefix(&format!("/v1/query/{id}/"))
            .unwrap_or_default();
        ensure!(
            rest == "final"
                || rest
                    .strip_prefix("page/")
                    .is_some_and(|n| !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit())),
            "unexpected continuation URI: {uri}"
        );
        if rest == "final" {
            ensure!(
                self.state == "Succeeded",
                "final URI before successful completion"
            );
            self.client
                .call(&uri, None, Duration::from_secs(15))
                .await?;
            self.finished = true;
            self.done = true;
        } else {
            if rows.is_empty() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            self.next_uri = Some(uri);
        }
        Ok(rows)
    }

    fn stop_heartbeat(&mut self) {
        if let Some(task) = self.heartbeat.take() {
            task.abort();
        }
    }

    fn journal(&mut self, error: Option<&anyhow::Error>) {
        self.event["query_id"] = json!(self.query_id);
        self.event["state"] = json!(self.state);
        self.event["rows"] = json!(self.rows);
        self.event["txn_state"] = self
            .client
            .session
            .get("txn_state")
            .cloned()
            .unwrap_or(Value::Null);
        self.event["elapsed_ms"] =
            json!((self.started.elapsed().as_secs_f64() * 100_000.0).round() / 100.0);
        if let Some(error) = error {
            self.event["exception"] = json!(format!("{error:#}"));
        }
        if let Some(journal) = &self.client.journal {
            journal.write(&self.event);
        }
    }

    /// Finish a fully read result. Errors if any page is still unread.
    pub async fn close(mut self) -> Result<usize> {
        self.stop_heartbeat();
        if !self.done {
            let error = anyhow::anyhow!("streamed result closed before its last page");
            return Err(self.abort(error).await);
        }
        self.journal(None);
        Ok(self.rows)
    }

    /// Best-effort kill/final of an unfinished query. Returns `error` for propagation.
    pub async fn abort(mut self, error: anyhow::Error) -> anyhow::Error {
        self.fail(error).await
    }

    async fn fail(&mut self, error: anyhow::Error) -> anyhow::Error {
        self.stop_heartbeat();
        if let Some(id) = self.query_id.clone()
            && !self.finished
        {
            if self.state != "Succeeded" && self.state != "Failed" {
                let _ = self
                    .client
                    .call(
                        &format!("/v1/query/{id}/kill"),
                        None,
                        Duration::from_secs(5),
                    )
                    .await;
            }
            let _ = self
                .client
                .call(
                    &format!("/v1/query/{id}/final"),
                    None,
                    Duration::from_secs(5),
                )
                .await;
            self.finished = true;
        }
        self.journal(Some(&error));
        error
    }
}

impl Drop for Pages<'_> {
    fn drop(&mut self) {
        self.stop_heartbeat();
    }
}
