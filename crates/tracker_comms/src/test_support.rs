//! A fake HTTP tracker for announce tests: a plain TCP listener speaking
//! just enough HTTP/1.1 to record every announce and answer a scripted
//! reply, which is what lets a test assert what goes on the wire (the
//! event, `left`, `downloaded`) rather than what the code meant to send.
//!
//! Shared between this crate's own tests and the session-level tests of
//! librqbit through the `test-support` feature, so the announce behaviour
//! of a whole session is asserted against the same harness rather than a
//! second one.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use parking_lot::Mutex;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use url::Url;

/// One recorded announce, as the tracker saw it.
#[derive(Clone, Debug)]
pub struct Announce {
    pub query: HashMap<String, String>,
    pub at: Instant,
}

impl Announce {
    pub fn event(&self) -> Option<&str> {
        self.query.get("event").map(|s| s.as_str())
    }

    pub fn left(&self) -> u64 {
        self.numeric("left")
    }

    pub fn downloaded(&self) -> u64 {
        self.numeric("downloaded")
    }

    pub fn uploaded(&self) -> u64 {
        self.numeric("uploaded")
    }

    fn numeric(&self, key: &str) -> u64 {
        self.query
            .get(key)
            .and_then(|v| v.parse().ok())
            .unwrap_or_default()
    }
}

/// What the fake tracker answers to one announce.
pub struct Reply {
    status: &'static str,
    headers: Vec<(&'static str, String)>,
    body: Vec<u8>,
}

impl Reply {
    pub fn ok(body: impl Into<Vec<u8>>) -> Self {
        Self {
            status: "200 OK",
            headers: Vec::new(),
            body: body.into(),
        }
    }

    pub fn status(status: &'static str) -> Self {
        Self {
            status,
            headers: Vec::new(),
            body: Vec::new(),
        }
    }

    pub fn with_header(mut self, name: &'static str, value: impl Into<String>) -> Self {
        self.headers.push((name, value.into()));
        self
    }
}

/// A bencoded answer with no peers and the given intervals.
pub fn body_empty(interval: u64, min_interval: Option<u64>) -> Vec<u8> {
    let mut s = format!("d8:completei3e10:downloadedi7e10:incompletei1e8:intervali{interval}e");
    if let Some(m) = min_interval {
        s.push_str(&format!("12:min intervali{m}e"));
    }
    s.push_str("5:peers0:e");
    s.into_bytes()
}

/// A bencoded answer carrying one compact peer, 127.0.0.1:6881.
pub fn body_one_peer(interval: u64) -> Vec<u8> {
    let mut v =
        format!("d8:completei3e10:incompletei1e8:intervali{interval}e5:peers6:").into_bytes();
    v.extend_from_slice(&[127, 0, 0, 1, 0x1a, 0xe1]);
    v.extend_from_slice(b"e");
    v
}

pub fn body_failure(reason: &str) -> Vec<u8> {
    format!("d14:failure reason{}:{}e", reason.len(), reason).into_bytes()
}

pub struct FakeTracker {
    pub url: Url,
    announces: Arc<Mutex<Vec<Announce>>>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for FakeTracker {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl FakeTracker {
    /// `reply` is called with the zero-based index of the announce.
    pub async fn start(reply: impl Fn(usize) -> Reply + Send + Sync + 'static) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        // A path and a query of its own: the announce URL of a private tracker
        // carries the passkey there, and the client must preserve both.
        let url = Url::parse(&format!("http://{addr}/announce/passkey?tid=7")).unwrap();
        let announces = Arc::new(Mutex::new(Vec::new()));

        let task = tokio::spawn({
            let announces = announces.clone();
            async move {
                loop {
                    let (mut sock, _) = match listener.accept().await {
                        Ok(v) => v,
                        Err(_) => return,
                    };
                    let mut buf = Vec::new();
                    let mut chunk = [0u8; 1024];
                    // The request has no body, so the head is the whole of it.
                    let target = loop {
                        let n = match sock.read(&mut chunk).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => n,
                        };
                        buf.extend_from_slice(&chunk[..n]);
                        if let Some(end) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                            let head = String::from_utf8_lossy(&buf[..end]).to_string();
                            break head.split_whitespace().nth(1).unwrap_or("").to_string();
                        }
                    };

                    let query = target
                        .split_once('?')
                        .map(|(_, q)| q)
                        .unwrap_or("")
                        .split('&')
                        .filter_map(|kv| kv.split_once('='))
                        .map(|(k, v)| (k.to_string(), v.to_string()))
                        .collect::<HashMap<_, _>>();

                    let index = {
                        let mut g = announces.lock();
                        g.push(Announce {
                            query,
                            at: Instant::now(),
                        });
                        g.len() - 1
                    };

                    let reply = reply(index);
                    let mut out = format!(
                        "HTTP/1.1 {}\r\nContent-Length: {}\r\nConnection: close\r\n",
                        reply.status,
                        reply.body.len()
                    );
                    for (name, value) in &reply.headers {
                        out.push_str(&format!("{name}: {value}\r\n"));
                    }
                    out.push_str("\r\n");
                    let mut out = out.into_bytes();
                    out.extend_from_slice(&reply.body);
                    let _ = sock.write_all(&out).await;
                    let _ = sock.flush().await;
                }
            }
        });

        Self {
            url,
            announces,
            task,
        }
    }

    pub fn announces(&self) -> Vec<Announce> {
        self.announces.lock().clone()
    }

    pub fn count(&self) -> usize {
        self.announces.lock().len()
    }

    pub async fn wait_for(&self, n: usize, within: Duration) {
        let deadline = Instant::now() + within;
        while self.count() < n {
            assert!(
                Instant::now() < deadline,
                "only {} announces after {within:?}, wanted {n}",
                self.count()
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}
