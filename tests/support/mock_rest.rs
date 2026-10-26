//! Minimal HTTP/1.1 server answering `GET /trade-api/v2/markets` with a
//! configurable market list (single page), and recording each query string.

#![allow(dead_code)]

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

#[derive(Default)]
pub struct RestState {
    pub markets: Vec<Value>,
    pub queries: Vec<String>,
}

pub struct MockRest {
    pub addr: SocketAddr,
    pub state: Arc<Mutex<RestState>>,
}

pub fn market(ticker: &str, status: &str, mve_collection: Option<&str>) -> Value {
    json!({
        "ticker": ticker,
        "status": status,
        "close_time": "2099-01-01T00:00:00Z",
        "mve_collection_ticker": mve_collection,
    })
}

impl MockRest {
    pub async fn start(markets: Vec<Value>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let state = Arc::new(Mutex::new(RestState {
            markets,
            queries: Vec::new(),
        }));
        let server_state = state.clone();
        tokio::spawn(async move {
            while let Ok((mut tcp, _)) = listener.accept().await {
                let state = server_state.clone();
                tokio::spawn(async move {
                    let mut buf = Vec::new();
                    let mut chunk = [0u8; 4096];
                    while !buf.windows(4).any(|w| w == b"\r\n\r\n") {
                        match tcp.read(&mut chunk).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => buf.extend_from_slice(&chunk[..n]),
                        }
                    }
                    let head = String::from_utf8_lossy(&buf).to_string();
                    let target = head.split_whitespace().nth(1).unwrap_or("").to_owned();
                    let (path, query) = target.split_once('?').unwrap_or((&target, ""));
                    let (status, body) = if path == "/trade-api/v2/markets" {
                        let mut st = state.lock().unwrap();
                        st.queries.push(query.to_owned());
                        (
                            "200 OK",
                            json!({ "markets": st.markets, "cursor": "" }).to_string(),
                        )
                    } else {
                        ("404 Not Found", "{}".to_owned())
                    };
                    let response = format!(
                        "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = tcp.write_all(response.as_bytes()).await;
                    let _ = tcp.shutdown().await;
                });
            }
        });
        Self { addr, state }
    }

    pub fn url(&self) -> String {
        format!("http://{}/trade-api/v2", self.addr)
    }

    pub fn set_markets(&self, markets: Vec<Value>) {
        self.state.lock().unwrap().markets = markets;
    }
}
