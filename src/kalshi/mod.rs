//! Kalshi exchange adapter: authentication, wire protocol, WebSocket
//! connections, market discovery and subscription management.

pub mod auth;
pub mod connection;
pub mod discovery;
pub mod manager;
pub mod protocol;

pub const PROD_WS_URL: &str = "wss://external-api-ws.kalshi.com/trade-api/ws/v2";
pub const PROD_REST_URL: &str = "https://external-api.kalshi.com/trade-api/v2";
