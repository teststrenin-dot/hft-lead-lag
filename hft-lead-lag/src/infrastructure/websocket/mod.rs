//! WebSocket infrastructure utilities
//! 
//! Provides low-level WebSocket handling with:
//! - Auto-reconnection with exponential backoff
//! - Heartbeat/ping-pong handling
//! - Message buffering

use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::{Message, protocol::frame::coding::CloseCode};
use tracing::debug;

/// WebSocket lifecycle states
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WsState {
    Disconnected,
    Connecting,
    Connected,
    Reconnecting(u64),
    Closing,
}

/// WebSocket configuration
#[derive(Debug, Clone)]
pub struct WsConfig {
    /// Initial reconnect delay in milliseconds
    pub initial_reconnect_delay_ms: u64,
    /// Maximum reconnect delay in milliseconds
    pub max_reconnect_delay_ms: u64,
    /// Reconnect delay multiplier
    pub reconnect_multiplier: f64,
    /// Ping interval in seconds
    pub ping_interval_sec: u64,
    /// Connection timeout in milliseconds
    pub connection_timeout_ms: u64,
}

impl Default for WsConfig {
    fn default() -> Self {
        Self {
            initial_reconnect_delay_ms: 100,
            max_reconnect_delay_ms: 30000,
            reconnect_multiplier: 2.0,
            ping_interval_sec: 30,
            connection_timeout_ms: 10000,
        }
    }
}

/// WebSocket event for external observers
#[derive(Debug, Clone)]
pub enum WsEvent {
    Connected,
    Disconnected,
    Reconnecting { delay_ms: u64 },
    MessageReceived { size: usize },
    Error { message: String },
}

/// WebSocket manager for handling connection lifecycle
#[allow(dead_code)]
pub struct WsManager {
    config: WsConfig,
    state: WsState,
    event_tx: Option<mpsc::UnboundedSender<WsEvent>>,
}

impl WsManager {
    pub fn new(config: WsConfig) -> Self {
        Self {
            config,
            state: WsState::Disconnected,
            event_tx: None,
        }
    }

    pub fn with_event_channel(tx: mpsc::UnboundedSender<WsEvent>) -> Self {
        Self {
            config: WsConfig::default(),
            state: WsState::Disconnected,
            event_tx: Some(tx),
        }
    }

    pub fn state(&self) -> WsState {
        self.state
    }

    pub fn is_connected(&self) -> bool {
        self.state == WsState::Connected
    }

    #[allow(dead_code)]
    fn set_state(&mut self, new_state: WsState) {
        debug!("WebSocket state change: {:?} -> {:?}", self.state, new_state);
        self.state = new_state;

        if let Some(tx) = &self.event_tx {
            let event = match &new_state {
                WsState::Connected => WsEvent::Connected,
                WsState::Disconnected => WsEvent::Disconnected,
                WsState::Reconnecting(delay_ms) => WsEvent::Reconnecting { delay_ms: *delay_ms },
                _ => return,
            };
            let _ = tx.send(event);
        }
    }

    /// Calculate next reconnect delay with exponential backoff
    pub fn next_reconnect_delay(&self, current_delay_ms: u64) -> u64 {
        let next = (current_delay_ms as f64 * self.config.reconnect_multiplier) as u64;
        next.min(self.config.max_reconnect_delay_ms)
    }

    /// Create close message for graceful shutdown
    pub fn close_message() -> Message {
        Message::Close(Some(tokio_tungstenite::tungstenite::protocol::CloseFrame {
            code: CloseCode::Normal,
            reason: "Client closing".into(),
        }))
    }

    /// Check if message is a ping
    pub fn is_ping(msg: &Message) -> bool {
        matches!(msg, Message::Ping(_))
    }

    /// Check if message is a pong
    pub fn is_pong(msg: &Message) -> bool {
        matches!(msg, Message::Pong(_))
    }

    /// Check if message is a close frame
    pub fn is_close(msg: &Message) -> bool {
        matches!(msg, Message::Close(_))
    }

    /// Extract text from message
    pub fn as_text(msg: &Message) -> Option<&str> {
        match msg {
            Message::Text(s) => Some(s),
            _ => None,
        }
    }

    /// Extract bytes from message
    pub fn as_bytes(msg: &Message) -> Option<&[u8]> {
        match msg {
            Message::Text(s) => Some(s.as_bytes()),
            Message::Binary(b) => Some(b),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_exponential_backoff() {
        let manager = WsManager::new(WsConfig::default());
        
        let mut delay = 100u64;
        delay = manager.next_reconnect_delay(delay);
        assert_eq!(delay, 200);
        
        delay = manager.next_reconnect_delay(delay);
        assert_eq!(delay, 400);
    }
}
