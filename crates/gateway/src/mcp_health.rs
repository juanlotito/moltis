//! MCP health polling and auto-restart background task.

use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use tracing::{info, warn};

use crate::{
    broadcast::{BroadcastOpts, broadcast},
    mcp_service::LiveMcpService,
    state::GatewayState,
};

const POLL_INTERVAL: Duration = Duration::from_secs(30);
/// With the backoff below this spans ~25 minutes of retries, enough to outlast a
/// slow remote server booting alongside us (see `needs_restart`).
const MAX_RESTART_ATTEMPTS: u32 = 10;
const BASE_BACKOFF: Duration = Duration::from_secs(5);
const MAX_BACKOFF: Duration = Duration::from_secs(300);

struct RestartState {
    count: u32,
    last_attempt: Instant,
}

/// Whether the health loop should try to bring a server back up.
///
/// A server is recoverable when it is enabled, not connected, and not waiting on
/// the user to finish an OAuth flow in the browser. Note this deliberately does
/// NOT require the server to have been running before: a remote server whose
/// FIRST handshake fails ends up with a `Closed` client (reported as "stopped")
/// and, because nothing ever reconnects it, stays that way forever while every
/// tool call returns "not ready (state: Closed)". That is the boot race that
/// happens whenever the host reboots and the gateway dials a sibling container
/// before it listens.
///
/// `mcp.disable` is the only way to shut a server down on purpose and it clears
/// `enabled`, so "enabled but not connected" is always a failure, never intent.
fn needs_restart(state: &str, enabled: bool, awaiting_auth: bool) -> bool {
    enabled && !awaiting_auth && matches!(state, "dead" | "stopped")
}

/// Run the health monitor loop. Checks all MCP servers periodically,
/// broadcasts status changes, and auto-restarts dead servers with backoff.
pub async fn run_health_monitor(state: Arc<GatewayState>, mcp: Arc<LiveMcpService>) {
    let mut prev_states: HashMap<String, String> = HashMap::new();
    let mut restart_states: HashMap<String, RestartState> = HashMap::new();

    loop {
        tokio::time::sleep(POLL_INTERVAL).await;

        let statuses = mcp.manager().status_all().await;

        let mut changed = false;
        for s in &statuses {
            let prev = prev_states.get(&s.name).map(String::as_str);
            if prev != Some(&s.state) {
                changed = true;
            }

            // Auto-restart, evaluated on every poll (not only on state changes):
            // a server stuck in a bad state never changes state again, so gating
            // this on a transition is exactly how the cold-start failure became
            // permanent.
            {
                let awaiting_auth = s.auth_state == Some(moltis_mcp::McpAuthState::AwaitingBrowser);
                if needs_restart(&s.state, s.enabled, awaiting_auth) {
                    let rs = restart_states
                        .entry(s.name.clone())
                        .or_insert(RestartState {
                            count: 0,
                            last_attempt: Instant::now() - MAX_BACKOFF,
                        });

                    if rs.count < MAX_RESTART_ATTEMPTS {
                        let backoff = std::cmp::min(
                            BASE_BACKOFF * 2u32.saturating_pow(rs.count),
                            MAX_BACKOFF,
                        );
                        if rs.last_attempt.elapsed() >= backoff {
                            info!(
                                server = %s.name,
                                attempt = rs.count + 1,
                                "auto-restarting dead MCP server"
                            );
                            rs.count += 1;
                            rs.last_attempt = Instant::now();

                            match mcp.manager().restart_server(&s.name).await {
                                Ok(()) => {
                                    mcp.sync_tools_if_ready().await;
                                    info!(server = %s.name, "MCP server auto-restarted");
                                },
                                Err(e) => {
                                    warn!(
                                        server = %s.name,
                                        error = %e,
                                        "MCP auto-restart failed"
                                    );
                                },
                            }
                        }
                    } else if rs.count == MAX_RESTART_ATTEMPTS {
                        warn!(
                            server = %s.name,
                            "MCP server exceeded max restart attempts, giving up"
                        );
                        rs.count += 1; // prevent repeating this warning
                    }
                }

                // Reset restart counter when a server comes back to running
                if s.state == "running" {
                    restart_states.remove(&s.name);
                }
            }
            prev_states.insert(s.name.clone(), s.state.clone());
        }

        // Remove entries for servers no longer in the registry
        prev_states.retain(|name, _| statuses.iter().any(|s| &s.name == name));
        restart_states.retain(|name, _| statuses.iter().any(|s| &s.name == name));

        if changed {
            let payload = serde_json::to_value(&statuses).unwrap_or_default();
            broadcast(&state, "mcp.status", payload, BroadcastOpts::default()).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backoff_calculation() {
        // Verify backoff growth: 5, 10, 20, 40, 80 (capped at 300)
        for i in 0..MAX_RESTART_ATTEMPTS {
            let backoff = std::cmp::min(BASE_BACKOFF * 2u32.saturating_pow(i), MAX_BACKOFF);
            assert!(backoff >= BASE_BACKOFF);
            assert!(backoff <= MAX_BACKOFF);
        }
    }

    #[test]
    fn test_needs_restart_covers_cold_start() {
        // The regression this guards: a server whose first connect failed reports
        // "stopped" and must still be retried, even though it was never running.
        assert!(needs_restart("stopped", true, false));
        assert!(needs_restart("dead", true, false));
    }

    #[test]
    fn test_needs_restart_respects_intent_and_transient_states() {
        // Disabled on purpose via mcp.disable -> leave it alone.
        assert!(!needs_restart("stopped", false, false));
        // Waiting for the user to finish OAuth in the browser -> restarting would
        // throw the flow away.
        assert!(!needs_restart("dead", true, true));
        // Healthy or mid-handshake -> nothing to do.
        assert!(!needs_restart("running", true, false));
        assert!(!needs_restart("connecting", true, false));
        assert!(!needs_restart("authenticating", true, false));
    }

    #[test]
    fn test_max_backoff_cap() {
        let backoff = std::cmp::min(BASE_BACKOFF * 2u32.saturating_pow(10), MAX_BACKOFF);
        assert_eq!(backoff, MAX_BACKOFF);
    }
}
