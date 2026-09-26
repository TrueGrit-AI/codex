use super::invalid_request;
use codex_app_server_protocol::JSONRPCErrorError;
use codex_core::CodexThread;
use codex_protocol::protocol::MultiAgentVersion;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::SubAgentSource;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

pub(super) const DIRECT_INPUT_TO_MULTI_AGENT_V2_SUBAGENT_ERROR: &str =
    "direct app-server input is not allowed for multi-agent v2 sub-agents";

/// Mirrors the ownership policy in request validation and thread capability responses.
pub(super) fn can_accept_direct_input(
    multi_agent_version: Option<MultiAgentVersion>,
    session_source: &SessionSource,
) -> bool {
    multi_agent_version != Some(MultiAgentVersion::V2)
        || !matches!(
            session_source,
            SessionSource::SubAgent(SubAgentSource::ThreadSpawn { .. })
        )
}

/// Set once when this server is a distributed agent-team node; see
/// [`allow_operator_input_for_subagents`].
static OPERATOR_INPUT_FOR_SUBAGENTS: AtomicBool = AtomicBool::new(false);

/// Let clients type into loaded MultiAgentV2 sub-agents. Distributed agent-team nodes host
/// members for leads on other machines; the operator steers them from a live viewer. Ownership
/// rules for resuming and rebuilding sub-agents are unchanged.
pub(crate) fn allow_operator_input_for_subagents() {
    OPERATOR_INPUT_FOR_SUBAGENTS.store(true, Ordering::Relaxed);
}

/// Whether a client may send input to a loaded thread: its ownership policy, relaxed for
/// agent-team members when this node allows operator input.
pub(super) fn accepts_operator_input(
    multi_agent_version: Option<MultiAgentVersion>,
    session_source: &SessionSource,
) -> bool {
    OPERATOR_INPUT_FOR_SUBAGENTS.load(Ordering::Relaxed)
        || can_accept_direct_input(multi_agent_version, session_source)
}

pub(super) async fn ensure_direct_input_allowed(
    thread: &CodexThread,
) -> Result<(), JSONRPCErrorError> {
    let config_snapshot = thread.config_snapshot().await;
    if !accepts_operator_input(
        thread.multi_agent_version(),
        &config_snapshot.session_source,
    ) {
        return Err(invalid_request(
            DIRECT_INPUT_TO_MULTI_AGENT_V2_SUBAGENT_ERROR,
        ));
    }

    Ok(())
}
