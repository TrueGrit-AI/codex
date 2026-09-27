//! Public helpers for host-owned `AgentControl` backends.
//!
//! A host backend runs agent threads outside this process's local agent runtime, so it cannot
//! reach the crate-private helpers the local backend uses to attribute messages and report
//! results. These wrappers expose exactly those conversions, keeping model-visible text
//! identical no matter which backend delivered it.

use crate::ThreadManager;
use crate::agent::control::build_forked_history;
use crate::agent::types::AgentMessage;
use crate::agent::types::MessageDeliveryMode;
use crate::agent::types::SpawnAgentForkMode;
use crate::config::Config;
use crate::session_prefix::format_inter_agent_completion_message;
use codex_history::RolloutItem;
use codex_protocol::AgentPath;
use codex_protocol::error::Result as CodexResult;
use codex_protocol::protocol::AgentStatus;
use codex_protocol::protocol::InterAgentCommunication;
use codex_protocol::protocol::MultiAgentVersion;
use codex_protocol::protocol::SessionSource;

/// Attribute an agent message the same way local delivery does, preserving its wake mode.
pub fn agent_message_communication(
    message: AgentMessage,
    author: AgentPath,
    recipient: AgentPath,
    mode: MessageDeliveryMode,
) -> InterAgentCommunication {
    message.into_communication(author, recipient, mode)
}

/// Build the queue-only result a finished child reports to its parent, or `None` for statuses
/// the local backend does not report (running, pending, interrupted) or a root-level path.
pub fn agent_completion_communication(
    child_agent_path: &AgentPath,
    status: &AgentStatus,
) -> Option<InterAgentCommunication> {
    let parent_agent_path = parent_agent_path(child_agent_path)?;
    let message = format_inter_agent_completion_message(
        parent_agent_path.clone(),
        child_agent_path.clone(),
        status,
    )?;
    Some(InterAgentCommunication::new(
        child_agent_path.clone(),
        parent_agent_path,
        Vec::new(),
        message,
        /*trigger_turn*/ false,
    ))
}

/// The immediate parent of an agent path, or `None` for the root.
pub fn parent_agent_path(agent_path: &AgentPath) -> Option<AgentPath> {
    agent_path
        .as_str()
        .rsplit_once('/')
        .and_then(|(parent, _)| AgentPath::try_from(parent).ok())
}

/// History for a MultiAgentV2 child forked from the parent's last `last_n_turns` turns, prepared
/// exactly as a local fork prepares it, for a host backend that starts the child elsewhere.
/// The child rebuilds its own initial context, so its host's instructions and tools apply.
pub async fn remote_fork_history(
    manager: &ThreadManager,
    child_config: &Config,
    child_source: &SessionSource,
    last_n_turns: usize,
) -> CodexResult<Vec<RolloutItem>> {
    let forked = build_forked_history(
        manager.state(),
        child_config,
        child_source,
        &SpawnAgentForkMode::LastNTurns(last_n_turns),
        /*usage_hints*/ None,
        MultiAgentVersion::V2,
    )
    .await?;
    Ok(forked.items)
}
