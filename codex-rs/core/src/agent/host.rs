//! Public helpers for host-owned `AgentControl` backends.
//!
//! A host backend runs agent threads outside this process's local agent runtime, so it cannot
//! reach the crate-private helpers the local backend uses to attribute messages and report
//! results. These wrappers expose exactly those conversions, keeping model-visible text
//! identical no matter which backend delivered it.

use crate::agent::types::AgentMessage;
use crate::agent::types::MessageDeliveryMode;
use crate::session_prefix::format_inter_agent_completion_message;
use codex_protocol::AgentPath;
use codex_protocol::protocol::AgentStatus;
use codex_protocol::protocol::InterAgentCommunication;

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
