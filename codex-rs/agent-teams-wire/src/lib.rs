//! Serializable messages between distributed agent-team nodes.
//!
//! A team is one agent tree whose threads may live in different app-server processes, possibly
//! on different machines. These types carry only data that can cross a process boundary; live
//! handles such as `Config` stay on the node that owns the thread. Every node rebuilds its
//! thread settings from its own configuration, so skills, MCP servers and credentials never
//! travel.

use codex_protocol::AgentPath;
use codex_protocol::ThreadId;
use codex_protocol::openai_models::ReasoningEffort;
use codex_protocol::protocol::AgentStatus;
use codex_protocol::protocol::InterAgentCommunication;
use serde::Deserialize;
use serde::Serialize;

/// Wire protocol version; bump on incompatible changes.
pub const PROTOCOL_VERSION: u32 = 1;

/// A process hosting agent threads, usually one app-server on one machine.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct NodeId(pub String);

impl NodeId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// What a node offers the team: the roles it serves and how many agents it can run at once.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeAdvertisement {
    pub node_id: NodeId,
    /// Role names this node serves. An empty list serves only unassigned (`None`) roles.
    pub roles: Vec<String>,
    pub max_concurrent_agents: usize,
}

/// One member of a team as every node sees it. The team's root thread has path `/root`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentRecord {
    /// The root thread's ID; also the session identity of every thread in the tree.
    pub team_id: ThreadId,
    pub thread_id: ThreadId,
    pub node_id: NodeId,
    pub agent_path: AgentPath,
    pub agent_nickname: Option<String>,
    pub agent_role: Option<String>,
    pub parent_thread_id: Option<ThreadId>,
    pub depth: i32,
}

/// Start a child thread on the receiving node.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SpawnAgent {
    pub team_id: ThreadId,
    /// Reserved by the spawning node so the child's identity is known before it starts.
    pub thread_id: ThreadId,
    pub parent_thread_id: ThreadId,
    pub depth: i32,
    pub agent_path: AgentPath,
    pub agent_nickname: Option<String>,
    pub agent_role: Option<String>,
    /// Model and effort the spawning tool asked for; `None` keeps the receiving node's default.
    pub model: Option<String>,
    pub reasoning_effort: Option<ReasoningEffort>,
    /// The attributed first message; it always starts the child's first turn.
    pub initial_message: InterAgentCommunication,
}

/// Commands a node accepts for threads it hosts.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum HostCommand {
    Spawn(SpawnAgent),
    /// Deliver attributed input; `communication.trigger_turn` keeps the sender's wake mode.
    Deliver {
        team_id: ThreadId,
        thread_id: ThreadId,
        communication: InterAgentCommunication,
    },
    Interrupt {
        team_id: ThreadId,
        thread_id: ThreadId,
    },
    Status {
        team_id: ThreadId,
        thread_id: ThreadId,
    },
}

impl HostCommand {
    pub fn team_id(&self) -> ThreadId {
        match self {
            Self::Spawn(spawn) => spawn.team_id,
            Self::Deliver { team_id, .. }
            | Self::Interrupt { team_id, .. }
            | Self::Status { team_id, .. } => *team_id,
        }
    }

    pub fn thread_id(&self) -> ThreadId {
        match self {
            Self::Spawn(spawn) => spawn.thread_id,
            Self::Deliver { thread_id, .. }
            | Self::Interrupt { thread_id, .. }
            | Self::Status { thread_id, .. } => *thread_id,
        }
    }
}

/// A node's answer. Acceptance means the input was queued, not that the model has read it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum HostReply {
    Spawned { status: AgentStatus },
    Delivered { submission_id: String },
    Status { status: AgentStatus },
}

/// Calls a node makes to the team relay. Mirrors the `TeamRelay` operations one to one.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RelayRequest {
    Place {
        team_id: ThreadId,
        role: Option<String>,
        spawning_node: NodeId,
    },
    Dispatch {
        node_id: NodeId,
        command: Box<HostCommandEnvelope>,
    },
    Register {
        record: AgentRecord,
    },
    Unregister {
        team_id: ThreadId,
        thread_id: ThreadId,
    },
    Agents {
        team_id: ThreadId,
    },
}

/// The relay's answers, one variant per request kind. Failures travel as transport errors.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RelayResponse {
    Placed { node_id: NodeId },
    Dispatched { reply: HostReply },
    Done,
    Agents { records: Vec<AgentRecord> },
}

/// Versioned envelope for transports that carry `HostCommand`s between processes.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HostCommandEnvelope {
    pub protocol_version: u32,
    /// Idempotency key: a node applies each command at most once.
    pub command_id: String,
    pub target_node_id: NodeId,
    pub command: HostCommand,
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn host_commands_round_trip_as_tagged_json() {
        let team_id = ThreadId::new();
        let thread_id = ThreadId::new();
        let command = HostCommand::Spawn(SpawnAgent {
            team_id,
            thread_id,
            parent_thread_id: team_id,
            depth: 1,
            agent_path: AgentPath::root().join("mail").expect("valid path"),
            agent_nickname: Some("mail".to_string()),
            agent_role: Some("mail".to_string()),
            model: None,
            reasoning_effort: None,
            initial_message: InterAgentCommunication::new(
                AgentPath::root(),
                AgentPath::root().join("mail").expect("valid path"),
                Vec::new(),
                "List today's events.".to_string(),
                /*trigger_turn*/ true,
            ),
        });
        let envelope = HostCommandEnvelope {
            protocol_version: PROTOCOL_VERSION,
            command_id: "c1".to_string(),
            target_node_id: NodeId::new("lightsail"),
            command,
        };
        let json = serde_json::to_value(&envelope).expect("serialize");
        assert_eq!(json["command"]["type"], "spawn");
        assert_eq!(json["target_node_id"], "lightsail");
        let decoded: HostCommandEnvelope = serde_json::from_value(json).expect("deserialize");
        assert_eq!(decoded, envelope);
        assert_eq!(
            (decoded.command.team_id(), decoded.command.thread_id()),
            (team_id, thread_id)
        );
    }
}
