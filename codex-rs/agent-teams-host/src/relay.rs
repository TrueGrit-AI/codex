//! Team coordination: placement, the team directory and command dispatch.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;

use codex_agent_teams_wire::AgentRecord;
use codex_agent_teams_wire::HostCommand;
use codex_agent_teams_wire::HostReply;
use codex_agent_teams_wire::NodeAdvertisement;
use codex_agent_teams_wire::NodeId;
use codex_protocol::ThreadId;
use codex_protocol::error::CodexErr;
use codex_protocol::error::Result;
use futures::future::BoxFuture;

use crate::HostEndpoint;

/// The coordination service every node talks to. A networked relay implements this over its
/// command log; [`InMemoryRelay`] implements it within one process.
pub trait TeamRelay: Send + Sync {
    /// Choose the node for a new agent. Role-less agents stay on the spawning node; a role
    /// goes to the least loaded node that serves it, falling back to the spawning node.
    fn place<'a>(
        &'a self,
        team_id: ThreadId,
        role: Option<&'a str>,
        spawning_node: &'a NodeId,
    ) -> BoxFuture<'a, Result<NodeId>>;

    /// Run a command on the node that hosts its thread.
    fn dispatch<'a>(
        &'a self,
        node_id: &'a NodeId,
        command: HostCommand,
    ) -> BoxFuture<'a, Result<HostReply>>;

    /// Add or replace a directory entry, keyed by thread.
    fn register(&self, record: AgentRecord) -> BoxFuture<'_, Result<()>>;

    /// Remove a directory entry, e.g. after its node refused the spawn.
    fn unregister(&self, team_id: ThreadId, thread_id: ThreadId) -> BoxFuture<'_, Result<()>>;

    /// Every known member of a team, in registration order.
    fn agents(&self, team_id: ThreadId) -> BoxFuture<'_, Result<Vec<AgentRecord>>>;
}

struct NodeEntry {
    advertisement: NodeAdvertisement,
    endpoint: Weak<HostEndpoint>,
}

#[derive(Default)]
struct InMemoryState {
    nodes: BTreeMap<NodeId, NodeEntry>,
    agents: Vec<AgentRecord>,
}

/// A relay whose nodes are endpoints in the same process. Used for tests and for single-host
/// deployments; it applies the same placement and routing rules as a networked relay.
#[derive(Default)]
pub struct InMemoryRelay {
    state: Mutex<InMemoryState>,
}

impl InMemoryRelay {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Make an endpoint reachable and advertise what it serves.
    pub fn connect(&self, advertisement: NodeAdvertisement, endpoint: &Arc<HostEndpoint>) {
        let mut state = self.lock();
        state.nodes.insert(
            advertisement.node_id.clone(),
            NodeEntry {
                advertisement,
                endpoint: Arc::downgrade(endpoint),
            },
        );
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, InMemoryState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl TeamRelay for InMemoryRelay {
    fn place<'a>(
        &'a self,
        _team_id: ThreadId,
        role: Option<&'a str>,
        spawning_node: &'a NodeId,
    ) -> BoxFuture<'a, Result<NodeId>> {
        Box::pin(async move {
            let Some(role) = role else {
                return Ok(spawning_node.clone());
            };
            let state = self.lock();
            let load = |node_id: &NodeId| {
                state
                    .agents
                    .iter()
                    .filter(|record| &record.node_id == node_id && record.depth > 0)
                    .count()
            };
            let chosen = state
                .nodes
                .values()
                .filter(|entry| {
                    entry
                        .advertisement
                        .roles
                        .iter()
                        .any(|served| served == role)
                        && entry.endpoint.strong_count() > 0
                        && load(&entry.advertisement.node_id)
                            < entry.advertisement.max_concurrent_agents
                })
                .min_by_key(|entry| load(&entry.advertisement.node_id))
                .map(|entry| entry.advertisement.node_id.clone());
            Ok(chosen.unwrap_or_else(|| {
                tracing::warn!(
                    "no connected node serves role `{role}`; placing on {spawning_node}"
                );
                spawning_node.clone()
            }))
        })
    }

    fn dispatch<'a>(
        &'a self,
        node_id: &'a NodeId,
        command: HostCommand,
    ) -> BoxFuture<'a, Result<HostReply>> {
        Box::pin(async move {
            let endpoint = self
                .lock()
                .nodes
                .get(node_id)
                .and_then(|entry| entry.endpoint.upgrade())
                .ok_or_else(|| {
                    CodexErr::UnsupportedOperation(format!("agent node {node_id} is not connected"))
                })?;
            endpoint.handle(command).await
        })
    }

    fn register(&self, record: AgentRecord) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            let mut state = self.lock();
            state
                .agents
                .retain(|existing| existing.thread_id != record.thread_id);
            state.agents.push(record);
            Ok(())
        })
    }

    fn unregister(&self, team_id: ThreadId, thread_id: ThreadId) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            self.lock()
                .agents
                .retain(|record| !(record.team_id == team_id && record.thread_id == thread_id));
            Ok(())
        })
    }

    fn agents(&self, team_id: ThreadId) -> BoxFuture<'_, Result<Vec<AgentRecord>>> {
        Box::pin(async move {
            Ok(self
                .lock()
                .agents
                .iter()
                .filter(|record| record.team_id == team_id)
                .cloned()
                .collect())
        })
    }
}
