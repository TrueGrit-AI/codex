//! Distributed `AgentControl` backend for MultiAgentV2 agent teams.
//!
//! Each process that hosts agent threads runs one [`HostEndpoint`]. Its factory, installed with
//! `ThreadManager::with_agent_control_factory`, gives every thread a [`DistributedAgentControl`]
//! bound to the thread's team. Team operations go through a [`TeamRelay`], which places new
//! agents on nodes by role, keeps the team directory and dispatches [`HostCommand`]s to the node
//! that owns each thread. The lead sees ordinary collaboration tools; whether a member runs in
//! the same process or on another machine is the relay's decision.
//!
//! [`HostCommand`]: codex_agent_teams_wire::HostCommand

mod control;
mod endpoint;
mod relay;

pub use control::DistributedAgentControl;
pub use endpoint::HostEndpoint;
pub use relay::InMemoryRelay;
pub use relay::TeamRelay;
