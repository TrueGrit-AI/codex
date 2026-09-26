//! One node's side of the team: builds controllers and runs commands on local threads.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::sync::Weak;

use codex_agent_teams_wire::AgentRecord;
use codex_agent_teams_wire::HostCommand;
use codex_agent_teams_wire::HostReply;
use codex_agent_teams_wire::NodeId;
use codex_agent_teams_wire::SpawnAgent;
use codex_core::AgentControl;
use codex_core::CodexThread;
use codex_core::StartThreadOptions;
use codex_core::ThreadManager;
use codex_core::config::Config;
use codex_protocol::AgentPath;
use codex_protocol::SessionId;
use codex_protocol::ThreadId;
use codex_protocol::error::CodexErr;
use codex_protocol::error::Result;
use codex_protocol::protocol::Op;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::SubAgentSource;
use codex_protocol::protocol::ThreadSource;
use codex_protocol::protocol::TurnEnvironmentSelection;
use codex_protocol::turn_input::TurnStartOptions;

use crate::DistributedAgentControl;
use crate::TeamRelay;

/// Settings this node applies to agent threads it starts for other nodes.
struct Attachment {
    manager: Weak<ThreadManager>,
    /// This node's own configuration; spawned members never inherit the spawner's.
    base_config: Config,
    environments: Option<Vec<TurnEnvironmentSelection>>,
}

/// A node that hosts agent threads for one or more teams.
pub struct HostEndpoint {
    node_id: NodeId,
    relay: Arc<dyn TeamRelay>,
    attachment: OnceLock<Attachment>,
    /// Children this node started, mapped to their team, so the controller factory binds them
    /// to the team's identity instead of starting a new tree.
    child_teams: Mutex<HashMap<ThreadId, ThreadId>>,
}

impl HostEndpoint {
    pub fn new(node_id: NodeId, relay: Arc<dyn TeamRelay>) -> Arc<Self> {
        Arc::new(Self {
            node_id,
            relay,
            attachment: OnceLock::new(),
            child_teams: Mutex::default(),
        })
    }

    pub fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    pub(crate) fn relay(&self) -> &Arc<dyn TeamRelay> {
        &self.relay
    }

    /// Bind the thread manager this node's commands run against. Call once, after the manager
    /// is built with [`Self::install`].
    pub fn attach(
        &self,
        manager: &Arc<ThreadManager>,
        base_config: Config,
        environments: Option<Vec<TurnEnvironmentSelection>>,
    ) -> Result<()> {
        self.attachment
            .set(Attachment {
                manager: Arc::downgrade(manager),
                base_config,
                environments,
            })
            .map_err(|_| {
                CodexErr::InvalidRequest(format!("agent node {} is already attached", self.node_id))
            })
    }

    /// Install this node's controller factory on a thread manager before it is shared.
    pub fn install(self: &Arc<Self>, manager: ThreadManager) -> ThreadManager {
        let endpoint = Arc::clone(self);
        manager.with_agent_control_factory(move |thread_id| {
            let endpoint = Arc::clone(&endpoint);
            async move { endpoint.control_for_thread(thread_id).await }
        })
    }

    /// The controller for a new or cold-resumed thread. Threads this node started as members
    /// join their team; every other thread roots a new team on this node.
    async fn control_for_thread(
        self: Arc<Self>,
        thread_id: ThreadId,
    ) -> Result<Arc<dyn AgentControl>> {
        let child_team = self
            .child_teams
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&thread_id)
            .copied();
        // Roots join the directory on their first spawn, so plain threads never need a relay.
        let team_id = child_team.unwrap_or(thread_id);
        Ok(Arc::new(DistributedAgentControl::new(team_id, self)))
    }

    /// Directory entry for a team's root thread, hosted on this node.
    pub(crate) fn root_record(&self, team_id: ThreadId) -> AgentRecord {
        AgentRecord {
            team_id,
            thread_id: team_id,
            node_id: self.node_id.clone(),
            agent_path: AgentPath::root(),
            agent_nickname: None,
            agent_role: None,
            parent_thread_id: None,
            depth: 0,
        }
    }

    fn attachment(&self) -> Result<&Attachment> {
        self.attachment.get().ok_or_else(|| {
            CodexErr::UnsupportedOperation(format!(
                "agent node {} is not attached to a thread manager",
                self.node_id
            ))
        })
    }

    pub(crate) fn manager(&self) -> Result<Arc<ThreadManager>> {
        self.attachment()?.manager.upgrade().ok_or_else(|| {
            CodexErr::UnsupportedOperation(format!("agent node {} has shut down", self.node_id))
        })
    }

    async fn team_thread(
        &self,
        team_id: ThreadId,
        thread_id: ThreadId,
    ) -> Result<Arc<CodexThread>> {
        let thread = self.manager()?.get_thread(thread_id).await?;
        if thread.startup_metadata().session_id != SessionId::from(team_id) {
            return Err(CodexErr::InvalidRequest(format!(
                "thread {thread_id} does not belong to team {team_id}"
            )));
        }
        Ok(thread)
    }

    /// Run a command addressed to this node.
    pub async fn handle(&self, command: HostCommand) -> Result<HostReply> {
        match command {
            HostCommand::Spawn(spawn) => self.spawn(spawn).await,
            HostCommand::Deliver {
                team_id,
                thread_id,
                communication,
            } => {
                let thread = self.team_thread(team_id, thread_id).await?;
                let submission_id = thread
                    .submit(Op::InterAgentCommunication {
                        communication,
                        start_options: TurnStartOptions::default(),
                    })
                    .await?;
                Ok(HostReply::Delivered { submission_id })
            }
            HostCommand::Interrupt { team_id, thread_id } => {
                let thread = self.team_thread(team_id, thread_id).await?;
                let status = thread.agent_status().await;
                thread.submit(Op::Interrupt).await?;
                Ok(HostReply::Status { status })
            }
            HostCommand::Status { team_id, thread_id } => {
                let thread = self.team_thread(team_id, thread_id).await?;
                Ok(HostReply::Status {
                    status: thread.agent_status().await,
                })
            }
        }
    }

    async fn spawn(&self, spawn: SpawnAgent) -> Result<HostReply> {
        let attachment = self.attachment()?;
        let manager = self.manager()?;
        let mut config = attachment.base_config.clone();
        if let Some(model) = spawn.model {
            config.model = Some(model);
        }
        if let Some(effort) = spawn.reasoning_effort {
            config.model_reasoning_effort = Some(effort);
        }
        self.child_teams
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(spawn.thread_id, spawn.team_id);
        let started = manager
            .start_thread(StartThreadOptions {
                reserved_thread_id: Some(spawn.thread_id),
                environments: attachment.environments.clone(),
                session_source: Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                    parent_thread_id: spawn.parent_thread_id,
                    depth: spawn.depth,
                    agent_path: Some(spawn.agent_path),
                    agent_nickname: spawn.agent_nickname,
                    agent_role: spawn.agent_role,
                })),
                thread_source: Some(ThreadSource::Subagent),
                ..StartThreadOptions::new(config)
            })
            .await;
        let started = match started {
            Ok(started) => started,
            Err(err) => {
                self.child_teams
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .remove(&spawn.thread_id);
                return Err(err);
            }
        };
        // Like a locally spawned agent: listeners attach, status is tracked, viewers can resume it.
        manager.notify_thread_created(started.thread_id);
        started
            .thread
            .submit(Op::InterAgentCommunication {
                communication: spawn.initial_message,
                start_options: TurnStartOptions::default(),
            })
            .await?;
        Ok(HostReply::Spawned {
            status: started.thread.agent_status().await,
        })
    }
}
