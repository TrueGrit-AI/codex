//! The per-thread controller that routes collaboration tools through the team relay.

use std::sync::Arc;
use std::sync::Mutex;

use codex_agent_teams_wire::AgentRecord;
use codex_agent_teams_wire::HostCommand;
use codex_agent_teams_wire::HostReply;
use codex_agent_teams_wire::MAX_FORK_HISTORY_BYTES;
use codex_agent_teams_wire::SpawnAgent;
use codex_core::AgentConfigUpdate;
use codex_core::AgentControl;
use codex_core::AgentExecutionGuard;
use codex_core::AgentInfo;
use codex_core::AgentInput;
use codex_core::AgentMetadata;
use codex_core::AgentTarget;
use codex_core::AgentTurnOutcome;
use codex_core::DeliveryReceipt;
use codex_core::GuardianRootSnapshot;
use codex_core::LiveAgent;
use codex_core::MessageDeliveryMode;
use codex_core::RolloutBudgetReminder;
use codex_core::SendRequest;
use codex_core::SpawnAgentForkMode;
use codex_core::SpawnRequest;
use codex_core::ThreadConfigSnapshot;
use codex_core::agent_host::agent_completion_communication;
use codex_core::agent_host::agent_message_communication;
use codex_core::agent_host::parent_agent_path;
use codex_core::agent_host::remote_fork_history;
use codex_history::RolloutItem;
use codex_protocol::AgentPath;
use codex_protocol::SessionId;
use codex_protocol::ThreadId;
use codex_protocol::error::CodexErr;
use codex_protocol::error::Result;
use codex_protocol::protocol::AgentStatus;
use codex_protocol::protocol::MultiAgentVersion;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::SubAgentSource;
use codex_protocol::protocol::ThreadSource;
use codex_protocol::protocol::TokenUsage;
use codex_rollout_trace::ThreadTraceContext;
use futures::future::BoxFuture;
use tracing::debug;
use tracing::warn;

use crate::HostEndpoint;

/// Controller for one thread of a distributed team.
///
/// The thread runs on this controller's node; the agents it spawns or messages may run on any
/// node. Every controller in a team reports the root thread's ID as its identity, which is the
/// session identity Core requires for all threads in one tree.
pub struct DistributedAgentControl {
    team_id: ThreadId,
    /// The thread this controller serves; usage is reported for it.
    thread_id: ThreadId,
    endpoint: Arc<HostEndpoint>,
    service_tier: Mutex<Option<String>>,
}

impl DistributedAgentControl {
    pub(crate) fn new(team_id: ThreadId, thread_id: ThreadId, endpoint: Arc<HostEndpoint>) -> Self {
        Self {
            team_id,
            thread_id,
            endpoint,
            service_tier: Mutex::default(),
        }
    }

    pub fn team_id(&self) -> ThreadId {
        self.team_id
    }

    async fn directory(&self) -> Result<Vec<AgentRecord>> {
        self.endpoint.relay().agents(self.team_id).await
    }

    async fn record_for_thread(&self, thread_id: ThreadId) -> Result<AgentRecord> {
        self.directory()
            .await?
            .into_iter()
            .find(|record| record.thread_id == thread_id)
            .ok_or_else(|| {
                CodexErr::UnsupportedOperation(format!("agent {thread_id} is not in this team"))
            })
    }

    async fn caller_path(&self, caller: ThreadId) -> AgentPath {
        self.record_for_thread(caller)
            .await
            .map(|record| record.agent_path)
            .unwrap_or_else(|_| AgentPath::root())
    }

    async fn resolve_reference(&self, current: &AgentPath, reference: &str) -> Result<ThreadId> {
        if let Ok(thread_id) = ThreadId::from_string(reference) {
            return Ok(thread_id);
        }
        let agent_path = current
            .resolve(reference)
            .map_err(CodexErr::UnsupportedOperation)?;
        self.directory()
            .await?
            .into_iter()
            .find(|record| record.agent_path == agent_path)
            .map(|record| record.thread_id)
            .ok_or_else(|| {
                CodexErr::UnsupportedOperation(format!(
                    "live agent path `{}` not found",
                    agent_path.as_str()
                ))
            })
    }

    async fn resolve_target(&self, caller: ThreadId, target: &AgentTarget) -> Result<AgentRecord> {
        let thread_id = match target {
            AgentTarget::Id(thread_id) => *thread_id,
            AgentTarget::Reference(reference) => {
                let current = self.caller_path(caller).await;
                self.resolve_reference(&current, reference).await?
            }
        };
        self.record_for_thread(thread_id).await
    }

    async fn dispatch(&self, record: &AgentRecord, command: HostCommand) -> Result<HostReply> {
        if &record.node_id == self.endpoint.node_id() {
            return self.endpoint.handle(command).await;
        }
        self.endpoint
            .relay()
            .dispatch(&record.node_id, command)
            .await
    }

    async fn status(&self, record: &AgentRecord) -> AgentStatus {
        let command = HostCommand::Status {
            team_id: self.team_id,
            thread_id: record.thread_id,
        };
        match self.dispatch(record, command).await {
            Ok(HostReply::Status { status }) => status,
            Ok(other) => {
                warn!(
                    "unexpected status reply for {}: {other:?}",
                    record.thread_id
                );
                AgentStatus::NotFound
            }
            Err(err) => {
                debug!("status query for {} failed: {err}", record.thread_id);
                AgentStatus::NotFound
            }
        }
    }

    async fn caller_snapshot(&self, caller: ThreadId) -> Result<ThreadConfigSnapshot> {
        Ok(self
            .endpoint
            .manager()?
            .get_thread(caller)
            .await?
            .config_snapshot()
            .await)
    }

    /// Settings reported for a member. The member's node owns its real configuration, so the
    /// snapshot starts from the caller's and carries the member's identity and requested model.
    async fn member_snapshot(
        &self,
        caller: ThreadId,
        record: &AgentRecord,
        model: Option<String>,
    ) -> Result<ThreadConfigSnapshot> {
        let mut snapshot = self.caller_snapshot(caller).await?;
        if let Some(model) = model {
            snapshot.model = model;
        }
        snapshot.session_source = SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
            parent_thread_id: record.parent_thread_id.unwrap_or(self.team_id),
            depth: record.depth,
            agent_path: Some(record.agent_path.clone()),
            agent_nickname: record.agent_nickname.clone(),
            agent_role: record.agent_role.clone(),
        });
        snapshot.parent_thread_id = record.parent_thread_id;
        snapshot.forked_from_thread_id = None;
        snapshot.thread_source = Some(ThreadSource::Subagent);
        Ok(snapshot)
    }
}

fn metadata(record: &AgentRecord) -> AgentMetadata {
    AgentMetadata {
        agent_id: Some(record.thread_id),
        agent_path: Some(record.agent_path.clone()),
        agent_nickname: record.agent_nickname.clone(),
        agent_role: record.agent_role.clone(),
    }
}

fn live_agent(record: &AgentRecord, status: AgentStatus) -> LiveAgent {
    LiveAgent {
        thread_id: record.thread_id,
        metadata: metadata(record),
        status,
    }
}

/// Serialize a forked history for another node, refusing one over the cap with an error the
/// spawning model can act on.
fn serialize_fork_history(items: &[RolloutItem], turns: usize) -> Result<Vec<serde_json::Value>> {
    let values = items
        .iter()
        .map(serde_json::to_value)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|err| CodexErr::Fatal(format!("cannot serialize forked history: {err}")))?;
    let bytes = serde_json::to_vec(&values)
        .map_err(|err| CodexErr::Fatal(format!("cannot serialize forked history: {err}")))?
        .len();
    if bytes > MAX_FORK_HISTORY_BYTES {
        return Err(CodexErr::InvalidRequest(format!(
            "fork_turns={turns} needs {} KiB of history, over the {} KiB limit for agents that may run on another machine; spawn with fewer turns or with fork_turns=\"none\" and put the needed context in the message",
            bytes.div_ceil(1024),
            MAX_FORK_HISTORY_BYTES / 1024
        )));
    }
    Ok(values)
}

fn agent_message(input: AgentInput) -> Result<(codex_core::AgentMessage, MessageDeliveryMode)> {
    match input {
        AgentInput::Message { message, mode } => Ok((message, mode)),
        AgentInput::UserInput(_) => Err(CodexErr::UnsupportedOperation(
            "distributed agents accept agent messages only; enable MultiAgentV2".to_string(),
        )),
    }
}

impl AgentControl for DistributedAgentControl {
    fn identity(&self) -> SessionId {
        SessionId::from(self.team_id)
    }

    fn resolve<'a>(
        &'a self,
        _caller: ThreadId,
        _parent: Option<ThreadId>,
        source: &'a SessionSource,
        target: &'a str,
    ) -> BoxFuture<'a, Result<ThreadId>> {
        Box::pin(async move {
            let current = source.get_agent_path().unwrap_or_else(AgentPath::root);
            self.resolve_reference(&current, target).await
        })
    }

    fn spawn(
        &self,
        request: SpawnRequest,
    ) -> BoxFuture<'_, Result<(LiveAgent, ThreadConfigSnapshot)>> {
        Box::pin(async move {
            let SpawnRequest {
                caller,
                config,
                input,
                source,
                options,
            } = request;
            let child_source = source.clone();
            let SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id,
                depth,
                agent_path: Some(agent_path),
                agent_nickname,
                agent_role,
            }) = source
            else {
                return Err(CodexErr::InvalidRequest(
                    "distributed spawn requires a MultiAgentV2 task path".to_string(),
                ));
            };
            let (message, mode) = agent_message(input)?;
            if mode != MessageDeliveryMode::TriggerTurn {
                return Err(CodexErr::InvalidRequest(
                    "spawn input must start the child turn".to_string(),
                ));
            }
            // D8: members start fresh unless the spawn asked for the parent's last turns and
            // those fit the cap; the full history never leaves this machine.
            let history = match options.fork_mode {
                None => None,
                Some(SpawnAgentForkMode::FullHistory) => {
                    warn!(
                        "distributed spawn of {} does not fork full history; the member starts fresh",
                        agent_path.as_str()
                    );
                    None
                }
                Some(SpawnAgentForkMode::LastNTurns(turns)) => {
                    let items = remote_fork_history(
                        self.endpoint.manager()?.as_ref(),
                        &config,
                        &child_source,
                        turns,
                    )
                    .await?;
                    Some(serialize_fork_history(&items, turns)?)
                }
            };
            // The member's node owns its settings. Forward only what this spawn changed from
            // the caller's own settings, e.g. an explicit model or a role's override.
            let caller_settings = self.caller_snapshot(caller).await?;
            let requested_model = config
                .model
                .clone()
                .filter(|model| *model != caller_settings.model);
            let requested_effort = config
                .model_reasoning_effort
                .filter(|effort| caller_settings.reasoning_effort.as_ref() != Some(effort));
            let relay = self.endpoint.relay();
            if caller == self.team_id {
                relay
                    .register(self.endpoint.root_record(self.team_id))
                    .await?;
            }
            let node_id = relay
                .place(self.team_id, agent_role.as_deref(), self.endpoint.node_id())
                .await?;
            let record = AgentRecord {
                team_id: self.team_id,
                thread_id: ThreadId::new(),
                node_id,
                agent_nickname: agent_nickname.or_else(|| Some(agent_path.name().to_string())),
                agent_path: agent_path.clone(),
                agent_role,
                parent_thread_id: Some(parent_thread_id),
                depth,
            };
            // Register first so the member can resolve its team as soon as its turn starts.
            relay.register(record.clone()).await?;
            let author = parent_agent_path(&agent_path).unwrap_or_else(AgentPath::root);
            let spawn = SpawnAgent {
                team_id: self.team_id,
                thread_id: record.thread_id,
                parent_thread_id,
                depth,
                agent_path: agent_path.clone(),
                agent_nickname: record.agent_nickname.clone(),
                agent_role: record.agent_role.clone(),
                model: requested_model.clone(),
                reasoning_effort: requested_effort,
                initial_message: agent_message_communication(
                    message,
                    author,
                    agent_path,
                    MessageDeliveryMode::TriggerTurn,
                ),
                history,
            };
            let status = match self.dispatch(&record, HostCommand::Spawn(spawn)).await {
                Ok(HostReply::Spawned { status }) => status,
                Ok(other) => {
                    relay.unregister(self.team_id, record.thread_id).await?;
                    return Err(CodexErr::UnsupportedOperation(format!(
                        "unexpected spawn reply: {other:?}"
                    )));
                }
                Err(err) => {
                    relay.unregister(self.team_id, record.thread_id).await?;
                    return Err(err);
                }
            };
            let snapshot = self
                .member_snapshot(caller, &record, requested_model)
                .await?;
            Ok((live_agent(&record, status), snapshot))
        })
    }

    fn send(&self, request: SendRequest) -> BoxFuture<'_, Result<DeliveryReceipt>> {
        Box::pin(async move {
            let SendRequest {
                caller,
                target,
                input,
                ..
            } = request;
            let record = self.resolve_target(caller, &target).await?;
            let (message, mode) = agent_message(input)?;
            if mode == MessageDeliveryMode::TriggerTurn && record.agent_path.is_root() {
                return Err(CodexErr::UnsupportedOperation(
                    "Follow-up tasks can't target the root agent".to_string(),
                ));
            }
            let author = self.caller_path(caller).await;
            let communication =
                agent_message_communication(message, author, record.agent_path.clone(), mode);
            let reply = self
                .dispatch(
                    &record,
                    HostCommand::Deliver {
                        team_id: self.team_id,
                        thread_id: record.thread_id,
                        communication,
                    },
                )
                .await?;
            let HostReply::Delivered { submission_id } = reply else {
                return Err(CodexErr::UnsupportedOperation(format!(
                    "unexpected delivery reply: {reply:?}"
                )));
            };
            Ok(DeliveryReceipt {
                thread_id: record.thread_id,
                metadata: metadata(&record),
                submission_id,
            })
        })
    }

    fn ensure_child_loaded(&self, parent: ThreadId, child: ThreadId) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            // Members stay loaded on their node for the life of the team in this backend.
            let record = self.record_for_thread(child).await?;
            if record.parent_thread_id != Some(parent) {
                return Err(CodexErr::InvalidRequest(format!(
                    "agent {child} is not a child of {parent}"
                )));
            }
            Ok(())
        })
    }

    fn interrupt(
        &self,
        caller: ThreadId,
        target: AgentTarget,
        _version: MultiAgentVersion,
    ) -> BoxFuture<'_, Result<AgentInfo>> {
        Box::pin(async move {
            let record = self.resolve_target(caller, &target).await?;
            if record.agent_path.is_root() || record.thread_id == caller {
                return Err(CodexErr::UnsupportedOperation(
                    "agents cannot interrupt the root or themselves".to_string(),
                ));
            }
            let reply = self
                .dispatch(
                    &record,
                    HostCommand::Interrupt {
                        team_id: self.team_id,
                        thread_id: record.thread_id,
                    },
                )
                .await?;
            let HostReply::Status { status } = reply else {
                return Err(CodexErr::UnsupportedOperation(format!(
                    "unexpected interrupt reply: {reply:?}"
                )));
            };
            let config = self.member_snapshot(caller, &record, None).await?;
            Ok(AgentInfo::Loaded {
                agent: live_agent(&record, status),
                config: Box::new(config),
            })
        })
    }

    fn list<'a>(
        &'a self,
        _caller: ThreadId,
        _parent: Option<ThreadId>,
        source: &'a SessionSource,
        path_prefix: Option<&'a str>,
    ) -> BoxFuture<'a, Result<Vec<LiveAgent>>> {
        Box::pin(async move {
            let prefix = match path_prefix {
                Some(prefix) => {
                    let current = source.get_agent_path().unwrap_or_else(AgentPath::root);
                    Some(
                        current
                            .resolve(prefix)
                            .map_err(CodexErr::UnsupportedOperation)?,
                    )
                }
                None => None,
            };
            let mut agents = Vec::new();
            for record in self.directory().await? {
                let matches = prefix.as_ref().is_none_or(|prefix| {
                    record.agent_path == *prefix
                        || record
                            .agent_path
                            .as_str()
                            .starts_with(&format!("{}/", prefix.as_str()))
                });
                if matches {
                    let status = self.status(&record).await;
                    agents.push(live_agent(&record, status));
                }
            }
            Ok(agents)
        })
    }

    fn child_agent_paths(&self, parent: ThreadId) -> BoxFuture<'_, Vec<AgentPath>> {
        Box::pin(async move {
            let mut paths: Vec<AgentPath> = self
                .directory()
                .await
                .unwrap_or_default()
                .into_iter()
                .filter(|record| record.parent_thread_id == Some(parent))
                .map(|record| record.agent_path)
                .collect();
            paths.sort_by(|left, right| left.as_str().cmp(right.as_str()));
            paths
        })
    }

    fn check_turn_admission(
        &self,
        _version: MultiAgentVersion,
        _source: &SessionSource,
    ) -> Result<()> {
        // Placement enforces each node's capacity when an agent is spawned.
        Ok(())
    }

    fn admit_turn(
        &self,
        _version: MultiAgentVersion,
        _source: &SessionSource,
    ) -> Option<AgentExecutionGuard> {
        None
    }

    fn record_usage(&self, usage: TokenUsage) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            match self
                .endpoint
                .relay()
                .record_usage(self.team_id, self.thread_id, usage.total_tokens)
                .await
            {
                Ok(true) => Err(CodexErr::SessionBudgetExceeded),
                Ok(false) => Ok(()),
                // Budget accounting must not stop agents when the relay is unreachable.
                Err(err) => {
                    warn!("team usage for {} was not recorded: {err}", self.thread_id);
                    Ok(())
                }
            }
        })
    }

    fn turn_finished<'a>(
        &'a self,
        outcome: AgentTurnOutcome,
        _trace: &'a ThreadTraceContext,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            let SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id,
                agent_path: Some(child_agent_path),
                ..
            }) = &outcome.source
            else {
                return;
            };
            let Some(communication) =
                agent_completion_communication(child_agent_path, &outcome.status)
            else {
                return;
            };
            let parent = match self.record_for_thread(*parent_thread_id).await {
                Ok(parent) => parent,
                Err(err) => {
                    warn!("cannot report {} to its parent: {err}", outcome.thread_id);
                    return;
                }
            };
            let command = HostCommand::Deliver {
                team_id: self.team_id,
                thread_id: parent.thread_id,
                communication,
            };
            if let Err(err) = self.dispatch(&parent, command).await {
                warn!(
                    "failed to notify parent {} of {}: {err}",
                    parent.thread_id, outcome.thread_id
                );
            }
        })
    }

    fn service_tier(&self) -> Option<String> {
        self.service_tier
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    fn propagate_config_update(&self, update: AgentConfigUpdate) {
        let AgentConfigUpdate::ServiceTier(tier) = update;
        *self
            .service_tier
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = tier;
    }

    fn get_guardian_package(
        &self,
        _agent: ThreadId,
    ) -> BoxFuture<'_, Option<GuardianRootSnapshot>> {
        Box::pin(async { None })
    }

    fn pending_budget_reminder<'a>(
        &'a self,
        _agent: ThreadId,
        _window: &'a str,
    ) -> BoxFuture<'a, Option<RolloutBudgetReminder>> {
        Box::pin(async { None })
    }

    fn mark_budget_reminder_delivered<'a>(
        &'a self,
        _agent: ThreadId,
        _window: &'a str,
        _reminder: RolloutBudgetReminder,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async {})
    }
}
