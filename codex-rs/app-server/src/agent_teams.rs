//! Distributed agent teams for this app-server.
//!
//! Enabled by `CODEX_AGENT_TEAMS_NODE_ID`. The server then gives every thread a distributed
//! agent controller, accepts relay-dispatched commands over `agentTeams/host/command`, and
//! reaches the relay through `agentTeams/relay` requests to the connection that registered with
//! `agentTeams/node/register` (normally the node's worker daemon).

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use codex_agent_teams_host::HostEndpoint;
use codex_agent_teams_host::TeamRelay;
use codex_agent_teams_wire::AgentRecord;
use codex_agent_teams_wire::HostCommand;
use codex_agent_teams_wire::HostCommandEnvelope;
use codex_agent_teams_wire::HostReply;
use codex_agent_teams_wire::NodeId;
use codex_agent_teams_wire::PROTOCOL_VERSION;
use codex_agent_teams_wire::RelayRequest;
use codex_agent_teams_wire::RelayResponse;
use codex_app_server_protocol::AgentTeamsHostCommandParams;
use codex_app_server_protocol::AgentTeamsHostCommandResponse;
use codex_app_server_protocol::AgentTeamsNodeRegisterResponse;
use codex_app_server_protocol::AgentTeamsRelayParams;
use codex_app_server_protocol::AgentTeamsRelayResponse;
use codex_app_server_protocol::JSONRPCErrorError;
use codex_app_server_protocol::ServerRequestPayload;
use codex_core::ThreadManager;
use codex_core::config::Config;
use codex_protocol::ThreadId;
use codex_protocol::error::CodexErr;
use codex_protocol::error::Result as CodexResult;
use futures::future::BoxFuture;
use tokio::time::timeout;

use crate::error_code::internal_error;
use crate::error_code::invalid_params;
use crate::error_code::invalid_request;
use crate::outgoing_message::ConnectionId;
use crate::outgoing_message::OutgoingMessageSender;

pub(crate) const NODE_ID_ENV_VAR: &str = "CODEX_AGENT_TEAMS_NODE_ID";

/// Spawning a member starts a thread on its node, which can take a while on a cold machine.
const RELAY_REQUEST_TIMEOUT: Duration = Duration::from_secs(120);

/// This server's membership in distributed agent teams.
pub(crate) struct AgentTeamsNode {
    endpoint: Arc<HostEndpoint>,
    relay: Arc<ClientRelay>,
}

impl AgentTeamsNode {
    /// The node configured by the environment, or `None` when distributed teams are off.
    pub(crate) fn from_env(outgoing: Arc<OutgoingMessageSender>) -> Option<Arc<Self>> {
        let node_id = std::env::var(NODE_ID_ENV_VAR)
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())?;
        let relay = Arc::new(ClientRelay {
            outgoing,
            link: Mutex::new(None),
        });
        let relay_handle: Arc<dyn TeamRelay> = relay.clone();
        let endpoint = HostEndpoint::new(NodeId::new(node_id), relay_handle);
        crate::request_processors::allow_operator_input_for_subagents();
        Some(Arc::new(Self { endpoint, relay }))
    }

    pub(crate) fn install(&self, manager: ThreadManager) -> ThreadManager {
        self.endpoint.install(manager)
    }

    /// Bind the built thread manager; members this node starts use `config` as their base.
    pub(crate) fn attach(&self, manager: &Arc<ThreadManager>, config: Config) {
        if let Err(err) = self.endpoint.attach(manager, config, /*environments*/ None) {
            tracing::warn!("agent-teams node could not attach: {err}");
        }
    }

    pub(crate) fn register(&self, connection_id: ConnectionId) -> AgentTeamsNodeRegisterResponse {
        *self
            .relay
            .link
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(connection_id);
        AgentTeamsNodeRegisterResponse {
            node_id: self.endpoint.node_id().to_string(),
        }
    }

    pub(crate) async fn host_command(
        &self,
        params: AgentTeamsHostCommandParams,
    ) -> Result<AgentTeamsHostCommandResponse, JSONRPCErrorError> {
        let envelope: HostCommandEnvelope = serde_json::from_value(params.envelope)
            .map_err(|err| invalid_params(format!("invalid host command envelope: {err}")))?;
        if envelope.protocol_version != PROTOCOL_VERSION {
            return Err(invalid_request(format!(
                "unsupported agent-teams protocol version {}; this node speaks {PROTOCOL_VERSION}",
                envelope.protocol_version
            )));
        }
        if &envelope.target_node_id != self.endpoint.node_id() {
            return Err(invalid_request(format!(
                "command for node {} reached node {}",
                envelope.target_node_id,
                self.endpoint.node_id()
            )));
        }
        let reply = self
            .endpoint
            .handle(envelope.command)
            .await
            .map_err(|err| invalid_request(err.to_string()))?;
        let reply = serde_json::to_value(reply)
            .map_err(|err| internal_error(format!("failed to encode host reply: {err}")))?;
        Ok(AgentTeamsHostCommandResponse { reply })
    }
}

/// A relay reached through server requests to the registered connection.
struct ClientRelay {
    outgoing: Arc<OutgoingMessageSender>,
    link: Mutex<Option<ConnectionId>>,
}

impl ClientRelay {
    async fn call(&self, request: RelayRequest) -> CodexResult<RelayResponse> {
        let link = (*self
            .link
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner))
        .ok_or_else(|| {
            CodexErr::UnsupportedOperation(
                "no agent-teams relay link is registered with this app-server".to_string(),
            )
        })?;
        let request = serde_json::to_value(request).map_err(|err| {
            CodexErr::InvalidRequest(format!("failed to encode relay request: {err}"))
        })?;
        let (request_id, rx) = self
            .outgoing
            .send_request_to_connections(
                Some(&[link]),
                ServerRequestPayload::AgentTeamsRelay(AgentTeamsRelayParams { request }),
                /*thread_id*/ None,
            )
            .await;
        let result = match timeout(RELAY_REQUEST_TIMEOUT, rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => {
                return Err(CodexErr::UnsupportedOperation(
                    "agent-teams relay request was canceled".to_string(),
                ));
            }
            Err(_) => {
                let _canceled = self.outgoing.cancel_request(&request_id).await;
                return Err(CodexErr::UnsupportedOperation(format!(
                    "agent-teams relay request timed out after {}s",
                    RELAY_REQUEST_TIMEOUT.as_secs()
                )));
            }
        };
        let value = result.map_err(|err| {
            CodexErr::UnsupportedOperation(format!("agent-teams relay error: {}", err.message))
        })?;
        let response: AgentTeamsRelayResponse = serde_json::from_value(value).map_err(|err| {
            CodexErr::UnsupportedOperation(format!("invalid agent-teams relay response: {err}"))
        })?;
        serde_json::from_value(response.response).map_err(|err| {
            CodexErr::UnsupportedOperation(format!("invalid agent-teams relay response: {err}"))
        })
    }
}

fn unexpected(response: RelayResponse) -> CodexErr {
    CodexErr::UnsupportedOperation(format!(
        "unexpected agent-teams relay response: {response:?}"
    ))
}

impl TeamRelay for ClientRelay {
    fn place<'a>(
        &'a self,
        team_id: ThreadId,
        role: Option<&'a str>,
        spawning_node: &'a NodeId,
    ) -> BoxFuture<'a, CodexResult<NodeId>> {
        Box::pin(async move {
            match self
                .call(RelayRequest::Place {
                    team_id,
                    role: role.map(str::to_string),
                    spawning_node: spawning_node.clone(),
                })
                .await?
            {
                RelayResponse::Placed { node_id } => Ok(node_id),
                other => Err(unexpected(other)),
            }
        })
    }

    fn dispatch<'a>(
        &'a self,
        node_id: &'a NodeId,
        command: HostCommand,
    ) -> BoxFuture<'a, CodexResult<HostReply>> {
        Box::pin(async move {
            let command = HostCommandEnvelope {
                protocol_version: PROTOCOL_VERSION,
                command_id: uuid::Uuid::now_v7().to_string(),
                target_node_id: node_id.clone(),
                command,
            };
            match self
                .call(RelayRequest::Dispatch {
                    node_id: node_id.clone(),
                    command: Box::new(command),
                })
                .await?
            {
                RelayResponse::Dispatched { reply } => Ok(reply),
                other => Err(unexpected(other)),
            }
        })
    }

    fn register(&self, record: AgentRecord) -> BoxFuture<'_, CodexResult<()>> {
        Box::pin(async move {
            match self.call(RelayRequest::Register { record }).await? {
                RelayResponse::Done => Ok(()),
                other => Err(unexpected(other)),
            }
        })
    }

    fn unregister(&self, team_id: ThreadId, thread_id: ThreadId) -> BoxFuture<'_, CodexResult<()>> {
        Box::pin(async move {
            match self
                .call(RelayRequest::Unregister { team_id, thread_id })
                .await?
            {
                RelayResponse::Done => Ok(()),
                other => Err(unexpected(other)),
            }
        })
    }

    fn agents(&self, team_id: ThreadId) -> BoxFuture<'_, CodexResult<Vec<AgentRecord>>> {
        Box::pin(async move {
            match self.call(RelayRequest::Agents { team_id }).await? {
                RelayResponse::Agents { records } => Ok(records),
                other => Err(unexpected(other)),
            }
        })
    }
}
