//! Two app-server processes form one agent team. The test plays the relay: it answers each
//! node's `agentTeams/relay` requests and forwards dispatches to the target node's
//! `agentTeams/host/command`, the job a worker daemon does in production.

use std::collections::HashMap;
use std::time::Duration;

use anyhow::Result;
use anyhow::bail;
use app_test_support::MockResponsesConfig;
use app_test_support::TestAppServer;
use codex_agent_teams_wire::AgentRecord;
use codex_agent_teams_wire::NodeId;
use codex_agent_teams_wire::RelayRequest;
use codex_agent_teams_wire::RelayResponse;
use codex_app_server_protocol::AgentTeamsHostCommandParams;
use codex_app_server_protocol::AgentTeamsHostCommandResponse;
use codex_app_server_protocol::AgentTeamsNodeRegisterResponse;
use codex_app_server_protocol::AgentTeamsRelayParams;
use codex_app_server_protocol::AgentTeamsRelayResponse;
use codex_app_server_protocol::ClientRequest;
use codex_app_server_protocol::JSONRPCMessage;
use codex_app_server_protocol::RequestId;
use codex_app_server_protocol::ThreadStartParams;
use codex_app_server_protocol::ThreadStartResponse;
use codex_app_server_protocol::TurnStartParams;
use codex_app_server_protocol::UserInput;
use codex_features::Feature;
use codex_protocol::ThreadId;
use core_test_support::responses;
use core_test_support::responses::ResponseMock;
use core_test_support::skip_if_no_network;
use pretty_assertions::assert_eq;
use tempfile::TempDir;
use tokio::time::timeout;
use wiremock::MockServer;

const NODE_ID_ENV_VAR: &str = "CODEX_AGENT_TEAMS_NODE_ID";
const NAMESPACE: &str = "collaboration";

async fn start_node(node_id: &str, server: &MockServer, home: &TempDir) -> Result<TestAppServer> {
    MockResponsesConfig::new(&server.uri())
        .enable_feature(Feature::Collab)
        .enable_feature(Feature::MultiAgentV2)
        .with_extra_config(
            r#"[agents.mail]
description = "Reads and sends email, checks the calendar"
"#,
        )
        .write(home.path())?;
    let mut node = TestAppServer::builder()
        .with_codex_home(home.path())
        .with_env_overrides(&[(NODE_ID_ENV_VAR, Some(node_id))])
        .build_initialized()
        .await?;
    let registered: AgentTeamsNodeRegisterResponse = node
        .request(|request_id| ClientRequest::AgentTeamsNodeRegister {
            request_id,
            params: Default::default(),
        })
        .await?;
    assert_eq!(registered.node_id, node_id);
    Ok(node)
}

fn call(id: &str, name: &str, arguments: serde_json::Value) -> String {
    responses::sse(vec![
        responses::ev_response_created(id),
        responses::ev_function_call_with_namespace(id, NAMESPACE, name, &arguments.to_string()),
        responses::ev_completed(id),
    ])
}

fn answer(id: &str, text: &str) -> String {
    responses::sse(vec![
        responses::ev_response_created(id),
        responses::ev_assistant_message(id, text),
        responses::ev_completed(id),
    ])
}

fn bodies(mock: &ResponseMock) -> Vec<String> {
    mock.requests()
        .iter()
        .map(|request| request.body_json().to_string())
        .collect()
}

/// The relay's state: which node serves which roles, and the team directory.
struct TestRelay {
    roles: HashMap<NodeId, Vec<String>>,
    agents: Vec<AgentRecord>,
    /// Host commands in flight: (target node, request) -> (asking node, its relay request).
    in_flight: HashMap<(NodeId, RequestId), (NodeId, RequestId)>,
    dispatches: Vec<(NodeId, NodeId)>,
}

enum Step {
    Answer(RelayResponse),
    Forward {
        target: NodeId,
        envelope: serde_json::Value,
    },
}

impl TestRelay {
    fn handle(&mut self, from: &NodeId, request: RelayRequest) -> Result<Step> {
        Ok(match request {
            RelayRequest::Place {
                role,
                spawning_node,
                ..
            } => {
                let serving = role.as_ref().and_then(|role| {
                    self.roles
                        .iter()
                        .find(|(_, roles)| roles.contains(role))
                        .map(|(node, _)| node.clone())
                });
                Step::Answer(RelayResponse::Placed {
                    node_id: serving.unwrap_or(spawning_node),
                })
            }
            RelayRequest::Register { record } => {
                self.agents
                    .retain(|existing| existing.thread_id != record.thread_id);
                self.agents.push(record);
                Step::Answer(RelayResponse::Done)
            }
            RelayRequest::Unregister { thread_id, .. } => {
                self.agents.retain(|record| record.thread_id != thread_id);
                Step::Answer(RelayResponse::Done)
            }
            RelayRequest::Agents { team_id } => Step::Answer(RelayResponse::Agents {
                records: self
                    .agents
                    .iter()
                    .filter(|record| record.team_id == team_id)
                    .cloned()
                    .collect(),
            }),
            RelayRequest::Dispatch { node_id, command } => {
                self.dispatches.push((from.clone(), node_id.clone()));
                Step::Forward {
                    target: node_id,
                    envelope: serde_json::to_value(command)?,
                }
            }
        })
    }
}

fn relay_response(response: RelayResponse) -> Result<serde_json::Value> {
    Ok(serde_json::to_value(AgentTeamsRelayResponse {
        response: serde_json::to_value(response)?,
    })?)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lead_and_member_in_separate_app_servers_form_one_team() -> Result<()> {
    skip_if_no_network!(Ok(()));
    let lead_server = responses::start_mock_server().await;
    let member_server = responses::start_mock_server().await;
    let lead_home = TempDir::new()?;
    let member_home = TempDir::new()?;

    // The member answers after a delay so the lead is already blocked in `wait_agent`.
    let member_mock = responses::mount_response_sequence(
        &member_server,
        vec![
            responses::sse_response(answer("m1", "Three events today."))
                .set_delay(Duration::from_millis(500)),
            responses::sse_response(answer("m2", "Tomorrow is free.")),
        ],
    )
    .await;
    let lead_mock = responses::mount_sse_sequence(
        &lead_server,
        vec![
            call(
                "spawn",
                "spawn_agent",
                serde_json::json!({
                    "task_name": "mail",
                    "agent_type": "mail",
                    "message": "List today's events.",
                    "fork_turns": "none",
                }),
            ),
            call("wait", "wait_agent", serde_json::json!({})),
            answer("l3", "Delegated."),
        ],
    )
    .await;

    let lead_id = NodeId::new("lead");
    let member_id = NodeId::new("member");
    let mut lead = start_node(lead_id.as_str(), &lead_server, &lead_home).await?;
    let member = start_node(member_id.as_str(), &member_server, &member_home).await?;

    let ThreadStartResponse { thread, .. } =
        lead.start_thread(ThreadStartParams::default()).await?;
    let team_id = ThreadId::from_string(&thread.id)?;
    let turn_request = lead
        .send_request(
            "turn/start",
            Some(serde_json::to_value(TurnStartParams {
                thread_id: thread.id.clone(),
                input: vec![UserInput::Text {
                    text: "Find out what is on my calendar.".to_string(),
                    text_elements: Vec::new(),
                }],
                ..Default::default()
            })?),
        )
        .await?;

    let mut relay = TestRelay {
        roles: HashMap::from([
            (lead_id.clone(), Vec::new()),
            (member_id.clone(), vec!["mail".to_string()]),
        ]),
        agents: Vec::new(),
        in_flight: HashMap::new(),
        dispatches: Vec::new(),
    };
    let mut nodes = Nodes {
        lead,
        member,
        lead_id: lead_id.clone(),
        member_id: member_id.clone(),
    };

    let mut turn_started = false;
    nodes
        .pump(&mut relay, |from, message| match message {
            JSONRPCMessage::Response(response)
                if from == &lead_id && response.id == RequestId::Integer(turn_request) =>
            {
                turn_started = true;
                false
            }
            JSONRPCMessage::Notification(notification) => {
                from == &lead_id && turn_started && notification.method == "turn/completed"
            }
            _ => false,
        })
        .await?;

    // The lead's request after `wait_agent` carries the member's result, delivered across
    // processes with the same notice local sub-agents produce.
    let lead_bodies = bodies(&lead_mock);
    assert_eq!(lead_bodies.len(), 3, "{lead_bodies:#?}");
    assert!(
        lead_bodies[2].contains("Message Type: FINAL_ANSWER")
            && lead_bodies[2].contains("Sender: /root/mail")
            && lead_bodies[2].contains("Three events today."),
        "{}",
        lead_bodies[2]
    );
    assert!(bodies(&member_mock)[0].contains("List today's events."));

    // The member lives in the member process and belongs to the lead's team. The lead only
    // reaches it through the relay: spawn, then the member's result coming back.
    let mail = relay
        .agents
        .iter()
        .find(|record| record.agent_path.name() == "mail")
        .expect("mail member registered")
        .clone();
    assert_eq!(
        (&mail.node_id, mail.team_id, mail.parent_thread_id),
        (&member_id, team_id, Some(team_id))
    );
    assert_eq!(
        relay.dispatches,
        vec![
            (lead_id.clone(), member_id.clone()),
            (member_id.clone(), lead_id.clone()),
        ]
    );

    // An operator viewer on the member's machine attaches to the member and types into it.
    let resume_request = nodes
        .member
        .send_request(
            "thread/resume",
            Some(serde_json::json!({ "threadId": mail.thread_id.to_string() })),
        )
        .await?;
    let mut resumed = None;
    nodes
        .pump(&mut relay, |from, message| match message {
            JSONRPCMessage::Response(response)
                if from == &member_id && response.id == RequestId::Integer(resume_request) =>
            {
                resumed = Some(response.result.clone());
                true
            }
            _ => false,
        })
        .await?;
    let resumed = resumed.expect("resume response");
    assert_eq!(
        (
            resumed["thread"]["canAcceptDirectInput"].clone(),
            resumed["thread"]["status"]["type"] != "notLoaded"
        ),
        (serde_json::json!(true), true),
        "{resumed:#}"
    );

    let operator_turn = nodes
        .member
        .send_request(
            "turn/start",
            Some(serde_json::to_value(TurnStartParams {
                thread_id: mail.thread_id.to_string(),
                input: vec![UserInput::Text {
                    text: "Operator: also check tomorrow.".to_string(),
                    text_elements: Vec::new(),
                }],
                ..Default::default()
            })?),
        )
        .await?;
    let mut operator_turn_started = false;
    let member_thread_id = mail.thread_id.to_string();
    nodes
        .pump(&mut relay, |from, message| match message {
            JSONRPCMessage::Response(response)
                if from == &member_id && response.id == RequestId::Integer(operator_turn) =>
            {
                operator_turn_started = true;
                false
            }
            JSONRPCMessage::Notification(notification) => {
                from == &member_id
                    && operator_turn_started
                    && notification.method == "turn/completed"
                    && notification.params.as_ref().is_some_and(|params| {
                        params["threadId"] == serde_json::json!(member_thread_id)
                    })
            }
            _ => false,
        })
        .await?;
    let member_bodies = bodies(&member_mock);
    assert!(
        member_bodies[1].contains("Operator: also check tomorrow."),
        "{}",
        member_bodies[1]
    );
    // The operator's turn is reported to the lead like any other member result.
    assert_eq!(
        relay.dispatches.last(),
        Some(&(member_id.clone(), lead_id.clone()))
    );
    Ok(())
}

/// Both app-server processes, with the test acting as their relay.
struct Nodes {
    lead: TestAppServer,
    member: TestAppServer,
    lead_id: NodeId,
    member_id: NodeId,
}

impl Nodes {
    fn server(&mut self, id: &NodeId) -> &mut TestAppServer {
        if id == &self.lead_id {
            &mut self.lead
        } else {
            &mut self.member
        }
    }

    /// Relay traffic between the nodes until `stop` accepts a message.
    async fn pump(
        &mut self,
        relay: &mut TestRelay,
        mut stop: impl FnMut(&NodeId, &JSONRPCMessage) -> bool,
    ) -> Result<()> {
        timeout(Duration::from_secs(30), async {
            loop {
                // Reads are cancellation safe, so waiting on both nodes cannot drop a message.
                let (from, message) = tokio::select! {
                    message = self.lead.read_next_message() => (self.lead_id.clone(), message?),
                    message = self.member.read_next_message() => (self.member_id.clone(), message?),
                };
                match &message {
                    JSONRPCMessage::Request(request) if request.method == "agentTeams/relay" => {
                        let params: AgentTeamsRelayParams =
                            serde_json::from_value(request.params.clone().unwrap_or_default())?;
                        match relay.handle(&from, serde_json::from_value(params.request)?)? {
                            Step::Answer(response) => {
                                self.server(&from)
                                    .send_response(request.id.clone(), relay_response(response)?)
                                    .await?;
                            }
                            Step::Forward { target, envelope } => {
                                let forwarded = self
                                    .server(&target)
                                    .send_request(
                                        "agentTeams/host/command",
                                        Some(serde_json::to_value(AgentTeamsHostCommandParams {
                                            envelope,
                                        })?),
                                    )
                                    .await?;
                                relay.in_flight.insert(
                                    (target, RequestId::Integer(forwarded)),
                                    (from.clone(), request.id.clone()),
                                );
                            }
                        }
                        continue;
                    }
                    JSONRPCMessage::Response(response) => {
                        if let Some((asker, relay_request)) =
                            relay.in_flight.remove(&(from.clone(), response.id.clone()))
                        {
                            let reply: AgentTeamsHostCommandResponse =
                                serde_json::from_value(response.result.clone())?;
                            self.server(&asker)
                                .send_response(
                                    relay_request,
                                    relay_response(RelayResponse::Dispatched {
                                        reply: serde_json::from_value(reply.reply)?,
                                    })?,
                                )
                                .await?;
                            continue;
                        }
                    }
                    JSONRPCMessage::Error(error) => {
                        if let Some((asker, relay_request)) =
                            relay.in_flight.remove(&(from.clone(), error.id.clone()))
                        {
                            self.server(&asker)
                                .send_error(relay_request, error.error.clone())
                                .await?;
                            continue;
                        }
                        bail!("{from} returned an error: {error:?}");
                    }
                    JSONRPCMessage::Request(request) => {
                        bail!("{from} sent an unexpected request: {}", request.method);
                    }
                    JSONRPCMessage::Notification(_) => {}
                }
                if stop(&from, &message) {
                    break Ok::<_, anyhow::Error>(());
                }
            }
        })
        .await?
    }
}
