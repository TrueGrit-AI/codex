//! Two nodes in one process, each with its own thread manager, configuration and model server,
//! joined by the in-memory relay. The lead uses ordinary MultiAgentV2 tools; the relay decides
//! where members run.
#![allow(clippy::expect_used)]

use std::sync::Arc;
use std::time::Duration;

use codex_agent_teams_host::HostEndpoint;
use codex_agent_teams_host::InMemoryRelay;
use codex_agent_teams_host::TeamRelay;
use codex_agent_teams_wire::HostCommand;
use codex_agent_teams_wire::HostReply;
use codex_agent_teams_wire::NodeAdvertisement;
use codex_agent_teams_wire::NodeId;
use codex_core::TurnInputRequest;
use codex_core::config::AgentRoleConfig;
use codex_features::Feature;
use codex_login::CodexAuth;
use codex_models_manager::manager::RefreshStrategy;
use codex_models_manager::model_info::model_info_from_slug;
use codex_protocol::AgentPath;
use codex_protocol::SessionId;
use codex_protocol::ThreadId;
use codex_protocol::openai_models::ModelInfo;
use codex_protocol::openai_models::ModelVisibility;
use codex_protocol::openai_models::ModelsResponse;
use codex_protocol::protocol::AgentStatus;
use codex_protocol::protocol::CodexErrorInfo;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::MultiAgentVersion;
use codex_protocol::user_input::UserInput;
use core_test_support::responses;
use core_test_support::responses::ResponseMock;
use core_test_support::skip_if_no_network;
use core_test_support::test_codex::TestCodex;
use core_test_support::test_codex::run_test_with_large_stack;
use core_test_support::test_codex::test_codex;
use core_test_support::wait_for_event;
use pretty_assertions::assert_eq;
use wiremock::MockServer;

const NAMESPACE: &str = "collaboration";

/// Poll until `done` holds, failing the test after ten seconds.
macro_rules! wait_until {
    ($what:expr, $done:expr) => {
        tokio::time::timeout(Duration::from_secs(10), async {
            while !$done {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {}", $what))
    };
}

struct Node {
    endpoint: Arc<HostEndpoint>,
    test: TestCodex,
}

async fn start_node(
    relay: &Arc<InMemoryRelay>,
    name: &str,
    roles: &[&str],
    server: &MockServer,
    v2_model: Option<&str>,
) -> anyhow::Result<Node> {
    // Sub-agents only get collaboration tools on models that declare MultiAgentV2 support.
    if let Some(slug) = v2_model {
        let mut model = ModelInfo {
            visibility: ModelVisibility::List,
            used_fallback_model_metadata: false,
            ..model_info_from_slug(slug)
        };
        model.multi_agent_version = Some(MultiAgentVersion::V2);
        responses::mount_models_once(
            server,
            ModelsResponse {
                models: vec![model],
            },
        )
        .await;
    }
    let relay_handle: Arc<dyn TeamRelay> = relay.clone();
    let endpoint = HostEndpoint::new(NodeId::new(name), relay_handle);
    relay.connect(
        NodeAdvertisement {
            node_id: NodeId::new(name),
            roles: roles.iter().map(ToString::to_string).collect(),
            max_concurrent_agents: 2,
        },
        &endpoint,
    );
    let mut builder = test_codex();
    if let Some(slug) = v2_model {
        builder = builder
            .with_auth(CodexAuth::create_dummy_chatgpt_auth_for_testing())
            .with_model(slug);
    }
    let test = builder
        .with_config(|config| {
            for feature in [Feature::Collab, Feature::MultiAgentV2] {
                config
                    .features
                    .enable(feature)
                    .expect("enable multi-agent v2");
            }
            for (role, description) in [
                ("mail", "Reads and sends email, checks the calendar"),
                ("notes", "Keeps meeting notes"),
            ] {
                config.agent_roles.insert(
                    role.to_string(),
                    AgentRoleConfig {
                        description: Some(description.to_string()),
                        config_file: None,
                        nickname_candidates: None,
                    },
                );
            }
            config.model_provider.request_max_retries = Some(0);
            config.model_provider.stream_max_retries = Some(0);
            config.model_provider.supports_websockets = false;
        })
        .with_thread_manager({
            let endpoint = Arc::clone(&endpoint);
            move |manager| endpoint.install(manager)
        })
        .build_with_auto_env(server)
        .await?;
    if let Some(slug) = v2_model {
        wait_until!(
            format!("model {slug} in the catalog"),
            test.thread_manager
                .get_models_manager()
                .list_models(
                    RefreshStrategy::Online,
                    codex_core::test_support::default_http_client_factory(),
                )
                .await
                .iter()
                .any(|model| model.model == slug)
        );
    }
    endpoint.attach(
        &test.thread_manager,
        test.config.clone(),
        Some(vec![test.executor_environment().selection().clone()]),
    )?;
    Ok(Node { endpoint, test })
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

async fn wait_for_request(mock: &ResponseMock, count: usize) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while mock.requests().len() < count {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for model request {count}"));
}

fn bodies(mock: &ResponseMock) -> Vec<String> {
    mock.requests()
        .iter()
        .map(|request| request.body_json().to_string())
        .collect()
}

#[test]
fn role_member_runs_on_its_node_and_reports_back_to_the_lead() -> anyhow::Result<()> {
    run_test_with_large_stack(
        "role_member_runs_on_its_node_and_reports_back_to_the_lead",
        role_member_runs_on_its_node_and_reports_back_to_the_lead_body,
    )
}

async fn role_member_runs_on_its_node_and_reports_back_to_the_lead_body() -> anyhow::Result<()> {
    skip_if_no_network!(Ok(()));
    let relay = InMemoryRelay::new();
    let lead_server = responses::start_mock_server().await;
    let member_server = responses::start_mock_server().await;
    let lead = start_node(&relay, "lead", &[], &lead_server, None).await?;
    let member = start_node(&relay, "member", &["mail"], &member_server, None).await?;
    let team_id = lead.test.session_configured.thread_id;

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
            call(
                "followup",
                "followup_task",
                serde_json::json!({"target": "mail", "message": "Also check tomorrow."}),
            ),
            answer("l5", "Asked for tomorrow."),
        ],
    )
    .await;

    lead.test
        .submit_turn("Find out what is on my calendar.")
        .await?;

    // The lead's third request follows `wait_agent` and carries the member's result, delivered
    // across nodes with the same text the local backend produces.
    let lead_bodies = bodies(&lead_mock);
    assert_eq!(lead_bodies.len(), 3, "{lead_bodies:#?}");
    assert!(
        lead_bodies[2].contains("Message Type: FINAL_ANSWER")
            && lead_bodies[2].contains("Sender: /root/mail")
            && lead_bodies[2].contains("Three events today."),
        "{}",
        lead_bodies[2]
    );

    // The member ran on the member node, with the member node's model server, and the lead
    // node never hosted it.
    let member_bodies = bodies(&member_mock);
    assert!(
        member_bodies[0].contains("List today's events."),
        "{}",
        member_bodies[0]
    );
    let team = relay.agents(team_id).await?;
    let mail = team
        .iter()
        .find(|record| record.agent_path == AgentPath::root().join("mail").expect("path"))
        .expect("mail member registered");
    assert_eq!(
        (
            mail.node_id.as_str(),
            mail.agent_role.as_deref(),
            mail.parent_thread_id
        ),
        ("member", Some("mail"), Some(team_id))
    );
    let remote = member
        .test
        .thread_manager
        .get_thread(mail.thread_id)
        .await?;
    assert_eq!(
        remote.startup_metadata().session_id,
        SessionId::from(team_id)
    );
    assert!(
        lead.test
            .thread_manager
            .get_thread(mail.thread_id)
            .await
            .is_err()
    );

    // A follow-up from the lead starts another turn on the member node.
    lead.test.submit_turn("Also ask about tomorrow.").await?;
    wait_for_request(&member_mock, 2).await;
    assert!(
        bodies(&member_mock)[1].contains("Also check tomorrow."),
        "{}",
        bodies(&member_mock)[1]
    );

    remote.shutdown_and_wait().await?;
    member.test.codex.shutdown_and_wait().await?;
    lead.test.codex.shutdown_and_wait().await?;
    drop(member.endpoint);
    Ok(())
}

#[test]
fn role_less_members_stay_on_the_spawning_node() -> anyhow::Result<()> {
    run_test_with_large_stack(
        "role_less_members_stay_on_the_spawning_node",
        role_less_members_stay_on_the_spawning_node_body,
    )
}

async fn role_less_members_stay_on_the_spawning_node_body() -> anyhow::Result<()> {
    skip_if_no_network!(Ok(()));
    let relay = InMemoryRelay::new();
    let lead_server = responses::start_mock_server().await;
    let member_server = responses::start_mock_server().await;
    let lead = start_node(&relay, "lead", &[], &lead_server, None).await?;
    let _member = start_node(&relay, "member", &["mail"], &member_server, None).await?;
    let team_id = lead.test.session_configured.thread_id;

    responses::mount_sse_sequence(
        &lead_server,
        vec![
            call(
                "spawn",
                "spawn_agent",
                serde_json::json!({
                    "task_name": "helper",
                    "message": "Summarize the notes.",
                    "fork_turns": "none",
                }),
            ),
            answer("l2", "Spawned."),
            answer("helper", "Summary."),
        ],
    )
    .await;
    lead.test.submit_turn("Get a summary.").await?;

    let helper = relay
        .agents(team_id)
        .await?
        .into_iter()
        .find(|record| record.agent_path.name() == "helper")
        .expect("helper registered");
    assert_eq!(helper.node_id, NodeId::new("lead"));
    let local = lead
        .test
        .thread_manager
        .get_thread(helper.thread_id)
        .await?;
    wait_for_event(&local, |event| matches!(event, EventMsg::TurnComplete(_))).await;
    local.shutdown_and_wait().await?;
    lead.test.codex.shutdown_and_wait().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unknown_role_falls_back_to_the_spawning_node() -> anyhow::Result<()> {
    let relay = InMemoryRelay::new();
    let relay_handle: Arc<dyn TeamRelay> = relay.clone();
    let lead = HostEndpoint::new(NodeId::new("lead"), relay_handle);
    relay.connect(
        NodeAdvertisement {
            node_id: NodeId::new("lead"),
            roles: Vec::new(),
            max_concurrent_agents: 1,
        },
        &lead,
    );
    let placed = relay
        .place(ThreadId::new(), Some("calendar"), lead.node_id())
        .await?;
    assert_eq!(placed, NodeId::new("lead"));
    Ok(())
}

#[test]
fn lead_interrupts_a_member_on_another_node() -> anyhow::Result<()> {
    run_test_with_large_stack(
        "lead_interrupts_a_member_on_another_node",
        lead_interrupts_a_member_on_another_node_body,
    )
}

async fn lead_interrupts_a_member_on_another_node_body() -> anyhow::Result<()> {
    skip_if_no_network!(Ok(()));
    let relay = InMemoryRelay::new();
    let lead_server = responses::start_mock_server().await;
    let member_server = responses::start_mock_server().await;
    let lead = start_node(&relay, "lead", &[], &lead_server, None).await?;
    let member = start_node(&relay, "member", &["mail"], &member_server, None).await?;
    let team_id = lead.test.session_configured.thread_id;

    // The member's model stalls, so its turn is still running when the lead interrupts it.
    let member_mock = responses::mount_response_sequence(
        &member_server,
        vec![responses::sse_response(answer("m1", "Too late.")).set_delay(Duration::from_secs(30))],
    )
    .await;
    responses::mount_sse_sequence(
        &lead_server,
        vec![call(
            "spawn",
            "spawn_agent",
            serde_json::json!({
                "task_name": "mail",
                "agent_type": "mail",
                "message": "Draft a long reply.",
                "fork_turns": "none",
            }),
        )],
    )
    .await;
    let lead_mock = responses::mount_sse_sequence(
        &lead_server,
        vec![
            answer("l2", "Spawned."),
            call(
                "interrupt",
                "interrupt_agent",
                serde_json::json!({"target": "mail"}),
            ),
            answer("l4", "Stopped it."),
        ],
    )
    .await;

    lead.test.submit_turn("Start drafting.").await?;
    wait_for_request(&member_mock, 1).await;
    let mail = relay
        .agents(team_id)
        .await?
        .into_iter()
        .find(|record| record.agent_path.name() == "mail")
        .expect("mail member registered");
    let remote = member
        .test
        .thread_manager
        .get_thread(mail.thread_id)
        .await?;
    assert_eq!(remote.agent_status().await, AgentStatus::Running);

    lead.test.submit_turn("Stop the draft.").await?;
    let bodies = bodies(&lead_mock);
    let interrupt_output = bodies
        .iter()
        .find(|body| body.contains("previous_status"))
        .unwrap_or_else(|| panic!("interrupt result not returned to the lead: {bodies:#?}"));
    assert!(interrupt_output.contains("running"), "{interrupt_output}");
    wait_until!(
        "member interruption",
        remote.agent_status().await == AgentStatus::Interrupted
    );

    remote.shutdown_and_wait().await?;
    member.test.codex.shutdown_and_wait().await?;
    lead.test.codex.shutdown_and_wait().await?;
    Ok(())
}

#[test]
fn members_on_different_nodes_message_each_other() -> anyhow::Result<()> {
    run_test_with_large_stack(
        "members_on_different_nodes_message_each_other",
        members_on_different_nodes_message_each_other_body,
    )
}

async fn members_on_different_nodes_message_each_other_body() -> anyhow::Result<()> {
    skip_if_no_network!(Ok(()));
    let relay = InMemoryRelay::new();
    let lead_server = responses::start_mock_server().await;
    let mail_server = responses::start_mock_server().await;
    let notes_server = responses::start_mock_server().await;
    let lead = start_node(&relay, "lead", &[], &lead_server, None).await?;
    let mail = start_node(
        &relay,
        "mail-node",
        &["mail"],
        &mail_server,
        Some("test-multi-agent-v2"),
    )
    .await?;
    let notes = start_node(&relay, "notes-node", &["notes"], &notes_server, None).await?;
    let team_id = lead.test.session_configured.thread_id;

    let notes_mock = responses::mount_response_sequence(
        &notes_server,
        vec![
            responses::sse_response(answer("n1", "Ready for notes.")),
            responses::sse_response(answer("n2", "Noted.")),
        ],
    )
    .await;
    // The mail member waits for the notes member to exist, then hands it a follow-up task.
    let mail_mock = responses::mount_response_sequence(
        &mail_server,
        vec![
            responses::sse_response(call(
                "followup",
                "followup_task",
                serde_json::json!({"target": "/root/notes", "message": "Meeting at 3pm."}),
            ))
            .set_delay(Duration::from_millis(500)),
            responses::sse_response(answer("m2", "Told notes.")),
        ],
    )
    .await;
    responses::mount_sse_sequence(
        &lead_server,
        vec![
            call(
                "spawn-notes",
                "spawn_agent",
                serde_json::json!({
                    "task_name": "notes",
                    "agent_type": "notes",
                    "message": "Take notes today.",
                    "fork_turns": "none",
                }),
            ),
            call(
                "spawn-mail",
                "spawn_agent",
                serde_json::json!({
                    "task_name": "mail",
                    "agent_type": "mail",
                    "message": "Tell notes about the 3pm meeting.",
                    "fork_turns": "none",
                }),
            ),
            answer("l3", "Delegated."),
        ],
    )
    .await;

    lead.test.submit_turn("Coordinate the team.").await?;
    wait_for_request(&mail_mock, 2).await;
    // `followup_task` reports success with empty output.
    assert_eq!(
        mail_mock.requests()[1].function_call_output_text("followup"),
        Some(String::new())
    );
    wait_until!(
        "notes to receive the follow-up",
        bodies(&notes_mock)
            .iter()
            .any(|body| body.contains("Meeting at 3pm.") && body.contains("/root/mail"))
    );

    let team = relay.agents(team_id).await?;
    let node_of = |name: &str| {
        team.iter()
            .find(|record| record.agent_path.name() == name)
            .map(|record| record.node_id.clone())
    };
    assert_eq!(
        (node_of("mail"), node_of("notes")),
        (
            Some(NodeId::new("mail-node")),
            Some(NodeId::new("notes-node"))
        )
    );

    for record in team.iter().filter(|record| record.depth > 0) {
        let node = if record.node_id == NodeId::new("mail-node") {
            &mail
        } else {
            &notes
        };
        node.test
            .thread_manager
            .get_thread(record.thread_id)
            .await?
            .shutdown_and_wait()
            .await?;
    }
    mail.test.codex.shutdown_and_wait().await?;
    notes.test.codex.shutdown_and_wait().await?;
    lead.test.codex.shutdown_and_wait().await?;
    Ok(())
}

#[test]
fn lead_learns_when_a_member_is_lost() -> anyhow::Result<()> {
    run_test_with_large_stack(
        "lead_learns_when_a_member_is_lost",
        lead_learns_when_a_member_is_lost_body,
    )
}

async fn lead_learns_when_a_member_is_lost_body() -> anyhow::Result<()> {
    skip_if_no_network!(Ok(()));
    let relay = InMemoryRelay::new();
    let lead_server = responses::start_mock_server().await;
    let member_server = responses::start_mock_server().await;
    let lead = start_node(&relay, "lead", &[], &lead_server, None).await?;
    let _member = start_node(&relay, "member", &["mail"], &member_server, None).await?;
    let team_id = lead.test.session_configured.thread_id;

    // The member never answers in time; the relay reports it lost instead.
    let member_mock = responses::mount_response_sequence(
        &member_server,
        vec![responses::sse_response(answer("m1", "Too late.")).set_delay(Duration::from_secs(60))],
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
                    "message": "Draft a long reply.",
                    "fork_turns": "none",
                }),
            ),
            call("wait", "wait_agent", serde_json::json!({})),
            answer("l3", "The mail agent was lost."),
        ],
    )
    .await;

    lead.test
        .codex
        .start_or_steer_turn(TurnInputRequest::user_input(vec![UserInput::Text {
            text: "Start drafting.".into(),
            text_elements: Vec::new(),
        }]))
        .await?;
    wait_for_request(&member_mock, 1).await;
    let mail = relay
        .agents(team_id)
        .await?
        .into_iter()
        .find(|record| record.agent_path.name() == "mail")
        .expect("mail member registered");
    let reply = relay
        .dispatch(
            &NodeId::new("lead"),
            HostCommand::AgentLost {
                team_id,
                thread_id: team_id,
                child_thread_id: mail.thread_id,
                child_agent_path: mail.agent_path.clone(),
                reason: "its node stopped reporting".to_string(),
            },
        )
        .await?;
    assert!(matches!(reply, HostReply::Delivered { .. }), "{reply:?}");
    wait_for_event(&lead.test.codex, |event| {
        matches!(event, EventMsg::TurnComplete(_))
    })
    .await;

    let lead_bodies = bodies(&lead_mock);
    assert!(
        lead_bodies[2].contains("Sender: /root/mail")
            && lead_bodies[2].contains("Agent errored: its node stopped reporting"),
        "{}",
        lead_bodies[2]
    );
    lead.test.codex.shutdown_and_wait().await?;
    Ok(())
}

#[test]
fn team_budget_stops_turns_once_exhausted() -> anyhow::Result<()> {
    run_test_with_large_stack(
        "team_budget_stops_turns_once_exhausted",
        team_budget_stops_turns_once_exhausted_body,
    )
}

async fn team_budget_stops_turns_once_exhausted_body() -> anyhow::Result<()> {
    skip_if_no_network!(Ok(()));
    let relay = InMemoryRelay::with_team_token_budget(50);
    let lead_server = responses::start_mock_server().await;
    let lead = start_node(&relay, "lead", &[], &lead_server, None).await?;
    let team_id = lead.test.session_configured.thread_id;
    responses::mount_sse_sequence(
        &lead_server,
        vec![responses::sse(vec![
            responses::ev_response_created("r1"),
            responses::ev_assistant_message("r1", "Working."),
            responses::ev_completed_with_tokens("r1", 80),
        ])],
    )
    .await;

    lead.test
        .codex
        .start_or_steer_turn(TurnInputRequest::user_input(vec![UserInput::Text {
            text: "Use the budget.".into(),
            text_elements: Vec::new(),
        }]))
        .await?;
    wait_for_event(&lead.test.codex, |event| {
        matches!(
            event,
            EventMsg::Error(error)
                if error.codex_error_info == Some(CodexErrorInfo::SessionBudgetExceeded)
        )
    })
    .await;
    assert!(relay.record_usage(team_id, team_id, 0).await?);
    lead.test.codex.shutdown_and_wait().await?;
    Ok(())
}
