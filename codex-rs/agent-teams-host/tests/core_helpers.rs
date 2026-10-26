//! Public helpers in `codex_core::agent_host` must produce the same model-visible text and wake
//! mode as local delivery.
#![allow(clippy::expect_used)]

use codex_core::AgentMessage;
use codex_core::MessageDeliveryMode;
use codex_core::agent_host::agent_completion_communication;
use codex_core::agent_host::agent_message_communication;
use codex_protocol::AgentPath;
use codex_protocol::protocol::AgentStatus;
use pretty_assertions::assert_eq;

fn path(value: &str) -> AgentPath {
    AgentPath::try_from(value).expect("valid agent path")
}

#[test]
fn completion_reports_to_the_immediate_parent_without_waking_it() {
    let communication = agent_completion_communication(
        &path("/root/mail"),
        &AgentStatus::Completed(Some("Sent.".to_string())),
    )
    .expect("completed children report");
    assert_eq!(
        (
            communication.author,
            communication.recipient,
            communication.trigger_turn
        ),
        (path("/root/mail"), path("/root"), false)
    );
    assert!(communication.content.contains("Sent."));
}

#[test]
fn completion_skips_statuses_the_local_backend_skips() {
    for status in [
        AgentStatus::PendingInit,
        AgentStatus::Running,
        AgentStatus::Interrupted,
    ] {
        assert_eq!(
            agent_completion_communication(&path("/root/mail"), &status),
            None
        );
    }
    assert_eq!(
        agent_completion_communication(&AgentPath::root(), &AgentStatus::Completed(None)),
        None
    );
}

#[test]
fn messages_keep_their_wake_mode() {
    for (mode, trigger_turn) in [
        (MessageDeliveryMode::QueueOnly, false),
        (MessageDeliveryMode::TriggerTurn, true),
    ] {
        let communication = agent_message_communication(
            AgentMessage::Plaintext("hello".to_string()),
            path("/root"),
            path("/root/mail"),
            mode,
        );
        assert_eq!(communication.trigger_turn, trigger_turn);
        assert!(communication.content.contains("hello"));
    }
}
