//! Distributed agent teams: node registration, inbound host commands and outbound relay calls.
//!
//! Payloads are the JSON forms of the `codex-agent-teams-wire` messages. They stay opaque here so
//! the relay protocol can evolve with its own version field instead of this schema.

use crate::JsonSchema;
use crate::TS;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value as JsonValue;

/// Make the calling connection this node's link to the team relay.
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct AgentTeamsNodeRegisterParams {}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct AgentTeamsNodeRegisterResponse {
    /// This node's ID, from `CODEX_AGENT_TEAMS_NODE_ID`.
    pub node_id: String,
}

/// Run a relay-dispatched command on a thread this node hosts.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct AgentTeamsHostCommandParams {
    /// A `HostCommandEnvelope`.
    pub envelope: JsonValue,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct AgentTeamsHostCommandResponse {
    /// A `HostReply`.
    pub reply: JsonValue,
}

/// Ask the registered relay link to place, look up or dispatch for this node's threads.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct AgentTeamsRelayParams {
    /// A `RelayRequest`.
    pub request: JsonValue,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct AgentTeamsRelayResponse {
    /// A `RelayResponse`.
    pub response: JsonValue,
}
