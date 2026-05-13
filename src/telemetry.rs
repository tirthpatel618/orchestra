use crate::NodeId;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RunStatus {
    Completed,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeStatus {
    Completed,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunTrace {
    pub status: RunStatus,
    pub started_at_ms: u128,
    pub completed_at_ms: u128,
    pub duration_ms: u128,
    pub nodes: HashMap<NodeId, NodeTrace>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeTrace {
    pub node: NodeId,
    pub dependencies: Vec<NodeId>,
    pub status: NodeStatus,
    pub started_at_ms: u128,
    pub completed_at_ms: u128,
    pub duration_ms: u128,
    pub output: Option<String>,
    pub error: Option<String>,
}

pub(crate) fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_millis()
}
