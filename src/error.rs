use crate::NodeId;
use thiserror::Error;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum OrchestraError {
    #[error("duplicate node id: {0}")]
    DuplicateNode(NodeId),

    #[error("missing dependency: node '{node}' depends on unknown node '{dependency}'")]
    MissingDependency { node: NodeId, dependency: NodeId },

    #[error("missing node: {0}")]
    MissingNode(NodeId),

    #[error("cycle detected in flow")]
    CycleDetected,

    #[error("node '{node}' failed: {message}")]
    NodeFailed { node: NodeId, message: String },

    #[error("scheduler task failed: {0}")]
    SchedulerJoin(String),
}
