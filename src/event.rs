use crate::{LlmUsage, NodeId, RunOutput};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RuntimeEvent {
    RunStarted,
    NodeStarted { node: NodeId },
    NodeOutput { node: NodeId, chunk: String },
    NodeLlmUsage { node: NodeId, usage: LlmUsage },
    NodeCompleted { node: NodeId, output: String },
    NodeFailed { node: NodeId, error: String },
    NodeCancelled { node: NodeId, reason: String },
    RunCompleted { outputs: RunOutput },
    RunFailed { error: String },
}
