use crate::{NodeId, OrchestraError, RuntimeEvent};
use std::{collections::HashMap, future::Future, pin::Pin};
use tokio::sync::mpsc;

pub type TaskOutput = String;
pub type TaskFuture<'a> =
    Pin<Box<dyn Future<Output = Result<TaskOutput, OrchestraError>> + Send + 'a>>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskInput {
    pub node: NodeId,
    pub dependency_outputs: HashMap<NodeId, String>,
}

pub trait Task: Send + Sync {
    fn execute<'a>(
        &'a self,
        input: TaskInput,
        events: Option<mpsc::Sender<RuntimeEvent>>,
    ) -> TaskFuture<'a>;
}
