use crate::{task::TaskFuture, OrchestraError, RuntimeEvent, Task, TaskInput};
use std::time::Duration;
use tokio::{sync::mpsc, time::sleep};

#[derive(Debug, Clone)]
pub struct FakeTask {
    delay: Duration,
    output: String,
    chunks: Vec<String>,
    failure: Option<String>,
    include_dependency_outputs: bool,
}

impl FakeTask {
    pub fn new(output: impl Into<String>) -> Self {
        Self {
            delay: Duration::ZERO,
            output: output.into(),
            chunks: Vec::new(),
            failure: None,
            include_dependency_outputs: false,
        }
    }

    pub fn delay(mut self, delay: Duration) -> Self {
        self.delay = delay;
        self
    }

    pub fn chunks<I, S>(mut self, chunks: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.chunks = chunks.into_iter().map(Into::into).collect();
        self
    }

    pub fn fail_with(mut self, message: impl Into<String>) -> Self {
        self.failure = Some(message.into());
        self
    }

    pub fn include_dependency_outputs(mut self) -> Self {
        self.include_dependency_outputs = true;
        self
    }
}

impl Task for FakeTask {
    fn execute<'a>(
        &'a self,
        input: TaskInput,
        events: Option<mpsc::Sender<RuntimeEvent>>,
    ) -> TaskFuture<'a> {
        Box::pin(async move {
            if !self.delay.is_zero() {
                sleep(self.delay).await;
            }

            for chunk in &self.chunks {
                if let Some(events) = &events {
                    let _ = events
                        .send(RuntimeEvent::NodeOutput {
                            node: input.node.clone(),
                            chunk: chunk.clone(),
                        })
                        .await;
                }
            }

            if let Some(message) = &self.failure {
                return Err(OrchestraError::NodeFailed {
                    node: input.node,
                    message: message.clone(),
                });
            }

            if !self.include_dependency_outputs {
                return Ok(self.output.clone());
            }

            let mut dependencies = input
                .dependency_outputs
                .iter()
                .map(|(node, output)| format!("{node}={output}"))
                .collect::<Vec<_>>();
            dependencies.sort();

            if dependencies.is_empty() {
                Ok(self.output.clone())
            } else {
                Ok(format!("{} [{}]", self.output, dependencies.join(", ")))
            }
        })
    }
}
