use crate::{
    telemetry::now_ms, Flow, NodeId, NodeStatus, NodeTrace, OrchestraError, RunStatus, RunTrace,
    RuntimeEvent, TaskInput,
};
use std::collections::{HashMap, VecDeque};
use std::time::Instant;
use tokio::{sync::mpsc, task::JoinSet};

pub type RunOutput = HashMap<NodeId, String>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunResult {
    pub outputs: RunOutput,
    pub trace: RunTrace,
}

#[derive(Debug, Clone)]
pub struct Pipeline {
    flow: Flow,
    event_buffer: usize,
}

impl Pipeline {
    pub fn new(flow: Flow) -> Self {
        Self {
            flow,
            event_buffer: 64,
        }
    }

    pub fn with_event_buffer(mut self, event_buffer: usize) -> Self {
        self.event_buffer = event_buffer.max(1);
        self
    }

    pub async fn execute(&self) -> Result<RunOutput, OrchestraError> {
        let (result, error) = run_flow(self.flow.clone(), None).await;
        match error {
            Some(error) => Err(error),
            None => Ok(result.outputs),
        }
    }

    pub async fn execute_with_trace(&self) -> Result<RunResult, OrchestraError> {
        let (result, error) = run_flow(self.flow.clone(), None).await;
        match error {
            Some(error) => Err(error),
            None => Ok(result),
        }
    }

    pub async fn execute_report(&self) -> RunResult {
        let (result, _) = run_flow(self.flow.clone(), None).await;
        result
    }

    pub fn run(&self) -> mpsc::Receiver<RuntimeEvent> {
        let flow = self.flow.clone();
        let (events, receiver) = mpsc::channel(self.event_buffer);

        tokio::spawn(async move {
            let _ = run_flow(flow, Some(events)).await;
        });

        receiver
    }
}

async fn run_flow(
    flow: Flow,
    events: Option<mpsc::Sender<RuntimeEvent>>,
) -> (RunResult, Option<OrchestraError>) {
    let run_started_at_ms = now_ms();
    let run_started_at = Instant::now();

    if let Err(error) = flow.validate() {
        send_event(
            &events,
            RuntimeEvent::RunFailed {
                error: error.to_string(),
            },
        )
        .await;
        return (
            failed_run_result(
                RunOutput::new(),
                HashMap::new(),
                run_started_at_ms,
                run_started_at,
                error.to_string(),
            ),
            Some(error),
        );
    }

    send_event(&events, RuntimeEvent::RunStarted).await;

    let mut remaining_dependencies = HashMap::new();
    let mut dependents: HashMap<NodeId, Vec<NodeId>> = HashMap::new();

    for (id, node) in flow.nodes() {
        remaining_dependencies.insert(id.clone(), node.dependencies.len());
        for dependency in &node.dependencies {
            dependents
                .entry(dependency.clone())
                .or_default()
                .push(id.clone());
        }
    }

    let mut ready = remaining_dependencies
        .iter()
        .filter_map(|(id, count)| (*count == 0).then_some(id.clone()))
        .collect::<VecDeque<_>>();
    let mut outputs = RunOutput::new();
    let mut node_traces = HashMap::new();
    let mut running = JoinSet::new();

    loop {
        while let Some(node_id) = ready.pop_front() {
            let Some(node) = flow.nodes().get(&node_id).cloned() else {
                let error = OrchestraError::MissingNode(node_id);
                send_event(
                    &events,
                    RuntimeEvent::RunFailed {
                        error: error.to_string(),
                    },
                )
                .await;
                return (
                    failed_run_result(
                        outputs,
                        node_traces,
                        run_started_at_ms,
                        run_started_at,
                        error.to_string(),
                    ),
                    Some(error),
                );
            };
            let node_dependencies = node.dependencies.clone();
            let dependency_outputs = node
                .dependencies
                .iter()
                .filter_map(|dependency| {
                    outputs
                        .get(dependency)
                        .map(|output| (dependency.clone(), output.clone()))
                })
                .collect();
            let task_events = events.clone();

            running.spawn(async move {
                let started_at_ms = now_ms();
                let started_at = Instant::now();
                send_event_to_sender(
                    &task_events,
                    RuntimeEvent::NodeStarted {
                        node: node_id.clone(),
                    },
                )
                .await;

                let input = TaskInput {
                    node: node_id.clone(),
                    dependency_outputs,
                };
                let result = node.task.execute(input, task_events.clone()).await;
                let completed_at_ms = now_ms();
                let duration_ms = started_at.elapsed().as_millis();

                match result {
                    Ok(output) => {
                        let trace = NodeTrace {
                            node: node_id.clone(),
                            dependencies: node_dependencies,
                            status: NodeStatus::Completed,
                            started_at_ms,
                            completed_at_ms,
                            duration_ms,
                            output: Some(output.clone()),
                            error: None,
                        };
                        send_event_to_sender(
                            &task_events,
                            RuntimeEvent::NodeCompleted {
                                node: node_id.clone(),
                                output: output.clone(),
                            },
                        )
                        .await;
                        NodeRunResult::Completed {
                            node: node_id,
                            output,
                            trace,
                        }
                    }
                    Err(error) => {
                        let trace = NodeTrace {
                            node: node_id.clone(),
                            dependencies: node_dependencies,
                            status: NodeStatus::Failed,
                            started_at_ms,
                            completed_at_ms,
                            duration_ms,
                            output: None,
                            error: Some(error.to_string()),
                        };
                        send_event_to_sender(
                            &task_events,
                            RuntimeEvent::NodeFailed {
                                node: node_id.clone(),
                                error: error.to_string(),
                            },
                        )
                        .await;
                        NodeRunResult::Failed {
                            node: node_id,
                            error,
                            trace,
                        }
                    }
                }
            });
        }

        if outputs.len() == flow.nodes().len() {
            let trace = RunTrace {
                status: RunStatus::Completed,
                started_at_ms: run_started_at_ms,
                completed_at_ms: now_ms(),
                duration_ms: run_started_at.elapsed().as_millis(),
                nodes: node_traces,
                error: None,
            };
            send_event(
                &events,
                RuntimeEvent::RunCompleted {
                    outputs: outputs.clone(),
                },
            )
            .await;
            return (RunResult { outputs, trace }, None);
        }

        let Some(joined) = running.join_next().await else {
            let error = OrchestraError::CycleDetected;
            send_event(
                &events,
                RuntimeEvent::RunFailed {
                    error: error.to_string(),
                },
            )
            .await;
            return (
                failed_run_result(
                    outputs,
                    node_traces,
                    run_started_at_ms,
                    run_started_at,
                    error.to_string(),
                ),
                Some(error),
            );
        };

        let node_result = match joined {
            Ok(node_result) => node_result,
            Err(error) => {
                let error = OrchestraError::SchedulerJoin(error.to_string());
                send_event(
                    &events,
                    RuntimeEvent::RunFailed {
                        error: error.to_string(),
                    },
                )
                .await;
                return (
                    failed_run_result(
                        outputs,
                        node_traces,
                        run_started_at_ms,
                        run_started_at,
                        error.to_string(),
                    ),
                    Some(error),
                );
            }
        };

        match node_result {
            NodeRunResult::Completed {
                node,
                output,
                trace,
            } => {
                node_traces.insert(node.clone(), trace);
                outputs.insert(node.clone(), output);

                if let Some(children) = dependents.get(&node) {
                    for child in children {
                        if let Some(count) = remaining_dependencies.get_mut(child) {
                            *count -= 1;
                            if *count == 0 {
                                ready.push_back(child.clone());
                            }
                        }
                    }
                }
            }
            NodeRunResult::Failed { node, error, trace } => {
                node_traces.insert(node, trace);
                send_event(
                    &events,
                    RuntimeEvent::RunFailed {
                        error: error.to_string(),
                    },
                )
                .await;
                return (
                    failed_run_result(
                        outputs,
                        node_traces,
                        run_started_at_ms,
                        run_started_at,
                        error.to_string(),
                    ),
                    Some(error),
                );
            }
        }
    }
}

fn failed_run_result(
    outputs: RunOutput,
    nodes: HashMap<NodeId, NodeTrace>,
    started_at_ms: u128,
    started_at: Instant,
    error: String,
) -> RunResult {
    RunResult {
        outputs,
        trace: RunTrace {
            status: RunStatus::Failed,
            started_at_ms,
            completed_at_ms: now_ms(),
            duration_ms: started_at.elapsed().as_millis(),
            nodes,
            error: Some(error),
        },
    }
}

async fn send_event(events: &Option<mpsc::Sender<RuntimeEvent>>, event: RuntimeEvent) {
    send_event_to_sender(events, event).await;
}

async fn send_event_to_sender(events: &Option<mpsc::Sender<RuntimeEvent>>, event: RuntimeEvent) {
    if let Some(events) = events {
        let _ = events.send(event).await;
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum NodeRunResult {
    Completed {
        node: NodeId,
        output: String,
        trace: NodeTrace,
    },
    Failed {
        node: NodeId,
        error: OrchestraError,
        trace: NodeTrace,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{FakeTask, NodeStatus, RunStatus};
    use std::time::{Duration, Instant};

    #[tokio::test]
    async fn runs_sequential_graph() {
        let mut flow = Flow::new();
        flow.add_node("researcher", FakeTask::new("research"))
            .unwrap();
        flow.add_node(
            "writer",
            FakeTask::new("write").include_dependency_outputs(),
        )
        .unwrap();
        flow.add_dependency("writer", "researcher").unwrap();

        let outputs = Pipeline::new(flow).execute().await.unwrap();

        assert_eq!(outputs["researcher"], "research");
        assert_eq!(outputs["writer"], "write [researcher=research]");
    }

    #[tokio::test]
    async fn runs_independent_nodes_concurrently() {
        let mut flow = Flow::new();
        flow.add_node("a", FakeTask::new("a").delay(Duration::from_millis(150)))
            .unwrap();
        flow.add_node("b", FakeTask::new("b").delay(Duration::from_millis(150)))
            .unwrap();

        let start = Instant::now();
        let outputs = Pipeline::new(flow).execute().await.unwrap();

        assert_eq!(outputs["a"], "a");
        assert_eq!(outputs["b"], "b");
        assert!(start.elapsed() < Duration::from_millis(260));
    }

    #[tokio::test]
    async fn runs_fan_in_graph() {
        let mut flow = Flow::new();
        flow.add_node("a", FakeTask::new("alpha")).unwrap();
        flow.add_node("b", FakeTask::new("beta")).unwrap();
        flow.add_node("c", FakeTask::new("combine").include_dependency_outputs())
            .unwrap();
        flow.add_dependency("c", "a").unwrap();
        flow.add_dependency("c", "b").unwrap();

        let outputs = Pipeline::new(flow).execute().await.unwrap();

        assert_eq!(outputs["c"], "combine [a=alpha, b=beta]");
    }

    #[test]
    fn missing_dependency_is_rejected() {
        let mut flow = Flow::new();
        flow.add_node("writer", FakeTask::new("write")).unwrap();

        let error = flow.add_dependency("writer", "researcher").unwrap_err();

        assert_eq!(
            error,
            OrchestraError::MissingDependency {
                node: "writer".to_string(),
                dependency: "researcher".to_string(),
            }
        );
    }

    #[test]
    fn duplicate_node_is_rejected() {
        let mut flow = Flow::new();
        flow.add_node("a", FakeTask::new("first")).unwrap();

        let error = flow.add_node("a", FakeTask::new("second")).unwrap_err();

        assert_eq!(error, OrchestraError::DuplicateNode("a".to_string()));
    }

    #[tokio::test]
    async fn failed_node_fails_the_run() {
        let mut flow = Flow::new();
        flow.add_node("a", FakeTask::new("a").fail_with("boom"))
            .unwrap();
        flow.add_node("b", FakeTask::new("b")).unwrap();
        flow.add_dependency("b", "a").unwrap();

        let error = Pipeline::new(flow).execute().await.unwrap_err();

        assert_eq!(
            error,
            OrchestraError::NodeFailed {
                node: "a".to_string(),
                message: "boom".to_string(),
            }
        );
    }

    #[tokio::test]
    async fn downstream_node_receives_dependency_outputs() {
        let mut flow = Flow::new();
        flow.add_node("a", FakeTask::new("hello")).unwrap();
        flow.add_node("b", FakeTask::new("world").include_dependency_outputs())
            .unwrap();
        flow.add_dependency("b", "a").unwrap();

        let outputs = Pipeline::new(flow).execute().await.unwrap();

        assert_eq!(outputs["b"], "world [a=hello]");
    }

    #[tokio::test]
    async fn run_stream_emits_runtime_events() {
        let mut flow = Flow::new();
        flow.add_node("a", FakeTask::new("done").chunks(["working"]))
            .unwrap();

        let mut events = Pipeline::new(flow).run();
        let mut seen = Vec::new();
        while let Some(event) = events.recv().await {
            seen.push(event);
        }

        assert!(seen.contains(&RuntimeEvent::RunStarted));
        assert!(seen.contains(&RuntimeEvent::NodeStarted {
            node: "a".to_string()
        }));
        assert!(seen.contains(&RuntimeEvent::NodeOutput {
            node: "a".to_string(),
            chunk: "working".to_string()
        }));
        assert!(seen.contains(&RuntimeEvent::NodeCompleted {
            node: "a".to_string(),
            output: "done".to_string()
        }));
        assert!(seen
            .iter()
            .any(|event| matches!(event, RuntimeEvent::RunCompleted { .. })));
    }

    #[tokio::test]
    async fn cycle_is_rejected_before_execution() {
        let mut flow = Flow::new();
        flow.add_node("a", FakeTask::new("a")).unwrap();
        flow.add_node("b", FakeTask::new("b")).unwrap();
        flow.add_dependency("a", "b").unwrap();
        flow.add_dependency("b", "a").unwrap();

        let error = Pipeline::new(flow).execute().await.unwrap_err();

        assert_eq!(error, OrchestraError::CycleDetected);
    }

    #[tokio::test]
    async fn execute_with_trace_records_completed_nodes() {
        let mut flow = Flow::new();
        flow.add_node("a", FakeTask::new("alpha")).unwrap();
        flow.add_node("b", FakeTask::new("beta").include_dependency_outputs())
            .unwrap();
        flow.add_dependency("b", "a").unwrap();

        let result = Pipeline::new(flow).execute_with_trace().await.unwrap();

        assert_eq!(result.trace.status, RunStatus::Completed);
        assert_eq!(result.trace.error, None);
        assert_eq!(result.outputs["b"], "beta [a=alpha]");

        let b_trace = &result.trace.nodes["b"];
        assert_eq!(b_trace.node, "b");
        assert_eq!(b_trace.dependencies, vec!["a".to_string()]);
        assert_eq!(b_trace.status, NodeStatus::Completed);
        assert_eq!(b_trace.output, Some("beta [a=alpha]".to_string()));
        assert_eq!(b_trace.error, None);
        assert!(b_trace.completed_at_ms >= b_trace.started_at_ms);
    }

    #[tokio::test]
    async fn execute_report_records_failed_nodes() {
        let mut flow = Flow::new();
        flow.add_node("a", FakeTask::new("a").fail_with("boom"))
            .unwrap();
        flow.add_node("b", FakeTask::new("b")).unwrap();
        flow.add_dependency("b", "a").unwrap();

        let result = Pipeline::new(flow).execute_report().await;

        assert_eq!(result.trace.status, RunStatus::Failed);
        assert!(result.trace.error.as_deref().unwrap().contains("boom"));
        assert!(result.outputs.is_empty());

        let a_trace = &result.trace.nodes["a"];
        assert_eq!(a_trace.status, NodeStatus::Failed);
        assert_eq!(a_trace.output, None);
        assert!(a_trace.error.as_deref().unwrap().contains("boom"));
        assert!(!result.trace.nodes.contains_key("b"));
    }
}
