use crate::{
    telemetry::now_ms, Flow, NodeId, NodeStatus, NodeTrace, OrchestraError, RunStatus, RunTrace,
    RuntimeEvent, TaskInput,
};
use std::{
    collections::{HashMap, VecDeque},
    time::Instant,
};
use tokio::{sync::mpsc, task::JoinSet};

pub type RunOutput = HashMap<NodeId, String>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunResult {
    pub outputs: RunOutput,
    pub trace: RunTrace,
}

impl RunResult {
    pub fn trace_json(&self) -> Result<String, serde_json::Error> {
        self.trace.to_json()
    }

    pub fn trace_json_pretty(&self) -> Result<String, serde_json::Error> {
        self.trace.to_json_pretty()
    }
}

#[derive(Debug, Clone)]
pub struct Pipeline {
    flow: Flow,
    event_buffer: usize,
    max_concurrency: Option<usize>,
}

impl Pipeline {
    pub fn new(flow: Flow) -> Self {
        Self {
            flow,
            event_buffer: 64,
            max_concurrency: None,
        }
    }

    pub fn with_event_buffer(mut self, event_buffer: usize) -> Self {
        self.event_buffer = event_buffer.max(1);
        self
    }

    pub fn with_max_concurrency(mut self, max_concurrency: usize) -> Self {
        self.max_concurrency = Some(max_concurrency.max(1));
        self
    }

    pub async fn execute(&self) -> Result<RunOutput, OrchestraError> {
        let (result, error) = run_flow(
            self.flow.clone(),
            None,
            self.event_buffer,
            self.max_concurrency,
        )
        .await;
        match error {
            Some(error) => Err(error),
            None => Ok(result.outputs),
        }
    }

    pub async fn execute_with_trace(&self) -> Result<RunResult, OrchestraError> {
        let (result, error) = run_flow(
            self.flow.clone(),
            None,
            self.event_buffer,
            self.max_concurrency,
        )
        .await;
        match error {
            Some(error) => Err(error),
            None => Ok(result),
        }
    }

    pub async fn execute_report(&self) -> RunResult {
        let (result, _) = run_flow(
            self.flow.clone(),
            None,
            self.event_buffer,
            self.max_concurrency,
        )
        .await;
        result
    }

    pub fn run(&self) -> mpsc::Receiver<RuntimeEvent> {
        let flow = self.flow.clone();
        let event_buffer = self.event_buffer;
        let max_concurrency = self.max_concurrency;
        let (events, receiver) = mpsc::channel(event_buffer);

        tokio::spawn(async move {
            let _ = run_flow(flow, Some(events), event_buffer, max_concurrency).await;
        });

        receiver
    }
}

async fn run_flow(
    flow: Flow,
    events: Option<mpsc::Sender<RuntimeEvent>>,
    event_buffer: usize,
    max_concurrency: Option<usize>,
) -> (RunResult, Option<OrchestraError>) {
    let run_started_at_ms = now_ms();
    let run_started_at = Instant::now();
    let max_concurrency = max_concurrency.unwrap_or(usize::MAX).max(1);

    if let Err(error) = flow.validate() {
        send_event(
            &events,
            RuntimeEvent::RunFailed {
                error: error.to_string(),
            },
        )
        .await;
        return (
            build_run_result(
                RunStatus::Failed,
                RunOutput::new(),
                HashMap::new(),
                run_started_at_ms,
                run_started_at,
                Some(error.to_string()),
                1,
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
    let mut running_nodes = HashMap::new();
    let mut running = JoinSet::new();
    let (task_event_tx, mut task_events) = mpsc::channel(event_buffer.max(1));

    loop {
        while running_nodes.len() < max_concurrency {
            let Some(node_id) = ready.pop_front() else {
                break;
            };

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
                    build_run_result(
                        RunStatus::Failed,
                        outputs,
                        node_traces,
                        run_started_at_ms,
                        run_started_at,
                        Some(error.to_string()),
                        2,
                    ),
                    Some(error),
                );
            };

            let dependencies = node.dependencies.clone();
            let dependency_outputs = dependencies
                .iter()
                .filter_map(|dependency| {
                    outputs
                        .get(dependency)
                        .map(|output| (dependency.clone(), output.clone()))
                })
                .collect();
            let started_at_ms = now_ms();
            let started_at = Instant::now();

            running_nodes.insert(
                node_id.clone(),
                RunningNode {
                    node: node_id.clone(),
                    dependencies,
                    started_at_ms,
                    started_at,
                    event_count: 1,
                    streamed_chunk_count: 0,
                },
            );

            send_event(
                &events,
                RuntimeEvent::NodeStarted {
                    node: node_id.clone(),
                },
            )
            .await;

            let node_id_for_task = node_id.clone();
            let task_event_tx = task_event_tx.clone();
            running.spawn(async move {
                let input = TaskInput {
                    node: node_id_for_task.clone(),
                    dependency_outputs,
                };
                let result = node.task.execute(input, Some(task_event_tx)).await;

                match result {
                    Ok(output) => NodeRunResult::Completed {
                        node: node_id_for_task,
                        output,
                    },
                    Err(error) => NodeRunResult::Failed {
                        node: node_id_for_task,
                        error,
                    },
                }
            });
        }

        if outputs.len() == flow.nodes().len() {
            let result = build_run_result(
                RunStatus::Completed,
                outputs,
                node_traces,
                run_started_at_ms,
                run_started_at,
                None,
                2,
            );
            send_event(
                &events,
                RuntimeEvent::RunCompleted {
                    outputs: result.outputs.clone(),
                },
            )
            .await;
            return (result, None);
        }

        if running_nodes.is_empty() {
            let error = OrchestraError::CycleDetected;
            send_event(
                &events,
                RuntimeEvent::RunFailed {
                    error: error.to_string(),
                },
            )
            .await;
            return (
                build_run_result(
                    RunStatus::Failed,
                    outputs,
                    node_traces,
                    run_started_at_ms,
                    run_started_at,
                    Some(error.to_string()),
                    2,
                ),
                Some(error),
            );
        }

        tokio::select! {
            Some(task_event) = task_events.recv() => {
                handle_task_event(task_event, &mut running_nodes, &events).await;
            }
            joined = running.join_next() => {
                drain_task_events(&mut task_events, &mut running_nodes, &events).await;

                let Some(joined) = joined else {
                    continue;
                };

                let node_result = match joined {
                    Ok(node_result) => node_result,
                    Err(error) => {
                        let error = OrchestraError::SchedulerJoin(error.to_string());
                        cancel_running_nodes(
                            &mut running,
                            &mut task_events,
                            &mut running_nodes,
                            &mut node_traces,
                            &events,
                            error.to_string(),
                        )
                        .await;
                        send_event(
                            &events,
                            RuntimeEvent::RunFailed {
                                error: error.to_string(),
                            },
                        )
                        .await;
                        return (
                            build_run_result(
                                RunStatus::Failed,
                                outputs,
                                node_traces,
                                run_started_at_ms,
                                run_started_at,
                                Some(error.to_string()),
                                2,
                            ),
                            Some(error),
                        );
                    }
                };

                match node_result {
                    NodeRunResult::Completed { node, output } => {
                        let Some(running_node) = running_nodes.remove(&node) else {
                            continue;
                        };
                        let trace = completed_node_trace(running_node, output.clone());
                        node_traces.insert(node.clone(), trace);
                        outputs.insert(node.clone(), output.clone());

                        send_event(
                            &events,
                            RuntimeEvent::NodeCompleted {
                                node: node.clone(),
                                output,
                            },
                        )
                        .await;

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
                    NodeRunResult::Failed { node, error } => {
                        if let Some(running_node) = running_nodes.remove(&node) {
                            let trace = failed_node_trace(running_node, error.to_string());
                            node_traces.insert(node.clone(), trace);
                        }

                        send_event(
                            &events,
                            RuntimeEvent::NodeFailed {
                                node,
                                error: error.to_string(),
                            },
                        )
                        .await;

                        cancel_running_nodes(
                            &mut running,
                            &mut task_events,
                            &mut running_nodes,
                            &mut node_traces,
                            &events,
                            error.to_string(),
                        )
                        .await;

                        send_event(
                            &events,
                            RuntimeEvent::RunFailed {
                                error: error.to_string(),
                            },
                        )
                        .await;
                        return (
                            build_run_result(
                                RunStatus::Failed,
                                outputs,
                                node_traces,
                                run_started_at_ms,
                                run_started_at,
                                Some(error.to_string()),
                                2,
                            ),
                            Some(error),
                        );
                    }
                }
            }
        }
    }
}

async fn cancel_running_nodes(
    running: &mut JoinSet<NodeRunResult>,
    task_events: &mut mpsc::Receiver<RuntimeEvent>,
    running_nodes: &mut HashMap<NodeId, RunningNode>,
    node_traces: &mut HashMap<NodeId, NodeTrace>,
    events: &Option<mpsc::Sender<RuntimeEvent>>,
    reason: String,
) {
    running.abort_all();
    drain_task_events(task_events, running_nodes, events).await;

    let node_ids = running_nodes.keys().cloned().collect::<Vec<_>>();
    for node in node_ids {
        let Some(running_node) = running_nodes.remove(&node) else {
            continue;
        };

        let trace = cancelled_node_trace(running_node, reason.clone());
        node_traces.insert(node.clone(), trace);
        send_event(
            events,
            RuntimeEvent::NodeCancelled {
                node,
                reason: reason.clone(),
            },
        )
        .await;
    }
}

async fn drain_task_events(
    task_events: &mut mpsc::Receiver<RuntimeEvent>,
    running_nodes: &mut HashMap<NodeId, RunningNode>,
    events: &Option<mpsc::Sender<RuntimeEvent>>,
) {
    while let Ok(task_event) = task_events.try_recv() {
        handle_task_event(task_event, running_nodes, events).await;
    }
}

async fn handle_task_event(
    event: RuntimeEvent,
    running_nodes: &mut HashMap<NodeId, RunningNode>,
    events: &Option<mpsc::Sender<RuntimeEvent>>,
) {
    if let Some(node) = event_node(&event) {
        if let Some(running_node) = running_nodes.get_mut(node) {
            running_node.event_count += 1;
            if matches!(event, RuntimeEvent::NodeOutput { .. }) {
                running_node.streamed_chunk_count += 1;
            }
        }
    }

    send_event(events, event).await;
}

fn event_node(event: &RuntimeEvent) -> Option<&NodeId> {
    match event {
        RuntimeEvent::NodeStarted { node }
        | RuntimeEvent::NodeOutput { node, .. }
        | RuntimeEvent::NodeCompleted { node, .. }
        | RuntimeEvent::NodeFailed { node, .. }
        | RuntimeEvent::NodeCancelled { node, .. } => Some(node),
        RuntimeEvent::RunStarted
        | RuntimeEvent::RunCompleted { .. }
        | RuntimeEvent::RunFailed { .. } => None,
    }
}

fn completed_node_trace(mut running_node: RunningNode, output: String) -> NodeTrace {
    running_node.event_count += 1;
    NodeTrace {
        node: running_node.node,
        dependencies: running_node.dependencies,
        status: NodeStatus::Completed,
        started_at_ms: running_node.started_at_ms,
        completed_at_ms: now_ms(),
        duration_ms: running_node.started_at.elapsed().as_millis(),
        event_count: running_node.event_count,
        streamed_chunk_count: running_node.streamed_chunk_count,
        output: Some(output),
        error: None,
    }
}

fn failed_node_trace(mut running_node: RunningNode, error: String) -> NodeTrace {
    running_node.event_count += 1;
    NodeTrace {
        node: running_node.node,
        dependencies: running_node.dependencies,
        status: NodeStatus::Failed,
        started_at_ms: running_node.started_at_ms,
        completed_at_ms: now_ms(),
        duration_ms: running_node.started_at.elapsed().as_millis(),
        event_count: running_node.event_count,
        streamed_chunk_count: running_node.streamed_chunk_count,
        output: None,
        error: Some(error),
    }
}

fn cancelled_node_trace(mut running_node: RunningNode, reason: String) -> NodeTrace {
    running_node.event_count += 1;
    NodeTrace {
        node: running_node.node,
        dependencies: running_node.dependencies,
        status: NodeStatus::Cancelled,
        started_at_ms: running_node.started_at_ms,
        completed_at_ms: now_ms(),
        duration_ms: running_node.started_at.elapsed().as_millis(),
        event_count: running_node.event_count,
        streamed_chunk_count: running_node.streamed_chunk_count,
        output: None,
        error: Some(reason),
    }
}

fn build_run_result(
    status: RunStatus,
    outputs: RunOutput,
    nodes: HashMap<NodeId, NodeTrace>,
    started_at_ms: u128,
    started_at: Instant,
    error: Option<String>,
    run_event_count: u64,
) -> RunResult {
    let node_event_count = nodes.values().map(|node| node.event_count).sum::<u64>();
    let streamed_chunk_count = nodes
        .values()
        .map(|node| node.streamed_chunk_count)
        .sum::<u64>();

    RunResult {
        outputs,
        trace: RunTrace {
            status,
            started_at_ms,
            completed_at_ms: now_ms(),
            duration_ms: started_at.elapsed().as_millis(),
            event_count: run_event_count + node_event_count,
            streamed_chunk_count,
            nodes,
            error,
        },
    }
}

async fn send_event(events: &Option<mpsc::Sender<RuntimeEvent>>, event: RuntimeEvent) {
    if let Some(events) = events {
        let _ = events.send(event).await;
    }
}

#[derive(Debug)]
struct RunningNode {
    node: NodeId,
    dependencies: Vec<NodeId>,
    started_at_ms: u128,
    started_at: Instant,
    event_count: u64,
    streamed_chunk_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum NodeRunResult {
    Completed { node: NodeId, output: String },
    Failed { node: NodeId, error: OrchestraError },
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
    async fn max_concurrency_one_runs_independent_nodes_sequentially() {
        let mut flow = Flow::new();
        flow.add_node("a", FakeTask::new("a").delay(Duration::from_millis(80)))
            .unwrap();
        flow.add_node("b", FakeTask::new("b").delay(Duration::from_millis(80)))
            .unwrap();

        let start = Instant::now();
        let outputs = Pipeline::new(flow)
            .with_max_concurrency(1)
            .execute()
            .await
            .unwrap();

        assert_eq!(outputs.len(), 2);
        assert!(start.elapsed() >= Duration::from_millis(145));
    }

    #[tokio::test]
    async fn max_concurrency_two_limits_initial_starts() {
        let mut flow = Flow::new();
        flow.add_node("a", FakeTask::new("a").delay(Duration::from_millis(80)))
            .unwrap();
        flow.add_node("b", FakeTask::new("b").delay(Duration::from_millis(80)))
            .unwrap();
        flow.add_node("c", FakeTask::new("c").delay(Duration::from_millis(80)))
            .unwrap();

        let mut events = Pipeline::new(flow).with_max_concurrency(2).run();
        let mut starts_before_first_completion = 0;
        while let Some(event) = events.recv().await {
            match event {
                RuntimeEvent::NodeStarted { .. } => starts_before_first_completion += 1,
                RuntimeEvent::NodeCompleted { .. } => break,
                _ => {}
            }
        }

        assert_eq!(starts_before_first_completion, 2);
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
    async fn failed_node_cancels_running_siblings() {
        let mut flow = Flow::new();
        flow.add_node("fail", FakeTask::new("fail").fail_with("boom"))
            .unwrap();
        flow.add_node(
            "slow",
            FakeTask::new("slow")
                .delay(Duration::from_secs(5))
                .chunks(["still working"]),
        )
        .unwrap();
        flow.add_node("blocked", FakeTask::new("blocked")).unwrap();
        flow.add_dependency("blocked", "fail").unwrap();

        let result = Pipeline::new(flow).execute_report().await;

        assert_eq!(result.trace.status, RunStatus::Failed);
        assert_eq!(result.trace.nodes["fail"].status, NodeStatus::Failed);
        assert_eq!(result.trace.nodes["slow"].status, NodeStatus::Cancelled);
        assert!(!result.trace.nodes.contains_key("blocked"));
    }

    #[tokio::test]
    async fn run_stream_emits_cancelled_events() {
        let mut flow = Flow::new();
        flow.add_node("fail", FakeTask::new("fail").fail_with("boom"))
            .unwrap();
        flow.add_node("slow", FakeTask::new("slow").delay(Duration::from_secs(5)))
            .unwrap();

        let mut events = Pipeline::new(flow).run();
        let mut seen_cancelled = false;
        while let Some(event) = events.recv().await {
            if matches!(event, RuntimeEvent::NodeCancelled { .. }) {
                seen_cancelled = true;
            }
            if matches!(event, RuntimeEvent::RunFailed { .. }) {
                break;
            }
        }

        assert!(seen_cancelled);
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
        assert_eq!(result.trace.event_count, 6);

        let b_trace = &result.trace.nodes["b"];
        assert_eq!(b_trace.node, "b");
        assert_eq!(b_trace.dependencies, vec!["a".to_string()]);
        assert_eq!(b_trace.status, NodeStatus::Completed);
        assert_eq!(b_trace.event_count, 2);
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

    #[tokio::test]
    async fn trace_counts_streamed_chunks() {
        let mut flow = Flow::new();
        flow.add_node("a", FakeTask::new("done").chunks(["one", "two"]))
            .unwrap();

        let result = Pipeline::new(flow).execute_with_trace().await.unwrap();

        assert_eq!(result.trace.streamed_chunk_count, 2);
        assert_eq!(result.trace.nodes["a"].streamed_chunk_count, 2);
        assert_eq!(result.trace.nodes["a"].event_count, 4);
    }

    #[tokio::test]
    async fn trace_serializes_to_json() {
        let mut flow = Flow::new();
        flow.add_node("a", FakeTask::new("alpha")).unwrap();

        let result = Pipeline::new(flow).execute_with_trace().await.unwrap();
        let json = result.trace_json_pretty().unwrap();
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();

        assert_eq!(value["status"], "Completed");
        assert_eq!(value["nodes"]["a"]["status"], "Completed");
    }
}
