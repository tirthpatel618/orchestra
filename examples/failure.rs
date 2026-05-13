use orchestra::{FakeTask, Flow, NodeTrace, Pipeline, RuntimeEvent};
use std::time::Duration;

/*
    source_ok ───────> report

    source_bad ──────> blocked
          x fails

    independent completes before the run fails.
    slow_sibling starts, then gets cancelled when source_bad fails.
*/

#[tokio::main]
async fn main() {
    let flow = build_flow();

    println!("Event stream");
    let mut events = Pipeline::new(flow.clone()).run();
    while let Some(event) = events.recv().await {
        println!("{event:?}");
        if matches!(
            event,
            RuntimeEvent::RunCompleted { .. } | RuntimeEvent::RunFailed { .. }
        ) {
            break;
        }
    }

    println!("\nFailure report");
    let result = Pipeline::new(flow).execute_report().await;
    println!("run_status: {:?}", result.trace.status);
    println!("run_error: {}", result.trace.error.as_deref().unwrap_or(""));
    println!("partial_outputs: {:#?}", result.outputs);
    print_node_trace_summary(result.trace.nodes.values());
}

fn build_flow() -> Flow {
    let mut flow = Flow::new();

    flow.add_node(
        "source_ok",
        FakeTask::new("usable data")
            .delay(Duration::from_millis(80))
            .chunks(["fetch ok"]),
    )
    .unwrap();

    flow.add_node(
        "source_bad",
        FakeTask::new("never returned")
            .delay(Duration::from_millis(120))
            .chunks(["fetch bad", "parse bad"])
            .fail_with("simulated upstream API failure"),
    )
    .unwrap();

    flow.add_node(
        "independent",
        FakeTask::new("side result")
            .delay(Duration::from_millis(50))
            .chunks(["side branch"]),
    )
    .unwrap();

    flow.add_node(
        "slow_sibling",
        FakeTask::new("too slow").delay(Duration::from_secs(5)),
    )
    .unwrap();

    flow.add_node(
        "report",
        FakeTask::new("report").include_dependency_outputs(),
    )
    .unwrap();
    flow.add_dependency("report", "source_ok").unwrap();
    flow.add_dependency("report", "source_bad").unwrap();

    flow.add_node(
        "blocked",
        FakeTask::new("should not run").include_dependency_outputs(),
    )
    .unwrap();
    flow.add_dependency("blocked", "report").unwrap();

    flow
}

fn print_node_trace_summary<'a>(nodes: impl Iterator<Item = &'a NodeTrace>) {
    let mut nodes = nodes.collect::<Vec<_>>();
    nodes.sort_by(|left, right| {
        left.started_at_ms
            .cmp(&right.started_at_ms)
            .then(left.node.cmp(&right.node))
    });

    for node in nodes {
        println!(
            "{:<12} status={:?} duration_ms={:<4} deps={:?} output={:?} error={:?}",
            node.node, node.status, node.duration_ms, node.dependencies, node.output, node.error
        );
    }
}
