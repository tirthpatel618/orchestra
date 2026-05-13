use orchestra::{FakeTask, Flow, Pipeline, RuntimeEvent};
use std::time::Duration;

#[tokio::main]
async fn main() {
    let mut flow = Flow::new();
    flow.add_node(
        "researcher",
        FakeTask::new("research notes")
            .delay(Duration::from_millis(100))
            .chunks(["searching", "summarizing"]),
    )
    .unwrap();
    flow.add_node(
        "writer",
        FakeTask::new("draft").include_dependency_outputs(),
    )
    .unwrap();
    flow.add_dependency("writer", "researcher").unwrap();

    let mut events = Pipeline::new(flow).run();
    while let Some(event) = events.recv().await {
        println!("{event:?}");
        if matches!(
            event,
            RuntimeEvent::RunCompleted { .. } | RuntimeEvent::RunFailed { .. }
        ) {
            break;
        }
    }
}
