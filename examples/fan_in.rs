use orchestra::{FakeTask, Flow, Pipeline, RuntimeEvent};
use std::time::Duration;

#[tokio::main]
async fn main() {
    let mut flow = Flow::new();
    flow.add_node(
        "research",
        FakeTask::new("facts")
            .delay(Duration::from_millis(80))
            .chunks(["researching"]),
    )
    .unwrap();
    flow.add_node(
        "outline",
        FakeTask::new("structure")
            .delay(Duration::from_millis(120))
            .chunks(["outlining"]),
    )
    .unwrap();
    flow.add_node(
        "writer",
        FakeTask::new("article").include_dependency_outputs(),
    )
    .unwrap();
    flow.add_dependency("writer", "research").unwrap();
    flow.add_dependency("writer", "outline").unwrap();

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
