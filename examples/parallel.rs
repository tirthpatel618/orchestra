use orchestra::{FakeTask, Flow, Pipeline, RuntimeEvent};
use std::time::Duration;

#[tokio::main]
async fn main() {
    let mut flow = Flow::new();
    flow.add_node(
        "a",
        FakeTask::new("alpha")
            .delay(Duration::from_millis(150))
            .chunks(["a:1", "a:2"]),
    )
    .unwrap();
    flow.add_node(
        "b",
        FakeTask::new("beta")
            .delay(Duration::from_millis(75))
            .chunks(["b:1", "b:2"]),
    )
    .unwrap();

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
