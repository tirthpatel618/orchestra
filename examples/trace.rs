use orchestra::{FakeTask, Flow, Pipeline};
use std::time::Duration;

#[tokio::main]
async fn main() {
    let mut flow = Flow::new();
    flow.add_node(
        "fetch",
        FakeTask::new("raw data").delay(Duration::from_millis(80)),
    )
    .unwrap();
    flow.add_node(
        "clean",
        FakeTask::new("clean data").include_dependency_outputs(),
    )
    .unwrap();
    flow.add_node(
        "summarize",
        FakeTask::new("summary").include_dependency_outputs(),
    )
    .unwrap();
    flow.add_dependency("clean", "fetch").unwrap();
    flow.add_dependency("summarize", "clean").unwrap();

    let result = Pipeline::new(flow).execute_with_trace().await.unwrap();

    println!("outputs: {:#?}", result.outputs);
    println!("trace: {:#?}", result.trace);
}
