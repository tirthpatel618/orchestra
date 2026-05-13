use orchestra::{FakeTask, Flow, Pipeline, RuntimeEvent};
use std::time::{Duration, Instant};

/*
  brief ───────┬─> architecture ─> prototype ─┬─> qa ───────────┐
               │                              └─> demo_script ──┤
  market ──────┬─> copy ────────┬─> launch_page ────────────────┤
               └─> pricing ─────┘                                │
  constraints ─┬─> architecture                                  │
               └─> pricing                                       │
                                                                 v
                                                          launch_packet
*/

#[tokio::main]
async fn main() {
    let mut flow = Flow::new();

    flow.add_node(
        "brief",
        FakeTask::new("product brief")
            .delay(Duration::from_millis(120))
            .chunks(["read prompt", "extract goals"]),
    )
    .unwrap();
    flow.add_node(
        "market",
        FakeTask::new("market scan")
            .delay(Duration::from_millis(260))
            .chunks(["scan competitors", "rank positioning"]),
    )
    .unwrap();
    flow.add_node(
        "constraints",
        FakeTask::new("constraints")
            .delay(Duration::from_millis(180))
            .chunks(["check budget", "check timeline"]),
    )
    .unwrap();

    flow.add_node(
        "architecture",
        FakeTask::new("architecture").include_dependency_outputs(),
    )
    .unwrap();
    flow.add_dependency("architecture", "brief").unwrap();
    flow.add_dependency("architecture", "constraints").unwrap();

    flow.add_node("copy", FakeTask::new("copy").include_dependency_outputs())
        .unwrap();
    flow.add_dependency("copy", "brief").unwrap();
    flow.add_dependency("copy", "market").unwrap();

    flow.add_node(
        "pricing",
        FakeTask::new("pricing").include_dependency_outputs(),
    )
    .unwrap();
    flow.add_dependency("pricing", "market").unwrap();
    flow.add_dependency("pricing", "constraints").unwrap();

    flow.add_node(
        "prototype",
        FakeTask::new("prototype")
            .delay(Duration::from_millis(300))
            .chunks(["build shell", "wire data", "polish flow"])
            .include_dependency_outputs(),
    )
    .unwrap();
    flow.add_dependency("prototype", "architecture").unwrap();

    flow.add_node(
        "launch_page",
        FakeTask::new("launch page").include_dependency_outputs(),
    )
    .unwrap();
    flow.add_dependency("launch_page", "copy").unwrap();
    flow.add_dependency("launch_page", "pricing").unwrap();

    flow.add_node(
        "qa",
        FakeTask::new("qa report")
            .delay(Duration::from_millis(150))
            .chunks(["test happy path", "test edge cases"])
            .include_dependency_outputs(),
    )
    .unwrap();
    flow.add_dependency("qa", "prototype").unwrap();
    flow.add_dependency("qa", "launch_page").unwrap();

    flow.add_node(
        "demo_script",
        FakeTask::new("demo script").include_dependency_outputs(),
    )
    .unwrap();
    flow.add_dependency("demo_script", "copy").unwrap();
    flow.add_dependency("demo_script", "prototype").unwrap();

    flow.add_node(
        "launch_packet",
        FakeTask::new("launch packet").include_dependency_outputs(),
    )
    .unwrap();
    flow.add_dependency("launch_packet", "qa").unwrap();
    flow.add_dependency("launch_packet", "demo_script").unwrap();
    flow.add_dependency("launch_packet", "launch_page").unwrap();

    let started = Instant::now();
    let mut events = Pipeline::new(flow).run();

    while let Some(event) = events.recv().await {
        println!("{:>4}ms  {event:?}", started.elapsed().as_millis());
        if matches!(
            event,
            RuntimeEvent::RunCompleted { .. } | RuntimeEvent::RunFailed { .. }
        ) {
            break;
        }
    }
}
