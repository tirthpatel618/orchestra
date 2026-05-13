use orchestra::{
    Flow, NodeTrace, OrchestraError, Pipeline, RuntimeEvent, Task, TaskFuture, TaskInput,
};
use std::time::Duration;
use tokio::{sync::mpsc, time::sleep};

/* Graph:

    a=2+3   b=7+7   c=8+9
       \     |     /
        \    |    /
         \   |   /
          \  |  /
            d=a+b+c
            e=a+b+c
             \   /
              \ /
               f=d*e
*/

#[tokio::main]
async fn main() {
    let mut flow = Flow::new();

    flow.add_node("a", AddTask::new(2, 3)).unwrap();
    flow.add_node("b", AddTask::new(7, 7)).unwrap();
    flow.add_node("c", AddTask::new(8, 9)).unwrap();

    flow.add_node("d", SumDependenciesTask).unwrap();
    flow.add_dependency("d", "a").unwrap();
    flow.add_dependency("d", "b").unwrap();
    flow.add_dependency("d", "c").unwrap();

    flow.add_node("e", AddTask::new(8, 7)).unwrap();
    flow.add_dependency("e", "a").unwrap();
    flow.add_dependency("e", "b").unwrap();
    flow.add_dependency("e", "c").unwrap();

    flow.add_node("f", MultiplyDependenciesTask).unwrap();
    flow.add_dependency("f", "d").unwrap();
    flow.add_dependency("f", "e").unwrap();

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

    let result = Pipeline::new(flow).execute_with_trace().await.unwrap();
    println!("\nTelemetry");
    println!("run_status: {:?}", result.trace.status);
    println!("run_duration_ms: {}", result.trace.duration_ms);
    println!("final_output: {}", result.outputs["f"]);
    print_node_trace_summary(result.trace.nodes.values());
}

#[derive(Debug, Clone)]
struct AddTask {
    left: i32,
    right: i32,
}

impl AddTask {
    fn new(left: i32, right: i32) -> Self {
        Self { left, right }
    }
}

impl Task for AddTask {
    fn execute<'a>(
        &'a self,
        input: TaskInput,
        events: Option<mpsc::Sender<RuntimeEvent>>,
    ) -> TaskFuture<'a> {
        Box::pin(async move {
            sleep(Duration::from_millis(100)).await;

            let output = self.left + self.right;
            emit(
                &events,
                RuntimeEvent::NodeOutput {
                    node: input.node,
                    chunk: format!("{} + {} = {output}", self.left, self.right),
                },
            )
            .await;

            Ok(output.to_string())
        })
    }
}

#[derive(Debug, Clone)]
struct SumDependenciesTask;

impl Task for SumDependenciesTask {
    fn execute<'a>(
        &'a self,
        input: TaskInput,
        events: Option<mpsc::Sender<RuntimeEvent>>,
    ) -> TaskFuture<'a> {
        Box::pin(async move {
            let mut values = parse_dependency_values(&input)?;
            values.sort_by(|(left, _), (right, _)| left.cmp(right));

            let sum = values.iter().map(|(_, value)| value).sum::<i32>();
            let expression = values
                .iter()
                .map(|(node, value)| format!("{node}={value}"))
                .collect::<Vec<_>>()
                .join(" + ");

            emit(
                &events,
                RuntimeEvent::NodeOutput {
                    node: input.node,
                    chunk: format!("{expression} => {sum}"),
                },
            )
            .await;

            Ok(sum.to_string())
        })
    }
}

#[derive(Debug, Clone)]
struct MultiplyDependenciesTask;

impl Task for MultiplyDependenciesTask {
    fn execute<'a>(
        &'a self,
        input: TaskInput,
        events: Option<mpsc::Sender<RuntimeEvent>>,
    ) -> TaskFuture<'a> {
        Box::pin(async move {
            let mut values = parse_dependency_values(&input)?;
            values.sort_by(|(left, _), (right, _)| left.cmp(right));

            let product = values.iter().map(|(_, value)| value).product::<i32>();
            let expression = values
                .iter()
                .map(|(node, value)| format!("{node}={value}"))
                .collect::<Vec<_>>()
                .join(" * ");

            emit(
                &events,
                RuntimeEvent::NodeOutput {
                    node: input.node,
                    chunk: format!("{expression} => {product}"),
                },
            )
            .await;

            Ok(product.to_string())
        })
    }
}

fn parse_dependency_values(input: &TaskInput) -> Result<Vec<(String, i32)>, OrchestraError> {
    input
        .dependency_outputs
        .iter()
        .map(|(node, output)| {
            output
                .parse::<i32>()
                .map(|value| (node.clone(), value))
                .map_err(|error| OrchestraError::NodeFailed {
                    node: input.node.clone(),
                    message: format!("dependency '{node}' did not return an integer: {error}"),
                })
        })
        .collect()
}

async fn emit(events: &Option<mpsc::Sender<RuntimeEvent>>, event: RuntimeEvent) {
    if let Some(events) = events {
        let _ = events.send(event).await;
    }
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
            "{:<8} status={:?} duration_ms={:<4} deps={:?} output={}",
            node.node,
            node.status,
            node.duration_ms,
            node.dependencies,
            node.output.as_deref().unwrap_or("")
        );
    }
}
