mod support;

use orchestra::{Pipeline, RunResult};
use std::{
    env,
    error::Error,
    time::{Duration, Instant},
};
use support::synthetic_graphs::{self, GraphSpecError, LayeredSpec, TreeSpec};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = env::args().collect::<Vec<_>>();
    let Some(kind) = args.get(1).map(String::as_str) else {
        print_usage(&args[0]);
        return Ok(());
    };

    let (flow, max_concurrency) = match kind {
        "chain" => (
            synthetic_graphs::chain(
                parse_required_arg(&args, 2, "length")?,
                parse_delay(&args, 3),
            )?,
            parse_optional_arg(&args, 4),
        ),
        "wide" => (
            synthetic_graphs::wide(
                parse_required_arg(&args, 2, "width")?,
                parse_delay(&args, 3),
            )?,
            parse_optional_arg(&args, 4),
        ),
        "fan-in" => (
            synthetic_graphs::fan_in(
                parse_required_arg(&args, 2, "width")?,
                parse_delay(&args, 3),
            )?,
            parse_optional_arg(&args, 4),
        ),
        "layered" => synthetic_graphs::layered(
            LayeredSpec {
                width: parse_required_arg(&args, 2, "width")?,
                depth: parse_required_arg(&args, 3, "depth")?,
            },
            parse_delay(&args, 4),
        )
        .map(|flow| (flow, parse_optional_arg(&args, 5)))?,
        "tree" => synthetic_graphs::tree(
            TreeSpec {
                depth: parse_required_arg(&args, 2, "depth")?,
                branching: parse_required_arg(&args, 3, "branching")?,
            },
            parse_delay(&args, 4),
        )
        .map(|flow| (flow, parse_optional_arg(&args, 5)))?,
        _ => {
            print_usage(&args[0]);
            return Ok(());
        }
    };

    let node_count = flow.nodes().len();
    let edge_count = flow
        .nodes()
        .values()
        .map(|node| node.dependencies.len())
        .sum::<usize>();

    let mut pipeline = Pipeline::new(flow);
    if let Some(max_concurrency) = max_concurrency {
        pipeline = pipeline.with_max_concurrency(max_concurrency);
    }

    let started = Instant::now();
    let result = pipeline.execute_with_trace().await?;
    let wall_duration_ms = started.elapsed().as_millis();

    print_summary(
        kind,
        node_count,
        edge_count,
        max_concurrency,
        wall_duration_ms,
        result,
    );
    Ok(())
}

fn print_summary(
    kind: &str,
    node_count: usize,
    edge_count: usize,
    max_concurrency: Option<usize>,
    wall_duration_ms: u128,
    result: RunResult,
) {
    let max_node_duration = result
        .trace
        .nodes
        .values()
        .map(|node| node.duration_ms)
        .max()
        .unwrap_or(0);

    println!("graph: {kind}");
    println!("nodes: {node_count}");
    println!("edges: {edge_count}");
    println!(
        "max_concurrency: {}",
        max_concurrency
            .map(|value| value.to_string())
            .unwrap_or_else(|| "unlimited".to_string())
    );
    println!("outputs: {}", result.outputs.len());
    println!("run_status: {:?}", result.trace.status);
    println!("trace_duration_ms: {}", result.trace.duration_ms);
    println!("wall_duration_ms: {wall_duration_ms}");
    println!("max_node_duration_ms: {max_node_duration}");
    println!("event_count: {}", result.trace.event_count);
    println!(
        "streamed_chunk_count: {}",
        result.trace.streamed_chunk_count
    );
}

fn parse_required_arg(args: &[String], index: usize, name: &str) -> Result<usize, GraphSpecError> {
    args.get(index)
        .ok_or_else(|| GraphSpecError::InvalidSpec(format!("missing argument: {name}")))?
        .parse::<usize>()
        .map_err(|error| GraphSpecError::InvalidSpec(format!("invalid argument '{name}': {error}")))
}

fn parse_delay(args: &[String], index: usize) -> Duration {
    let delay_ms = args
        .get(index)
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0);
    Duration::from_millis(delay_ms)
}

fn parse_optional_arg(args: &[String], index: usize) -> Option<usize> {
    args.get(index)
        .and_then(|value| value.parse::<usize>().ok())
}

fn print_usage(binary: &str) {
    println!("usage:");
    println!("  {binary} chain <length> [delay_ms] [max_concurrency]");
    println!("  {binary} wide <width> [delay_ms] [max_concurrency]");
    println!("  {binary} fan-in <width> [delay_ms] [max_concurrency]");
    println!("  {binary} layered <width> <depth> [delay_ms] [max_concurrency]");
    println!("  {binary} tree <depth> <branching> [delay_ms] [max_concurrency]");
}
