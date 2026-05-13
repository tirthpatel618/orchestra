#[path = "support/arithmetic_graphs.rs"]
mod arithmetic_graphs;

use arithmetic_graphs::{ArithmeticGraph, ArithmeticGraphError, LayeredSpec, TreeSpec, MODULUS};
use orchestra::{Pipeline, RunResult};
use std::{
    env,
    error::Error,
    time::{Duration, Instant},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = env::args().collect::<Vec<_>>();
    let Some(kind) = args.get(1).map(String::as_str) else {
        print_usage(&args[0]);
        return Ok(());
    };

    let (graph, max_concurrency) = match kind {
        "chain" => (
            arithmetic_graphs::chain(
                parse_required_arg(&args, 2, "length")?,
                parse_delay(&args, 3),
            )?,
            parse_optional_arg(&args, 4),
        ),
        "wide" => (
            arithmetic_graphs::wide(
                parse_required_arg(&args, 2, "width")?,
                parse_delay(&args, 3),
            )?,
            parse_optional_arg(&args, 4),
        ),
        "fan-in" => (
            arithmetic_graphs::fan_in(
                parse_required_arg(&args, 2, "width")?,
                parse_delay(&args, 3),
            )?,
            parse_optional_arg(&args, 4),
        ),
        "layered" => arithmetic_graphs::layered(
            LayeredSpec {
                width: parse_required_arg(&args, 2, "width")?,
                depth: parse_required_arg(&args, 3, "depth")?,
            },
            parse_delay(&args, 4),
        )
        .map(|graph| (graph, parse_optional_arg(&args, 5)))?,
        "tree" => arithmetic_graphs::tree(
            TreeSpec {
                depth: parse_required_arg(&args, 2, "depth")?,
                branching: parse_required_arg(&args, 3, "branching")?,
            },
            parse_delay(&args, 4),
        )
        .map(|graph| (graph, parse_optional_arg(&args, 5)))?,
        _ => {
            print_usage(&args[0]);
            return Ok(());
        }
    };

    let node_count = graph.flow.nodes().len();
    let edge_count = graph
        .flow
        .nodes()
        .values()
        .map(|node| node.dependencies.len())
        .sum::<usize>();

    let mut pipeline = Pipeline::new(graph.flow.clone());
    if let Some(max_concurrency) = max_concurrency {
        pipeline = pipeline.with_max_concurrency(max_concurrency);
    }

    let started = Instant::now();
    let result = pipeline.execute_with_trace().await?;
    let wall_duration_ms = started.elapsed().as_millis();
    let actual_answer = graph.answer_from_outputs(&result.outputs)?;
    let correct = actual_answer == graph.expected_answer;

    print_summary(
        kind,
        &graph,
        node_count,
        edge_count,
        max_concurrency,
        wall_duration_ms,
        result,
        actual_answer,
        correct,
    );

    Ok(())
}

fn print_summary(
    kind: &str,
    graph: &ArithmeticGraph,
    node_count: usize,
    edge_count: usize,
    max_concurrency: Option<usize>,
    wall_duration_ms: u128,
    result: RunResult,
    actual_answer: u64,
    correct: bool,
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
    println!("modulus: {MODULUS}");
    println!(
        "answer_nodes: {}",
        if graph.answer_nodes.len() == 1 {
            graph.answer_nodes[0].clone()
        } else {
            format!("{} nodes", graph.answer_nodes.len())
        }
    );
    println!(
        "max_concurrency: {}",
        max_concurrency
            .map(|value| value.to_string())
            .unwrap_or_else(|| "unlimited".to_string())
    );
    println!("outputs: {}", result.outputs.len());
    println!("expected_answer: {}", graph.expected_answer);
    println!("actual_answer: {actual_answer}");
    println!("correct: {correct}");
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

fn parse_required_arg(
    args: &[String],
    index: usize,
    name: &str,
) -> Result<usize, ArithmeticGraphError> {
    args.get(index)
        .ok_or_else(|| ArithmeticGraphError::InvalidSpec(format!("missing argument: {name}")))?
        .parse::<usize>()
        .map_err(|error| {
            ArithmeticGraphError::InvalidSpec(format!("invalid argument '{name}': {error}"))
        })
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
