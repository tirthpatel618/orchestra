#[path = "../examples/support/arithmetic_graphs.rs"]
mod arithmetic_graphs;

use arithmetic_graphs::{LayeredSpec, TreeSpec};
use orchestra::Pipeline;
use std::time::Duration;

#[tokio::test]
async fn chain_computes_expected_answer() {
    let graph = arithmetic_graphs::chain(8, Duration::ZERO).unwrap();
    assert_graph_computes_expected_answer(graph).await;
}

#[tokio::test]
async fn wide_computes_expected_answer() {
    let graph = arithmetic_graphs::wide(16, Duration::ZERO).unwrap();
    assert_graph_computes_expected_answer(graph).await;
}

#[tokio::test]
async fn fan_in_computes_expected_answer() {
    let graph = arithmetic_graphs::fan_in(16, Duration::ZERO).unwrap();
    assert_graph_computes_expected_answer(graph).await;
}

#[tokio::test]
async fn layered_computes_expected_answer() {
    let graph =
        arithmetic_graphs::layered(LayeredSpec { width: 4, depth: 5 }, Duration::ZERO).unwrap();
    assert_graph_computes_expected_answer(graph).await;
}

#[tokio::test]
async fn tree_computes_expected_answer() {
    let graph = arithmetic_graphs::tree(
        TreeSpec {
            depth: 5,
            branching: 3,
        },
        Duration::ZERO,
    )
    .unwrap();
    assert_graph_computes_expected_answer(graph).await;
}

async fn assert_graph_computes_expected_answer(graph: arithmetic_graphs::ArithmeticGraph) {
    let result = Pipeline::new(graph.flow.clone())
        .execute_with_trace()
        .await
        .unwrap();
    let actual = graph.answer_from_outputs(&result.outputs).unwrap();

    assert_eq!(actual, graph.expected_answer);
}
