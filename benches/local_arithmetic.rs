#[path = "../examples/support/arithmetic_graphs.rs"]
mod arithmetic_graphs;

use arithmetic_graphs::{ArithmeticGraph, LayeredSpec, TreeSpec};
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use orchestra::Pipeline;
use std::time::Duration;
use tokio::runtime::Runtime;

fn bench_local_arithmetic(c: &mut Criterion) {
    let runtime = Runtime::new().expect("tokio runtime");

    bench_chain(c, &runtime);
    bench_wide(c, &runtime);
    bench_fan_in(c, &runtime);
    bench_layered(c, &runtime);
    bench_tree(c, &runtime);
}

fn bench_chain(c: &mut Criterion, runtime: &Runtime) {
    let mut group = c.benchmark_group("local_arithmetic/chain");
    for length in [100, 1_000] {
        let graph = arithmetic_graphs::chain(length, Duration::ZERO).unwrap();
        verify_graph(&runtime, &graph);
        let pipeline = Pipeline::new(graph.flow.clone());
        let compiled = pipeline.compile().unwrap();
        group.throughput(Throughput::Elements(length as u64));
        group.bench_function(BenchmarkId::new("pipeline", length), |bench| {
            bench.to_async(runtime).iter(|| async {
                let result = pipeline.execute_with_trace().await.unwrap();
                black_box(result.outputs.len());
                black_box(result.trace.duration_ms);
            });
        });
        group.bench_function(BenchmarkId::new("compiled", length), |bench| {
            bench.to_async(runtime).iter(|| async {
                let result = compiled.execute_with_trace().await.unwrap();
                black_box(result.outputs.len());
                black_box(result.trace.duration_ms);
            });
        });
    }
    group.finish();
}

fn bench_wide(c: &mut Criterion, runtime: &Runtime) {
    let mut group = c.benchmark_group("local_arithmetic/wide");
    for width in [100, 1_000, 10_000] {
        let graph = arithmetic_graphs::wide(width, Duration::ZERO).unwrap();
        verify_graph(&runtime, &graph);
        let pipeline = Pipeline::new(graph.flow.clone()).with_max_concurrency(1_024);
        let compiled = pipeline.compile().unwrap();
        group.throughput(Throughput::Elements(width as u64));
        group.bench_function(BenchmarkId::new("pipeline", width), |bench| {
            bench.to_async(runtime).iter(|| async {
                let result = pipeline.execute_with_trace().await.unwrap();
                black_box(result.outputs.len());
                black_box(result.trace.duration_ms);
            });
        });
        group.bench_function(BenchmarkId::new("compiled", width), |bench| {
            bench.to_async(runtime).iter(|| async {
                let result = compiled.execute_with_trace().await.unwrap();
                black_box(result.outputs.len());
                black_box(result.trace.duration_ms);
            });
        });
    }
    group.finish();
}

fn bench_fan_in(c: &mut Criterion, runtime: &Runtime) {
    let mut group = c.benchmark_group("local_arithmetic/fan_in");
    for width in [100, 1_000, 10_000] {
        let graph = arithmetic_graphs::fan_in(width, Duration::ZERO).unwrap();
        verify_graph(&runtime, &graph);
        let pipeline = Pipeline::new(graph.flow.clone()).with_max_concurrency(1_024);
        let compiled = pipeline.compile().unwrap();
        group.throughput(Throughput::Elements(width as u64));
        group.bench_function(BenchmarkId::new("pipeline", width), |bench| {
            bench.to_async(runtime).iter(|| async {
                let result = pipeline.execute_with_trace().await.unwrap();
                black_box(result.outputs.len());
                black_box(result.trace.duration_ms);
            });
        });
        group.bench_function(BenchmarkId::new("compiled", width), |bench| {
            bench.to_async(runtime).iter(|| async {
                let result = compiled.execute_with_trace().await.unwrap();
                black_box(result.outputs.len());
                black_box(result.trace.duration_ms);
            });
        });
    }
    group.finish();
}

fn bench_layered(c: &mut Criterion, runtime: &Runtime) {
    let mut group = c.benchmark_group("local_arithmetic/layered");
    for (width, depth) in [(10, 10), (25, 10), (50, 20)] {
        let graph =
            arithmetic_graphs::layered(LayeredSpec { width, depth }, Duration::ZERO).unwrap();
        verify_graph(&runtime, &graph);
        let pipeline = Pipeline::new(graph.flow.clone()).with_max_concurrency(1_024);
        let compiled = pipeline.compile().unwrap();
        group.throughput(Throughput::Elements((width * depth) as u64));
        group.bench_function(
            BenchmarkId::new("pipeline", format!("{width}x{depth}")),
            |bench| {
                bench.to_async(runtime).iter(|| async {
                    let result = pipeline.execute_with_trace().await.unwrap();
                    black_box(result.outputs.len());
                    black_box(result.trace.duration_ms);
                });
            },
        );
        group.bench_function(
            BenchmarkId::new("compiled", format!("{width}x{depth}")),
            |bench| {
                bench.to_async(runtime).iter(|| async {
                    let result = compiled.execute_with_trace().await.unwrap();
                    black_box(result.outputs.len());
                    black_box(result.trace.duration_ms);
                });
            },
        );
    }
    group.finish();
}

fn bench_tree(c: &mut Criterion, runtime: &Runtime) {
    let mut group = c.benchmark_group("local_arithmetic/tree");
    for (depth, branching) in [(5, 3), (7, 3), (8, 3)] {
        let graph = arithmetic_graphs::tree(TreeSpec { depth, branching }, Duration::ZERO).unwrap();
        verify_graph(&runtime, &graph);
        let pipeline = Pipeline::new(graph.flow.clone()).with_max_concurrency(1_024);
        let compiled = pipeline.compile().unwrap();
        group.throughput(Throughput::Elements(graph.flow.nodes().len() as u64));
        group.bench_function(
            BenchmarkId::new("pipeline", format!("{depth}x{branching}")),
            |bench| {
                bench.to_async(runtime).iter(|| async {
                    let result = pipeline.execute_with_trace().await.unwrap();
                    black_box(result.outputs.len());
                    black_box(result.trace.duration_ms);
                });
            },
        );
        group.bench_function(
            BenchmarkId::new("compiled", format!("{depth}x{branching}")),
            |bench| {
                bench.to_async(runtime).iter(|| async {
                    let result = compiled.execute_with_trace().await.unwrap();
                    black_box(result.outputs.len());
                    black_box(result.trace.duration_ms);
                });
            },
        );
    }
    group.finish();
}

fn verify_graph(runtime: &Runtime, graph: &ArithmeticGraph) {
    runtime.block_on(async {
        let result = Pipeline::new(graph.flow.clone())
            .with_max_concurrency(1_024)
            .execute_with_trace()
            .await
            .unwrap();
        let actual = graph.answer_from_outputs(&result.outputs).unwrap();
        assert_eq!(actual, graph.expected_answer);
    });
}

criterion_group!(benches, bench_local_arithmetic);
criterion_main!(benches);
