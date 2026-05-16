# Orchestra

## Summary

Orchestra is a Rust-first DAG orchestration runtime designed to run dependency-aware task graphs with low scheduler overhead. The current project goal is to prove the scheduling core before expanding into a full agent framework.

At this stage, Orchestra can:

- Define task graphs as `Flow`s.
- Validate dependencies and reject invalid graphs.
- Compile reusable graph metadata into a `CompiledPipeline`.
- Run ready nodes concurrently with Tokio.
- Pass dependency outputs into downstream tasks.
- Stream runtime events.
- Record per-run and per-node telemetry.
- Expose the compiled scheduler through a Python package using PyO3.

The long-term goal is to compare Orchestra against Python-native orchestration systems, especially LangGraph, for large agent-style workflows. The benchmark work is starting with arithmetic DAGs because they isolate scheduler overhead from network latency, API cost, and model behavior.

The current benchmark results show a clear early pattern:

- Orchestra is much faster than LangGraph on local arithmetic DAGs.
- A minimal Python `asyncio` scheduler is faster than Orchestra on medium and large tiny-task DAGs.
- This suggests Orchestra is already lighter than LangGraph, but its Python-facing benchmark path still has overhead to isolate and optimize.

This is a useful position. Orchestra is not yet being presented as universally faster than all Python scheduling approaches. The current claim is narrower and more defensible:

> Orchestra currently outperforms LangGraph on local arithmetic DAG scheduler-overhead benchmarks, while the simple asyncio baseline identifies optimization work still needed in Orchestra's Python/package boundary and tiny-task execution path.

## How Orchestra Works

Orchestra represents a workflow as a directed acyclic graph. Each node is a task, and each edge means one node depends on another node's output. A task does not run until all of its dependencies have completed.

The scheduler has two phases:

1. Build and validate the graph.
2. Execute the graph by repeatedly running ready nodes.

During compilation, Orchestra validates the graph and precomputes reusable scheduler metadata:

- Each node's dependency count.
- Each node's downstream dependents.
- The initial root nodes with no dependencies.

At runtime, the scheduler keeps a queue of ready nodes. It starts ready nodes concurrently up to the configured concurrency limit. When a node finishes, its output is stored, and each dependent node has its remaining dependency count decremented. When a dependent reaches zero remaining dependencies, it becomes ready to run.

The basic runtime flow is:

```text
validate graph
compile roots / dependency counts / dependents
start root nodes
wait for node completions
unlock downstream nodes
repeat until complete or failed
```

This design is intentionally narrower than a full agent framework. It avoids persistence, checkpoints, dynamic routing, retries, and general state reducers in the first scheduler. That narrower scope is what gives Orchestra a chance to have lower overhead than general-purpose graph frameworks.

Orchestra is also exposed as a Python package through PyO3. Python builds the graph, compiles the pipeline, and calls into the Rust scheduler. This is important for the final comparison because LangGraph is a Python framework, so the practical comparison should be:

```text
Orchestra through Python bindings
vs
LangGraph from Python
```

## Benchmark Setup

The current benchmark suite compares three implementations:

| Implementation | Description |
| --- | --- |
| `orchestra-python` | Orchestra's Rust scheduler called through the PyO3 Python package. |
| `python-asyncio` | A small purpose-built Python asyncio DAG scheduler. |
| `langgraph-local` | LangGraph using ordinary Python function nodes, with no LLM calls. |

The benchmark uses generated arithmetic DAGs. Arithmetic is used because it keeps task bodies local, deterministic, cheap, and easy to verify. This isolates orchestration overhead from API latency, network jitter, token costs, and LLM correctness issues.

The graph shapes are:

| Shape | Purpose |
| --- | --- |
| `chain` | Tests long dependency depth with little parallelism. |
| `wide` | Tests many independent root nodes. |
| `fan_in` | Tests many parallel producers feeding one reducer. |
| `layered` | Tests repeated fan-out/fan-in across layers. |
| `tree` | Tests hierarchical dependency structure. |

All implementations use the same generated graph specification. Each graph includes:

- Node IDs.
- Arithmetic operation per node.
- Dependency edges.
- Expected final answer.
- Modulus for bounded integer outputs.

For each implementation, graph construction and compilation happen outside the timed loop. The measured loop repeatedly executes the already-built graph. Each run checks the final answer against the independently computed expected answer.

The suite records results as JSONL under:

```text
benchmarks/results/
```

Important output fields include:

- `implementation`
- `graph`
- `node_count`
- `answer`
- `expected_answer`
- `correct`
- `median_ms`
- `p95_ms`
- `nodes_per_second_median`
- `suite_status`

The benchmark includes multiple scale buckets: small, medium, large, and very large. The purpose is not only to see which implementation is faster, but to identify where the performance difference becomes substantial as node and edge counts grow.

## Current Results

All completed small, medium, and large runs passed correctness checks.

### Small Graphs

| Graph | Nodes | Orchestra median | asyncio median | LangGraph median | Orchestra vs LangGraph | asyncio vs Orchestra |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `chain/20` | 20 | 1.593 ms | 0.415 ms | 4.496 ms | 2.82x faster | 3.84x faster |
| `wide/50` | 50 | 0.672 ms | 0.466 ms | 18.892 ms | 28.12x faster | 1.44x faster |
| `fan_in/50` | 51 | 0.864 ms | 0.698 ms | 18.605 ms | 21.52x faster | 1.24x faster |
| `layered/5x4` | 20 | 0.681 ms | 0.216 ms | 7.330 ms | 10.77x faster | 3.15x faster |
| `tree/4x3` | 40 | 0.910 ms | 0.326 ms | 12.738 ms | 14.00x faster | 2.79x faster |

### Medium Graphs

| Graph | Nodes | Orchestra median | asyncio median | LangGraph median | Orchestra vs LangGraph | asyncio vs Orchestra |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `chain/1000` | 1,000 | 73.683 ms | 18.234 ms | 844.146 ms | 11.46x faster | 4.04x faster |
| `wide/1000` | 1,000 | 13.366 ms | 5.710 ms | 1036.219 ms | 77.53x faster | 2.34x faster |
| `fan_in/1000` | 1,001 | 16.776 ms | 6.136 ms | 1069.410 ms | 63.74x faster | 2.73x faster |
| `layered/25x10` | 250 | 17.828 ms | 2.981 ms | 118.424 ms | 6.64x faster | 5.98x faster |
| `tree/6x3` | 364 | 7.466 ms | 2.306 ms | 157.834 ms | 21.14x faster | 3.24x faster |

### Large Graphs

| Graph | Nodes | Orchestra median | asyncio median | LangGraph median | Orchestra vs LangGraph | asyncio vs Orchestra |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `chain/10000` | 10,000 | 746.247 ms | 194.500 ms | 60920.955 ms | 81.64x faster | 3.84x faster |
| `wide/10000` | 10,000 | 144.827 ms | 62.431 ms | 94007.888 ms | 649.11x faster | 2.32x faster |
| `fan_in/10000` | 10,001 | 175.746 ms | 64.856 ms | 101772.307 ms | 579.09x faster | 2.71x faster |
| `layered/50x20` | 1,000 | 105.684 ms | 19.442 ms | 942.699 ms | 8.92x faster | 5.44x faster |
| `tree/8x3` | 3,280 | 59.330 ms | 20.798 ms | 8108.814 ms | 136.67x faster | 2.85x faster |

## Interpretation

The strongest result is that Orchestra is consistently faster than LangGraph for these local arithmetic DAGs. This is true across every completed graph shape and size.

That result is expected directionally. LangGraph is a general-purpose graph framework with broader semantics around state, reducers, graph execution, and integration with the LangChain ecosystem. Orchestra is currently a narrower DAG scheduler. For local deterministic arithmetic, the narrower runtime has less framework machinery to execute.

The asyncio result is equally important. The minimal Python asyncio scheduler is faster than Orchestra in the current benchmark, especially as graphs get larger. This does not mean Rust scheduling is inherently slower than Python scheduling. It means the current Orchestra Python benchmark path still includes overhead that the tiny asyncio baseline does not pay.

Likely sources of Orchestra overhead in the current benchmark:

- Tokio task spawn/join overhead for extremely small tasks.
- `String` outputs for arithmetic values.
- `HashMap<NodeId, String>` dependency passing.
- Full output map construction for every node.
- Python dict conversion for every node output.
- Trace collection through `execute_with_trace()`.
- PyO3 boundary cost when returning all outputs to Python.

The current finding should therefore be stated carefully:

> Orchestra is much faster than LangGraph on local arithmetic DAG benchmarks. The simple asyncio baseline is faster than Orchestra on tiny arithmetic workloads, which identifies the next optimization target: reducing tracing, output conversion, and tiny-task overhead in Orchestra's Python-facing benchmark path.

This is still a good milestone. The benchmark is doing its job: it is not only showing where Orchestra is strong, but also showing where the implementation needs to improve before making broader scheduler-performance claims.

## Fairness Controls

The benchmark attempts to keep the comparison focused on scheduler/runtime overhead.

Current controls:

- All implementations use the same graph generator.
- All implementations run the same graph shapes and dependency edges.
- All implementations compute the same expected answer.
- All implementations use local arithmetic tasks only.
- Graph construction and compilation happen before the timed loop.
- Warmups run before measurements.
- The timed section measures repeated graph execution.
- Correctness is checked on every measured run.
- The same `max_concurrency` setting is passed to each implementation.
- Integer outputs are bounded with the same modulus.
- Results record Python version and platform.
- LLM calls are excluded from local scheduler benchmarks.

The suite also records subprocess wall time separately from measured graph execution time. This prevents package import, process startup, and graph construction from being mixed into the scheduler timing.

**Command: local comparison**

```bash
python benchmarks/python/run_local_suite.py --preset large --timeout-seconds 1800
```

**Command: single graph comparison**

```bash
python benchmarks/python/run_local_suite.py \
  --shape fan_in \
  --size 10000 \
  --repeats 10 \
  --warmups 2 \
  --timeout-seconds 1800
```

## Limitations

These results are useful, but they are not the final answer for all orchestration workloads.

Current limitations:

- The benchmark uses tiny arithmetic tasks. This intentionally magnifies scheduler overhead, but it does not represent workflows where each node does substantial work.
- Orchestra currently returns all node outputs to Python. Large output maps add conversion overhead.
- Orchestra's benchmark runner uses `execute_with_trace()`, so telemetry cost is included.
- The asyncio baseline is a purpose-built minimal scheduler, not a full framework.
- LangGraph supports many features not exercised here.
- No persistence, checkpointing, retries, conditional routing, or dynamic graph behavior is being compared.
- The current completed result set covers small, medium, and large, but not a completed very-large suite.
- The benchmark does not yet record peak memory usage.
- Hosted environments can be noisy; results should be repeated before drawing final conclusions.

The most important limitation is that local arithmetic benchmarks isolate scheduler overhead, not agent workflow quality. They are the right first step, but they should not be used to claim that one framework is faster for every real LLM application.

## Next Engineering Work

The next goal is to separate scheduler cost from Python boundary, output conversion, and telemetry cost.

Planned benchmark variants:

1. `execute()` benchmark mode.
   - Use `pipeline.execute()` instead of `execute_with_trace()`.
   - This measures the existing execution path without trace object creation.

2. Answer-only output mode.
   - Return only selected final answer nodes to Python.
   - Avoid converting every node output into a Python dict.

3. No-trace / low-telemetry mode.
   - Allow benchmark runs to disable per-node trace collection.
   - Keep trace mode available for observability.

4. Native numeric arithmetic path.
   - Avoid formatting arithmetic outputs as strings.
   - Avoid parsing dependency outputs from strings.

5. Memory reporting.
   - Track peak RSS for each implementation.
   - This is especially important for very-large graphs.

6. Result summarizer.
   - Read JSONL result files.
   - Generate comparison tables automatically.
   - Flag incomplete suites and failed cases.

After those changes, the asyncio comparison will be more informative. If asyncio remains faster, the scheduler itself needs work. If Orchestra catches or passes asyncio, the current gap was mostly Python/output/trace overhead.

## LLM Benchmark Plan

The next benchmark category is the same DAG structure, but with an LLM call at each node instead of a local arithmetic task.

This matters because the local arithmetic benchmark isolates scheduler overhead, while LLM graphs represent the kind of workload Orchestra is ultimately meant to run. The comparison should start small because API calls introduce latency, rate limits, and cost. After the small LLM graphs work reliably, the same idea can broaden into larger graphs and more realistic agent-style workflows.

The first version should keep the prompts simple, such as arithmetic questions, so correctness can still be checked. Later versions can move beyond arithmetic into more agent-like tasks once the scheduling and rate-limit behavior is understood.

This will let us compare:

- Local scheduler overhead.
- End-to-end graph runtime with real provider calls.
- How concurrency and rate limits affect each system.
- Whether the scheduler difference still matters when each node performs network-bound LLM work.
