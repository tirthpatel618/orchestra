# Orchestra Benchmark Report Notes

This document tracks the benchmark methodology and caveats for comparing Orchestra against Python asyncio and LangGraph. It is intentionally a working document; we will keep adding observations, results, and interpretation as the benchmark suite matures.

## Current Benchmark Scope

The local benchmark suite compares three implementations on the same generated arithmetic DAGs:

- `orchestra-python`: Orchestra's Rust scheduler through the PyO3 Python package.
- `python-asyncio`: a small local Python asyncio DAG scheduler.
- `langgraph-local`: LangGraph using ordinary Python function nodes, with no LLM calls.

The benchmark shapes are:

- `chain`
- `wide`
- `fan_in`
- `layered`
- `tree`

All local benchmark runs are arithmetic-only. LLM benchmarks are separate because provider latency, API rate limits, retries, and token costs would otherwise hide scheduler overhead.

## Hosted Runtime Option

Yes, these benchmarks can run in GitHub Codespaces. A codespace runs in a Docker container on a GitHub-hosted virtual machine. GitHub's current Codespaces documentation says machine types can range from 2 cores, 8 GB RAM, and 32 GB storage up to 32 cores, 128 GB RAM, and 128 GB storage, depending on availability and account/organization policy.

Codespaces can also be changed to a different machine type after creation, if multiple machine types are available for the repository/account.

Sources:

- GitHub Codespaces overview: https://docs.github.com/en/codespaces/about-codespaces/what-are-codespaces
- Changing a codespace machine type: https://docs.github.com/en/codespaces/customizing-your-codespace/changing-the-machine-type-for-your-codespace

We are not adding a GitHub Actions workflow. If we run in Codespaces later, the manual setup is:

```bash
cd orchestra
python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install maturin
python -m pip install -r benchmarks/python/requirements.txt
maturin develop
```

## Benchmark Scale Plan

The full benchmark should compare all three implementations across multiple graph sizes so we can see where the performance gap becomes substantial.

The intended scale buckets are:

- Small: quick correctness and baseline overhead.
- Medium: first meaningful comparison.
- Large: bigger DAGs that may still complete on a normal development machine.
- Very large: tens of thousands of arithmetic nodes; this may be better suited to Codespaces or another hosted machine.

The important part is not only whether Orchestra is faster overall. The writeup should identify at what graph sizes and graph shapes the difference becomes meaningful. A runtime may look similar on tiny DAGs but diverge sharply on wide, fan-in, layered, or tree graphs as node and edge counts grow.

## Fairness Rules

The comparison should isolate scheduler/runtime overhead as much as possible.

Current fairness controls:

- Same graph generator for Orchestra, asyncio, and LangGraph.
- Same graph shape, size, edge structure, and expected answer.
- Same arithmetic operation semantics.
- Same modulus to keep outputs bounded and avoid huge integer artifacts.
- Same `repeats`, `warmups`, and `max_concurrency`.
- Graph construction and compilation happen outside the measured loop.
- The timed section measures repeated execution/invocation, not package import or process startup.
- The suite records `suite_wall_ns` separately from `median_ns`, so command startup is not confused with scheduler runtime.
- Correctness is checked on every run using `answer`, `expected_answer`, and `correct`.
- Python version and platform are recorded in each result.
- LLM calls are excluded from local scheduler benchmarks.

Known fairness caveats:

- Orchestra currently returns Python-visible outputs for all nodes, which adds Python dict conversion cost.
- Orchestra's local runner uses `execute_with_trace()`, so trace collection is included. This is useful for observability but may not represent a minimum-overhead scheduler mode.
- The asyncio baseline is intentionally simple; it is not a full framework.
- LangGraph provides more general workflow features than this benchmark uses. That extra machinery is part of what we are measuring, but it should be acknowledged in the writeup.
- Very tiny arithmetic tasks can make scheduler overhead dominate. That is intentional for a scheduler-overhead benchmark, but it does not represent workloads where each node does seconds of real work.
- Hosted runners can be noisy. Results from GitHub-hosted VMs should be repeated and treated as comparative, not absolute.

## Result Fields

The suite writes JSONL results under `benchmarks/results/`.

Important fields:

- `implementation`: `orchestra-python`, `python-asyncio`, or `langgraph-local`.
- `graph`: graph shape and size, such as `fan_in/1000`.
- `node_count`: number of DAG nodes.
- `answer`: computed result from the final node(s).
- `expected_answer`: independently computed expected result.
- `correct`: whether `answer == expected_answer`.
- `median_ns` / `median_ms`: median measured execution time.
- `p95_ns` / `p95_ms`: tail latency across repeats.
- `nodes_per_second_median`: throughput normalized by node count.
- `suite_wall_ns`: full subprocess wall time, including import, build, compile, and benchmark execution.
- `suite_status`: `ok` or `failed`.
- `error`, `stdout_tail`, `stderr_tail`: present for failed runs.

Use `median_ms`, `p95_ms`, and `nodes_per_second_median` for scheduler comparison. Use `suite_wall_ns` only for end-to-end command cost.

## LLM Benchmark Separation

LLM experiments are deliberately separate:

```bash
python benchmarks/python/orchestra_llm.py --mode single --repeats 1 --rpm 30 --pretty
python benchmarks/python/orchestra_llm.py --mode dag --repeats 1 --rpm 30 --max-concurrency 1 --pretty
```

For LLM comparisons, the writeup must report:

- Provider and model.
- Prompt shape.
- Token counts.
- Rate limit settings.
- Retry/429 behavior.
- Correctness.
- Cost assumptions.

LLM benchmark results should not be mixed with local scheduler-only benchmark results.
