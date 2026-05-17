# Orchestra

Orchestra is a Rust-first DAG orchestration runtime for running dependency-aware task graphs. Python code can define a workflow, then Orchestra compiles and executes the graph in Rust while streaming events, traces, and outputs back to the caller.

The current focus is proving the scheduler core: graph validation, dependency scheduling, concurrent node execution, output passing, failure handling, telemetry, and Python bindings.

See [report.md](./report.md) for the benchmark setup, current results, limitations, and roadmap.
