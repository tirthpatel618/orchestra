# Orchestra

## Orchestration/runtime layer 
Python user code defines workflow
Rust runtime executes workflow, then streams events/traces back

## What Orchestra Will Own: 
- DAG validation
- dependency scheduling
- task lifecycle
- concurrency
- bounded event channels
- timeout/cancellation hooks
- retry policy
- telemetry
- run traces
- Python event stream

## Want to measure: 
 Measured coordination overhead of a Rust scheduler vs equivalent Python asyncio scheduler across sequential chains, parallel fan-out, and fan-in workflows.

## Final python API shape

