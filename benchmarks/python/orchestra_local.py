from __future__ import annotations

import argparse
import time

from benchmark_common import add_common_args, benchmark_report, graph_from_args, print_report
from orchestra import Flow


def build_orchestra_flow(spec):
    flow = Flow()

    for node in spec.nodes:
        flow.add_arithmetic_node(
            node.id,
            node.operation,
            operands=list(node.operands),
            modulus=spec.modulus,
        )

    for node in spec.nodes:
        for dependency in node.dependencies:
            flow.add_dependency(node.id, dependency)

    return flow


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark Orchestra Python bindings locally.")
    add_common_args(parser)
    args = parser.parse_args()

    spec = graph_from_args(args)
    flow = build_orchestra_flow(spec)
    pipeline = flow.compile(max_concurrency=args.max_concurrency)

    for _ in range(args.warmups):
        outputs = pipeline.execute()
        answer = spec.answer_from_outputs(outputs)
        if answer != spec.expected_answer:
            raise AssertionError(f"wrong answer: {answer} != {spec.expected_answer}")

    durations_ns = []
    answer = None
    last_trace = None

    for _ in range(args.repeats):
        start = time.perf_counter_ns()
        result = pipeline.execute_with_trace()
        durations_ns.append(time.perf_counter_ns() - start)

        answer = spec.answer_from_outputs(result.outputs)
        if answer != spec.expected_answer:
            raise AssertionError(f"wrong answer: {answer} != {spec.expected_answer}")
        last_trace = result

    assert answer is not None
    assert last_trace is not None

    report = benchmark_report(
        implementation="orchestra-python",
        spec=spec,
        repeats=args.repeats,
        warmups=args.warmups,
        max_concurrency=args.max_concurrency,
        durations_ns=durations_ns,
        answer=answer,
        extra={
            "last_trace_duration_ms": last_trace.duration_ms,
            "last_trace_event_count": last_trace.event_count,
            "last_trace_streamed_chunk_count": last_trace.streamed_chunk_count,
        },
    )
    print_report(report, args.pretty)


if __name__ == "__main__":
    main()
