from __future__ import annotations

import argparse
import sys
import time
from typing import Annotated, TypedDict

from benchmark_common import add_common_args, benchmark_report, graph_from_args, print_report
from graph_shapes import GraphSpec, NodeSpec, compute_node, terminal_nodes

try:
    from langgraph.graph import END, START, StateGraph
except ImportError:  # pragma: no cover
    END = START = StateGraph = None


def merge_values(left: dict[str, int] | None, right: dict[str, int] | None) -> dict[str, int]:
    merged = dict(left or {})
    merged.update(right or {})
    return merged


class State(TypedDict):
    values: Annotated[dict[str, int], merge_values]


def make_node(spec: GraphSpec, node: NodeSpec):
    def run(state: State) -> dict[str, dict[str, int]]:
        values = state.get("values", {})
        dependency_outputs = [values[dependency] for dependency in node.dependencies]
        output = compute_node(node, dependency_outputs, spec.modulus)
        return {"values": {node.id: output}}

    return run


def build_langgraph(spec: GraphSpec):
    if StateGraph is None:
        raise RuntimeError(
            "langgraph is not installed. Install it in the venv with: "
            "python -m pip install langgraph"
        )

    builder = StateGraph(State)

    for node in spec.nodes:
        builder.add_node(node.id, make_node(spec, node))

    for root in spec.roots():
        builder.add_edge(START, root)

    for node in spec.nodes:
        if not node.dependencies:
            continue
        if len(node.dependencies) == 1:
            builder.add_edge(node.dependencies[0], node.id)
        else:
            builder.add_edge(list(node.dependencies), node.id)

    terminals = terminal_nodes(spec)
    if len(terminals) == 1:
        builder.add_edge(terminals[0], END)
    else:
        builder.add_edge(terminals, END)

    return builder.compile()


def invoke_graph(graph, max_concurrency: int):
    return graph.invoke({"values": {}}, config={"max_concurrency": max_concurrency})


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark LangGraph with local Python functions.")
    add_common_args(parser)
    args = parser.parse_args()

    spec = graph_from_args(args)

    try:
        graph = build_langgraph(spec)
    except RuntimeError as error:
        print(str(error), file=sys.stderr)
        raise SystemExit(2)

    for _ in range(args.warmups):
        result = invoke_graph(graph, args.max_concurrency)
        answer = spec.answer_from_outputs(result["values"])
        if answer != spec.expected_answer:
            raise AssertionError(f"wrong answer: {answer} != {spec.expected_answer}")

    durations_ns = []
    answer = None

    for _ in range(args.repeats):
        start = time.perf_counter_ns()
        result = invoke_graph(graph, args.max_concurrency)
        durations_ns.append(time.perf_counter_ns() - start)

        answer = spec.answer_from_outputs(result["values"])
        if answer != spec.expected_answer:
            raise AssertionError(f"wrong answer: {answer} != {spec.expected_answer}")

    assert answer is not None

    report = benchmark_report(
        implementation="langgraph-local",
        spec=spec,
        repeats=args.repeats,
        warmups=args.warmups,
        max_concurrency=args.max_concurrency,
        durations_ns=durations_ns,
        answer=answer,
    )
    print_report(report, args.pretty)


if __name__ == "__main__":
    main()
