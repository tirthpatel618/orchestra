from __future__ import annotations

import argparse
import asyncio
import time
from collections import deque
from dataclasses import dataclass

from benchmark_common import add_common_args, benchmark_report, graph_from_args, print_report
from graph_shapes import GraphSpec, NodeSpec, compute_node


@dataclass(frozen=True)
class CompiledAsyncGraph:
    spec: GraphSpec
    nodes: dict[str, NodeSpec]
    remaining_dependencies: dict[str, int]
    dependents: dict[str, list[str]]
    roots: tuple[str, ...]
    max_concurrency: int

    @classmethod
    def compile(cls, spec: GraphSpec, max_concurrency: int) -> "CompiledAsyncGraph":
        return cls(
            spec=spec,
            nodes={node.id: node for node in spec.nodes},
            remaining_dependencies={
                node.id: len(node.dependencies) for node in spec.nodes
            },
            dependents=spec.dependents_by_node(),
            roots=tuple(spec.roots()),
            max_concurrency=max(1, max_concurrency),
        )

    async def execute(self) -> dict[str, int]:
        remaining_dependencies = dict(self.remaining_dependencies)
        ready = deque(self.roots)
        outputs: dict[str, int] = {}
        running: dict[asyncio.Task[tuple[str, int]], str] = {}

        while len(outputs) < len(self.nodes):
            while ready and len(running) < self.max_concurrency:
                node_id = ready.popleft()
                node = self.nodes[node_id]
                dependency_outputs = [outputs[dependency] for dependency in node.dependencies]
                task = asyncio.create_task(
                    run_node(node, dependency_outputs, self.spec.modulus)
                )
                running[task] = node_id

            if not running:
                raise RuntimeError("graph made no progress")

            done, _ = await asyncio.wait(
                running.keys(),
                return_when=asyncio.FIRST_COMPLETED,
            )

            for task in done:
                running.pop(task)
                node_id, output = task.result()
                outputs[node_id] = output

                for child in self.dependents.get(node_id, []):
                    remaining_dependencies[child] -= 1
                    if remaining_dependencies[child] == 0:
                        ready.append(child)

        return outputs


async def run_node(node: NodeSpec, dependency_outputs: list[int], modulus: int) -> tuple[str, int]:
    return node.id, compute_node(node, dependency_outputs, modulus)


async def run_benchmark(args: argparse.Namespace) -> None:
    spec = graph_from_args(args)
    graph = CompiledAsyncGraph.compile(spec, args.max_concurrency)

    for _ in range(args.warmups):
        outputs = await graph.execute()
        answer = spec.answer_from_outputs(outputs)
        if answer != spec.expected_answer:
            raise AssertionError(f"wrong answer: {answer} != {spec.expected_answer}")

    durations_ns = []
    answer = None

    for _ in range(args.repeats):
        start = time.perf_counter_ns()
        outputs = await graph.execute()
        durations_ns.append(time.perf_counter_ns() - start)

        answer = spec.answer_from_outputs(outputs)
        if answer != spec.expected_answer:
            raise AssertionError(f"wrong answer: {answer} != {spec.expected_answer}")

    assert answer is not None

    report = benchmark_report(
        implementation="python-asyncio",
        spec=spec,
        repeats=args.repeats,
        warmups=args.warmups,
        max_concurrency=args.max_concurrency,
        durations_ns=durations_ns,
        answer=answer,
    )
    print_report(report, args.pretty)


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark a local Python asyncio DAG scheduler.")
    add_common_args(parser)
    args = parser.parse_args()
    asyncio.run(run_benchmark(args))


if __name__ == "__main__":
    main()
