from __future__ import annotations

import argparse
import json
import platform
import statistics
import sys
from typing import Any

from graph_shapes import MODULUS, GraphSpec, build_graph


def add_common_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--shape",
        choices=["chain", "wide", "fan_in", "layered", "tree"],
        default="chain",
    )
    parser.add_argument("--size", type=int, default=1000)
    parser.add_argument("--width", type=int, default=25)
    parser.add_argument("--depth", type=int, default=10)
    parser.add_argument("--branching", type=int, default=3)
    parser.add_argument("--repeats", type=int, default=100)
    parser.add_argument("--warmups", type=int, default=10)
    parser.add_argument("--max-concurrency", type=int, default=256)
    parser.add_argument("--modulus", type=int, default=MODULUS)
    parser.add_argument("--pretty", action="store_true")


def graph_from_args(args: argparse.Namespace) -> GraphSpec:
    return build_graph(
        args.shape,
        size=args.size,
        width=args.width,
        depth=args.depth,
        branching=args.branching,
        modulus=args.modulus,
    )


def percentile(values: list[int], percentile_value: float) -> int:
    if not values:
        raise ValueError("cannot compute percentile for empty values")

    ordered = sorted(values)
    index = round((percentile_value / 100) * (len(ordered) - 1))
    return ordered[index]


def benchmark_report(
    *,
    implementation: str,
    spec: GraphSpec,
    repeats: int,
    warmups: int,
    max_concurrency: int | None,
    durations_ns: list[int],
    answer: int,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    median_ns = int(statistics.median(durations_ns))
    p95_ns = percentile(durations_ns, 95)
    node_count = len(spec.nodes)

    report: dict[str, Any] = {
        "implementation": implementation,
        "graph": spec.name,
        "node_count": node_count,
        "answer": answer,
        "expected_answer": spec.expected_answer,
        "correct": answer == spec.expected_answer,
        "repeats": repeats,
        "warmups": warmups,
        "max_concurrency": max_concurrency,
        "median_ns": median_ns,
        "p95_ns": p95_ns,
        "min_ns": min(durations_ns),
        "max_ns": max(durations_ns),
        "median_ms": median_ns / 1_000_000,
        "p95_ms": p95_ns / 1_000_000,
        "nodes_per_second_median": node_count / (median_ns / 1_000_000_000),
        "python": sys.version.split()[0],
        "platform": platform.platform(),
    }

    if extra:
        report.update(extra)

    return report


def print_report(report: dict[str, Any], pretty: bool) -> None:
    if pretty:
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        print(json.dumps(report, sort_keys=True))
