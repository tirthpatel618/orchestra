from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from benchmark_common import add_common_args


HERE = Path(__file__).resolve().parent
DEFAULT_RESULTS_DIR = HERE.parent / "results"

RUNNERS = {
    "orchestra": HERE / "orchestra_local.py",
    "asyncio": HERE / "asyncio_local.py",
    "langgraph": HERE / "langgraph_local.py",
}

PRESETS: dict[str, list[dict[str, int | str]]] = {
    "smoke": [
        {"shape": "chain", "size": 20, "repeats": 3, "warmups": 1},
        {"shape": "wide", "size": 50, "repeats": 3, "warmups": 1},
        {"shape": "fan_in", "size": 50, "repeats": 3, "warmups": 1},
        {"shape": "layered", "width": 5, "depth": 4, "repeats": 3, "warmups": 1},
        {
            "shape": "tree",
            "depth": 4,
            "branching": 3,
            "repeats": 3,
            "warmups": 1,
        },
    ],
    "medium": [
        {"shape": "chain", "size": 1_000, "repeats": 30, "warmups": 5},
        {"shape": "wide", "size": 1_000, "repeats": 30, "warmups": 5},
        {"shape": "fan_in", "size": 1_000, "repeats": 30, "warmups": 5},
        {"shape": "layered", "width": 25, "depth": 10, "repeats": 20, "warmups": 3},
        {
            "shape": "tree",
            "depth": 6,
            "branching": 3,
            "repeats": 20,
            "warmups": 3,
        },
    ],
    "large": [
        {"shape": "chain", "size": 10_000, "repeats": 10, "warmups": 2},
        {"shape": "wide", "size": 10_000, "repeats": 10, "warmups": 2},
        {"shape": "fan_in", "size": 10_000, "repeats": 10, "warmups": 2},
        {"shape": "layered", "width": 50, "depth": 20, "repeats": 8, "warmups": 2},
        {
            "shape": "tree",
            "depth": 8,
            "branching": 3,
            "repeats": 8,
            "warmups": 2,
        },
    ],
}


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run Orchestra, asyncio, and LangGraph on identical local arithmetic DAGs."
    )
    add_common_args(parser)
    parser.add_argument(
        "--preset",
        choices=["single", *PRESETS.keys()],
        default="single",
        help="single uses --shape/--size/etc; presets run several graph cases.",
    )
    parser.add_argument(
        "--implementations",
        nargs="+",
        choices=sorted(RUNNERS.keys()),
        default=["orchestra", "asyncio", "langgraph"],
    )
    parser.add_argument("--output", type=Path)
    parser.add_argument("--timeout-seconds", type=float)
    args = parser.parse_args()

    cases = preset_cases(args) if args.preset != "single" else [single_case(args)]
    output = args.output or default_output_path(args.preset)
    output.parent.mkdir(parents=True, exist_ok=True)

    print(f"writing results: {output}")
    entries = []

    with output.open("w", encoding="utf-8") as results_file:
        for case_index, case in enumerate(cases):
            print(case_label(case_index, case))
            case_entries = []

            for implementation in args.implementations:
                entry = run_case(
                    implementation=implementation,
                    case=case,
                    timeout_seconds=args.timeout_seconds,
                )
                entry["suite_preset"] = args.preset
                entry["case_index"] = case_index
                entry["case"] = case

                results_file.write(json.dumps(entry, sort_keys=True) + "\n")
                results_file.flush()

                entries.append(entry)
                case_entries.append(entry)
                print_entry(entry)

            print_case_comparison(case_entries)

    print_summary(entries, output)


def single_case(args: argparse.Namespace) -> dict[str, int | str]:
    return {
        "shape": args.shape,
        "size": args.size,
        "width": args.width,
        "depth": args.depth,
        "branching": args.branching,
        "repeats": args.repeats,
        "warmups": args.warmups,
        "max_concurrency": args.max_concurrency,
        "modulus": args.modulus,
    }


def preset_cases(args: argparse.Namespace) -> list[dict[str, int | str]]:
    cases = []
    for preset_case in PRESETS[args.preset]:
        case = single_case(args)
        case.update(preset_case)
        cases.append(case)
    return cases


def default_output_path(preset: str) -> Path:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return DEFAULT_RESULTS_DIR / f"local_{preset}_{timestamp}.jsonl"


def run_case(
    *,
    implementation: str,
    case: dict[str, int | str],
    timeout_seconds: float | None,
) -> dict[str, Any]:
    command = command_for(implementation, case)
    started = time.perf_counter_ns()

    try:
        completed = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
        )
        wall_ns = time.perf_counter_ns() - started
    except subprocess.TimeoutExpired as error:
        return failure_entry(
            implementation=implementation,
            command=command,
            wall_ns=time.perf_counter_ns() - started,
            returncode=None,
            error=f"timed out after {timeout_seconds} seconds",
            stdout=error.stdout or "",
            stderr=error.stderr or "",
        )

    if completed.returncode != 0:
        return failure_entry(
            implementation=implementation,
            command=command,
            wall_ns=wall_ns,
            returncode=completed.returncode,
            error="runner exited with non-zero status",
            stdout=completed.stdout,
            stderr=completed.stderr,
        )

    try:
        report = json.loads(completed.stdout)
    except json.JSONDecodeError as error:
        return failure_entry(
            implementation=implementation,
            command=command,
            wall_ns=wall_ns,
            returncode=completed.returncode,
            error=f"runner did not print JSON: {error}",
            stdout=completed.stdout,
            stderr=completed.stderr,
        )

    report["suite_status"] = "ok"
    report["suite_wall_ns"] = wall_ns
    report["suite_command"] = command
    return report


def failure_entry(
    *,
    implementation: str,
    command: list[str],
    wall_ns: int,
    returncode: int | None,
    error: str,
    stdout: str,
    stderr: str,
) -> dict[str, Any]:
    return {
        "implementation": implementation,
        "suite_status": "failed",
        "suite_wall_ns": wall_ns,
        "suite_command": command,
        "returncode": returncode,
        "error": error,
        "stdout_tail": tail(stdout),
        "stderr_tail": tail(stderr),
    }


def command_for(implementation: str, case: dict[str, int | str]) -> list[str]:
    runner = RUNNERS[implementation]
    command = [
        sys.executable,
        str(runner),
        "--shape",
        str(case["shape"]),
        "--repeats",
        str(case["repeats"]),
        "--warmups",
        str(case["warmups"]),
        "--max-concurrency",
        str(case["max_concurrency"]),
        "--modulus",
        str(case["modulus"]),
    ]

    shape = case["shape"]
    if shape in ("chain", "wide", "fan_in"):
        command.extend(["--size", str(case["size"])])
    elif shape == "layered":
        command.extend(["--width", str(case["width"]), "--depth", str(case["depth"])])
    elif shape == "tree":
        command.extend(
            ["--depth", str(case["depth"]), "--branching", str(case["branching"])]
        )
    else:
        raise ValueError(f"unknown shape: {shape}")

    return command


def case_label(index: int, case: dict[str, int | str]) -> str:
    shape = case["shape"]
    if shape in ("chain", "wide", "fan_in"):
        detail = f"size={case['size']}"
    elif shape == "layered":
        detail = f"width={case['width']} depth={case['depth']}"
    else:
        detail = f"depth={case['depth']} branching={case['branching']}"

    return (
        f"\ncase {index}: {shape} {detail} "
        f"repeats={case['repeats']} warmups={case['warmups']}"
    )


def print_entry(entry: dict[str, Any]) -> None:
    implementation = entry["implementation"]
    if entry["suite_status"] != "ok":
        print(f"  {implementation:16} failed: {entry['error']}")
        return

    print(
        f"  {implementation:16} "
        f"median={entry['median_ms']:.3f}ms "
        f"p95={entry['p95_ms']:.3f}ms "
        f"nodes/s={entry['nodes_per_second_median']:.0f} "
        f"correct={entry['correct']}"
    )


def print_case_comparison(entries: list[dict[str, Any]]) -> None:
    ok_entries = [entry for entry in entries if entry["suite_status"] == "ok"]
    orchestra = next(
        (entry for entry in ok_entries if entry["implementation"] == "orchestra-python"),
        None,
    )
    if orchestra is None:
        return

    for entry in ok_entries:
        if entry is orchestra:
            continue

        speedup = entry["median_ns"] / orchestra["median_ns"]
        print(
            f"  orchestra vs {entry['implementation']}: "
            f"{speedup:.2f}x by median latency"
        )


def print_summary(entries: list[dict[str, Any]], output: Path) -> None:
    failures = [entry for entry in entries if entry["suite_status"] != "ok"]
    incorrect = [
        entry for entry in entries if entry["suite_status"] == "ok" and not entry["correct"]
    ]

    print(f"\nresults written to {output}")
    print(f"runs: {len(entries)}")
    print(f"failures: {len(failures)}")
    print(f"incorrect: {len(incorrect)}")


def tail(value: str, max_chars: int = 2_000) -> str:
    return value[-max_chars:]


if __name__ == "__main__":
    main()
