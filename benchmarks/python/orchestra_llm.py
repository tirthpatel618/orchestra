from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path

from orchestra import Flow


def load_dotenv(path: str = ".env") -> None:
    env_path = Path(path)
    if not env_path.exists():
        return

    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def build_single_call() -> tuple[Flow, str, int, int]:
    flow = Flow()
    flow.add_groq_llm_node("answer", "3 + 5", max_tokens=4)
    return flow, "answer", 8, 1


def build_arithmetic_dag() -> tuple[Flow, str, int, int]:
    flow = Flow()
    flow.add_groq_llm_node("a", "2 + 3", max_tokens=4)
    flow.add_groq_llm_node("b", "7 + 7", max_tokens=4)
    flow.add_groq_llm_node("c", "8 + 9", max_tokens=4)
    flow.add_groq_llm_node(
        "d",
        "{a} + {b} + {c}",
        max_tokens=4,
        substitute_dependency_outputs=True,
    )
    flow.add_groq_llm_node("e", "8 + 7", max_tokens=4)
    flow.add_groq_llm_node(
        "f",
        "{d} * {e}",
        max_tokens=4,
        substitute_dependency_outputs=True,
    )

    flow.add_dependency("d", "a")
    flow.add_dependency("d", "b")
    flow.add_dependency("d", "c")
    flow.add_dependency("f", "d")
    flow.add_dependency("f", "e")

    return flow, "f", 540, 6


def main() -> None:
    parser = argparse.ArgumentParser(description="Run small, labeled Orchestra LLM experiments.")
    parser.add_argument("--mode", choices=["single", "dag"], default="single")
    parser.add_argument("--repeats", type=int, default=1)
    parser.add_argument("--rpm", type=int, default=30)
    parser.add_argument("--max-concurrency", type=int, default=1)
    parser.add_argument("--pretty", action="store_true")
    args = parser.parse_args()

    load_dotenv()
    if not os.environ.get("GROQ_API_KEY"):
        raise SystemExit("Set GROQ_API_KEY in the environment or .env first.")

    if args.mode == "single":
        flow, answer_node, expected, calls_per_run = build_single_call()
    else:
        flow, answer_node, expected, calls_per_run = build_arithmetic_dag()

    pipeline = flow.compile(max_concurrency=args.max_concurrency)
    min_run_interval_seconds = (calls_per_run / args.rpm) * 60
    runs = []

    for index in range(args.repeats):
        started = time.perf_counter()
        result = pipeline.execute_with_trace()
        elapsed = time.perf_counter() - started

        answer = int(result.outputs[answer_node])
        runs.append(
            {
                "index": index,
                "answer": answer,
                "expected_answer": expected,
                "correct": answer == expected,
                "elapsed_seconds": elapsed,
                "trace_duration_ms": result.duration_ms,
                "event_count": result.event_count,
            }
        )

        if answer != expected:
            raise AssertionError(f"wrong answer: {answer} != {expected}")

        remaining_sleep = min_run_interval_seconds - elapsed
        if index < args.repeats - 1 and remaining_sleep > 0:
            time.sleep(remaining_sleep)

    report = {
        "implementation": "orchestra-python-llm",
        "mode": args.mode,
        "provider": "groq",
        "calls_per_run": calls_per_run,
        "target_rpm": args.rpm,
        "max_concurrency": args.max_concurrency,
        "repeats": args.repeats,
        "runs": runs,
    }

    if args.pretty:
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        print(json.dumps(report, sort_keys=True))


if __name__ == "__main__":
    main()
