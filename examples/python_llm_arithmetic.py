import os

from orchestra import Flow


def load_dotenv(path=".env"):
    if not os.path.exists(path):
        return

    with open(path, "r", encoding="utf-8") as env_file:
        for line in env_file:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue

            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def main():
    load_dotenv()

    if not os.environ.get("GROQ_API_KEY"):
        raise SystemExit("Set GROQ_API_KEY before running this example.")

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

    pipeline = flow.compile(max_concurrency=2)
    result = pipeline.execute_with_trace()

    answer = int(result.outputs["f"])
    expected = ((2 + 3) + (7 + 7) + (8 + 9)) * (8 + 7)

    print("outputs:", result.outputs)
    print("answer:", answer)
    print("expected:", expected)
    print("status:", result.status)
    print("duration_ms:", result.duration_ms)
    print("event_count:", result.event_count)
    print("trace_json:", result.trace_json_pretty())

    assert answer == expected


if __name__ == "__main__":
    main()
