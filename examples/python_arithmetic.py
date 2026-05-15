from orchestra import Flow


def main():
    flow = Flow()

    flow.add_arithmetic_node("a", "add", operands=[2, 3])
    flow.add_arithmetic_node("b", "add", operands=[7, 7])
    flow.add_arithmetic_node("c", "add", operands=[8, 9])
    flow.add_arithmetic_node("d", "sum")
    flow.add_arithmetic_node("e", "add", operands=[8, 7])
    flow.add_arithmetic_node("f", "product")

    flow.add_dependency("d", "a")
    flow.add_dependency("d", "b")
    flow.add_dependency("d", "c")
    flow.add_dependency("f", "d")
    flow.add_dependency("f", "e")

    pipeline = flow.compile(max_concurrency=4)
    result = pipeline.execute_with_trace()

    answer = int(result.outputs["f"])
    expected = ((2 + 3) + (7 + 7) + (8 + 9)) * (8 + 7)

    print("outputs:", result.outputs)
    print("answer:", answer)
    print("expected:", expected)
    print("status:", result.status)
    print("duration_ms:", result.duration_ms)
    print("event_count:", result.event_count)

    assert answer == expected


if __name__ == "__main__":
    main()
