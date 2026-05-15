import os
import unittest

from orchestra import Flow


class BindingTests(unittest.TestCase):
    def test_flow_starts_empty(self):
        flow = Flow()

        self.assertEqual(flow.node_count(), 0)

    def test_duplicate_node_is_rejected(self):
        flow = Flow()
        flow.add_fake_node("a", "first")

        with self.assertRaisesRegex(RuntimeError, "duplicate node id"):
            flow.add_fake_node("a", "second")

    def test_missing_dependency_is_rejected(self):
        flow = Flow()
        flow.add_fake_node("writer", "write")

        with self.assertRaisesRegex(RuntimeError, "missing dependency"):
            flow.add_dependency("writer", "researcher")

    def test_local_arithmetic_dag_returns_expected_answer(self):
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

        result = flow.compile(max_concurrency=4).execute_with_trace()

        self.assertEqual(int(result.outputs["f"]), 540)
        self.assertEqual(result.status, "Completed")

    def test_local_arithmetic_can_use_modulus(self):
        flow = Flow()
        flow.add_arithmetic_node("a", "const", operands=[1_000_000_008], modulus=1_000_000_007)

        outputs = flow.compile().execute()

        self.assertEqual(outputs["a"], "1")

    def test_failed_task_reports_failure(self):
        flow = Flow()
        flow.add_fake_node("fail", "unused", fail_with="boom")

        result = flow.compile().execute_report()

        self.assertEqual(result.status, "Failed")
        self.assertIn("boom", result.error)
        self.assertEqual(result.outputs, {})

    @unittest.skipUnless(
        os.environ.get("RUN_LLM_TESTS") == "1" and os.environ.get("GROQ_API_KEY"),
        "set RUN_LLM_TESTS=1 and GROQ_API_KEY to run live LLM binding tests",
    )
    def test_groq_llm_node_can_run_tiny_arithmetic(self):
        flow = Flow()
        flow.add_groq_llm_node("answer", "3 + 5", max_tokens=4)

        outputs = flow.compile(max_concurrency=1).execute()

        self.assertEqual(int(outputs["answer"]), 8)


if __name__ == "__main__":
    unittest.main()
