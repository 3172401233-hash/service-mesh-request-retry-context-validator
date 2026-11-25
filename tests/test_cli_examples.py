import contextlib
import io
import json
import os
import unittest

from retryctx.cli import main
from retryctx.validator import validate_scenario


ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
EXAMPLE_DIR = os.path.join(ROOT, "examples")


class ExampleAndCliTests(unittest.TestCase):
    def test_every_example_validates_and_is_json_serializable(self):
        names = sorted(name for name in os.listdir(EXAMPLE_DIR) if name.endswith(".json"))
        self.assertGreaterEqual(len(names), 3)
        for name in names:
            with self.subTest(example=name):
                with open(os.path.join(EXAMPLE_DIR, name), "r", encoding="utf-8") as handle:
                    scenario = json.load(handle)
                report = validate_scenario(scenario)
                # Headline outputs always exist.
                self.assertIn("idempotency_key", report)
                self.assertIn("retryable_positions", report)
                self.assertIn("duplicate_side_effect_risks", report)
                # The whole report must be JSON friendly.
                json.dumps(report, ensure_ascii=False)

    def test_demo_cli_emits_json_report(self):
        buffer = io.StringIO()
        with contextlib.redirect_stdout(buffer):
            exit_code = main(["--demo", "--format", "json"])
        self.assertEqual(exit_code, 0)
        report = json.loads(buffer.getvalue())
        self.assertTrue(report["idempotency_key"].startswith("idem-demo"))
        self.assertIn("positions", report)

    def test_text_format_mentions_key_and_risks(self):
        buffer = io.StringIO()
        with contextlib.redirect_stdout(buffer):
            exit_code = main(["--demo", "--format", "text"])
        self.assertEqual(exit_code, 0)
        output = buffer.getvalue()
        self.assertIn("idempotency_key", output)
        self.assertIn("duplicate side-effect risks", output)

    def test_example_file_cli(self):
        path = os.path.join(EXAMPLE_DIR, "retry-chain.json")
        buffer = io.StringIO()
        with contextlib.redirect_stdout(buffer):
            exit_code = main([path, "--format", "json"])
        self.assertEqual(exit_code, 0)
        report = json.loads(buffer.getvalue())
        self.assertEqual(report["summary"]["high_duplicate_side_effect_risk_count"], 1)


if __name__ == "__main__":
    unittest.main()
