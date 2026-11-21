import unittest
from contextlib import redirect_stderr, redirect_stdout
from io import StringIO
from json import dumps, loads
from pathlib import Path
from tempfile import TemporaryDirectory

from kafka_oncall.cli import main
from kafka_oncall.evaluation import evaluate
from kafka_oncall.snapshot import InputError, parse_snapshot, read_json

SCENARIOS = Path(__file__).resolve().parents[1] / "scenarios"


def cli(*args: str) -> tuple[int, str, str]:
    stdout = StringIO()
    stderr = StringIO()
    with redirect_stdout(stdout), redirect_stderr(stderr):
        status = main(list(args))
    return status, stdout.getvalue(), stderr.getvalue()


class InputTests(unittest.TestCase):
    def test_parser_rejects_bad_versions_types_and_fields(self):
        mutations = (
            ("format", "kafka-oncall/snapshot-v2", "snapshot/format"),
            ("as_of", "2026-11-21T10:00:10", "as_of"),
            ("target", "bad-target", "snapshot/target"),
            ("observations", {"hpa": None}, "must be an object"),
            ("observations", {"not-a-source": 0}, "unknown"),
        )
        for field, value, error in mutations:
            with self.subTest(field=field, value=value):
                fixture = read_json(SCENARIOS / "partition-cap.json")
                fixture[field] = value
                with self.assertRaisesRegex(InputError, error):
                    parse_snapshot(fixture)

    def test_boolean_counts_are_not_integers(self):
        fixture = read_json(SCENARIOS / "partition-cap.json")
        fixture["observations"]["kafka"]["lag"] = True
        with self.assertRaisesRegex(InputError, "kafka/lag"):
            parse_snapshot(fixture)

    def test_duplicate_pods_are_invalid(self):
        fixture = read_json(SCENARIOS / "cpu-scheduling.json")
        items = fixture["observations"]["pods"]["items"]
        items.append(items[0])
        with self.assertRaisesRegex(InputError, "duplicate pod"):
            parse_snapshot(fixture)

    def test_duplicate_json_keys_are_invalid(self):
        with TemporaryDirectory() as temp:
            fixture = Path(temp) / "duplicates.json"
            fixture.write_text('{"id": "first", "id": "second"}', encoding="utf-8")
            with self.assertRaisesRegex(InputError, "duplicate JSON key"):
                read_json(fixture)


class CliTests(unittest.TestCase):
    def test_evaluation_passes_all_scenarios(self):
        status, out, err = cli("evaluate", str(SCENARIOS / "suite.json"))
        self.assertEqual((status, err), (0, ""))
        self.assertIn("3/3 scenarios passed", out)

    def test_unknown_is_a_valid_replay_and_report_is_deterministic(self):
        args = ("replay", str(SCENARIOS / "missing-metric.json"), "--json")
        status, out, err = cli(*args)
        self.assertEqual((status, err), (0, ""))
        self.assertEqual(cli(*args), (status, out, err))
        parsed = loads(out)
        self.assertEqual(parsed["format"], "kafka-oncall/report-v1")
        self.assertEqual(parsed["cause"], "unknown")
        self.assertEqual(parsed["blocked_on"], "/observations/external_metric")

    def test_wrong_answer_missing_citation_and_wrong_blocker_fail_evaluation(self):
        with TemporaryDirectory() as temp:
            suite_dir = Path(temp)
            fixture = SCENARIOS / "missing-metric.json"
            (suite_dir / fixture.name).write_bytes(fixture.read_bytes())
            suite = suite_dir / "suite.json"
            data = read_json(SCENARIOS / "suite.json")
            data["cases"] = [data["cases"][-1]]
            expected = data["cases"][0]["expected"]
            expected["required_evidence"].append("/observations/external_metric")
            expected["blocked_on"] = "/observations/pods"
            suite.write_text(dumps(data), encoding="utf-8")
            status, out, err = cli("evaluate", str(suite), "--json")
            self.assertEqual((status, err), (1, ""))
            self.assertEqual(
                loads(out)["cases"][0]["checks"],
                {"cause": True, "evidence": False, "blocker": False},
            )

            expected["cause"] = "partition_cap"
            expected["blocked_on"] = None
            suite.write_text(dumps(data), encoding="utf-8")
            self.assertFalse(evaluate(suite)[0].checks["cause"])

    def test_suite_references_cannot_escape_its_directory(self):
        with TemporaryDirectory() as temp:
            suite = Path(temp) / "suite.json"
            original = (SCENARIOS / "suite.json").read_text(encoding="utf-8")
            suite.write_text(original.replace("partition-cap.json", "../partition-cap.json"), encoding="utf-8")
            status, out, err = cli("evaluate", str(suite))
            self.assertEqual((status, out), (2, ""))
            self.assertIn("/snapshot", err)

    def test_invalid_input_reports_error_without_traceback(self):
        status, out, err = cli("replay", str(SCENARIOS / "suite.json"))
        self.assertEqual((status, out), (2, ""))
        self.assertIn("input error:", err)
        self.assertNotIn("Traceback", err)
