import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import subprocess

from experiment.compare import (
    RESULT_PATH, cases, finding_matches_schema, parse_finding, prepare, run_container, score, unpack,
)
from experiment.demo import record
from experiment.reader import observation
from kafka_oncall.runbook import investigate
from kafka_oncall.snapshot import Source, parse_snapshot


class ExperimentTests(unittest.TestCase):
    def test_all_five_baselines_match_independent_labels(self):
        inputs = list(cases())
        self.assertEqual(len(inputs), 5)
        for fixture, expected in inputs:
            with self.subTest(case=fixture["id"]):
                report = investigate(parse_snapshot(fixture)).to_dict()
                self.assertTrue(all(score(report, report["trace"], expected).values()))

    def test_blinded_copy_has_readers_but_no_answers(self):
        fixture, _ = next(cases())
        with tempfile.TemporaryDirectory() as tmp:
            lab = Path(tmp) / "lab"
            prepare(lab, fixture)
            blinded = json.loads((lab / "snapshot.json").read_text())
            self.assertEqual(blinded["id"], "snapshot")
            self.assertEqual(blinded["observations"], fixture["observations"])
            self.assertFalse((lab / "kafka_oncall/runbook.py").exists())
            config = json.loads((lab / "tools.yaml").read_text())
            tools = config["toolsets"]["snapshot"]["tools"]
            self.assertEqual({t["command"] for t in tools},
                             {f"cd /lab && python -m experiment.reader {s.value}" for s in Source})
            self.assertIn("data.partitions", next(t for t in tools if t["name"] == "read_kafka")["description"])
            response_format = json.loads((lab / "response-format.json").read_text())
            self.assertTrue(response_format["json_schema"]["strict"])

    @patch("experiment.compare.subprocess.run")
    def test_container_copies_files_without_bind_mounts(self, run):
        run.return_value.stdout = "container-id\n"
        lab, output = Path("temp/lab"), Path("temp/result.json")
        run_container(lab, output, "question", "openai/model", "OPENAI_API_KEY")
        create, upload, start, download, cleanup = [call.args[0] for call in run.call_args_list]
        self.assertEqual(create[:2], ["docker", "create"])
        self.assertIn("OPENAI_API_KEY", create)
        self.assertNotIn("-v", create)
        self.assertEqual(upload, ["docker", "cp", str(lab), "container-id:/lab"])
        self.assertEqual(start, ["docker", "start", "--attach", "container-id"])
        self.assertEqual(download, ["docker", "cp", f"container-id:{RESULT_PATH}", str(output)])
        self.assertEqual(cleanup, ["docker", "rm", "--force", "container-id"])

    @patch("experiment.compare.subprocess.run")
    def test_container_is_removed_after_failed_start(self, run):
        run.side_effect = [
            subprocess.CompletedProcess([], 0, stdout="container-id\n"),
            subprocess.CompletedProcess([], 0),
            subprocess.CalledProcessError(1, "docker start"),
            subprocess.CompletedProcess([], 0),
        ]
        with self.assertRaises(subprocess.CalledProcessError):
            run_container(Path("lab"), Path("result.json"), "question", "model", "OPENAI_API_KEY")
        self.assertEqual(run.call_args.args[0], ["docker", "rm", "--force", "container-id"])

    def test_score_requires_witnessed_reads_not_just_right_labels(self):
        fixture, expected = list(cases())[2]
        report = investigate(parse_snapshot(fixture)).to_dict()
        checks = score(report, [], expected)
        self.assertTrue(checks["cause"])
        self.assertFalse(checks["citations"])
        self.assertFalse(checks["blocker"])

    def test_tool_request_without_matching_result_is_not_verified(self):
        fixture, _ = next(cases())
        snapshot = parse_snapshot(fixture)
        call = {"id": "r1", "function": {"name": "read_hpa"}}
        response = {"result": "{}", "messages": [{"role": "assistant", "tool_calls": [call]}]}
        self.assertEqual(unpack(response, snapshot)[1][0]["status"], "unverified")
        response["messages"].append({"role": "tool", "tool_call_id": "r1",
                                     "content": json.dumps(observation(snapshot, Source.HPA))})
        self.assertEqual(unpack(response, snapshot)[1][0]["status"], "ok")

    def test_recording_never_substitutes_runbook_for_missing_model_trace(self):
        with self.assertRaisesRegex(ValueError, "No agent trace"):
            record({"model": None, "cases": [{"agent": None}]}, agent=True)

    def test_parse_finding_accepts_one_fenced_object_but_not_prose_or_multiple_blocks(self):
        block = '```json\n{"cause": "partition_cap"}\n```'
        answer, kind = parse_finding(f"## Finding\n{block}\n")
        self.assertEqual((answer, kind), ({"cause": "partition_cap"}, "fenced_json"))
        self.assertEqual(parse_finding("It looks like a cap."), ({}, "not_json"))
        self.assertEqual(parse_finding(f"{block}\n\n{block}"), ({}, "not_json"))

    def test_valid_answer_still_fails_when_required_metric_was_not_read(self):
        fixture, expected = next(cases())
        report = investigate(parse_snapshot(fixture)).to_dict()
        reads = [r for r in report["trace"] if r["source"] != Source.EXTERNAL_METRIC.pointer]
        checks = score(report, reads, expected)
        self.assertTrue(checks["cause"])
        self.assertFalse(checks["citations"])

    def test_schema_rejects_nested_cause_and_made_up_blocker(self):
        good = {"cause": "unknown", "blocked_on": "/observations/kafka",
                "summary": "Broker evidence could not be verified.", "evidence": ["/observations/hpa"]}
        self.assertTrue(finding_matches_schema(good))
        self.assertFalse(finding_matches_schema({**good, "cause": {"type": "unknown"}}))
        self.assertFalse(finding_matches_schema({**good, "blocked_on": "/observations/partitions"}))
