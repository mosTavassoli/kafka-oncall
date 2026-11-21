import json
from pathlib import Path
import tempfile
import unittest

from experiment.compare import cases, prepare, score, unpack
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

    def test_blinded_mount_has_readers_but_no_answers(self):
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
