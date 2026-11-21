import unittest
from pathlib import Path

from kafka_oncall.runbook import Cause, investigate
from kafka_oncall.snapshot import Source, parse_snapshot, read_json

SCENARIOS = Path(__file__).resolve().parents[1] / "scenarios"


def load(name: str) -> dict:
    return read_json(SCENARIOS / f"{name}.json")


class RunbookTests(unittest.TestCase):
    def test_partition_cap_uses_kafka_and_external_metric_not_pods(self):
        report = investigate(parse_snapshot(load("partition-cap")))
        self.assertEqual(report.cause, Cause.PARTITION_CAP)
        self.assertIn("800 messages", report.summary)
        self.assertIn("KEDA exposed 400", report.summary)
        self.assertEqual(
            [step.source for step in report.trace],
            [source.pointer for source in (
                Source.SCALED_OBJECT, Source.HPA, Source.DEPLOYMENT,
                Source.KAFKA, Source.EXTERNAL_METRIC,
            )],
        )

    def test_cpu_branch_uses_current_pod_conditions_not_kafka(self):
        fixture = load("cpu-scheduling")
        del fixture["observations"]["kafka"]
        del fixture["observations"]["external_metric"]
        report = investigate(parse_snapshot(fixture))
        self.assertEqual(report.cause, Cause.CPU_SCHEDULING)
        self.assertIn("8 replicas; 4 are ready", report.summary)
        self.assertIn("all 4 remaining pods", report.summary)
        self.assertEqual(
            [step.source for step in report.trace],
            [source.pointer for source in (
                Source.SCALED_OBJECT, Source.HPA, Source.DEPLOYMENT, Source.PODS,
            )],
        )

    def test_missing_metric_is_unknown_even_with_matching_partition_count(self):
        report = investigate(parse_snapshot(load("missing-metric")))
        self.assertEqual(report.cause, Cause.UNKNOWN)
        self.assertEqual(report.blocked_on, Source.EXTERNAL_METRIC.pointer)
        self.assertEqual(report.trace[-1].status, "missing")
        self.assertNotIn(Source.EXTERNAL_METRIC.pointer, report.evidence)

    def test_incomplete_but_valid_snapshot_is_unknown(self):
        fixture = load("partition-cap")
        fixture["observations"] = {}
        report = investigate(parse_snapshot(fixture))
        self.assertEqual(report.cause, Cause.UNKNOWN)
        self.assertEqual(report.blocked_on, Source.SCALED_OBJECT.pointer)
        self.assertEqual(report.evidence, ())

    def test_mixed_or_unconfirmed_scheduling_causes_are_unknown(self):
        mutations = (
            {"message": "0/2 nodes are available: 1 Insufficient cpu, 1 Insufficient memory."},
            {"message": "Pending because a different pod reported Insufficient cpu."},
            {"scheduled": True},
            {"scheduled": None},
            {"phase": "Running"},
            {"reason": "SchedulingGated"},
        )
        for fields in mutations:
            with self.subTest(fields=fields):
                fixture = load("cpu-scheduling")
                fixture["observations"]["pods"]["items"][-1].update(fields)
                report = investigate(parse_snapshot(fixture))
                self.assertEqual(report.cause, Cause.UNKNOWN)
                self.assertEqual(report.blocked_on, Source.PODS.pointer)

    def test_one_confirmed_cpu_failure_does_not_explain_an_incomplete_pod_list(self):
        fixture = load("cpu-scheduling")
        del fixture["observations"]["pods"]["items"][-1]
        report = investigate(parse_snapshot(fixture))
        self.assertEqual(report.cause, Cause.UNKNOWN)
        self.assertEqual(report.blocked_on, Source.PODS.pointer)

    def test_hpa_and_deployment_counts_must_agree_before_diagnosing(self):
        fixture = load("cpu-scheduling")
        fixture["observations"]["deployment"]["desired_replicas"] = 4
        report = investigate(parse_snapshot(fixture))
        self.assertEqual(report.cause, Cause.UNKNOWN)
        self.assertEqual(report.blocked_on, Source.DEPLOYMENT.pointer)
        self.assertNotIn(Source.PODS.pointer, report.evidence)

    def test_partition_cap_uses_pinned_scaler_integer_boundary(self):
        for lag, expected in ((400, Cause.UNKNOWN), (499, Cause.UNKNOWN), (500, Cause.PARTITION_CAP)):
            with self.subTest(lag=lag):
                fixture = load("partition-cap")
                fixture["observations"]["kafka"]["lag"] = lag
                self.assertEqual(investigate(parse_snapshot(fixture)).cause, expected)

    def test_rejects_unisolated_cap_causes(self):
        mutations = (
            (Source.SCALED_OBJECT, "allow_idle_consumers", True),
            (Source.SCALED_OBJECT, "plain_lag_mode", False),
            (Source.SCALED_OBJECT, "scaler_profile", "different-version"),
            (Source.HPA, "min_replicas", 4),
            (Source.HPA, "max_replicas", 4),
            (Source.HPA, "scaling_limited", True),
            (Source.HPA, "scaling_limited", None),
            (Source.HPA, "target_average_value", 200),
            (Source.EXTERNAL_METRIC, "value", 800),
        )
        for source, field, value in mutations:
            with self.subTest(source=source, field=field, value=value):
                fixture = load("partition-cap")
                fixture["observations"][source][field] = value
                report = investigate(parse_snapshot(fixture))
                self.assertEqual(report.cause, Cause.UNKNOWN)
                self.assertEqual(report.blocked_on, source.pointer)

    def test_checks_scope_at_every_evidence_hop(self):
        mutations = (
            (Source.SCALED_OBJECT, "target", "other/orders-worker"),
            (Source.HPA, "name", "unrelated-hpa"),
            (Source.HPA, "metric", "unrelated-metric"),
            (Source.DEPLOYMENT, "target", "demo/other-worker"),
            (Source.KAFKA, "topic", "unrelated-topic"),
            (Source.KAFKA, "consumer_group", "unrelated-group"),
            (Source.KAFKA, "kafka_cluster", "other-kafka"),
            (Source.EXTERNAL_METRIC, "namespace", "other"),
            (Source.EXTERNAL_METRIC, "scaled_object", "other-trigger"),
            (Source.PODS, "target", "demo/other-worker"),
        )
        for source, field, value in mutations:
            with self.subTest(source=source, field=field):
                fixture = load("cpu-scheduling" if source == Source.PODS else "partition-cap")
                fixture["observations"][source][field] = value
                report = investigate(parse_snapshot(fixture))
                self.assertEqual(report.cause, Cause.UNKNOWN)
                self.assertEqual(report.blocked_on, source.pointer)
                self.assertEqual(report.trace[-1].status, "wrong_scope")
                self.assertNotIn(source.pointer, report.evidence)

    def test_freshness_window_includes_exactly_thirty_seconds(self):
        dates = (
            ("2026-11-21T09:59:39.999999Z", "stale"),
            ("2026-11-21T09:59:40Z", "ok"),
            ("2026-11-21T10:00:10Z", "ok"),
            ("2026-11-21T10:00:10.000001Z", "future"),
        )
        for at, status in dates:
            with self.subTest(at=at):
                fixture = load("partition-cap")
                fixture["observations"]["external_metric"]["observed_at"] = at
                report = investigate(parse_snapshot(fixture))
                self.assertEqual(report.trace[-1].status, status)
                expected = Cause.PARTITION_CAP if status == "ok" else Cause.UNKNOWN
                self.assertEqual(report.cause, expected)

    def test_scenario_id_does_not_drive_the_result(self):
        fixture = load("partition-cap")
        fixture["id"] = "cpu-scheduling"
        report = investigate(parse_snapshot(fixture))
        self.assertEqual(report.cause, Cause.PARTITION_CAP)
