"""Score the runbook without a model or generated answer text."""

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from kafka_oncall.runbook import Cause, Report, investigate
from kafka_oncall.snapshot import Fields, InputError, Source, parse_snapshot, read_json

SUITE_FORMAT = "kafka-oncall/suite-v1"
EVALUATION_FORMAT = "kafka-oncall/evaluation-v1"
_POINTERS = {source.pointer for source in Source}


@dataclass(frozen=True, slots=True)
class CaseResult:
    snapshot: str
    report: Report
    expected_cause: Cause
    required_evidence: tuple[str, ...]
    expected_blocker: str | None

    @property
    def checks(self) -> dict[str, bool]:
        return {
            "cause": self.report.cause == self.expected_cause,
            "evidence": set(self.required_evidence).issubset(self.report.evidence),
            "blocker": self.report.blocked_on == self.expected_blocker,
        }

    @property
    def passed(self) -> bool:
        return all(self.checks.values())

    def to_dict(self) -> dict[str, Any]:
        return {
            "snapshot": self.snapshot,
            "passed": self.passed,
            "checks": self.checks,
            "expected": {
                "cause": self.expected_cause,
                "required_evidence": self.required_evidence,
                "blocked_on": self.expected_blocker,
            },
            "report": self.report.to_dict(),
        }


def evaluate(path: Path) -> tuple[CaseResult, ...]:
    root = Fields(read_json(path), "suite", "format cases")
    if root.value["format"] != SUITE_FORMAT:
        raise InputError(f"suite/format must be {SUITE_FORMAT!r}")
    cases = root.array("cases")
    if not cases:
        raise InputError("suite/cases must not be empty")
    suite_dir = path.parent.resolve()
    used = set()
    results = []
    for index, value in enumerate(cases):
        f = Fields(value, f"suite/cases/{index}", "snapshot expected")
        name = f.text("snapshot", r"[a-z0-9-]{1,100}\.json")
        try:
            fixture_path = (suite_dir / name).resolve()
        except (OSError, RuntimeError) as exc:
            raise InputError(f"{f.where}/snapshot could not be resolved") from exc
        if fixture_path.parent != suite_dir:
            raise InputError(f"{f.where}/snapshot must stay in the suite directory")
        if fixture_path in used:
            raise InputError(f"{f.where}/snapshot is already in the suite")
        used.add(fixture_path)

        expected = Fields(
            f.value["expected"], f"{f.where}/expected", "cause required_evidence blocked_on"
        )
        try:
            cause = Cause(expected.text("cause"))
        except ValueError as exc:
            raise InputError(f"{expected.where}/cause is not a supported cause") from exc
        refs = expected.array("required_evidence")
        if any(not isinstance(ref, str) or ref not in _POINTERS for ref in refs):
            raise InputError(f"{expected.where}/required_evidence contains an unknown evidence pointer")
        if len(set(refs)) != len(refs):
            raise InputError(f"{expected.where}/required_evidence contains duplicate pointers")
        blocker = expected.value["blocked_on"]
        if cause == Cause.UNKNOWN:
            if not isinstance(blocker, str) or blocker not in _POINTERS:
                raise InputError(f"{expected.where}/blocked_on must name the blocked observation")
        elif blocker is not None:
            raise InputError(f"{expected.where}/blocked_on must be null for a diagnosis")

        snapshot = parse_snapshot(read_json(fixture_path))
        results.append(CaseResult(name, investigate(snapshot), cause, tuple(refs), blocker))
    return tuple(results)
