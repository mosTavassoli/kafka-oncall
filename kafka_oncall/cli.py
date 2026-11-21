"""Local replay and evaluation commands."""

import argparse
import json
import sys
from pathlib import Path

from kafka_oncall.evaluation import EVALUATION_FORMAT, evaluate
from kafka_oncall.runbook import Report, investigate
from kafka_oncall.snapshot import InputError, parse_snapshot, read_json


def _print_report(report: Report) -> None:
    print(f"{report.scenario} | {report.target} | {report.as_of} (runbook replay)")
    print(f"{report.cause}: {report.summary}")
    if report.blocked_on:
        print(f"Blocked on: {report.blocked_on}")
    if report.evidence:
        print(f"Evidence: {', '.join(report.evidence)}")
    print("Read trace:")
    for index, read in enumerate(report.trace, 1):
        timestamp = f" @ {read.observed_at}" if read.observed_at else ""
        print(f"  {index}. {read.status}: {read.source}{timestamp}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="kafka-oncall", description="Replay Kafka/KEDA scaling incidents offline."
    )
    commands = parser.add_subparsers(dest="command", required=True)
    replay = commands.add_parser("replay", help="Run the deterministic runbook on one snapshot")
    replay.add_argument("snapshot", type=Path)
    suite = commands.add_parser("evaluate", help="Score the runbook against labeled snapshots")
    suite.add_argument("suite", type=Path)
    for command in (replay, suite):
        command.add_argument("--json", action="store_true", help="Emit versioned JSON")
    args = parser.parse_args(argv)

    try:
        if args.command == "replay":
            report = investigate(parse_snapshot(read_json(args.snapshot)))
            if args.json:
                print(json.dumps(report.to_dict(), indent=2))
            else:
                _print_report(report)
            return 0

        results = evaluate(args.suite)
        passed = sum(result.passed for result in results)
        if args.json:
            print(json.dumps({
                "format": EVALUATION_FORMAT,
                "runner": "deterministic-runbook",
                "total": len(results),
                "passed": passed,
                "cases": [result.to_dict() for result in results],
            }, indent=2))
        else:
            for result in results:
                status = "PASS" if result.passed else "FAIL"
                failed = ", ".join(name for name, ok in result.checks.items() if not ok)
                suffix = f"; failed checks: {failed}" if failed else ""
                print(
                    f"{status} {result.report.scenario}: {result.report.cause} "
                    f"({len(result.report.trace)} reads){suffix}"
                )
            print(f"{passed}/{len(results)} scenarios passed (deterministic runbook).")
        return 0 if passed == len(results) else 1
    except (InputError, OSError) as exc:
        print(f"input error: {exc}", file=sys.stderr)
        return 2
