"""Create a portable, timed terminal recording from measured comparison reports."""

import argparse
import json
from pathlib import Path


def display(text):
    return "".join(char if char.isprintable() else " " for char in str(text))


def record(data, agent=False):
    label = f"HolmesGPT: {display(data['model'])}" if agent else "Deterministic baseline (not a model run)"
    lines = [json.dumps({"version": 2, "width": 100, "height": 30, "title": label})]

    def emit(t, text):
        lines.append(json.dumps([t, "o", text.replace("\n", "\r\n")]))

    for i, case in enumerate(data["cases"][:3]):
        start = i * 30
        run = case["agent" if agent else "baseline"]
        if run is None:
            raise ValueError("No agent trace in this report. Record a live comparison first.")
        finding = run["finding"] if agent else run
        emit(start, f"\x1b[2J\x1b[H{label}\n\nKafka lag is high. Why are only four pods ready?\n\n")
        for j, read in enumerate(run["trace"]):
            emit(start + 2 + j * 15 / max(1, len(run["trace"])),
                 f"  {j+1}. {display(read['source']):<34} {display(read['status'])}\n")
        emit(start + 19, f"\n{display(finding.get('cause', 'invalid finding'))}: {display(finding.get('summary', ''))}\n")
        if finding.get("blocked_on"):
            emit(start + 24, f"Missing proof: {display(finding['blocked_on'])}\n")
    emit(90, "\nEnd of replay.\n")
    return "\n".join(lines) + "\n"


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("report", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--agent", action="store_true")
    args = parser.parse_args()
    args.output.write_text(record(json.loads(args.report.read_text(encoding="utf8")), args.agent),
                           encoding="utf8")
