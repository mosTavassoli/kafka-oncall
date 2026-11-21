"""Run one isolated HolmesGPT investigation per fixture; summarize calls and findings."""

import argparse
import json
from pathlib import Path
import re
import shutil
import subprocess
from tempfile import TemporaryDirectory

from kafka_oncall.runbook import investigate
from kafka_oncall.snapshot import Source, parse_snapshot, read_json
from experiment.reader import observation

ROOT = Path(__file__).resolve().parents[1]
IMAGE = ("us-central1-docker.pkg.dev/genuine-flight-317411/devel/holmes:0.46.0"
         "@sha256:bec9d14eea6a8890ba498c91ab47adcce3af9b6d0c6ee26d642d4e3e0df6218e")
QUESTION = "Kafka lag is high. Why are only four pods ready?"


def cases():
    scenarios = ROOT / "scenarios"
    for case in read_json(scenarios / "suite.json")["cases"]:
        yield read_json(scenarios / case["snapshot"]), case["expected"]
    for name, source, field, value in [
        ("old-sample", Source.EXTERNAL_METRIC, "observed_at", "2026-11-21T09:55:00Z"),
        ("other-group", Source.KAFKA, "consumer_group", "orders-audit"),
    ]:
        fixture = read_json(scenarios / "partition-cap.json")
        fixture["id"] = name
        fixture["observations"][source][field] = value
        required = [Source.SCALED_OBJECT, Source.HPA, Source.DEPLOYMENT]
        if source == Source.EXTERNAL_METRIC:
            required.append(Source.KAFKA)
        yield fixture, {"cause": "unknown", "required_evidence": [s.pointer for s in required],
                        "blocked_on": source.pointer}


def prepare(lab, fixture):
    lab.mkdir()
    # Only data and read tools are mounted. No fixture names, suite or runbook source.
    (lab / "snapshot.json").write_text(json.dumps({**fixture, "id": "snapshot"}), encoding="utf8")
    for package, files in {"kafka_oncall": ["__init__.py", "snapshot.py", "window.py"],
                           "experiment": ["__init__.py", "reader.py"]}.items():
        (lab / package).mkdir()
        for file in files:
            shutil.copyfile(ROOT / package / file, lab / package / file)
    shutil.copytree(ROOT / "skills", lab / "skills")
    toolsets = {"snapshot": {"enabled": True, "description": "Read normalized incident evidence",
                             "tools": [
            {"name": f"read_{s.value}", "description": f"Read the current {s.value} observation",
             "command": f"cd /lab && python -m experiment.reader {s.value}"} for s in Source
        ]}}
    (lab / "tools.yaml").write_text(json.dumps({"toolsets": toolsets}), encoding="utf8")
    config = {
        "custom_skill_paths": ["/lab/skills"], "custom_toolsets": ["/lab/tools.yaml"],
        "toolsets": {"bash": {"enabled": False}, "kubernetes/core": {"enabled": False},
                     "kubernetes/logs": {"enabled": False}, "snapshot": {"enabled": True}},
    }
    (lab / "config.yaml").write_text(json.dumps(config), encoding="utf8")


def score(finding, reads, expected):
    citations = finding.get("evidence")
    used = {r["source"] for r in reads if r["status"] == "ok"}
    blocker = expected["blocked_on"]
    return {
        "cause": finding.get("cause") == expected["cause"],
        "citations": isinstance(citations, (list, tuple)) and all(isinstance(p, str) for p in citations)
        and set(expected["required_evidence"]) <= set(citations) <= used
        and not (blocker and blocker in citations),
        "blocker": "blocked_on" in finding and finding["blocked_on"] == blocker
        and (not blocker or any(r["source"] == blocker for r in reads)),
        "explanation_present": isinstance(finding.get("summary"), str) and bool(finding["summary"].strip()),
    }


def unpack(response, snapshot):
    calls = []
    used_skill = False
    unmatched = []
    decoded = {}
    for message in response["messages"]:
        if message.get("role") == "tool":
            decoded[message.get("tool_call_id")] = message.get("content", "")
    for message in response["messages"]:
        for call in message.get("tool_calls") or []:
            name = call["function"]["name"]
            content = decoded.get(call["id"], "")
            if not isinstance(content, str):
                content = json.dumps(content)
            if name == "fetch_skill":
                used_skill |= "cpu_scheduling" in content and "partition_cap" in content
            elif name in {f"read_{s.value}" for s in Source}:
                source = Source(name.removeprefix("read_"))
                expected = observation(snapshot, source)
                # Verify a returned payload, not merely a requested tool call.
                ok = json.dumps(expected) in content
                if not ok:
                    try:
                        ok = json.loads(content) == expected
                    except ValueError:
                        pass
                calls.append({**expected, "status": expected["status"] if ok else "unverified"})
            else:
                unmatched.append(name)
    result = re.sub(r"^```(?:json)?\s*|\s*```$", "", response["result"].strip())
    try:
        answer = json.loads(result)
    except ValueError:
        answer = {}
    return answer if isinstance(answer, dict) else {}, calls, used_skill, unmatched


def live(fixture, model, key_env, expected):
    from os import environ

    if not environ.get(key_env):
        raise ValueError(f"Configure {key_env} locally before running HolmesGPT.")
    with TemporaryDirectory() as tmp:
        lab, out = Path(tmp) / "lab", Path(tmp) / "out"
        prepare(lab, fixture)
        out.mkdir()
        prompt = (f"{QUESTION} Deployment {fixture['target']}, as of {fixture['as_of']}. "
                  "Fetch the kafka-scaling skill. Use only that skill and read_* snapshot tools. "
                  "Return the skill's JSON finding.")
        cmd = ["docker", "run", "--rm", "--cap-drop=ALL", "--security-opt=no-new-privileges",
               "-v", f"{lab}:/lab:ro", "-v", f"{out}:/out", "-e", key_env,
               IMAGE, "ask", prompt, "--config", "/lab/config.yaml", "--model", model,
               "--no-interactive", "--bash-always-deny", "--max-steps", "12",
               "--json-output-file", "/out/result.json"]
        subprocess.run(cmd, stdin=subprocess.DEVNULL, check=True, timeout=300)
        response = json.loads((out / "result.json").read_text(encoding="utf8"))
        answer, reads, skill, other = unpack(response, parse_snapshot(fixture))
        checks = score(answer, reads, expected)
        checks.update(skill_loaded=skill, only_snapshot_tools=not other,
                      verified_returns=all(r["status"] != "unverified" for r in reads))
        return {"finding": answer, "trace": reads, "checks": checks, "other_tools": other}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model", help="Run HolmesGPT in Docker with this provider/model")
    parser.add_argument("--key-env", choices=["OPENAI_API_KEY", "ANTHROPIC_API_KEY", "GEMINI_API_KEY"],
                        default="OPENAI_API_KEY")
    parser.add_argument("--output", type=Path, help="Save normalized reports; no raw provider messages")
    args = parser.parse_args()
    rows = []
    passed = True
    for fixture, expected in cases():
        baseline = investigate(parse_snapshot(fixture)).to_dict()
        row = {"case": fixture["id"], "baseline": baseline, "agent": None}
        checks = score(baseline, baseline["trace"], expected)
        row["baseline_checks"] = checks
        if args.model:
            try:
                row["agent"] = live(fixture, args.model, args.key_env, expected)
            except (ValueError, OSError, KeyError, subprocess.SubprocessError) as exc:
                parser.exit(2, f"HolmesGPT run incomplete: {exc}\n")
        verdict = "not run" if not row["agent"] else (
            f"{row['agent']['finding'].get('cause', 'invalid')} ({len(row['agent']['trace'])} reads)"
        )
        passed &= all(checks.values()) and (not args.model or all(row["agent"]["checks"].values()))
        print(f"{fixture['id']}: runbook={baseline['cause']} ({len(baseline['trace'])} reads); agent={verdict}")
        rows.append(row)
    if args.output:
        args.output.write_text(json.dumps({"image": IMAGE, "model": args.model, "cases": rows}, indent=2),
                               encoding="utf8")
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
