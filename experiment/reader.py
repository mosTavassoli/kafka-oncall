"""Fixed, parameterless Holmes tools call this module with a literal source."""

import json
from dataclasses import asdict
from pathlib import Path
import sys

from kafka_oncall.snapshot import Snapshot, Source, parse_snapshot, read_json, utc_string
from kafka_oncall.window import MAX_SAMPLE_AGE


def observation(snapshot: Snapshot, source: Source) -> dict:
    sample = getattr(snapshot, source.value)
    result = {"source": source.pointer, "status": "missing"}
    if sample is None:
        return result
    age = snapshot.as_of - sample.observed_at
    result["observed_at"] = utc_string(sample.observed_at)
    if age.total_seconds() < 0 or age > MAX_SAMPLE_AGE:
        result["status"] = "future" if age.total_seconds() < 0 else "stale"
        return result
    result.update(status="ok", data=asdict(sample))
    del result["data"]["observed_at"]
    return result


if __name__ == "__main__":
    snapshot = parse_snapshot(read_json(Path("/lab/snapshot.json")))
    print(json.dumps(observation(snapshot, Source(sys.argv[1]))))
