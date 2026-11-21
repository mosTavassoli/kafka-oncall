# Comparing the runbook with HolmesGPT

Both investigators get the same normalized observations and the same question.
The six parameterless tools each return one source; no tool takes a filename,
command, cluster name or arbitrary query. The model chooses reads. The reader
validates the input and marks old or future samples; the skill tells the model
how to check identity, counts and competing explanations. The baseline uses
the same 30-second window and does its own deterministic diagnosis.

## Offline: reproduce the baseline

```sh
python -m experiment.compare --output baseline.json
python -m experiment.demo baseline.json baseline.cast
```

The [90-second terminal recording](baseline.cast) is the **baseline**, assembled
from the three demo reports with pauses for reading; playback time is not
measured latency. Play locally with `asciinema play docs/baseline.cast`.

| Fixture | Baseline result | Reads |
| --- | --- | ---: |
| Partition cap | `partition_cap` | 5 |
| Four pods pending | `cpu_scheduling` | 4 |
| Missing external metric | `unknown`, blocked on external metric | 5 |
| Old external metric | `unknown`, blocked on external metric | 5 |
| Different Kafka group | `unknown`, blocked on Kafka | 4 |

The last two are matched variants of the cap fixture: only the external
metric's timestamp or the broker sample's consumer group changes. They check
whether the investigator notices bad evidence, not whether it memorized a
new diagnosis. Five synthetic cases cannot establish general accuracy.

## Live: run the same five cases with a model

Start Docker with Linux containers and configure `OPENAI_API_KEY` in the
local environment. From the source checkout:

```sh
python -m experiment.compare --case partition-cap --model openai/gpt-5.4-nano --output smoke.json
```

Check that `agent` reports a diagnosis and its checks pass before running the
full suite. Inside the pinned image the driver disables unrelated built-ins
and verifies the SDK's effective catalog contains exactly `fetch_skill` and
six snapshot reads before starting the model. The model must support
structured outputs as well as tool calling; the SDK requests a strict
finding schema. To run the full comparison:

```sh
python -m experiment.compare --model openai/gpt-5.4-nano --output comparison.json
python -m experiment.demo comparison.json agent.cast --agent
```

This makes **five separate model investigations**, each limited to 12 steps.
Use a model that supports tool calls. For another provider, set its key locally
and pass its model name and `--key-env ANTHROPIC_API_KEY` or `GEMINI_API_KEY`.
No social account or Kubernetes credentials are involved. The first run pulls
the HolmesGPT 0.46.0 CLI image.

The driver copies an anonymous fixture, the skill, and the readers into
each disposable container and copies out only the result. It uses no host
bind mounts, so Docker Desktop does not need access to the host temp folder
through file sharing. It does not send the repo or cluster configuration.
Tool commands are fixed; the evaluator rejects a run
that calls tools other than `fetch_skill` and the six reads. The host extracts
the final JSON and checks the calls against the returned evidence. A successful
score needs the expected cause, required citations, an attempted blocking read
for `unknown`, skill use, and verifiable tool returns. Read counts include
repeat reads but exclude skill loading. Explanation text is checked for presence;
a human should inspect it for unsupported claims.

Exit `0` means all scored checks passed; `1` means a completed comparison
failed checks; `2` means the live experiment did not complete. Compare results
in `comparison.json`, including the ordered traces. Model quality, latency,
and live integration have not been validated by the offline tests.

## Measured result

One live run with `openai/gpt-5.4-nano` passed all checks on all five
synthetic cases. Both the runbook and the agent found the partition cap
and CPU scheduling block, and both returned `unknown` with the expected
blocker for the missing metric, stale metric, and mismatched consumer
group. The runbook made **23 reads**; the agent made **24**, excluding the
skill fetch. The extra read was the KEDA metric on the wrong-group case.

The [90-second agent replay](agent-nano.cast) shows the first three cases
from that live run. Play it with `asciinema play docs/agent-nano.cast`.
The timing is edited for readability, not measured latency. This run
demonstrates tool use and evidence-based abstention; it does not show an
accuracy or efficiency improvement over the runbook.
