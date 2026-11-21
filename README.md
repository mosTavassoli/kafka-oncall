# Kafka On-Call

**Kafka lag is high and only four pods are ready. Did autoscaling stop at
four, or did Kubernetes ask for more pods and fail to schedule them?**

Kafka On-Call is a small, read-only investigation lab for that question. It
lines up Kafka lag, KEDA's external metric, the Horizontal Pod Autoscaler's
(HPA's) desired replica count, and the workload's ready pods.

**[Watch the 90-second browser demo](https://mostavassoli.github.io/kafka-oncall/demo.html)** ·
[Evaluation results](docs/experiment.md#measured-result) ·
[Investigation skill](skills/kafka-scaling/SKILL.md)

## Try it

From the repo root, with Python 3.11 or newer:

```sh
python -m kafka_oncall replay scenarios/partition-cap.json
python -m kafka_oncall replay scenarios/cpu-scheduling.json
python -m kafka_oncall replay scenarios/missing-metric.json
python -m kafka_oncall evaluate scenarios/suite.json
```

On Windows, use `py` instead of `python` if you use the Python launcher. No
install, cluster, API key, or model is needed. The three snapshots are
synthetic, not captures from production.

| Snapshot | Broker lag | HPA desired / ready | What the runbook finds |
| --- | ---: | ---: | --- |
| `partition-cap` | 800 | 4 / 4 | KEDA exposed 400: four partitions times a target of 100 messages per replica. |
| `cpu-scheduling` | 800 | 8 / 4 | All four remaining pods are `Pending` with current `Insufficient cpu` scheduling failures. |
| `missing-metric` | 800 | 4 / 4 | `unknown`: the external metric is missing. Four partitions and four ready pods are not proof on their own. |

Each replay prints the evidence pointers and the reads the runbook made.
Add `--json` to either command for machine-readable results. `unknown` is a
valid replay result, not a CLI error.

## What's built, and what's next

Replay recomputes the **deterministic baseline**. The optional
[HolmesGPT experiment](docs/experiment.md) uses a [reusable investigation
skill](skills/kafka-scaling/SKILL.md) and six read-only snapshot tools. There
is no live cluster collector.

[Kedify](https://github.com/kedify/kubectl-kedify/blob/main/debug.sh) can show
the scaler metric, and [HolmesGPT](https://holmesgpt.dev/latest/reference/skills/)
already supports troubleshooting skills and evaluations. `python -m
experiment.compare` runs the five-case baseline. Add `--model` for a paired
run. In one live run, GPT-5.4 nano and the runbook both passed all five
synthetic cases. See the [results and agent replay](docs/experiment.md#measured-result).

See [the design](docs/design.md) for the snapshot contract, diagnosis rules,
and how to extend the evaluation.

## Tests

```sh
python -m unittest discover -s tests -v
```
