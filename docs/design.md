# Design: offline Kafka/KEDA investigations

## Scope

The investigation answers **why the workload did not get more ready replicas
in this snapshot**, not why lag originally rose. One point-in-time lag sample
cannot establish a trend. The first version recognizes a Kafka partition cap
and a CPU-only scheduling block. Everything else is `unknown`. There is no
automatic remediation, no shell access, and no connection to a cluster or a
model. The fixture data is hand-authored to match the documented interfaces.

## Snapshot contract

A `kafka-oncall/snapshot-v1` JSON document contains `format`, `id`, `target`
(`namespace/deployment`), `as_of` (UTC), and an `observations` object. The keys
below map to normalized observations, not raw Kubernetes or Kafka responses.
Any observation can be left out; a missing required read yields `unknown`.
If present, all the fields for that observation must be valid. See
[`partition-cap.json`](../scenarios/partition-cap.json) for a complete example.

| Observation | Where its fields come from |
| --- | --- |
| `scaled_object` | ScaledObject `scaleTargetRef`, `status.hpaName`, generated metric name and Kafka trigger configuration. |
| `hpa` | The generated HPA's Deployment target, external metric name and `AverageValue` target, min/max replicas, `status.desiredReplicas` and the `ScalingLimited` condition (`null` if unavailable). |
| `deployment` | Deployment `spec.replicas` (desired) and `status.readyReplicas` (ready). |
| `kafka` | Broker cluster identity, exact group and topic, topic partition count, and total raw lag for that group and topic. |
| `external_metric` | KEDA's external metrics API: select the metric by namespace, ScaledObject and metric name. Use the total metric, not the per-replica average in HPA status. |
| `pods` | Complete, non-terminating pod set for this deployment's current ReplicaSet, including each pod's `Ready` and `PodScheduled` conditions. `scheduled` is `null` if the condition is absent. `reason` and `message` come from `PodScheduled`, not a historical event. |

A snapshot is scoped to **one Kubernetes cluster**. Before assembling one,
verify the cluster context, follow owner references to scope pods to the
deployment, and verify that the ScaledObject targets it. The reader checks
the recorded names and cross-source relationships; it cannot authenticate
where an offline sample came from. Each observation must have `observed_at`
in UTC, using the source's sample time when available. A read is usable only
when `observed_at` is within the **30 seconds ending at `as_of`**, inclusive.
This is a freshness check, not an atomic cross-system snapshot guarantee.

Counts are integers. Convert Kubernetes quantities to whole message counts
before creating a fixture (`400000m` becomes `400`; do not round fractional
values). Identifiers use a limited set of printable name characters. Unknown
keys, duplicate JSON keys, and invalid types are input errors. Do not put
credentials, Kafka message bodies, logs, or production snapshots here.

## Decision rules

Start from the ScaledObject, its linked HPA, and the Deployment. Require their
targets to match and the HPA's desired count to match the Deployment's desired
count. Then:

- **Desired > ready.** Read the pod set. The counts must reconcile, and every
  non-ready pod must currently be `Pending`, `PodScheduled=False`, and
  `Unschedulable` for CPU only. This explains the gap between desired and ready
  replicas; it does not verify the Kafka lag or prove how the HPA chose its
  desired count. No need to read the broker to establish the scheduling block.
- **Desired = ready, above zero.** For `partition_cap`, require the classic
  `kafka` scaler in KEDA `v2.20.0`, exactly one Kafka trigger with plain,
  whole-topic lag and valid committed offsets, and `allowIdleConsumers=false`.
  Set `plain_lag_mode=true` only when this evidence is available and there is no
  partition subset, persistent-lag exclusion, lagged-partitions-only mode,
  even distribution, active fallback, or other metric transformation. The
  HPA must use that trigger's average target, be strictly inside its replica
  bounds, and not report `ScalingLimited`. For `P` partitions and target `T`
  messages per replica, require both `raw_lag // T > P` and **observed**
  `external_metric == P * T`, with `P` replicas desired and ready. The floor
  division matches the pinned [KEDA implementation][scaler]; this is not a
  simulator of [HPA tolerances and scaling policies][hpa].
- **Evidence missing, stale, mismatched, or inconclusive.** Report `unknown`
  and name the blocking observation. A Pending pod alone, a matching replica
  and partition count, or an old CPU event is not enough to infer a cause.

[scaler]: https://github.com/kedacore/keda/blob/v2.20.0/pkg/scalers/kafka_scaler.go#L825-L842
[hpa]: https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/#algorithm-details

## Reports and the baseline

`replay --json` emits `kafka-oncall/report-v1`: `cause`, `summary`,
`blocked_on` (a JSON Pointer or `null`), `evidence` pointers, and an ordered
`trace`. Each read is `ok`, `missing`, `stale`, `future`, or `wrong_scope`.
An `ok` read passed freshness and scope checks; its contents may still rule
out a diagnosis. The trace covers runbook reads, not the file parser or any
network calls.

[`suite.json`](../scenarios/suite.json) holds expected causes, required
evidence pointers, and expected blockers *separately* from the snapshots.
`evaluate` passes a case only if all three checks pass. It also reports how
many reads the runbook attempted. It does not judge prose or call an LLM.
Exit codes: `0` means replay completed or the suite passed, `1` means a suite
expectation failed, and `2` means the CLI or the input was invalid.

## Agent experiment

Keep the snapshot data and scoring rules fixed. Give a skill-backed agent the
question, target, and the same observations via typed, read-only tools. The
agent chooses the next read; code handles identity checks and arithmetic.
Do not pass scenario IDs, filenames, or expected answers to the model. Add
unseen cases with contradictory or missing data and compare against the
runbook before adding live adapters. The [experiment guide](experiment.md)
describes the HolmesGPT adapter, the two additional cases and how to record
a real model trace.
