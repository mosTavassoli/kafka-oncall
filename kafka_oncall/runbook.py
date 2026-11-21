"""Deterministic baseline for two causes of a Kafka/KEDA scaling symptom."""

import re
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from enum import StrEnum
from typing import Any, Literal, TypeVar

from kafka_oncall.window import MAX_SAMPLE_AGE
from kafka_oncall.snapshot import (
    Deployment, Hpa, Observation, Pod, ScaledObject, Snapshot, Source, utc_string,
)

REPORT_FORMAT = "kafka-oncall/report-v1"
CAP_PROFILE = "keda-2.20.0-kafka"
_T = TypeVar("_T", bound=Observation)
_CPU_ONLY = re.compile(r"0/([1-9][0-9]*) nodes are available: \1 Insufficient cpu\.?")
_SOURCE_LABEL = {
    Source.SCALED_OBJECT: "ScaledObject",
    Source.HPA: "HPA",
    Source.DEPLOYMENT: "deployment",
    Source.KAFKA: "Kafka lag",
    Source.EXTERNAL_METRIC: "KEDA external metric",
    Source.PODS: "pod",
}
ReadStatus = Literal["ok", "missing", "stale", "future", "wrong_scope"]


class Cause(StrEnum):
    PARTITION_CAP = "partition_cap"
    CPU_SCHEDULING = "cpu_scheduling"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class Read:
    source: str
    status: ReadStatus
    observed_at: str | None = None


@dataclass(frozen=True, slots=True)
class Report:
    scenario: str
    target: str
    as_of: str
    cause: Cause
    summary: str
    blocked_on: str | None
    evidence: tuple[str, ...]
    trace: tuple[Read, ...]

    def to_dict(self) -> dict[str, Any]:
        return {"format": REPORT_FORMAT, **asdict(self)}


class Unknown(Exception):
    def __init__(self, source: Source, detail: str) -> None:
        super().__init__(detail)
        self.source = source


class Reader:
    """Use only snapshot reads, and record why a sample could not be used."""

    def __init__(self, as_of: datetime) -> None:
        self.as_of = as_of
        self.trace: list[Read] = []

    def read(
        self, source: Source, sample: _T | None, matches: Callable[[_T], bool], wrong_scope: str
    ) -> _T:
        label = _SOURCE_LABEL[source]
        if sample is None:
            self.trace.append(Read(source.pointer, "missing"))
            raise Unknown(source, f"No {label} observation is available.")
        observed_at = utc_string(sample.observed_at)
        age = self.as_of - sample.observed_at
        if age < timedelta(0) or age > MAX_SAMPLE_AGE:
            status: ReadStatus = "future" if age < timedelta(0) else "stale"
            self.trace.append(Read(source.pointer, status, observed_at))
            raise Unknown(source, f"The {label} observation is outside the snapshot window.")
        if not matches(sample):
            self.trace.append(Read(source.pointer, "wrong_scope", observed_at))
            raise Unknown(source, wrong_scope)
        self.trace.append(Read(source.pointer, "ok", observed_at))
        return sample


def _is_cpu_blocked(pod: Pod) -> bool:
    if pod.ready or pod.phase != "Pending" or pod.scheduled is not False:
        return False
    if pod.reason != "Unschedulable" or pod.message is None:
        return False
    # Keep only the initial scheduling decision, not its preemption commentary.
    primary, _, _ = pod.message.partition(". preemption:")
    return _CPU_ONLY.fullmatch(primary) is not None


def _cpu_scheduling(snapshot: Snapshot, reader: Reader, workload: Deployment) -> str:
    pods = reader.read(
        Source.PODS, snapshot.pods, lambda sample: sample.target == snapshot.target,
        "The pod list belongs to a different deployment.",
    )
    ready = [pod for pod in pods.items if pod.ready]
    if (
        len(pods.items) != workload.desired_replicas
        or len(ready) != workload.ready_replicas
        or any(pod.phase != "Running" or pod.scheduled is not True for pod in ready)
    ):
        raise Unknown(Source.PODS, "Pod observations do not match the deployment's replica counts.")
    pending = [pod for pod in pods.items if not pod.ready]
    if not all(_is_cpu_blocked(pod) for pod in pending):
        raise Unknown(
            Source.PODS,
            "Not every non-ready pod is Pending with a current CPU-only scheduling failure.",
        )
    names = ", ".join(pod.name for pod in pending)
    return (
        f"The HPA requested {workload.desired_replicas} replicas; "
        f"{workload.ready_replicas} are ready. The scheduler reports Insufficient cpu "
        f"for all {len(pending)} remaining pods: {names}."
    )


def _partition_cap(snapshot: Snapshot, reader: Reader, scaled: ScaledObject, hpa: Hpa) -> str:
    if scaled.scaler_profile != CAP_PROFILE or not scaled.plain_lag_mode:
        raise Unknown(Source.SCALED_OBJECT, "The trigger is outside the supported Kafka cap profile.")
    if scaled.allow_idle_consumers:
        raise Unknown(Source.SCALED_OBJECT, "This trigger allows idle consumers; no default cap applies.")
    if hpa.target_average_value != scaled.lag_threshold:
        raise Unknown(Source.HPA, "The HPA average target differs from the Kafka lag threshold.")

    kafka = reader.read(
        Source.KAFKA, snapshot.kafka,
        lambda sample: (sample.kafka_cluster, sample.topic, sample.consumer_group)
        == (scaled.kafka_cluster, scaled.topic, scaled.consumer_group),
        "Kafka lag was measured for a different cluster, topic, or consumer group.",
    )
    if (
        hpa.scaling_limited is not False
        or hpa.desired_replicas != kafka.partitions
        or not hpa.min_replicas < kafka.partitions < hpa.max_replicas
    ):
        raise Unknown(Source.HPA, "HPA status and limits do not isolate the partition cap.")

    threshold = scaled.lag_threshold
    # The v2.20.0 scaler divides integers before checking its upper bound.
    if kafka.lag // threshold <= kafka.partitions:
        raise Unknown(Source.KAFKA, "Raw lag does not cross this scaler's partition-cap threshold.")

    namespace, _ = snapshot.target.split("/", 1)
    metric = reader.read(
        Source.EXTERNAL_METRIC, snapshot.external_metric,
        lambda sample: (sample.namespace, sample.scaled_object, sample.metric)
        == (namespace, scaled.name, scaled.metric),
        "The external metric does not belong to this trigger.",
    )
    if metric.value != kafka.partitions * threshold:
        raise Unknown(Source.EXTERNAL_METRIC, "The observed external metric does not match the cap.")

    return (
        f"Kafka shows {kafka.lag} messages of lag for {scaled.consumer_group}/{scaled.topic}. "
        f"KEDA exposed {metric.value}, matching {kafka.partitions} partitions x "
        f"{threshold} messages per replica with idle consumers disabled. "
        f"The HPA requested {hpa.desired_replicas} replicas and all are ready."
    )


def _decide(snapshot: Snapshot, reader: Reader) -> tuple[Cause, str]:
    scaled = reader.read(
        Source.SCALED_OBJECT, snapshot.scaled_object, lambda sample: sample.target == snapshot.target,
        "The ScaledObject targets a different deployment.",
    )
    hpa = reader.read(
        Source.HPA, snapshot.hpa,
        lambda sample: (sample.target, sample.name, sample.metric)
        == (snapshot.target, scaled.hpa, scaled.metric),
        "The HPA target, name, or metric does not match the ScaledObject.",
    )
    workload = reader.read(
        Source.DEPLOYMENT, snapshot.deployment, lambda sample: sample.target == snapshot.target,
        "The replica counts belong to a different deployment.",
    )
    if hpa.desired_replicas != workload.desired_replicas:
        raise Unknown(Source.DEPLOYMENT, "The deployment has not observed the HPA's desired count.")
    if not hpa.min_replicas <= hpa.desired_replicas <= hpa.max_replicas:
        raise Unknown(Source.HPA, "The HPA's desired count is outside its captured replica bounds.")
    if workload.ready_replicas > workload.desired_replicas or hpa.desired_replicas == 0:
        raise Unknown(Source.DEPLOYMENT, "The replica state is outside this runbook's scope.")
    if workload.ready_replicas < workload.desired_replicas:
        return Cause.CPU_SCHEDULING, _cpu_scheduling(snapshot, reader, workload)
    return Cause.PARTITION_CAP, _partition_cap(snapshot, reader, scaled, hpa)


def investigate(snapshot: Snapshot) -> Report:
    reader = Reader(snapshot.as_of)
    blocked_on = None
    try:
        cause, summary = _decide(snapshot, reader)
    except Unknown as exc:
        cause, summary = Cause.UNKNOWN, str(exc)
        blocked_on = exc.source.pointer
    trace = tuple(reader.trace)
    return Report(
        scenario=snapshot.id,
        target=snapshot.target,
        as_of=utc_string(snapshot.as_of),
        cause=cause,
        summary=summary,
        blocked_on=blocked_on,
        evidence=tuple(read.source for read in trace if read.status == "ok"),
        trace=trace,
    )
