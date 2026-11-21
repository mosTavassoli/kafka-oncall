"""Parse local, normalized observations. No cluster or model access."""

import json
import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from pathlib import Path
from typing import Any, TypeVar

SNAPSHOT_FORMAT = "kafka-oncall/snapshot-v1"
MAX_INPUT_BYTES = 256 * 1024
_LABEL = r"[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?"
_TARGET = rf"{_LABEL}/{_LABEL}"
_IDENTIFIER = r"[A-Za-z0-9_.-]{1,253}"
_TIMESTAMP = r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,6})?Z"
_T = TypeVar("_T")


class InputError(ValueError):
    """The input is not valid for this version of the contract."""


class Source(StrEnum):
    SCALED_OBJECT = "scaled_object"
    HPA = "hpa"
    DEPLOYMENT = "deployment"
    KAFKA = "kafka"
    EXTERNAL_METRIC = "external_metric"
    PODS = "pods"

    @property
    def pointer(self) -> str:
        return f"/observations/{self.value}"


@dataclass(frozen=True, slots=True)
class Observation:
    observed_at: datetime


@dataclass(frozen=True, slots=True)
class ScaledObject(Observation):
    target: str
    name: str
    hpa: str
    metric: str
    kafka_cluster: str
    topic: str
    consumer_group: str
    lag_threshold: int
    scaler_profile: str
    plain_lag_mode: bool
    allow_idle_consumers: bool


@dataclass(frozen=True, slots=True)
class Hpa(Observation):
    target: str
    name: str
    metric: str
    target_average_value: int
    min_replicas: int
    max_replicas: int
    desired_replicas: int
    scaling_limited: bool | None


@dataclass(frozen=True, slots=True)
class Deployment(Observation):
    target: str
    desired_replicas: int
    ready_replicas: int


@dataclass(frozen=True, slots=True)
class Kafka(Observation):
    kafka_cluster: str
    topic: str
    consumer_group: str
    partitions: int
    lag: int


@dataclass(frozen=True, slots=True)
class ExternalMetric(Observation):
    namespace: str
    scaled_object: str
    metric: str
    value: int


@dataclass(frozen=True, slots=True)
class Pod:
    name: str
    phase: str
    ready: bool
    scheduled: bool | None
    reason: str | None
    message: str | None


@dataclass(frozen=True, slots=True)
class Pods(Observation):
    target: str
    items: tuple[Pod, ...]


@dataclass(frozen=True, slots=True)
class Snapshot:
    id: str
    target: str
    as_of: datetime
    scaled_object: ScaledObject | None
    hpa: Hpa | None
    deployment: Deployment | None
    kafka: Kafka | None
    external_metric: ExternalMetric | None
    pods: Pods | None


def _unique_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise InputError(f"duplicate JSON key {key!r}")
        result[key] = value
    return result


def read_json(path: Path) -> Any:
    try:
        with path.open("rb") as stream:
            raw = stream.read(MAX_INPUT_BYTES + 1)
        if len(raw) > MAX_INPUT_BYTES:
            raise InputError(f"file exceeds {MAX_INPUT_BYTES} bytes")
        return json.loads(raw.decode("utf-8"), object_pairs_hook=_unique_keys)
    except (OSError, ValueError, RecursionError) as exc:
        raise InputError(f"{str(path)!r}: {exc}") from exc


class Fields:
    """Validate the small JSON contract with errors that name the input field."""

    def __init__(
        self, value: Any, where: str, required: str, optional: str = ""
    ) -> None:
        if not isinstance(value, dict):
            raise InputError(f"{where} must be an object")
        keys = set(value)
        required_keys = set(required.split())
        missing = required_keys - keys
        extra = keys - required_keys - set(optional.split())
        if missing or extra:
            raise InputError(f"{where}: missing {sorted(missing)!r}, unknown {sorted(extra)!r}")
        self.value = value
        self.where = where

    def text(self, key: str, pattern: str = _IDENTIFIER) -> str:
        value = self.value[key]
        if not isinstance(value, str) or not re.fullmatch(pattern, value):
            raise InputError(f"{self.where}/{key}: invalid string")
        return value

    def number(self, key: str, minimum: int = 0) -> int:
        value = self.value[key]
        if type(value) is not int or not minimum <= value <= 2**63 - 1:
            raise InputError(f"{self.where}/{key}: expected an integer >= {minimum} (int64)")
        return value

    def boolean(self, key: str) -> bool:
        value = self.value[key]
        if type(value) is not bool:
            raise InputError(f"{self.where}/{key}: expected a boolean")
        return value

    def optional_boolean(self, key: str) -> bool | None:
        return None if self.value[key] is None else self.boolean(key)

    def timestamp(self, key: str) -> datetime:
        value = self.text(key, _TIMESTAMP)
        try:
            return datetime.fromisoformat(value)
        except ValueError as exc:
            raise InputError(f"{self.where}/{key}: invalid UTC timestamp") from exc

    def array(self, key: str) -> list[Any]:
        value = self.value[key]
        if not isinstance(value, list):
            raise InputError(f"{self.where}/{key}: expected an array")
        return value


def utc_string(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def _scaled_object(value: Any) -> ScaledObject:
    f = Fields(
        value, Source.SCALED_OBJECT.pointer,
        "observed_at target name hpa metric kafka_cluster topic consumer_group "
        "lag_threshold scaler_profile plain_lag_mode allow_idle_consumers",
    )
    return ScaledObject(
        observed_at=f.timestamp("observed_at"),
        target=f.text("target", _TARGET),
        name=f.text("name", _LABEL),
        hpa=f.text("hpa", _LABEL),
        metric=f.text("metric", _LABEL),
        kafka_cluster=f.text("kafka_cluster"),
        topic=f.text("topic"),
        consumer_group=f.text("consumer_group"),
        lag_threshold=f.number("lag_threshold", 1),
        scaler_profile=f.text("scaler_profile"),
        plain_lag_mode=f.boolean("plain_lag_mode"),
        allow_idle_consumers=f.boolean("allow_idle_consumers"),
    )


def _hpa(value: Any) -> Hpa:
    f = Fields(
        value, Source.HPA.pointer,
        "observed_at target name metric target_average_value min_replicas max_replicas "
        "desired_replicas scaling_limited",
    )
    return Hpa(
        observed_at=f.timestamp("observed_at"),
        target=f.text("target", _TARGET),
        name=f.text("name", _LABEL),
        metric=f.text("metric", _LABEL),
        target_average_value=f.number("target_average_value", 1),
        min_replicas=f.number("min_replicas"),
        max_replicas=f.number("max_replicas", 1),
        desired_replicas=f.number("desired_replicas"),
        scaling_limited=f.optional_boolean("scaling_limited"),
    )


def _deployment(value: Any) -> Deployment:
    f = Fields(value, Source.DEPLOYMENT.pointer, "observed_at target desired_replicas ready_replicas")
    return Deployment(
        f.timestamp("observed_at"), f.text("target", _TARGET),
        f.number("desired_replicas"), f.number("ready_replicas"),
    )


def _kafka(value: Any) -> Kafka:
    f = Fields(
        value, Source.KAFKA.pointer, "observed_at kafka_cluster topic consumer_group partitions lag"
    )
    return Kafka(
        f.timestamp("observed_at"), f.text("kafka_cluster"), f.text("topic"),
        f.text("consumer_group"), f.number("partitions", 1), f.number("lag"),
    )


def _external_metric(value: Any) -> ExternalMetric:
    f = Fields(
        value, Source.EXTERNAL_METRIC.pointer, "observed_at namespace scaled_object metric value"
    )
    return ExternalMetric(
        f.timestamp("observed_at"), f.text("namespace", _LABEL), f.text("scaled_object", _LABEL),
        f.text("metric", _LABEL), f.number("value"),
    )


def _pods(value: Any) -> Pods:
    f = Fields(value, Source.PODS.pointer, "observed_at target items")
    items: list[Pod] = []
    for index, item in enumerate(f.array("items")):
        p = Fields(item, f"{f.where}/items/{index}", "name phase ready scheduled reason message")
        items.append(Pod(
            name=p.text("name", _LABEL),
            phase=p.text("phase", r"Pending|Running|Succeeded|Failed|Unknown"),
            ready=p.boolean("ready"),
            scheduled=p.optional_boolean("scheduled"),
            reason=None if p.value["reason"] is None else p.text("reason"),
            message=None if p.value["message"] is None else p.text("message", r"[ -~]{1,4096}"),
        ))
    if len({item.name for item in items}) != len(items):
        raise InputError(f"{f.where}/items: duplicate pod names")
    return Pods(f.timestamp("observed_at"), f.text("target", _TARGET), tuple(items))


def parse_snapshot(value: Any) -> Snapshot:
    root = Fields(value, "snapshot", "format id target as_of observations")
    if root.value["format"] != SNAPSHOT_FORMAT:
        raise InputError(f"snapshot/format must be {SNAPSHOT_FORMAT!r}")
    observations = Fields(root.value["observations"], "/observations", "", " ".join(Source)).value

    def take(source: Source, parse: Callable[[Any], _T]) -> _T | None:
        return parse(observations[source]) if source in observations else None

    return Snapshot(
        id=root.text("id", _LABEL),
        target=root.text("target", _TARGET),
        as_of=root.timestamp("as_of"),
        scaled_object=take(Source.SCALED_OBJECT, _scaled_object),
        hpa=take(Source.HPA, _hpa),
        deployment=take(Source.DEPLOYMENT, _deployment),
        kafka=take(Source.KAFKA, _kafka),
        external_metric=take(Source.EXTERNAL_METRIC, _external_metric),
        pods=take(Source.PODS, _pods),
    )
