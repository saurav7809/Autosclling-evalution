"""Kubernetes access and HTTP measurements for a staging benchmark."""

from __future__ import annotations

import asyncio
import math
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal

import httpx
from kubernetes import client as kube_client, config as kube_config
from kubernetes.config.config_exception import ConfigException
from kubernetes.utils.quantity import parse_quantity
from urllib3.exceptions import HTTPError as KubernetesTransportError

MODE = "STAGING_BENCHMARK"
SOURCE = "KUBERNETES_LIVE"
OPT_IN_LABEL = "autoscaling-evaluation/staging"
MAX_REPLICAS = 8
MAX_RPS = 20
MIN_CPU_COVERAGE = 0.8
KUBE_TIMEOUT = (2, 3)
HTTP_TIMEOUT = 2


class StagingError(RuntimeError):
    """A benchmark error that can be shown in the API."""


class TargetChanged(StagingError):
    """The original Deployment is gone and cannot be restored."""


@dataclass(frozen=True)
class StagingTarget:
    namespace: str
    deployment: str
    service: str
    port: int
    path: str

    @classmethod
    def configured(cls) -> StagingTarget | None:
        names = [os.getenv(f"STAGING_{key}", "") for key in ("NAMESPACE", "DEPLOYMENT", "SERVICE")]
        if not any(names):
            return None
        dns_label = r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?"
        if any(not re.fullmatch(dns_label, name) for name in names):
            raise StagingError("Set STAGING_NAMESPACE, STAGING_DEPLOYMENT, and STAGING_SERVICE to DNS labels.")
        port = os.getenv("STAGING_SERVICE_PORT", "80")
        if not re.fullmatch(r"[0-9]{1,5}", port) or not 1 <= int(port) <= 65535:
            raise StagingError("STAGING_SERVICE_PORT must be a valid TCP port.")
        path = os.getenv("STAGING_REQUEST_PATH", "/")
        if len(path) > 200 or not re.fullmatch(r"/(?:[A-Za-z0-9._~/-]*)", path) or path.startswith("//"):
            raise StagingError("STAGING_REQUEST_PATH must be an absolute HTTP path.")
        return cls(*names, int(port), path)

    @property
    def key(self) -> str:
        return f"{self.namespace}/{self.deployment}"

    @property
    def url(self) -> str:
        return f"http://{self.service}.{self.namespace}.svc:{self.port}{self.path}"


@dataclass(frozen=True)
class DeploymentSnapshot:
    uid: str
    requested: int
    ready: int
    active: int
    settled: bool
    cpu_percent: float | None = None


class KubernetesStaging:
    def __init__(self, target: StagingTarget):
        configuration = kube_client.Configuration()
        configuration.retries = 0
        try:
            kube_config.load_incluster_config(client_configuration=configuration)
        except ConfigException:
            raise StagingError("Live benchmarks require an in-cluster staging service account.") from None
        self.target = target
        self.api = kube_client.ApiClient(configuration)
        self.apps = kube_client.AppsV1Api(self.api)
        self.core = kube_client.CoreV1Api(self.api)
        self.metrics = kube_client.CustomObjectsApi(self.api)
        self.autoscaling = kube_client.AutoscalingV2Api(self.api)
        self.selector = ""

    def close(self):
        self.api.close()

    def _deployment(self):
        target = self.target
        try:
            deployment = self.apps.read_namespaced_deployment(
                target.deployment, target.namespace, _request_timeout=KUBE_TIMEOUT,
            )
        except kube_client.ApiException as exc:
            if exc.status == 404:
                raise TargetChanged("The staging Deployment no longer exists.") from None
            raise
        if (deployment.metadata.labels or {}).get(OPT_IN_LABEL) != "true":
            raise StagingError(f"The staging Deployment must have the {OPT_IN_LABEL}=true label.")
        return deployment

    def _check_autoscaler(self):
        target = self.target
        hpas = self.autoscaling.list_namespaced_horizontal_pod_autoscaler(
            target.namespace, _request_timeout=KUBE_TIMEOUT,
        )
        for hpa in hpas.items:
            ref = hpa.spec.scale_target_ref
            if ref.kind == "Deployment" and ref.name == target.deployment:
                raise StagingError("The benchmark Deployment has an active HorizontalPodAutoscaler.")

    def preflight(self, image_ref: str) -> DeploymentSnapshot:
        deployment = self._deployment()
        target = self.target
        service = self.core.read_namespaced_service(target.service, target.namespace, _request_timeout=KUBE_TIMEOUT)
        labels = deployment.spec.selector.match_labels
        if not labels or deployment.spec.selector.match_expressions:
            raise StagingError("The staging Deployment must use a nonempty matchLabels selector.")
        if (
            service.spec.type != "ClusterIP"
            or service.spec.cluster_ip in (None, "", "None")
            or (service.metadata.labels or {}).get(OPT_IN_LABEL) != "true"
            or service.spec.selector != labels
            or not any(port.port == target.port and port.protocol in (None, "TCP") for port in service.spec.ports)
        ):
            raise StagingError("The opted-in staging ClusterIP Service must select the Deployment and expose the configured port.")
        if image_ref not in {container.image for container in deployment.spec.template.spec.containers}:
            raise StagingError("The registered image must match an image on the staging Deployment.")
        if deployment.spec.replicas > MAX_REPLICAS:
            raise StagingError(f"The staging Deployment must start with at most {MAX_REPLICAS} replicas.")
        self._check_autoscaler()
        self.selector = ",".join(f"{key}={value}" for key, value in sorted(labels.items()))
        return self._snapshot(deployment, None)

    @staticmethod
    def _snapshot(deployment, cpu_percent: float | None) -> DeploymentSnapshot:
        status = deployment.status
        requested = deployment.spec.replicas
        ready = status.ready_replicas or 0
        active = status.replicas or 0
        settled = (
            (status.observed_generation or 0) >= deployment.metadata.generation
            and (status.updated_replicas or 0) == requested
            and ready == active == requested
        )
        return DeploymentSnapshot(deployment.metadata.uid, requested, ready, active, settled, cpu_percent)

    def snapshot(self, uid: str, include_cpu: bool = False) -> DeploymentSnapshot:
        deployment = self._deployment()
        if deployment.metadata.uid != uid:
            raise TargetChanged("The staging Deployment was replaced.")
        cpu = self._cpu_percent(deployment.status.ready_replicas or 0) if include_cpu else None
        return self._snapshot(deployment, cpu)

    def _cpu_percent(self, ready_count: int) -> float | None:
        if not ready_count:
            return None
        try:
            pods = self.core.list_namespaced_pod(
                self.target.namespace, label_selector=self.selector, _request_timeout=KUBE_TIMEOUT,
            ).items
            ready = [
                pod for pod in pods if pod.metadata.deletion_timestamp is None
                and any(condition.type == "Ready" and condition.status == "True" for condition in pod.status.conditions or [])
            ]
            if len(ready) != ready_count:
                return None
            metrics = self.metrics.list_namespaced_custom_object(
                "metrics.k8s.io", "v1beta1", self.target.namespace, "pods",
                label_selector=self.selector, _request_timeout=KUBE_TIMEOUT,
            )
            by_name = {item["metadata"]["name"]: item for item in metrics["items"]}
            usage = requested = Decimal(0)
            now = datetime.now(timezone.utc)
            for pod in ready:
                sample = by_name[pod.metadata.name]
                timestamp = datetime.fromisoformat(sample["timestamp"].replace("Z", "+00:00"))
                if not -5 <= (now - timestamp).total_seconds() <= 60:
                    return None
                container_cpu = {item["name"]: item["usage"]["cpu"] for item in sample["containers"]}
                for container in pod.spec.containers:
                    limit = parse_quantity(container.resources.requests["cpu"])
                    used = parse_quantity(container_cpu[container.name])
                    if not limit.is_finite() or not used.is_finite() or limit <= 0 or used < 0:
                        return None
                    requested += limit
                    usage += used
            utilization = float(100 * usage / requested) if requested else None
            return utilization if utilization is not None and math.isfinite(utilization) else None
        except (kube_client.ApiException, KubernetesTransportError, ArithmeticError, AttributeError, KeyError, TypeError, ValueError):
            return None

    def scale(self, uid: str, allowed_current: tuple[int, ...], replicas: int) -> None:
        deployment = self._deployment()
        if deployment.metadata.uid != uid:
            raise TargetChanged("The staging Deployment was replaced.")
        target = self.target
        scale = self.apps.read_namespaced_deployment_scale(
            target.deployment, target.namespace, _request_timeout=KUBE_TIMEOUT,
        )
        if scale.metadata.uid != uid:
            raise TargetChanged("The staging Deployment was replaced.")
        if scale.spec.replicas not in allowed_current:
            raise StagingError("The Deployment's requested replica count changed outside the benchmark.")
        if scale.spec.replicas == replicas:
            return
        self._check_autoscaler()
        if not scale.metadata.resource_version:
            raise StagingError("The Deployment scale resource has no version.")
        scale.spec.replicas = replicas
        applied = self.apps.replace_namespaced_deployment_scale(
            target.deployment, target.namespace, scale, _request_timeout=KUBE_TIMEOUT,
        )
        if applied.metadata.uid != uid or applied.spec.replicas != replicas:
            raise StagingError("Kubernetes did not confirm the requested replica count.")


async def apply_scale(cluster: KubernetesStaging, uid: str, current: tuple[int, ...], replicas: int) -> None:
    # A cancelled await must settle its in-flight write before restoration starts.
    update = asyncio.create_task(asyncio.to_thread(cluster.scale, uid, current, replicas))
    try:
        await asyncio.shield(update)
    except asyncio.CancelledError:
        await asyncio.gather(update, return_exceptions=True)
        raise


@dataclass(frozen=True)
class TrafficWindow:
    offset: int
    seconds: int
    rps: int
    phase: str


@dataclass(frozen=True)
class RequestMeasurement:
    responded: bool
    latency_ms: float | None = None


def traffic_windows(duration: int, interval: int, base_rps: int, peak_rps: int) -> list[TrafficWindow]:
    spike_start = duration // 4
    spike_end = math.ceil(duration * 0.65)
    return [
        TrafficWindow(offset, min(interval, end - offset), rate, phase)
        for start, end, rate, phase in (
            (0, spike_start, base_rps, "BEFORE_SPIKE"),
            (spike_start, spike_end, peak_rps, "SPIKE"),
            (spike_end, duration, base_rps, "AFTER_SPIKE"),
        )
        for offset in range(start, end, interval)
    ]


async def measure_request(client: httpx.AsyncClient, url: str) -> RequestMeasurement:
    clock = asyncio.get_running_loop()
    started = clock.time()
    responded = False
    try:
        async with asyncio.timeout(HTTP_TIMEOUT):
            async with client.stream("GET", url) as response:
                responded = True
                if not response.is_success:
                    return RequestMeasurement(responded)
                received = 0
                async for chunk in response.aiter_raw(chunk_size=16384):
                    received += len(chunk)
                    if received > 131072:
                        return RequestMeasurement(responded)
    except (TimeoutError, httpx.HTTPError):
        return RequestMeasurement(responded)
    return RequestMeasurement(responded, (clock.time() - started) * 1000)


async def measure_window(
    client: httpx.AsyncClient, url: str, starts_at: float, window: TrafficWindow,
) -> list[RequestMeasurement]:
    clock = asyncio.get_running_loop()
    requests = []
    try:
        for index in range(window.seconds * window.rps):
            scheduled = starts_at + index / window.rps
            await asyncio.sleep(max(0, scheduled - clock.time()))
            if clock.time() - scheduled > 0.5:
                raise StagingError("The load generator fell behind the configured request rate.")
            requests.append(asyncio.create_task(measure_request(client, url)))
        return await asyncio.gather(*requests)
    finally:
        for request in requests:
            if not request.done():
                request.cancel()
        await asyncio.gather(*requests, return_exceptions=True)


def p95(values: list[float]) -> float | None:
    if not values:
        return None
    return round(sorted(values)[math.ceil(len(values) * 0.95) - 1], 3)


def trial_summary(strategy: str, samples: list[dict], latencies: list[float], config: dict) -> dict:
    target = config["target_latency"]
    max_errors = config["staging"]["target_error_percent"]
    requests = sum(sample["requests"] for sample in samples)
    errors = sum(sample["failed_requests"] for sample in samples)
    error_percent = 100 * errors / requests
    p95_latency = p95(latencies)
    cpu_samples = [sample["cpu_percent"] for sample in samples if sample["cpu_percent"] is not None]
    cpu_coverage = len(cpu_samples) / len(samples)
    valid = strategy not in {"CPU", "TREND"} or cpu_coverage >= MIN_CPU_COVERAGE
    meets_targets = None
    if valid:
        meets_targets = p95_latency is not None and p95_latency <= target and error_percent <= max_errors
    breaches = [
        index for index, sample in enumerate(samples)
        if sample["p95_latency_ms"] is not None and sample["p95_latency_ms"] > target
    ]
    spike_breach = next((index for index in breaches if samples[index]["phase"] == "SPIKE"), None)
    recovery = 0 if spike_breach is None else None
    if spike_breach is not None:
        for sample in samples[spike_breach + 1:]:
            if sample["p95_latency_ms"] is not None and sample["p95_latency_ms"] <= target:
                recovery = sample["elapsed_seconds"] - samples[spike_breach]["elapsed_seconds"]
                break
    elif not any(sample["phase"] == "SPIKE" and sample["p95_latency_ms"] is not None for sample in samples):
        recovery = None
    return {
        "source": SOURCE, "samples": len(samples), "requests": requests, "failed_requests": errors,
        "error_rate_percent": round(error_percent, 3), "p95_latency_ms": p95_latency,
        "latency_windows": sum(sample["p95_latency_ms"] is not None for sample in samples),
        "target_latency_ms": target, "target_error_percent": max_errors,
        "seconds_above_latency_target": sum(samples[index]["window_seconds"] for index in breaches),
        "spike_recovery_seconds": recovery,
        "replica_minutes": round(sum(sample["active_replicas"] * sample["window_seconds"] for sample in samples) / 60, 4),
        "scaling_events": sum(sample["scale_applied"] for sample in samples),
        "cpu_coverage": round(cpu_coverage, 4),
        "quality": "COMPLETE" if valid else "INSUFFICIENT_CPU",
        "meets_targets": meets_targets,
        "evaluation_score": None,
    }
