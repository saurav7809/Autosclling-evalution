import json
import socket
import time
from dataclasses import asdict
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Lock, Thread
from types import SimpleNamespace

import httpx
import pytest
import uvicorn
from fastapi.testclient import TestClient

import app
import staging
from test_app import authenticate, local_database, wait_for_status


@pytest.fixture
def staging_target(monkeypatch):
    monkeypatch.setenv("STAGING_NAMESPACE", "autoscaling-staging")
    monkeypatch.setenv("STAGING_DEPLOYMENT", "benchmark-app")
    monkeypatch.setenv("STAGING_SERVICE", "benchmark-app")
    monkeypatch.setenv("STAGING_SERVICE_PORT", "80")
    monkeypatch.setenv("STAGING_REQUEST_PATH", "/")
    monkeypatch.setenv("STAGING_OPERATOR_TOKEN", "test-staging-operator-token-for-local-tests")
    return staging.StagingTarget.configured()


def live_options(**options):
    return {
        "name": "Measured staging test", "image_ref": "registry.k8s.io/hpa-example",
        "mode": staging.MODE, "duration_seconds": 15, "max_replicas": 3, **options,
    }


class ClusterDouble:
    def __init__(self, target, requested=3):
        self.target = target
        self.requested = requested
        self.writes = []

    def snapshot(self, uid, include_cpu=False):
        assert uid == "staging-uid"
        cpu = (90 if self.requested == 1 else 40) if include_cpu else None
        return staging.DeploymentSnapshot(uid, self.requested, self.requested, self.requested, True, cpu)

    def preflight(self, image_ref):
        assert image_ref == "registry.k8s.io/hpa-example"
        return self.snapshot("staging-uid")

    def scale(self, uid, allowed_current, replicas):
        assert uid == "staging-uid"
        assert self.requested in allowed_current
        self.writes.append((self.requested, replicas))
        self.requested = replicas

    def close(self):
        pass


@pytest.fixture
def measured_http_target():
    count = 0
    lock = Lock()

    class Workload(BaseHTTPRequestHandler):
        def do_GET(self):
            nonlocal count
            assert self.path == "/"
            with lock:
                count += 1
                fail = count % 7 == 0
            time.sleep(0.01)
            self.send_response(503 if fail else 200)
            self.end_headers()
            self.wfile.write(b"staging workload")

        def log_message(self, *_):
            pass

    with ThreadingHTTPServer(("127.0.0.1", 0), Workload) as server:
        worker = Thread(target=server.serve_forever, daemon=True)
        worker.start()
        try:
            yield f"http://127.0.0.1:{server.server_port}/", lambda: count
        finally:
            server.shutdown()
            worker.join(timeout=5)


@pytest.fixture
def live_api(local_database):
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
        server = uvicorn.Server(uvicorn.Config(app.app, host="127.0.0.1", port=port, log_level="warning"))
        worker = Thread(target=server.run, kwargs={"sockets": [listener]}, daemon=True)
        worker.start()
        try:
            with httpx.Client(base_url=f"http://127.0.0.1:{port}", timeout=10, trust_env=False) as client:
                for _ in range(100):
                    try:
                        if client.get("/healthz", timeout=0.5).status_code == 200:
                            break
                    except httpx.TransportError:
                        pass
                    time.sleep(0.05)
                else:
                    pytest.fail("The live API did not start")
                authenticate(client)
                yield client
        finally:
            server.should_exit = True
            worker.join(timeout=10)
            assert not worker.is_alive()


def read_events(response):
    kind = event_id = None
    for line in response.iter_lines():
        if line.startswith("id: "):
            event_id = int(line.removeprefix("id: "))
        elif line.startswith("event: "):
            kind = line.removeprefix("event: ")
        elif line.startswith("data: "):
            yield kind, event_id, json.loads(line.removeprefix("data: "))
            kind = event_id = None


def test_live_run_streams_measured_http_outcomes_and_restores_deployment(
    live_api, staging_target, measured_http_target, monkeypatch,
):
    cluster = ClusterDouble(staging_target)
    url, count = measured_http_target
    monkeypatch.setattr(staging, "KubernetesStaging", lambda target: cluster)
    monkeypatch.setattr(staging.StagingTarget, "url", property(lambda target: url))
    monkeypatch.setattr(staging, "traffic_windows", lambda *_: [
        staging.TrafficWindow(0, 1, 2, "BEFORE_SPIKE"),
        staging.TrafficWindow(1, 1, 4, "SPIKE"),
        staging.TrafficWindow(2, 1, 2, "AFTER_SPIKE"),
    ])
    warmup = app.warmup_staging

    async def short_warmup(http, target, options):
        await warmup(http, target, options | {"warmup_seconds": 1})

    monkeypatch.setattr(app, "warmup_staging", short_warmup)
    client = live_api
    options = live_options(staging={"target_error_percent": 25}, target_latency=500)
    client.post("/api/v1/images", json={"image_ref": options["image_ref"]}).raise_for_status()
    assert client.post("/api/v1/experiments", json=options).status_code == 403
    headers = {"X-Staging-Token": "test-staging-operator-token-for-local-tests"}
    created = client.post("/api/v1/experiments", json=options, headers=headers).raise_for_status().json()
    assert created["source"] == staging.SOURCE
    assert client.post("/api/v1/experiments", json=options, headers=headers).status_code == 409

    windows, reports = [], []
    last_id = None
    with client.stream("GET", created["stream_url"]) as response:
        response.raise_for_status()
        for kind, event_id, payload in read_events(response):
            if kind == "sample":
                if not windows:
                    # A client receives measurements while the benchmark is still running.
                    current = client.get(f"/api/v1/experiments/{created['id']}").raise_for_status().json()
                    assert current["status"] == "RUNNING"
                    assert current["samples"][0]["p95_latency_ms"] > 0
                windows.append(payload)
                last_id = event_id
            elif kind == "report":
                reports.append(payload)

    final = reports[-1]
    assert final["status"] == "COMPLETED"
    assert final["staging"]["restoration"]["status"] == "RESTORED"
    assert final["staging"]["progress"]["phase"] == "FINISHED"
    assert cluster.requested == 3
    assert (1, 2) in cluster.writes
    assert any(report["status"] == "RUNNING" and report["strategies"] for report in reports)
    assert {row["strategy"] for row in final["strategies"]} == {"CPU", "TREND", "LATENCY"}
    assert final["baseline"]["fixed_replicas"] == 2
    assert final["baseline"]["replica_minutes"] == 0.1
    assert len(windows) == 12
    assert sum(window["requests"] for window in windows) == 32
    assert sum(window["responses"] for window in windows) == 32
    assert sum(window["failed_requests"] for window in windows) > 0
    assert count() == 40  # 32 measured requests plus 8 warm-up requests
    assert all(window["source"] == staging.SOURCE for window in windows)
    assert all(window["ready_replicas"] <= window["active_replicas"] for window in windows)
    assert final["recommended_strategies"]
    with client.stream("GET", created["stream_url"], headers={"Last-Event-ID": str(last_id)}) as response:
        resumed = list(read_events(response.raise_for_status()))
    assert [kind for kind, _, _ in resumed] == ["report"]


def test_expired_staging_run_restores_a_scale_write_that_may_have_applied(
    local_database, staging_target, monkeypatch,
):
    cluster = ClusterDouble(staging_target, requested=4)
    monkeypatch.setattr(staging, "KubernetesStaging", lambda target: cluster)
    app.init_db()
    with app.db() as c:
        c.execute(
            "INSERT INTO experiments(id,name,image_ref,strategy,workload,duration_seconds,min_replicas,max_replicas,"
            "target_cpu,target_latency,status,source,created_at,lease_until) "
            "VALUES('expired-stage','Staging run','registry.k8s.io/hpa-example','CPU','SPIKE',15,1,8,65,250,"
            "'RUNNING',?,?,?)",
            (staging.SOURCE, app.now_iso(), time.time() - 1),
        )
        c.execute(
            "INSERT INTO staging_runs(experiment_id,target_key,target_json,progress_json,deployment_uid,"
            "original_replicas,last_replicas,pending_replicas) VALUES(?,?,?,?,?,?,?,?)",
            ("expired-stage", staging_target.key, json.dumps(asdict(staging_target)), "{}",
             "staging-uid", 3, 2, 4),
        )
    with TestClient(app.app) as client:
        wait_for_status(client, "expired-stage", "INTERRUPTED")
        for _ in range(100):
            detail = client.get("/api/v1/experiments/expired-stage").raise_for_status().json()
            if detail["staging"]["restoration"]["status"] == "RESTORED":
                break
            time.sleep(0.025)
        else:
            pytest.fail("The persisted staging restoration was not applied")
    assert cluster.requested == 3
    assert cluster.writes == [(4, 3)]


def test_stage_scale_uses_resource_version_and_blocks_outside_changes(staging_target):
    cluster = staging.KubernetesStaging.__new__(staging.KubernetesStaging)
    cluster.target = staging_target
    cluster._deployment = lambda: SimpleNamespace(metadata=SimpleNamespace(uid="staging-uid"))
    cluster._check_autoscaler = lambda: None
    scale = staging.kube_client.V1Scale(
        metadata=staging.kube_client.V1ObjectMeta(uid="staging-uid", resource_version="10"),
        spec=staging.kube_client.V1ScaleSpec(replicas=2),
    )
    applied = []

    def replace(name, namespace, body, **options):
        applied.append((name, namespace, body.metadata.resource_version, body.spec.replicas))
        return body

    cluster.apps = SimpleNamespace(
        read_namespaced_deployment_scale=lambda *args, **options: scale,
        replace_namespaced_deployment_scale=replace,
    )
    with pytest.raises(staging.StagingError, match="changed outside"):
        cluster.scale("staging-uid", (1,), 3)
    assert not applied
    cluster.scale("staging-uid", (2,), 3)
    assert applied == [("benchmark-app", "autoscaling-staging", "10", 3)]


def test_missing_cpu_never_counts_as_meeting_staging_targets():
    window = {
        "phase": "SPIKE", "requests": 8, "failed_requests": 0, "cpu_percent": None,
        "p95_latency_ms": 10.0, "active_replicas": 2, "window_seconds": 5, "scale_applied": False,
    }
    options = {"target_latency": 100, "staging": {"target_error_percent": 1}}
    result = staging.trial_summary("CPU", [window], [10.0], options)
    assert result["p95_latency_ms"] == 10.0
    assert result["quality"] == "INSUFFICIENT_CPU"
    assert result["meets_targets"] is None


def test_traffic_schedule_keeps_the_spike_with_long_sample_intervals():
    windows = staging.traffic_windows(15, 10, 2, 8)
    assert [(window.offset, window.seconds, window.rps) for window in windows] == [
        (0, 3, 2), (3, 7, 8), (10, 5, 2),
    ]


def test_cpu_uses_fresh_metrics_for_the_target_ready_pods(staging_target):
    cluster = staging.KubernetesStaging.__new__(staging.KubernetesStaging)
    cluster.target = staging_target
    cluster.selector = "app=benchmark-app"
    container = SimpleNamespace(name="app", resources=SimpleNamespace(requests={"cpu": "200m"}))
    pod = SimpleNamespace(
        metadata=SimpleNamespace(name="benchmark-pod", deletion_timestamp=None),
        status=SimpleNamespace(conditions=[SimpleNamespace(type="Ready", status="True")]),
        spec=SimpleNamespace(containers=[container]),
    )
    cluster.core = SimpleNamespace(list_namespaced_pod=lambda *args, **kwargs: SimpleNamespace(items=[pod]))
    sample = {
        "metadata": {"name": "benchmark-pod"}, "timestamp": datetime.now(timezone.utc).isoformat(),
        "containers": [{"name": "app", "usage": {"cpu": "75000000n"}}],
    }
    cluster.metrics = SimpleNamespace(list_namespaced_custom_object=lambda *args, **kwargs: {"items": [sample]})
    assert cluster._cpu_percent(1) == 37.5
    sample["timestamp"] = "2000-01-01T00:00:00Z"
    assert cluster._cpu_percent(1) is None
