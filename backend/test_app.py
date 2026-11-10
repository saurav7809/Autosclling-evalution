import asyncio
import json
import time

import httpx
import pytest
from fastapi.testclient import TestClient

import app


@pytest.fixture
def local_database(tmp_path, monkeypatch):
    monkeypatch.setattr(app, "DB_PATH", tmp_path / "autoscaling.db")
    monkeypatch.setattr(app, "JWT_SECRET", "test-signing-secret-for-local-api-tests")
    monkeypatch.setattr(app, "PROMETHEUS_URL", "")


@pytest.fixture
def api_client(local_database):
    with TestClient(app.app) as client:
        yield client


def authenticate(client):
    registered = client.post("/api/v1/auth/register", json={
        "username": "test-user", "email": "test@example.test", "display_name": "Test User",
        "password": "local-test-password", "accepted_tos": True,
    })
    assert registered.status_code == 201, registered.text
    token = registered.json()["access_token"]
    client.headers["Authorization"] = f"Bearer {token}"


@pytest.fixture
def client(api_client):
    authenticate(api_client)
    return api_client


def start_experiment(client, **options):
    ref = "test.local/eval:latest"
    assert client.post("/api/v1/images", json={"image_ref": ref}).status_code == 201
    response = client.post("/api/v1/experiments", json={
        "name": "Quick strategy test", "image_ref": ref, "duration_seconds": 15,
        "sample_interval_seconds": 10, **options,
    })
    assert response.status_code == 202, response.text
    return response.json()["id"]


def wait_for_status(client, experiment_id, expected):
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        response = client.get(f"/api/v1/experiments/{experiment_id}")
        assert response.status_code == 200, response.text
        detail = response.json()
        if detail["status"] == expected:
            return detail
        if detail["status"] in {"COMPLETED", "FAILED", "INTERRUPTED"}:
            break
        time.sleep(0.025)
    pytest.fail(f"Expected {expected}, got {detail['status']}: {detail['error']}")


def test_health(api_client):
    r = api_client.get('/healthz')
    assert r.status_code == 200
    assert r.json()['status'] == 'ok'


def test_startup_requires_signing_secret(local_database, monkeypatch):
    monkeypatch.setattr(app, "JWT_SECRET", "")
    with pytest.raises(RuntimeError, match="JWT_SECRET"):
        with TestClient(app.app):
            pass

def test_image_registration_and_duplicate(client):
    ref = 'test.local/service:latest'
    first = client.post('/api/v1/images', json={'image_ref': ref, 'display_name': 'Test service'})
    assert first.status_code == 201, first.text
    image_id = first.json()['id']
    assert first.json()['image_ref'] == ref
    second = client.post('/api/v1/images', json={'image_ref': ref})
    assert second.status_code == 201
    assert second.json()['id'] == image_id
    assert 'already registered' in second.json()['message']

def test_validation_of_image_ref(client):
    r = client.post('/api/v1/images', json={'image_ref': 'bad ref with spaces'})
    assert r.status_code == 422

def test_experiment_requires_registered_image(client):
    r = client.post('/api/v1/experiments', json={'name':'Missing image test','image_ref':'missing/service:latest'})
    assert r.status_code == 400

def test_metric_ingestion_round_trip(client):
    payload = {'cpu_percent': 42.5, 'memory_percent': 50, 'latency_ms': 120, 'request_rate': 65, 'replicas': 2, 'source':'MANUAL'}
    r = client.post('/api/v1/metrics/ingest', json=payload)
    assert r.status_code == 201, r.text
    assert r.json()['source'] == 'MANUAL'
    samples = client.get('/api/v1/metrics').json()
    assert any(s['cpu_percent'] == 42.5 for s in samples)

def test_simulation_benchmark_with_fixed_replica_baseline(client, monkeypatch):
    monkeypatch.setattr(app, "PROMETHEUS_URL", "http://prometheus.test")
    experiment_id = start_experiment(client, mode="SIMULATION", workload="SPIKE")
    detail = wait_for_status(client, experiment_id, "COMPLETED")
    assert detail["mode"] == "SIMULATION"
    assert len(detail["samples"]) == 20
    comparison = client.get(f"/api/v1/experiments/{experiment_id}/compare")
    assert comparison.status_code == 200
    report = comparison.json()
    assert {row["strategy"] for row in report["strategies"]} == {"CPU", "TREND", "LATENCY"}
    assert all(row["source"] == "SIMULATED_WORKLOAD" for row in report["strategies"])
    assert all(0 <= row["evaluation_score"] <= 100 for row in report["strategies"])
    baseline = report["baseline"]
    assert baseline["fixed_replicas"] == 2
    assert baseline["replica_minutes"] == 0.5
    assert baseline["seconds_above_latency_target"] == 6
    assert baseline["spike_recovery_seconds"] == 6
    assert baseline["scaling_events"] == 0
    for result in report["strategies"]:
        samples = [sample for sample in detail["samples"] if sample["note"].startswith(f"{result['strategy']} ")]
        assert len(samples) == 5
        assert result["replica_minutes"] == round(sum(sample["replicas"] for sample in samples) * 3 / 60, 3)
        assert result["scaling_events"] == sum(sample["scaling_event"] is not None for sample in samples)

def test_prometheus_status_is_explicit_when_unconfigured(api_client):
    r = api_client.get('/api/v1/prometheus/status')
    assert r.status_code == 200
    assert r.json()['configured'] is False
    r = api_client.get('/api/v1/prometheus/snapshot')
    assert r.status_code == 503
    assert 'PROMETHEUS_URL' in r.json()['detail']


def test_resource_writes_require_authentication(api_client):
    for method, path in (
        ("POST", "/images"), ("POST", "/images/discover-docker"), ("DELETE", "/images/any-id"),
        ("POST", "/experiments"), ("POST", "/experiments/any-id/retry"),
        ("POST", "/metrics/ingest"), ("GET", "/infrastructure/recommendations"),
    ):
        response = api_client.request(method, f"/api/v1{path}", json={})
        assert response.status_code == 401, (method, path, response.text)
        assert response.headers["WWW-Authenticate"] == "Bearer"
    response = api_client.post("/api/v1/experiments", headers={"Authorization": "Bearer invalid"}, json={})
    assert response.status_code == 401


def test_login_token_can_authenticate(client):
    logged_in = client.post("/api/v1/auth/login", json={"username": "test-user", "password": "local-test-password"})
    assert logged_in.status_code == 200
    headers = {"Authorization": f"Bearer {logged_in.json()['access_token']}"}
    user = client.get("/api/v1/auth/me", headers=headers).json()
    assert user["username"] == "test-user"
    assert "password_hash" not in user


def test_replay_outage_and_partial_recovery(client, monkeypatch):
    monkeypatch.setattr(app, "PROMETHEUS_URL", "http://prometheus.test")
    monkeypatch.setattr(app, "sampling_plan", lambda config: (5, 0.0))
    values = dict(zip(app.REPLAY_METRICS, (80, 40, 300, 0)))
    queries = {query: key for key, query in app.PROMETHEUS_QUERIES.items()}
    unavailable = True
    latency_queries = 0

    def reply(request):
        nonlocal latency_queries
        if unavailable:
            raise httpx.ReadTimeout("Synthetic Prometheus timeout", request=request)
        key = queries[request.url.params["query"]]
        value = values[key]
        if key == "p95_latency_ms":
            latency_queries += 1
            if latency_queries == 2:
                value = "NaN"
        return httpx.Response(200, json={
            "status": "success", "data": {"result": [{"value": [0, str(value)]}]},
        })

    async_client = httpx.AsyncClient
    monkeypatch.setattr(app.httpx, "AsyncClient", lambda **options: async_client(
        transport=httpx.MockTransport(reply), **options,
    ))
    experiment_id = start_experiment(client, mode="TELEMETRY_REPLAY", strategies=["TREND", "CPU", "LATENCY"])
    failed = wait_for_status(client, experiment_id, "FAILED")
    assert failed["data_quality"]["valid_windows"] == 0
    assert failed["data_quality"]["status"] == "INSUFFICIENT"
    assert failed["samples"] == []
    comparison = client.get(f"/api/v1/experiments/{experiment_id}/compare").json()
    assert comparison["strategies"] == []
    assert comparison["score_formula"] is None
    assert "0 of 5" in comparison["error"]

    unavailable = False
    retry_path = f"/api/v1/experiments/{experiment_id}/retry"
    retried = client.post(retry_path)
    assert retried.status_code == 202, retried.text
    retry_id = retried.json()["id"]
    assert retry_id != experiment_id
    assert client.post(retry_path).json()["id"] == retry_id
    completed = wait_for_status(client, retry_id, "COMPLETED")
    assert completed["retry_of"] == experiment_id
    assert completed["config"] == failed["config"]
    assert completed["data_quality"]["coverage"] == 0.8
    assert completed["data_quality"]["status"] == "PARTIAL"
    assert completed["data_quality"]["missing_by_metric"]["p95_latency_ms"] == 1
    assert len(completed["samples"]) == 12
    assert all(sample["latency_ms"] == 300 and sample["request_rate"] == 0 for sample in completed["samples"])
    report = client.get(f"/api/v1/experiments/{retry_id}/compare").json()
    assert report["baseline"] is None
    assert report["score_formula"] is None
    assert [row["strategy"] for row in report["strategies"]] == ["TREND", "CPU", "LATENCY"]
    assert all(row["evaluation_score"] is None for row in report["strategies"])
    assert all(row["avg_cpu_percent"] == 80 and row["avg_latency_ms"] == 300 for row in report["strategies"])
    assert report["strategies"][0]["avg_replicas"] < report["strategies"][1]["avg_replicas"]
    assert client.get(f"/api/v1/experiments/{experiment_id}").json()["status"] == "FAILED"


def test_runner_error_is_visible_and_retryable(client, monkeypatch):
    async def broken_runner(experiment_id, config):
        raise RuntimeError("test failure")

    with monkeypatch.context() as failure:
        failure.setattr(app, "run_experiment", broken_runner)
        experiment_id = start_experiment(client)
        detail = wait_for_status(client, experiment_id, "FAILED")
        assert "Check server logs" in detail["error"]
    retry = client.post(f"/api/v1/experiments/{experiment_id}/retry")
    assert retry.status_code == 202
    wait_for_status(client, retry.json()["id"], "COMPLETED")


def test_shutdown_interrupts_in_progress_run(local_database, monkeypatch):
    async def wait_for_shutdown(experiment_id, config):
        await asyncio.Event().wait()

    with monkeypatch.context() as shutdown:
        shutdown.setattr(app, "run_experiment", wait_for_shutdown)
        with TestClient(app.app) as client:
            authenticate(client)
            experiment_id = start_experiment(client, baseline_replicas=3)
            wait_for_status(client, experiment_id, "RUNNING")
            retry = client.post(f"/api/v1/experiments/{experiment_id}/retry")
            assert retry.status_code == 409
            headers = dict(client.headers)
    with TestClient(app.app) as client:
        detail = wait_for_status(client, experiment_id, "INTERRUPTED")
        assert detail["completed_at"]
        retry = client.post(f"/api/v1/experiments/{experiment_id}/retry", headers=headers)
        assert retry.status_code == 202
        recovered = wait_for_status(client, retry.json()["id"], "COMPLETED")
        assert recovered["baseline"]["fixed_replicas"] == 3


def test_startup_migrates_legacy_runs(local_database):
    with app.db() as connection:
        connection.executescript("""
            CREATE TABLE experiments (
              id TEXT PRIMARY KEY, name TEXT NOT NULL, image_ref TEXT NOT NULL,
              strategy TEXT NOT NULL, workload TEXT NOT NULL, duration_seconds INTEGER NOT NULL,
              min_replicas INTEGER NOT NULL, max_replicas INTEGER NOT NULL, target_cpu REAL NOT NULL,
              target_latency REAL NOT NULL, status TEXT NOT NULL, source TEXT NOT NULL,
              created_at TEXT NOT NULL, completed_at TEXT, results_json TEXT
            );
        """)
        for experiment_id, status, result in (
            ("stale-run", "RUNNING", None),
            ("old-replay", "COMPLETED", json.dumps({"CPU": {"evaluation_score": 100, "avg_latency_ms": 0}})),
        ):
            connection.execute(
                "INSERT INTO experiments VALUES (?,?,?,'CPU','SPIKE',15,1,8,65,250,?,'PROMETHEUS',?,NULL,?)",
                (experiment_id, "Legacy run", "test.local/service:old", status, app.now_iso(), result),
            )
    with TestClient(app.app) as client:
        stale = wait_for_status(client, "stale-run", "INTERRUPTED")
        assert "Retry" in stale["error"]
        historic = client.get("/api/v1/experiments/old-replay/compare").json()
        assert historic["data_quality"] == {"status": "UNVERIFIED"}
        assert historic["strategies"][0]["evaluation_score"] is None
        assert historic["score_formula"] is None


def test_startup_leaves_active_worker_and_interrupts_expired_worker(local_database):
    app.init_db()
    now = time.time()
    with app.db() as connection:
        for experiment_id, lease_until in (("expired", now - 1), ("active", now + 60)):
            connection.execute(
                "INSERT INTO experiments(id,name,image_ref,strategy,workload,duration_seconds,"
                "min_replicas,max_replicas,target_cpu,target_latency,status,source,created_at,lease_until) "
                "VALUES (?,?,'test.local/service:latest','CPU','STEADY',15,1,8,65,250,"
                "'RUNNING','SIMULATED_WORKLOAD',?,?)",
                (experiment_id, "Worker lease test", app.now_iso(), lease_until),
            )
    with TestClient(app.app) as client:
        expired = client.get("/api/v1/experiments/expired").json()
        active = client.get("/api/v1/experiments/active").json()
        assert expired["status"] == "INTERRUPTED"
        assert expired["error"]
        assert active["status"] == "RUNNING"
        assert active["error"] is None
