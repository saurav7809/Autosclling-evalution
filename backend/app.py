from __future__ import annotations
import asyncio, json, os, re, shutil, sqlite3, subprocess, time, uuid
import hmac
import logging
import math
from collections.abc import Iterator
from contextlib import asynccontextmanager, closing, contextmanager
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Literal, NamedTuple, Optional

import psutil
import docker
import httpx
from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from prometheus_fastapi_instrumentator import Instrumentator
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, Field, field_validator, model_validator
from jose import jwt as jose_jwt, JWTError
from passlib.context import CryptContext

import staging

DATA_DIR = Path(os.getenv("DATA_DIR", "/data"))
DB_PATH = DATA_DIR / "autoscaling.db"
PROMETHEUS_URL = os.getenv("PROMETHEUS_URL", "").rstrip("/")
STRATEGIES = ("CPU", "TREND", "LATENCY")
REPLAY_METRICS = ("cpu_percent", "memory_percent", "p95_latency_ms", "request_rate")
PROMETHEUS_TIMEOUT = 4.0
MIN_REPLAY_COVERAGE = 0.8
EXPERIMENT_LEASE_SECONDS = 45
INTERRUPTED_MESSAGE = "The worker stopped before the experiment finished. Retry to start a new attempt."
SCORE_FORMULA = "100 - 35*(avg_latency/latency_target) - 0.25*avg_cpu - 0.7*scaling_events; clamped 0..100"
SPIKE_START, SPIKE_END = 0.25, 0.65

JWT_SECRET = os.getenv("JWT_SECRET", "")
JWT_ALGORITHM = "HS256"
JWT_EXPIRY_HOURS = 24
bearer = HTTPBearer(auto_error=False)
logger = logging.getLogger(__name__)

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

import threading as _threading
from collections import deque as _deque

# Rolling metric history (updated by prometheus_scrape_loop, consumed by advisor)
_metric_history: _deque[float] = _deque(maxlen=15)  # ~75 s at 5 s interval
_history_lock = _threading.Lock()

# Advisor recommendation tracker for live scaling event emission
_last_advisor_recs: dict = {}  # strategy -> last recommended replicas
_advisor_lock = _threading.Lock()


# ── Workload replica detection: K8s → kubectl → Docker workload filter → 1 ──
_INFRA_NAMES = {"prometheus", "grafana", "cadvisor", "dashboard"}


def _get_workload_replicas() -> tuple[int, str]:
    """Returns (count, source) via K8s → kubectl → Docker workload filter → 1."""
    # 1. Kubernetes Python client (incluster or kubeconfig)
    try:
        from kubernetes import client as k8s_client, config as k8s_config  # type: ignore
        try:
            k8s_config.load_incluster_config()
        except Exception:
            k8s_config.load_kube_config()
        apps = k8s_client.AppsV1Api()
        deps = apps.list_deployment_for_all_namespaces(_request_timeout=3)
        ready = sum((d.status.ready_replicas or 0) for d in deps.items)
        if ready > 0:
            return ready, "kubernetes"
    except Exception:
        pass

    # 2. kubectl subprocess fallback
    try:
        result = subprocess.run(
            ["kubectl", "get", "deployments", "--all-namespaces",
             "-o", "jsonpath={.items[*].status.readyReplicas}"],
            capture_output=True, text=True, timeout=3,
        )
        if result.returncode == 0 and result.stdout.strip():
            nums = [int(x) for x in result.stdout.split() if x.strip().isdigit()]
            total = sum(nums)
            if total > 0:
                return total, "kubectl"
    except Exception:
        pass

    # 3. Docker workload containers (exclude infra services)
    try:
        dc = docker.DockerClient(base_url="unix:///var/run/docker.sock", timeout=3)
        dc.ping()
        workload = [c for c in dc.containers.list()
                    if not any(p in c.name.lower() for p in _INFRA_NAMES)]
        dc.close()
        if workload:
            return len(workload), "docker_workload"
    except Exception:
        pass

    return 1, "default"


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@contextmanager
def db() -> Iterator[sqlite3.Connection]:
    with closing(sqlite3.connect(DB_PATH, check_same_thread=False)) as conn, conn:
        conn.row_factory = sqlite3.Row
        yield conn


def init_db() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with db() as c:
        c.executescript("""
        CREATE TABLE IF NOT EXISTS images (
          id TEXT PRIMARY KEY, image_ref TEXT NOT NULL UNIQUE, display_name TEXT NOT NULL,
          registry TEXT NOT NULL, tag TEXT NOT NULL, digest TEXT, source TEXT NOT NULL,
          created_at TEXT NOT NULL, last_checked_at TEXT, status TEXT NOT NULL DEFAULT 'REGISTERED'
        );
        CREATE TABLE IF NOT EXISTS experiments (
          id TEXT PRIMARY KEY, name TEXT NOT NULL, image_ref TEXT NOT NULL,
          strategy TEXT NOT NULL, workload TEXT NOT NULL, duration_seconds INTEGER NOT NULL,
          min_replicas INTEGER NOT NULL, max_replicas INTEGER NOT NULL, target_cpu REAL NOT NULL,
          target_latency REAL NOT NULL, status TEXT NOT NULL, source TEXT NOT NULL,
          created_at TEXT NOT NULL, completed_at TEXT, results_json TEXT
        );
        CREATE TABLE IF NOT EXISTS samples (
          id INTEGER PRIMARY KEY AUTOINCREMENT, experiment_id TEXT, timestamp TEXT NOT NULL,
          cpu_percent REAL NOT NULL, memory_percent REAL NOT NULL, latency_ms REAL NOT NULL,
          request_rate REAL NOT NULL, replicas INTEGER NOT NULL, scaling_event TEXT,
          source TEXT NOT NULL, note TEXT,
          FOREIGN KEY(experiment_id) REFERENCES experiments(id)
        );
        CREATE INDEX IF NOT EXISTS idx_samples_experiment_time ON samples(experiment_id, timestamp);
        CREATE TABLE IF NOT EXISTS scaling_events (
          id INTEGER PRIMARY KEY AUTOINCREMENT, experiment_id TEXT, timestamp TEXT NOT NULL,
          strategy TEXT NOT NULL, previous_replicas INTEGER NOT NULL, desired_replicas INTEGER NOT NULL,
          reason TEXT NOT NULL, source TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS users (
          id TEXT PRIMARY KEY,
          username TEXT NOT NULL UNIQUE,
          email TEXT NOT NULL UNIQUE,
          display_name TEXT NOT NULL,
          password_hash TEXT NOT NULL,
          accepted_tos INTEGER NOT NULL DEFAULT 0,
          accepted_tos_at TEXT,
          created_at TEXT NOT NULL,
          role TEXT NOT NULL DEFAULT 'user'
        );
        CREATE TABLE IF NOT EXISTS staging_runs (
          experiment_id TEXT PRIMARY KEY REFERENCES experiments(id),
          target_key TEXT NOT NULL, target_json TEXT NOT NULL, progress_json TEXT NOT NULL,
          deployment_uid TEXT, original_replicas INTEGER, last_replicas INTEGER, pending_replicas INTEGER,
          restore_state TEXT NOT NULL DEFAULT 'PENDING', restore_error TEXT, restore_lease REAL
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_staging_active_target
          ON staging_runs(target_key) WHERE restore_state='PENDING';
        CREATE TABLE IF NOT EXISTS staging_samples (
          id INTEGER PRIMARY KEY AUTOINCREMENT, experiment_id TEXT NOT NULL REFERENCES experiments(id),
          sample_json TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_staging_samples_experiment ON staging_samples(experiment_id, id);
        """)
        # Serialize schema inspection and upgrades across processes sharing the database.
        c.execute("BEGIN IMMEDIATE")
        columns = {row["name"] for row in c.execute("PRAGMA table_info(experiments)")}
        for column, kind in (
            ("config_json", "TEXT"),
            ("error", "TEXT"),
            ("retry_of", "TEXT"),
            ("lease_until", "REAL"),
            ("data_quality_json", "TEXT"),
        ):
            if column not in columns:
                c.execute(f"ALTER TABLE experiments ADD COLUMN {column} {kind}")
        c.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_experiments_retry_of ON experiments(retry_of)")


class ExperimentInterrupted(RuntimeError):
    pass


def renew_experiment_lease(c: sqlite3.Connection, experiment_id: str) -> None:
    now = time.time()
    updated = c.execute(
        "UPDATE experiments SET lease_until=? WHERE id=? AND status='RUNNING' AND lease_until>?",
        (now + EXPERIMENT_LEASE_SECONDS, experiment_id, now),
    )
    if not updated.rowcount:
        raise ExperimentInterrupted


def end_failed_experiment(experiment_id: str, status: str, error: str, data_quality: dict | None = None) -> None:
    with db() as c:
        c.execute(
            "UPDATE experiments SET status=?, completed_at=?, error=?, lease_until=NULL, data_quality_json=? "
            "WHERE id=? AND status IN ('QUEUED','RUNNING')",
            (status, now_iso(), error, json.dumps(data_quality) if data_quality is not None else None, experiment_id),
        )


def interrupt_expired_experiments() -> None:
    with db() as c:
        c.execute(
            "UPDATE experiments SET status='INTERRUPTED', completed_at=?, error=?, lease_until=NULL "
            "WHERE status IN ('QUEUED','RUNNING') AND (lease_until IS NULL OR lease_until<=?)",
            (now_iso(), INTERRUPTED_MESSAGE, time.time()),
        )


async def watch_experiment_leases():
    while True:
        try:
            interrupt_expired_experiments()
            await recover_staging_runs()
        except sqlite3.Error:
            logger.exception("Could not check experiment leases")
        await asyncio.sleep(5)


def prometheus_value(response: httpx.Response) -> float | None:
    response.raise_for_status()
    payload = response.json()
    if payload.get("status") != "success":
        return None
    values = []
    for item in payload.get("data", {}).get("result", []):
        try:
            value = float(item["value"][1])
        except (KeyError, IndexError, TypeError, ValueError, OverflowError):
            continue
        if math.isfinite(value) and value >= 0:
            values.append(value)
    average = sum(values) / len(values) if values else None
    return round(average, 3) if average is not None and math.isfinite(average) else None


async def read_prometheus_metrics(client: httpx.AsyncClient, keys: tuple[str, ...]) -> dict[str, float | None]:
    async def fetch(key: str) -> float | None:
        try:
            async with asyncio.timeout(PROMETHEUS_TIMEOUT):
                response = await client.get(
                    f"{PROMETHEUS_URL}/api/v1/query", params={"query": PROMETHEUS_QUERIES[key]},
                )
            return prometheus_value(response)
        except (TimeoutError, httpx.HTTPError, ValueError, TypeError, AttributeError):
            return None

    return dict(zip(keys, await asyncio.gather(*(fetch(key) for key in keys))))


def query_prometheus_metrics(keys: tuple[str, ...]) -> dict[str, float | None]:
    values = {}
    with httpx.Client(timeout=PROMETHEUS_TIMEOUT) as client:
        for key in keys:
            try:
                response = client.get(f"{PROMETHEUS_URL}/api/v1/query", params={"query": PROMETHEUS_QUERIES[key]})
                values[key] = prometheus_value(response)
            except (httpx.HTTPError, ValueError, TypeError, AttributeError):
                values[key] = None
    return values


async def prometheus_scrape_loop():
    """Record complete observations and reset trend history across missing windows."""
    async with httpx.AsyncClient(timeout=PROMETHEUS_TIMEOUT) as client:
        while True:
            values = await read_prometheus_metrics(client, (*REPLAY_METRICS, "running_containers"))
            cpu, memory, latency, rate = (values[key] for key in REPLAY_METRICS)
            replicas = values["running_containers"]
            with _history_lock:
                if rate is None:
                    _metric_history.clear()
                else:
                    _metric_history.append(rate)
            if None not in (cpu, memory, latency, rate, replicas):
                try:
                    with db() as c:
                        c.execute(
                            "INSERT INTO samples(experiment_id,timestamp,cpu_percent,memory_percent,"
                            "latency_ms,request_rate,replicas,source,note) VALUES(?,?,?,?,?,?,?,?,?)",
                            (None, now_iso(), cpu, memory, latency, rate, int(replicas),
                             "PROMETHEUS", "Live Prometheus scrape"),
                        )
                except sqlite3.Error:
                    logger.exception("Could not record the Prometheus sample")
            await asyncio.sleep(5)


@asynccontextmanager
async def lifespan(app: FastAPI):
    if len(JWT_SECRET.encode("utf-8")) < 32:
        raise RuntimeError("Set JWT_SECRET to a random secret of at least 32 bytes.")
    init_db()
    interrupt_expired_experiments()
    app.state.experiment_tasks = {}
    background = [asyncio.create_task(watch_experiment_leases())]
    if PROMETHEUS_URL:
        background.append(asyncio.create_task(prometheus_scrape_loop()))
    try:
        yield
    finally:
        runs = app.state.experiment_tasks.copy()
        tasks = [*background, *runs.values()]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        for experiment_id in runs:
            end_failed_experiment(experiment_id, "INTERRUPTED", INTERRUPTED_MESSAGE)

app = FastAPI(title="Cloud-Agnostic Auto-Scaling Evaluation API", version="1.0.0",
              description="Register container image references, evaluate scaling policies, inspect telemetry and compare experiments.",
              lifespan=lifespan)
Instrumentator().instrument(app).expose(app, endpoint="/metrics")
app.add_middleware(CORSMiddleware, allow_origins=os.getenv("CORS_ORIGINS", "http://localhost:5173,http://localhost:4173").split(","), allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

class ImageRegistration(BaseModel):
    image_ref: str = Field(min_length=3, max_length=255, description="Registry/repository:tag or registry/repository@sha256:digest")
    display_name: Optional[str] = Field(default=None, max_length=100)
    source: Literal["MANUAL", "DOCKER_DAEMON", "KUBERNETES"] = "MANUAL"

    @field_validator("image_ref")
    @classmethod
    def valid_ref(cls, v: str) -> str:
        v = v.strip()
        if " " in v or not re.match(r"^[a-zA-Z0-9][a-zA-Z0-9._/:@-]*$", v):
            raise ValueError("Use a valid image reference, e.g. nginx:1.27 or ghcr.io/org/app:1.0")
        return v

class StagingOptions(BaseModel):
    base_rps: int = Field(default=2, ge=1, le=staging.MAX_RPS)
    peak_rps: int = Field(default=10, ge=2, le=staging.MAX_RPS)
    warmup_seconds: int = Field(default=20, ge=5, le=30)
    cooldown_seconds: int = Field(default=10, ge=1, le=60)
    target_error_percent: float = Field(default=1.0, ge=0, le=25)

    @model_validator(mode="after")
    def validate_spike(self) -> StagingOptions:
        if self.peak_rps <= self.base_rps:
            raise ValueError("peak_rps must exceed base_rps")
        return self


class ExperimentCreate(BaseModel):
    name: str = Field(min_length=3, max_length=100)
    image_ref: str = Field(min_length=3, max_length=255)
    strategies: list[Literal["CPU", "TREND", "LATENCY"]] = Field(
        default=["CPU", "TREND", "LATENCY"], min_length=1,
    )
    workload: Literal["RAMP", "SPIKE", "STEADY", "WAVE"] = "RAMP"
    duration_seconds: int = Field(default=60, ge=15, le=3600)
    min_replicas: int = Field(default=1, ge=1, le=100)
    max_replicas: int = Field(default=8, ge=1, le=100)
    target_cpu: float = Field(default=65, ge=10, le=95)
    target_latency: float = Field(default=250, ge=10, le=10000)
    sample_interval_seconds: int = Field(default=2, ge=1, le=10)
    mode: Literal["SIMULATION", "TELEMETRY_REPLAY", "STAGING_BENCHMARK"] | None = None
    baseline_replicas: int | None = Field(default=None, ge=1, le=100)
    staging: StagingOptions | None = None

    @model_validator(mode="after")
    def validate_replica_bounds(self) -> ExperimentCreate:
        if self.max_replicas < self.min_replicas:
            raise ValueError("max_replicas must be >= min_replicas")
        if self.baseline_replicas is not None and not self.min_replicas <= self.baseline_replicas <= self.max_replicas:
            raise ValueError("baseline_replicas must be between min_replicas and max_replicas")
        if self.mode == staging.MODE:
            if self.workload != "SPIKE" and "workload" in self.model_fields_set:
                raise ValueError("Staging benchmarks use the SPIKE workload")
            if self.duration_seconds > 120 or self.max_replicas > staging.MAX_REPLICAS:
                raise ValueError(f"Staging trials support at most 120 seconds and {staging.MAX_REPLICAS} replicas")
        elif self.staging is not None:
            raise ValueError("Staging options require STAGING_BENCHMARK mode")
        return self

class MetricIngest(BaseModel):
    experiment_id: Optional[str] = None
    cpu_percent: float = Field(ge=0, le=100)
    memory_percent: float = Field(ge=0, le=100)
    latency_ms: float = Field(ge=0)
    request_rate: float = Field(ge=0)
    replicas: int = Field(ge=0)
    source: Literal["PROMETHEUS", "OTEL", "KUBERNETES", "MANUAL"] = "MANUAL"
    note: Optional[str] = None


class UserRegister(BaseModel):
    username: str = Field(min_length=3, max_length=50)
    email: str = Field(min_length=5, max_length=150)
    password: str = Field(min_length=8, max_length=128)
    display_name: str = Field(min_length=2, max_length=100)
    accepted_tos: bool


class UserLogin(BaseModel):
    username: str
    password: str


def _create_jwt(user_id: str, username: str, role: str) -> str:
    expire = datetime.now(timezone.utc) + timedelta(hours=JWT_EXPIRY_HOURS)
    payload = {"sub": user_id, "username": username, "role": role, "exp": expire}
    return jose_jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)


def get_current_user(credentials: HTTPAuthorizationCredentials | None = Depends(bearer)) -> dict:
    unauthorized = {"status_code": 401, "headers": {"WWW-Authenticate": "Bearer"}}
    if credentials is None:
        raise HTTPException(detail="Bearer token required", **unauthorized)
    try:
        payload = jose_jwt.decode(
            credentials.credentials, JWT_SECRET, algorithms=[JWT_ALGORITHM],
            options={"require_exp": True, "require_sub": True},
        )
    except JWTError:
        raise HTTPException(detail="Invalid or expired token", **unauthorized) from None
    with db() as c:
        user = c.execute("SELECT * FROM users WHERE id=?", (payload["sub"],)).fetchone()
    if not user:
        raise HTTPException(detail="User not found", **unauthorized)
    return rowdict(user)


def rowdict(row):
    return dict(row) if row is not None else None


def split_image(image_ref: str):
    # Digests count as tags for display only; the exact reference is always preserved.
    tail = image_ref.rsplit("/", 1)[-1]
    if "@" in tail:
        return image_ref.rsplit("/", 1)[0], tail.split("@", 1)[-1]
    if ":" in tail:
        return image_ref.rsplit(":", 1)[0], tail.rsplit(":", 1)[-1]
    return image_ref, "latest"

@app.get("/healthz")
def health():
    return {"status": "ok", "service": "autoscaling-evaluation-api", "timestamp": now_iso()}


# ── Auth routes ───────────────────────────────────────────────────────────────

@app.post("/api/v1/auth/register", status_code=201)
def auth_register(payload: UserRegister):
    if not payload.accepted_tos:
        raise HTTPException(status_code=400, detail="You must accept the terms of service.")
    # bcrypt limit: truncate to 72 bytes before hashing
    password_hash = pwd_context.hash(payload.password[:72])
    user_id = str(uuid.uuid4())
    now = now_iso()
    with db() as c:
        if c.execute("SELECT 1 FROM users WHERE username=?", (payload.username,)).fetchone():
            raise HTTPException(status_code=409, detail="Username already taken")
        if c.execute("SELECT 1 FROM users WHERE email=?", (payload.email,)).fetchone():
            raise HTTPException(status_code=409, detail="Email already registered")
        c.execute(
            "INSERT INTO users(id,username,email,display_name,password_hash,accepted_tos,accepted_tos_at,created_at,role) "
            "VALUES(?,?,?,?,?,?,?,?,?)",
            (user_id, payload.username, payload.email, payload.display_name,
             password_hash, 1, now, now, "user"),
        )
    token = _create_jwt(user_id, payload.username, "user")
    return {"access_token": token, "token_type": "bearer",
            "user": {"id": user_id, "username": payload.username,
                     "email": payload.email, "display_name": payload.display_name,
                     "role": "user", "created_at": now}}


@app.post("/api/v1/auth/login")
def auth_login(payload: UserLogin):
    with db() as c:
        user = c.execute("SELECT * FROM users WHERE username=?", (payload.username,)).fetchone()
    if not user or not pwd_context.verify(payload.password[:72], user["password_hash"]):
        raise HTTPException(status_code=401, detail="Invalid username or password")
    u = rowdict(user)
    token = _create_jwt(u["id"], u["username"], u["role"])
    u.pop("password_hash", None)
    return {"access_token": token, "token_type": "bearer", "user": u}


@app.get("/api/v1/auth/me")
def auth_me(user: dict = Depends(get_current_user)):
    user.pop("password_hash", None)
    return user

@app.get("/api/v1/overview")
def overview():
    with db() as c:
        image_count = c.execute("SELECT COUNT(*) FROM images").fetchone()[0]
        exp_count = c.execute("SELECT COUNT(*) FROM experiments").fetchone()[0]
        running = c.execute("SELECT COUNT(*) FROM experiments WHERE status='RUNNING'").fetchone()[0]
        sample_count = c.execute("SELECT COUNT(*) FROM samples").fetchone()[0]
        recent = c.execute("SELECT * FROM samples ORDER BY id DESC LIMIT 1").fetchone()
        events = c.execute("SELECT * FROM scaling_events ORDER BY id DESC LIMIT 6").fetchall()
    cpu = psutil.cpu_percent(interval=0.05)
    mem = psutil.virtual_memory().percent
    # If experiment data exists, dashboard displays latest run sample as application telemetry;
    # host metrics remain separately identified.
    latest = rowdict(recent)
    return {"timestamp": now_iso(), "images_registered": image_count, "experiments_total": exp_count,
            "experiments_running": running, "samples_total": sample_count,
            "host_metrics": {"cpu_percent": cpu, "memory_percent": mem, "source": "OBSERVED_HOST", "timestamp": now_iso()},
            "latest_application_sample": latest, "recent_scaling_events": [rowdict(e) for e in events]}

@app.get("/api/v1/images")
def list_images():
    with db() as c:
        return [rowdict(r) for r in c.execute("SELECT * FROM images ORDER BY created_at DESC").fetchall()]

@app.post("/api/v1/images", status_code=201, dependencies=[Depends(get_current_user)])
def register_image(payload: ImageRegistration):
    image_ref = payload.image_ref
    repository, tag = split_image(image_ref)
    display = payload.display_name or repository.rsplit("/", 1)[-1]
    image_id = str(uuid.uuid4())
    digest = tag if tag.startswith("sha256:") else None
    with db() as c:
        existing = c.execute("SELECT * FROM images WHERE image_ref=?", (image_ref,)).fetchone()
        if existing:
            return rowdict(existing) | {"message": "Image reference already registered"}
        c.execute("INSERT INTO images(id,image_ref,display_name,registry,tag,digest,source,created_at,status) VALUES(?,?,?,?,?,?,?,?,?)",
                  (image_id, image_ref, display, repository.split("/")[0] if "/" in repository else "docker.io", tag, digest, payload.source, now_iso(), "REGISTERED"))
        return rowdict(c.execute("SELECT * FROM images WHERE id=?", (image_id,)).fetchone())

@app.post("/api/v1/images/discover-docker", dependencies=[Depends(get_current_user)])
def discover_docker_images():
    try:
        client = docker.DockerClient(
            base_url="unix:///var/run/docker.sock",
            timeout=8,
        )
        client.ping()
        items = []
        try:
            for image in client.images.list():
                for ref in image.tags:
                    if not ref or "<none>" in ref:
                        continue
                    item = register_image(
                        ImageRegistration(
                            image_ref=ref,
                            source="DOCKER_DAEMON",
                        )
                    )
                    items.append(item)
        finally:
            client.close()

        return {
            "discovered": len(items),
            "items": items,
            "source": "DOCKER_DAEMON",
        }
    except docker.errors.DockerException as exc:
        raise HTTPException(
            status_code=503,
            detail=f"Cannot connect to Docker Engine: {exc}",
        )

@app.delete("/api/v1/images/{image_id}", status_code=204, dependencies=[Depends(get_current_user)])
def delete_image(image_id: str):
    with db() as c:
        cur = c.execute("DELETE FROM images WHERE id=?", (image_id,))
        if not cur.rowcount:
            raise HTTPException(404, "Image registration not found")

def experiment_mode(source: str) -> str:
    if source == staging.SOURCE:
        return staging.MODE
    return "TELEMETRY_REPLAY" if source == "PROMETHEUS" else "SIMULATION"


def experiment_data_quality(row: sqlite3.Row) -> dict | None:
    if row["data_quality_json"] is not None:
        return json.loads(row["data_quality_json"])
    if row["source"] == "PROMETHEUS" and row["status"] == "COMPLETED":
        return {"status": "UNVERIFIED"}
    return None


def experiment_results(row: sqlite3.Row) -> tuple[dict, dict | None]:
    results = json.loads(row["results_json"]) if row["results_json"] else {}
    baseline = results.pop("BASELINE", None)
    if row["source"] == "PROMETHEUS":
        for result in results.values():
            result["evaluation_score"] = None
    return results, baseline


def experiment_response(row: sqlite3.Row, *, include_config: bool = False) -> dict:
    item = dict(row)
    config_json = item.pop("config_json")
    item.pop("lease_until")
    item.pop("data_quality_json")
    item.pop("results_json")
    item["mode"] = experiment_mode(row["source"])
    item["data_quality"] = experiment_data_quality(row)
    results, baseline = experiment_results(row)
    if results or include_config:
        item["results"] = results or None
    if baseline is not None:
        item["baseline"] = baseline
    if include_config:
        item["config"] = json.loads(config_json) if config_json else None
    return item


def staging_info(c: sqlite3.Connection, experiment_id: str) -> dict | None:
    row = c.execute("SELECT * FROM staging_runs WHERE experiment_id=?", (experiment_id,)).fetchone()
    if row is None:
        return None
    return {
        "target": json.loads(row["target_json"]),
        "progress": json.loads(row["progress_json"]),
        "restoration": {
            "status": row["restore_state"], "error": row["restore_error"],
            "original_replicas": row["original_replicas"],
        },
    }


def staging_samples(c: sqlite3.Connection, experiment_id: str, after_id: int = 0, limit: int = -1) -> list[dict]:
    rows = c.execute(
        "SELECT id,sample_json FROM staging_samples WHERE experiment_id=? AND id>? ORDER BY id LIMIT ?",
        (experiment_id, after_id, limit),
    )
    return [json.loads(row["sample_json"]) | {"id": row["id"]} for row in rows]


@app.get("/api/v1/experiments")
def list_experiments():
    with db() as c:
        rows = c.execute("SELECT * FROM experiments ORDER BY created_at DESC").fetchall()
    return [experiment_response(row) for row in rows]

@app.get("/api/v1/experiments/{experiment_id}")
def get_experiment(experiment_id: str):
    with db() as c:
        exp = c.execute("SELECT * FROM experiments WHERE id=?", (experiment_id,)).fetchone()
        if not exp: raise HTTPException(404, "Experiment not found")
        live = staging_info(c, experiment_id) if exp["source"] == staging.SOURCE else None
        if live is not None:
            samples = staging_samples(c, experiment_id)
        else:
            samples = [dict(row) for row in c.execute(
                "SELECT * FROM samples WHERE experiment_id=? ORDER BY id", (experiment_id,),
            )]
        events = c.execute("SELECT * FROM scaling_events WHERE experiment_id=? ORDER BY id", (experiment_id,)).fetchall()
    item = experiment_response(exp, include_config=True)
    if live is not None:
        item["staging"] = live
    item["samples"] = samples
    item["scaling_events"] = [rowdict(x) for x in events]
    return item


def workload_rate(kind: str, pct: float) -> float:
    if kind == "STEADY": return 80
    if kind == "SPIKE": return 35 if pct < SPIKE_START else (220 if pct < SPIKE_END else 55)
    if kind == "WAVE": return 45 + 125 * (0.5 + 0.5 * math.sin(pct * 6.283 * 2))
    return 25 + 175 * pct


def choose_replicas(strategy: str, cpu: float | None, latency: float | None, rates: list[float], current: int,
                    mn: int, mx: int, target_cpu: float, target_latency: float) -> tuple[int, str]:
    """Deterministic policies so identical workload inputs are comparable."""
    if strategy in {"CPU", "TREND"} and cpu is None:
        return current, "Waiting for workload CPU telemetry"
    if strategy == "CPU":
        desired = round(current * cpu / max(target_cpu, 1))
        reason = f"CPU {cpu:.1f}% versus target {target_cpu:.1f}%"
        if cpu > target_cpu * 1.08: desired = max(current + 1, desired)
        elif cpu < target_cpu * .55: desired = min(current - 1, desired)
    elif strategy == "TREND":
        rising = len(rates) >= 3 and rates[-1] > rates[-2] > rates[-3]
        falling = len(rates) >= 3 and rates[-1] < rates[-2] < rates[-3]
        desired = current + (1 if rising and cpu > target_cpu * .70 else -1 if falling and cpu < target_cpu * .85 else 0)
        reason = "Request-rate trend rising" if desired > current else "Request-rate trend falling" if desired < current else "Request-rate trend stable"
    else:
        if latency is None:
            return current, "Waiting for successful latency measurements"
        desired = current + (1 if latency > target_latency else -1 if latency < target_latency * .55 else 0)
        reason = f"Latency {latency:.1f} ms versus target {target_latency:.1f} ms"
    return max(mn, min(mx, desired)), reason

class Observation(NamedTuple):
    timestamp: str
    cpu: float
    memory: float
    latency: float
    rate: float


def sampling_plan(config: dict) -> tuple[int, float]:
    duration = config["duration_seconds"]
    steps = max(5, min(180, duration // config["sample_interval_seconds"]))
    return steps, duration / steps


def record_experiment_sample(
    experiment_id: str, strategy: str, observation: Observation,
    current: int, desired: int, reason: str, source: str,
) -> bool:
    scaling = None
    simulated = source == "SIMULATED_WORKLOAD"
    with db() as c:
        renew_experiment_lease(c, experiment_id)
        if desired != current:
            scaling = "SCALE_UP" if desired > current else "SCALE_DOWN"
            c.execute(
                "INSERT INTO scaling_events(experiment_id,timestamp,strategy,previous_replicas,"
                "desired_replicas,reason,source) VALUES(?,?,?,?,?,?,?)",
                (experiment_id, observation.timestamp, strategy, current, desired, reason, source),
            )
        note = "simulated workload" if simulated else "Prometheus trace replay"
        c.execute(
            "INSERT INTO samples(experiment_id,timestamp,cpu_percent,memory_percent,latency_ms,"
            "request_rate,replicas,scaling_event,source,note) VALUES(?,?,?,?,?,?,?,?,?,?)",
            (experiment_id, *observation, current if simulated else desired, scaling, source, f"{strategy} policy; {note}"),
        )
    return scaling is not None


def summarize_strategy(
    samples: list[tuple[Observation, int]], scaling_events: int, target_latency: float, source: str,
) -> dict:
    count = len(samples)
    latencies = sorted(observation.latency for observation, _ in samples)
    return {
        "samples": count,
        "avg_cpu_percent": round(sum(observation.cpu for observation, _ in samples) / count, 2),
        "avg_memory_percent": round(sum(observation.memory for observation, _ in samples) / count, 2),
        "avg_latency_ms": round(sum(latencies) / count, 2),
        "p95_latency_ms": round(latencies[math.ceil(count * 0.95) - 1], 2),
        "avg_replicas": round(sum(replicas for _, replicas in samples) / count, 2),
        "max_replicas_used": max(replicas for _, replicas in samples),
        "scaling_events": scaling_events,
        "target_latency_ms": target_latency,
        "source": source,
    }


def simulation_score(result: dict) -> float:
    penalty = (
        35 * result["avg_latency_ms"] / result["target_latency_ms"]
        + 0.25 * result["avg_cpu_percent"]
        + 0.7 * result["scaling_events"]
    )
    return round(max(0, min(100, 100 - penalty)), 2)


def spike_recovery_seconds(latencies: list[float], target: float, step_seconds: float) -> float | None:
    windows = len(latencies)
    breach = next((
        index for index, latency in enumerate(latencies)
        if SPIKE_START <= index / windows < SPIKE_END and latency > target
    ), None)
    if breach is None:
        return 0.0
    recovery = next((index for index in range(breach + 1, windows) if latencies[index] <= target), None)
    return round((recovery - breach) * step_seconds, 3) if recovery is not None else None


async def run_experiment(experiment_id: str, config: dict) -> dict:
    source = "SIMULATED_WORKLOAD"
    steps, step_seconds = sampling_plan(config)
    target = config["target_latency"]
    results = {}
    for strategy in (*config["strategies"], "BASELINE"):
        fixed = strategy == "BASELINE"
        replicas = config["baseline_replicas"] if fixed else config["min_replicas"]
        rates, samples = [], []
        scaling_events = 0
        for step in range(steps):
            phase = step / steps
            rate = workload_rate(config["workload"], phase)
            rates.append(rate)
            jitter = 3 * math.sin(step * 1.71)
            cpu = max(4, min(100, rate * 0.72 / replicas + jitter + 8))
            memory = max(12, min(96, 24 + replicas * 5 + rate * 0.05))
            latency = 26 + rate * 1.20 / replicas + max(0, cpu - 60) * 6
            if config["workload"] == "SPIKE" and SPIKE_START <= phase < SPIKE_END:
                latency += 20
            observation = Observation(now_iso(), round(cpu, 2), round(memory, 2), round(latency, 2), round(rate, 2))
            if fixed:
                desired, reason = replicas, "Fixed replica baseline"
            else:
                desired, reason = choose_replicas(
                    strategy, observation.cpu, observation.latency, rates, replicas,
                    config["min_replicas"], config["max_replicas"], config["target_cpu"], target,
                )
            scaling_events += record_experiment_sample(
                experiment_id, strategy, observation, replicas, desired, reason, source,
            )
            samples.append((observation, replicas))
            replicas = desired
            await asyncio.sleep(min(0.05, step_seconds / 100))

        result = summarize_strategy(samples, scaling_events, target, source)
        latencies = [observation.latency for observation, _ in samples]
        result["seconds_above_latency_target"] = round(sum(latency > target for latency in latencies) * step_seconds, 3)
        result["replica_minutes"] = round(sum(count for _, count in samples) * step_seconds / 60, 3)
        result["spike_recovery_seconds"] = (
            spike_recovery_seconds(latencies, target, step_seconds) if config["workload"] == "SPIKE" else None
        )
        result["evaluation_score"] = simulation_score(result)
        if fixed:
            result["fixed_replicas"] = config["baseline_replicas"]
        results[strategy] = result
    return results


async def collect_replay_trace(experiment_id: str, config: dict) -> tuple[list[Observation | None], dict]:
    steps, step_seconds = sampling_plan(config)
    observations = []
    missing_by_metric = dict.fromkeys(REPLAY_METRICS, 0)
    clock = asyncio.get_running_loop()
    started = clock.time()
    async with httpx.AsyncClient(timeout=PROMETHEUS_TIMEOUT) as client:
        for step in range(steps):
            if step:
                await asyncio.sleep(max(0, started + step * step_seconds - clock.time()))
            with db() as c:
                renew_experiment_lease(c, experiment_id)
            values = await read_prometheus_metrics(client, REPLAY_METRICS)
            missing = [key for key in REPLAY_METRICS if values[key] is None]
            for key in missing:
                missing_by_metric[key] += 1
            if missing:
                observations.append(None)
            else:
                observations.append(Observation(now_iso(), *(values[key] for key in REPLAY_METRICS)))
    valid = sum(observation is not None for observation in observations)
    coverage = valid / steps
    status = "INSUFFICIENT" if coverage < MIN_REPLAY_COVERAGE else "PARTIAL" if valid < steps else "COMPLETE"
    return observations, {
        "status": status, "attempted_windows": steps, "valid_windows": valid,
        "coverage": round(coverage, 4), "minimum_coverage": MIN_REPLAY_COVERAGE,
        "missing_by_metric": missing_by_metric,
    }


async def run_experiment_real(experiment_id: str, config: dict) -> tuple[dict | None, dict]:
    """Replay each policy against the same observations and record its recommendations."""
    observations, quality = await collect_replay_trace(experiment_id, config)
    if quality["status"] == "INSUFFICIENT":
        return None, quality
    source = "PROMETHEUS"
    results = {}
    for strategy in config["strategies"]:
        replicas = config["min_replicas"]
        rates, samples = [], []
        scaling_events = 0
        for observation in observations:
            if observation is None:
                rates.clear()
                continue
            rates.append(observation.rate)
            desired, reason = choose_replicas(
                strategy, observation.cpu, observation.latency, rates, replicas,
                config["min_replicas"], config["max_replicas"], config["target_cpu"], config["target_latency"],
            )
            scaling_events += record_experiment_sample(
                experiment_id, strategy, observation, replicas, desired, reason, source,
            )
            replicas = desired
            samples.append((observation, replicas))
            await asyncio.sleep(0)
        results[strategy] = summarize_strategy(samples, scaling_events, config["target_latency"], source)
    return results, quality


def set_staging_progress(experiment_id: str, progress: dict, results: dict | None = None) -> None:
    with db() as c:
        renew_experiment_lease(c, experiment_id)
        c.execute("UPDATE staging_runs SET progress_json=? WHERE experiment_id=?", (json.dumps(progress), experiment_id))
        if results is not None:
            c.execute("UPDATE experiments SET results_json=? WHERE id=?", (json.dumps(results), experiment_id))


async def resize_staging(
    experiment_id: str, cluster: staging.KubernetesStaging, uid: str, current: int, desired: int,
) -> None:
    if current == desired:
        return
    with db() as c:
        renew_experiment_lease(c, experiment_id)
        c.execute("UPDATE staging_runs SET pending_replicas=? WHERE experiment_id=?", (desired, experiment_id))
    await staging.apply_scale(cluster, uid, (current,), desired)
    with db() as c:
        renew_experiment_lease(c, experiment_id)
        c.execute(
            "UPDATE staging_runs SET last_replicas=?, pending_replicas=NULL WHERE experiment_id=?",
            (desired, experiment_id),
        )


async def restore_staging_target(experiment_id: str) -> bool:
    now = time.time()
    lease = now + EXPERIMENT_LEASE_SECONDS
    with db() as c:
        row = c.execute(
            "UPDATE staging_runs SET restore_lease=? WHERE experiment_id=? AND restore_state='PENDING' "
            "AND (restore_lease IS NULL OR restore_lease<=?) RETURNING *",
            (lease, experiment_id, now),
        ).fetchone()
    if row is None:
        return False

    error = None
    status = "NOT_NEEDED"
    if row["original_replicas"] is not None:
        try:
            target = staging.StagingTarget.configured()
            if target is None or asdict(target) != json.loads(row["target_json"]):
                raise staging.StagingError("Restore the original staging target configuration to retry restoration.")
            original = row["original_replicas"]
            allowed = tuple(count for count in (original, row["last_replicas"], row["pending_replicas"]) if count is not None)
            cluster = staging.KubernetesStaging(target)
            try:
                await staging.apply_scale(cluster, row["deployment_uid"], allowed, original)
            finally:
                cluster.close()
            status = "RESTORED"
        except staging.TargetChanged as exc:
            status, error = "TARGET_CHANGED", str(exc)
        except staging.StagingError as exc:
            status, error = "PENDING", str(exc)
        except Exception:
            logger.exception("Could not restore the staging Deployment for %s", experiment_id)
            status, error = "PENDING", "Kubernetes restoration failed. Check cluster access and server logs."
    with db() as c:
        c.execute(
            "UPDATE staging_runs SET restore_state=?, restore_error=?, restore_lease=NULL "
            "WHERE experiment_id=? AND restore_lease=?",
            (status, error, experiment_id, lease),
        )
    return status in {"RESTORED", "NOT_NEEDED"}


async def recover_staging_runs() -> None:
    with db() as c:
        experiments = c.execute(
            "SELECT s.experiment_id FROM staging_runs s JOIN experiments e ON e.id=s.experiment_id "
            "WHERE s.restore_state='PENDING' AND e.status IN ('FAILED','INTERRUPTED','COMPLETED') "
            "AND (s.restore_lease IS NULL OR s.restore_lease<=?)",
            (time.time(),),
        ).fetchall()
    for row in experiments:
        await restore_staging_target(row["experiment_id"])


async def wait_staging_ready(
    experiment_id: str, cluster: staging.KubernetesStaging, uid: str, desired: int,
) -> None:
    clock = asyncio.get_running_loop()
    deadline = clock.time() + 60
    while clock.time() < deadline:
        with db() as c:
            renew_experiment_lease(c, experiment_id)
        snapshot = await asyncio.to_thread(cluster.snapshot, uid)
        if snapshot.requested != desired:
            raise staging.StagingError("The Deployment's requested replica count changed outside the benchmark.")
        if snapshot.settled:
            return
        await asyncio.sleep(1)
    raise staging.StagingError("The staging replicas did not become ready within 60 seconds.")


def save_staging_sample(experiment_id: str, sample: dict, progress: dict) -> None:
    with db() as c:
        renew_experiment_lease(c, experiment_id)
        c.execute(
            "INSERT INTO staging_samples(experiment_id,sample_json) VALUES(?,?)",
            (experiment_id, json.dumps(sample)),
        )
        c.execute("UPDATE staging_runs SET progress_json=? WHERE experiment_id=?", (json.dumps(progress), experiment_id))
        if sample["scale_applied"]:
            c.execute(
                "INSERT INTO scaling_events(experiment_id,timestamp,strategy,previous_replicas,"
                "desired_replicas,reason,source) VALUES(?,?,?,?,?,?,?)",
                (experiment_id, sample["timestamp"], sample["strategy"], sample["requested_replicas"],
                 sample["next_replicas"], sample["decision"], staging.SOURCE),
            )


async def run_staging_trial(
    experiment_id: str, config: dict, progress: dict, cluster: staging.KubernetesStaging,
    uid: str, requested: int, http: httpx.AsyncClient,
) -> tuple[dict, int]:
    options = config["staging"]
    strategy = progress["strategy"]
    windows = staging.traffic_windows(
        config["duration_seconds"], config["sample_interval_seconds"], options["base_rps"], options["peak_rps"],
    )
    clock = asyncio.get_running_loop()
    started = clock.time() + 0.05
    ends = started + config["duration_seconds"]
    traffic = [
        asyncio.create_task(staging.measure_window(http, cluster.target.url, started + window.offset, window))
        for window in windows
    ]
    last_change = float("-inf")
    rates, samples, latencies = [], [], []
    try:
        for index, (window, requests) in enumerate(zip(windows, traffic)):
            elapsed = window.offset + window.seconds
            await asyncio.sleep(max(0, started + elapsed - clock.time()))
            outcomes = await requests
            observed = await asyncio.to_thread(cluster.snapshot, uid, True)
            if observed.requested != requested:
                raise staging.StagingError("The Deployment's requested replica count changed outside the benchmark.")
            successful = [outcome.latency_ms for outcome in outcomes if outcome.latency_ms is not None]
            responses = sum(outcome.responded for outcome in outcomes)
            latencies.extend(successful)
            p95_latency = staging.p95(successful)
            rates.append(responses / window.seconds)
            desired = requested
            if strategy == "BASELINE":
                reason = "Fixed replica baseline"
            elif index == len(windows) - 1 or clock.time() >= ends:
                reason = "Trial measurement finished"
            elif not observed.settled:
                reason = "Waiting for requested replicas to become ready"
            elif clock.time() - last_change < options["cooldown_seconds"]:
                reason = "Scaling cooldown"
            else:
                desired, reason = choose_replicas(
                    strategy, observed.cpu_percent, p95_latency, rates, requested,
                    config["min_replicas"], config["max_replicas"], config["target_cpu"], config["target_latency"],
                )
            applied = desired != requested
            if applied:
                await resize_staging(experiment_id, cluster, uid, requested, desired)
                last_change = clock.time()
            sample = {
                "experiment_id": experiment_id, "timestamp": now_iso(), "source": staging.SOURCE,
                "strategy": strategy, "phase": window.phase, "elapsed_seconds": elapsed,
                "window_seconds": window.seconds, "offered_rps": window.rps, "request_rate": rates[-1],
                "requests": len(outcomes), "responses": responses, "failed_requests": len(outcomes) - len(successful),
                "error_rate_percent": round(100 * (len(outcomes) - len(successful)) / len(outcomes), 3),
                "p95_latency_ms": p95_latency, "cpu_percent": observed.cpu_percent,
                "requested_replicas": requested, "ready_replicas": observed.ready, "active_replicas": observed.active,
                "next_replicas": desired, "decision": reason, "scale_applied": applied,
            }
            samples.append(sample)
            save_staging_sample(experiment_id, sample, progress | {"elapsed_seconds": elapsed})
            requested = desired
    finally:
        for task in traffic:
            if not task.done():
                task.cancel()
        await asyncio.gather(*traffic, return_exceptions=True)
    result = staging.trial_summary(strategy, samples, latencies, config)
    if strategy == "BASELINE":
        result["fixed_replicas"] = config["baseline_replicas"]
    return result, requested


async def warmup_staging(http: httpx.AsyncClient, target: staging.StagingTarget, options: dict) -> None:
    clock = asyncio.get_running_loop()
    start = clock.time()
    window = staging.TrafficWindow(0, options["warmup_seconds"], options["base_rps"], "WARMUP")
    measured = await staging.measure_window(http, target.url, start, window)
    if not any(outcome.latency_ms is not None for outcome in measured):
        raise staging.StagingError("The staging Service returned no successful requests during warm-up.")
    await asyncio.sleep(max(0, start + window.seconds - clock.time()))


async def run_staging_benchmark(experiment_id: str, config: dict) -> tuple[dict, dict]:
    target = staging.StagingTarget(**config["staging_target"])
    strategies = (*config["strategies"], "BASELINE")
    progress = {"phase": "PREFLIGHT", "finished_trials": 0, "total_trials": len(strategies)}
    set_staging_progress(experiment_id, progress)
    cluster = staging.KubernetesStaging(target)
    try:
        original = await asyncio.to_thread(cluster.preflight, config["image_ref"])
        with db() as c:
            renew_experiment_lease(c, experiment_id)
            c.execute(
                "UPDATE staging_runs SET deployment_uid=?, original_replicas=?, last_replicas=? WHERE experiment_id=?",
                (original.uid, original.requested, original.requested, experiment_id),
            )
        requested = original.requested
        results = {}
        limits = httpx.Limits(max_connections=staging.MAX_RPS * (staging.HTTP_TIMEOUT + 1), max_keepalive_connections=0)
        async with httpx.AsyncClient(limits=limits, trust_env=False, follow_redirects=False) as http:
            for strategy in strategies:
                progress = {
                    "strategy": strategy, "phase": "RESETTING", "finished_trials": len(results),
                    "total_trials": len(strategies), "elapsed_seconds": 0,
                }
                set_staging_progress(experiment_id, progress)
                initial = config["baseline_replicas"] if strategy == "BASELINE" else config["min_replicas"]
                await resize_staging(experiment_id, cluster, original.uid, requested, initial)
                await wait_staging_ready(experiment_id, cluster, original.uid, initial)
                set_staging_progress(experiment_id, progress | {"phase": "WARMUP"})
                await warmup_staging(http, target, config["staging"])
                progress["phase"] = "MEASURING"
                set_staging_progress(experiment_id, progress)
                result, requested = await run_staging_trial(experiment_id, config, progress, cluster, original.uid, initial, http)
                results[strategy] = result
                set_staging_progress(
                    experiment_id, progress | {
                        "phase": "TRIAL_COMPLETE", "finished_trials": len(results),
                        "elapsed_seconds": config["duration_seconds"],
                    }, results,
                )
        set_staging_progress(experiment_id, {"phase": "RESTORING", "finished_trials": len(results), "total_trials": len(strategies)})
        coverage = {strategy: result["cpu_coverage"] for strategy, result in results.items()}
        incomplete = [strategy for strategy, result in results.items() if result["quality"] != "COMPLETE"]
        quality = {"status": "PARTIAL" if incomplete else "COMPLETE", "cpu_coverage": coverage, "insufficient_cpu": incomplete}
        return results, quality
    finally:
        cluster.close()


async def execute_experiment(experiment_id: str, config: dict) -> None:
    try:
        now = time.time()
        with db() as c:
            claimed = c.execute(
                "UPDATE experiments SET status='RUNNING', lease_until=? "
                "WHERE id=? AND status='QUEUED' AND lease_until>?",
                (now + EXPERIMENT_LEASE_SECONDS, experiment_id, now),
            ).rowcount
        if not claimed:
            return
        quality = None
        if config["mode"] == "TELEMETRY_REPLAY":
            results, quality = await run_experiment_real(experiment_id, config)
            if results is None:
                error = (
                    f"Prometheus returned {quality['valid_windows']} of {quality['attempted_windows']} complete "
                    f"windows. Replay requires {MIN_REPLAY_COVERAGE:.0%} coverage."
                )
                end_failed_experiment(experiment_id, "FAILED", error, quality)
                return
        elif config["mode"] == staging.MODE:
            try:
                results, quality = await run_staging_benchmark(experiment_id, config)
            finally:
                restored = await restore_staging_target(experiment_id)
            if not restored:
                raise staging.StagingError("The staging Deployment could not be restored. Check the restoration status.")
        else:
            results = await run_experiment(experiment_id, config)
        with db() as c:
            renew_experiment_lease(c, experiment_id)
            if config["mode"] == staging.MODE:
                progress = {"phase": "FINISHED", "finished_trials": len(results), "total_trials": len(results)}
                c.execute(
                    "UPDATE staging_runs SET progress_json=? WHERE experiment_id=?",
                    (json.dumps(progress), experiment_id),
                )
            c.execute(
                "UPDATE experiments SET status='COMPLETED', completed_at=?, results_json=?, "
                "lease_until=NULL, data_quality_json=? WHERE id=?",
                (now_iso(), json.dumps(results), json.dumps(quality) if quality is not None else None, experiment_id),
            )
    except asyncio.CancelledError:
        end_failed_experiment(experiment_id, "INTERRUPTED", INTERRUPTED_MESSAGE)
        raise
    except ExperimentInterrupted:
        end_failed_experiment(experiment_id, "INTERRUPTED", INTERRUPTED_MESSAGE)
    except staging.StagingError as exc:
        end_failed_experiment(experiment_id, "FAILED", str(exc))
    except Exception:
        logger.exception("Experiment %s failed", experiment_id)
        end_failed_experiment(experiment_id, "FAILED", "The experiment failed. Check server logs before retrying.")


def experiment_receipt(experiment_id: str, status: str, source: str, retry_of: str | None) -> dict:
    mode = experiment_mode(source)
    if mode == staging.MODE:
        return {
            "id": experiment_id, "status": status, "source": source, "mode": mode, "retry_of": retry_of,
            "message": "Live staging trials send real HTTP requests and stream measured results.",
            "stream_url": f"/api/v1/experiments/{experiment_id}/stream",
        }
    message = (
        "This run collects Prometheus observations and then replays each policy against the shared trace."
        if mode == "TELEMETRY_REPLAY" else
        "This run compares simulated scaling policies with a fixed replica baseline."
    )
    return {"id": experiment_id, "status": status, "source": source, "mode": mode, "message": message, "retry_of": retry_of}


def authorize_staging(token: str | None) -> staging.StagingTarget:
    try:
        target = staging.StagingTarget.configured()
    except staging.StagingError as exc:
        raise HTTPException(503, str(exc)) from None
    expected = os.getenv("STAGING_OPERATOR_TOKEN", "").encode("utf-8")
    if target is None or len(expected) < 32:
        raise HTTPException(503, "Configure a staging target and STAGING_OPERATOR_TOKEN to enable live benchmarks.")
    if token is None or not hmac.compare_digest(token.encode("utf-8"), expected):
        raise HTTPException(403, "A valid X-Staging-Token is required to start a live benchmark.")
    return target


def queue_experiment(
    payload: ExperimentCreate, *, retry_of: str | None = None, live_target: staging.StagingTarget | None = None,
) -> dict:
    cfg = payload.model_dump()
    cfg["strategies"] = list(dict.fromkeys(cfg["strategies"]))
    mode = cfg["mode"] or ("TELEMETRY_REPLAY" if PROMETHEUS_URL else "SIMULATION")
    cfg["mode"] = mode
    experiment_id = str(uuid.uuid4())
    with db() as c:
        c.execute("BEGIN IMMEDIATE")
        if retry_of is not None:
            retry = c.execute("SELECT id,status,source FROM experiments WHERE retry_of=?", (retry_of,)).fetchone()
            if retry is not None:
                return experiment_receipt(retry["id"], retry["status"], retry["source"], retry_of)
        if mode == "TELEMETRY_REPLAY":
            if not PROMETHEUS_URL:
                raise HTTPException(503, "Configure PROMETHEUS_URL to run a telemetry replay")
            if cfg["baseline_replicas"] is not None:
                raise HTTPException(422, "baseline_replicas is available for simulation and staging benchmarks")
            source = "PROMETHEUS"
        else:
            source = "SIMULATED_WORKLOAD"
            if cfg["baseline_replicas"] is None:
                cfg["baseline_replicas"] = max(cfg["min_replicas"], min(2, cfg["max_replicas"]))
        if mode == staging.MODE:
            if live_target is None:
                raise HTTPException(403, "Staging target authorization is required.")
            if c.execute(
                "SELECT 1 FROM staging_runs WHERE target_key=? AND restore_state='PENDING'", (live_target.key,),
            ).fetchone():
                raise HTTPException(409, "A live benchmark or restoration already owns this staging Deployment.")
            source = staging.SOURCE
            cfg["workload"] = "SPIKE"
            cfg["staging"] = cfg["staging"] or StagingOptions().model_dump()
            cfg["staging_target"] = asdict(live_target)
        if not c.execute("SELECT 1 FROM images WHERE image_ref=?", (cfg["image_ref"],)).fetchone():
            raise HTTPException(400, "Register the container image before creating an experiment")
        c.execute(
            "INSERT INTO experiments(id,name,image_ref,strategy,workload,duration_seconds,min_replicas,max_replicas,"
            "target_cpu,target_latency,status,source,created_at,config_json,retry_of,lease_until) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (experiment_id, cfg["name"], cfg["image_ref"], ",".join(cfg["strategies"]), cfg["workload"],
             cfg["duration_seconds"], cfg["min_replicas"], cfg["max_replicas"], cfg["target_cpu"], cfg["target_latency"],
             "QUEUED", source, now_iso(), json.dumps(cfg), retry_of, time.time() + EXPERIMENT_LEASE_SECONDS),
        )
        if live_target is not None:
            c.execute(
                "INSERT INTO staging_runs(experiment_id,target_key,target_json,progress_json) VALUES(?,?,?,?)",
                (experiment_id, live_target.key, json.dumps(cfg["staging_target"]),
                 json.dumps({"phase": "QUEUED", "finished_trials": 0, "total_trials": len(cfg["strategies"]) + 1})),
            )
    tasks = app.state.experiment_tasks
    task = asyncio.create_task(execute_experiment(experiment_id, cfg))
    tasks[experiment_id] = task
    task.add_done_callback(lambda _: tasks.pop(experiment_id, None))
    return experiment_receipt(experiment_id, "QUEUED", source, retry_of)


@app.post("/api/v1/experiments", status_code=202, dependencies=[Depends(get_current_user)])
async def create_experiment(payload: ExperimentCreate, staging_token: str | None = Header(default=None, alias="X-Staging-Token")):
    target = authorize_staging(staging_token) if payload.mode == staging.MODE else None
    return queue_experiment(payload, live_target=target)


@app.post("/api/v1/experiments/{experiment_id}/retry", status_code=202, dependencies=[Depends(get_current_user)])
async def retry_experiment(experiment_id: str, staging_token: str | None = Header(default=None, alias="X-Staging-Token")):
    with db() as c:
        previous = c.execute("SELECT * FROM experiments WHERE id=?", (experiment_id,)).fetchone()
    if previous is None:
        raise HTTPException(404, "Experiment not found")
    if previous["status"] not in ("FAILED", "INTERRUPTED"):
        raise HTTPException(409, "Only failed or interrupted experiments can be retried")
    if previous["config_json"] is not None:
        config = json.loads(previous["config_json"])
    else:
        config = {key: previous[key] for key in (
            "name", "image_ref", "workload", "duration_seconds", "min_replicas", "max_replicas", "target_cpu", "target_latency",
        )}
        config["max_replicas"] = max(config["max_replicas"], config["min_replicas"])
        config["strategies"] = previous["strategy"].split(",")
        config["mode"] = experiment_mode(previous["source"])
    target = None
    if previous["source"] == staging.SOURCE:
        target = authorize_staging(staging_token)
        if config.get("staging_target") != asdict(target):
            raise HTTPException(409, "Retry requires the original staging target configuration.")
    return queue_experiment(ExperimentCreate.model_validate(config), retry_of=experiment_id, live_target=target)


@app.post("/api/v1/metrics/ingest", status_code=201, dependencies=[Depends(get_current_user)])
def ingest_metric(payload: MetricIngest):
    if payload.experiment_id:
        with db() as c:
            if not c.execute("SELECT 1 FROM experiments WHERE id=?", (payload.experiment_id,)).fetchone():
                raise HTTPException(404, "Experiment not found")
    with db() as c:
        cur = c.execute("INSERT INTO samples(experiment_id,timestamp,cpu_percent,memory_percent,latency_ms,request_rate,replicas,source,note) VALUES(?,?,?,?,?,?,?,?,?)",
                         (payload.experiment_id, now_iso(), payload.cpu_percent, payload.memory_percent, payload.latency_ms, payload.request_rate, payload.replicas, payload.source, payload.note))
        return rowdict(c.execute("SELECT * FROM samples WHERE id=?", (cur.lastrowid,)).fetchone())

@app.get("/api/v1/metrics")
def list_metrics(experiment_id: Optional[str] = None, limit: int = Query(300, ge=1, le=2000)):
    with db() as c:
        if experiment_id:
            rows = c.execute("SELECT * FROM samples WHERE experiment_id=? ORDER BY id DESC LIMIT ?", (experiment_id, limit)).fetchall()
        else:
            rows = c.execute("SELECT * FROM samples ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
    return list(reversed([rowdict(r) for r in rows]))

@app.get("/api/v1/scaling-events")
def list_scaling_events(experiment_id: Optional[str] = None, limit: int = Query(100, ge=1, le=500)):
    with db() as c:
        if experiment_id:
            rows = c.execute("SELECT * FROM scaling_events WHERE experiment_id=? ORDER BY id DESC LIMIT ?", (experiment_id, limit)).fetchall()
        else:
            rows = c.execute("SELECT * FROM scaling_events ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
    return [rowdict(r) for r in rows]

def comparison_report(row: sqlite3.Row, live: dict | None = None) -> dict:
    mode = experiment_mode(row["source"])
    report = {
        "experiment_id": row["id"], "status": row["status"], "source": row["source"], "mode": mode,
        "error": row["error"], "data_quality": experiment_data_quality(row),
        "score_formula": None, "strategies": [], "baseline": None,
    }
    if row["status"] != "COMPLETED" and mode != staging.MODE:
        return report
    results, baseline = experiment_results(row)
    if mode == "SIMULATION":
        strategies = [{"strategy": name, **result, "evaluation_score": simulation_score(result)}
                      for name, result in results.items()]
        report["strategies"] = sorted(strategies, key=lambda result: result["evaluation_score"], reverse=True)
        report["score_formula"] = SCORE_FORMULA
        if baseline is not None:
            report["baseline"] = {"strategy": "BASELINE", **baseline}
    elif mode == staging.MODE:
        eligibility = {True: 0, False: 1, None: 2}
        report["strategies"] = sorted(
            ({"strategy": name, **result} for name, result in results.items()),
            key=lambda result: (eligibility[result["meets_targets"]], result["replica_minutes"]),
        )
        report["staging"] = live
        if baseline is not None:
            report["baseline"] = {"strategy": "BASELINE", **baseline}
        report["recommended_strategies"] = []
        if row["status"] == "COMPLETED":
            all_trials = results if baseline is None else results | {"BASELINE": baseline}
            eligible = {name: trial["replica_minutes"] for name, trial in all_trials.items() if trial["meets_targets"] is True}
            if eligible:
                best = min(eligible.values())
                report["recommended_strategies"] = [name for name, usage in eligible.items() if usage == best]
    else:
        report["strategies"] = [{"strategy": name, **result} for name, result in results.items()]
    return report


@app.get("/api/v1/experiments/{experiment_id}/compare")
def compare_experiment(experiment_id: str):
    with db() as c:
        row = c.execute("SELECT * FROM experiments WHERE id=?", (experiment_id,)).fetchone()
        if row is None:
            raise HTTPException(404, "Experiment not found")
        live = staging_info(c, experiment_id) if row["source"] == staging.SOURCE else None
    return comparison_report(row, live)


@app.get("/api/v1/experiments/{experiment_id}/stream", dependencies=[Depends(get_current_user)])
async def stream_staging_experiment(
    experiment_id: str, request: Request, last_event_id: int = Header(default=0, alias="Last-Event-ID", ge=0),
):
    with db() as c:
        run = c.execute("SELECT source FROM experiments WHERE id=?", (experiment_id,)).fetchone()
    if run is None:
        raise HTTPException(404, "Experiment not found")
    if run["source"] != staging.SOURCE:
        raise HTTPException(409, "This stream is for live staging benchmarks.")

    async def events():
        cursor = last_event_id
        previous = None
        heartbeat = time.monotonic()
        while not await request.is_disconnected():
            with db() as c:
                row = c.execute("SELECT * FROM experiments WHERE id=?", (experiment_id,)).fetchone()
                live = staging_info(c, experiment_id)
                samples = staging_samples(c, experiment_id, cursor, 100)
            for sample in samples:
                cursor = sample["id"]
                yield f"id: {cursor}\nevent: sample\ndata: {json.dumps(sample)}\n\n"
            report = comparison_report(row, live)
            if report != previous:
                yield f"event: report\ndata: {json.dumps(report)}\n\n"
                previous = report
            if len(samples) == 100:
                continue
            if row["status"] in {"COMPLETED", "FAILED", "INTERRUPTED"} and live["restoration"]["status"] != "PENDING":
                return
            if time.monotonic() - heartbeat >= 15:
                yield ": heartbeat\n\n"
                heartbeat = time.monotonic()
            await asyncio.sleep(0.5)

    return StreamingResponse(
        events(), media_type="text/event-stream", headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.get("/api/v1/stream")
async def stream():
    async def events():
        last_id = 0
        while True:
            with db() as c:
                rows = c.execute("SELECT * FROM samples WHERE id>? ORDER BY id LIMIT 100", (last_id,)).fetchall()
            for r in rows:
                item = rowdict(r); last_id = item["id"]
                yield f"data: {json.dumps(item)}\n\n"
            if not rows:
                yield f"event: heartbeat\ndata: {json.dumps({'timestamp': now_iso()})}\n\n"
            await asyncio.sleep(1)
    return StreamingResponse(events(), media_type="text/event-stream", headers={"Cache-Control":"no-cache", "X-Accel-Buffering":"no"})

@app.get("/api/v1/host/metrics")
def host_metrics():
    return {"timestamp": now_iso(), "source": "OBSERVED_HOST", "cpu_percent": psutil.cpu_percent(interval=.1), "memory_percent": psutil.virtual_memory().percent, "logical_cpus": psutil.cpu_count(), "memory_total_bytes": psutil.virtual_memory().total}


@app.get("/api/v1/infrastructure/status")
def infrastructure_status():
    """Live status: Docker containers, Prometheus health, Grafana health."""
    containers = []
    docker_connected = False
    try:
        dc = docker.DockerClient(base_url="unix:///var/run/docker.sock", timeout=5)
        dc.ping()
        for c in dc.containers.list():
            containers.append({
                "id": c.short_id,
                "name": c.name,
                "image": c.image.tags[0] if c.image.tags else c.image.short_id,
                "status": c.status,
                "health": c.attrs.get("State", {}).get("Health", {}).get("Status", "none"),
            })
        docker_connected = True
        dc.close()
    except Exception:
        pass

    prom_healthy = False
    if PROMETHEUS_URL:
        try:
            with httpx.Client(timeout=2.0) as hc:
                prom_healthy = hc.get(f"{PROMETHEUS_URL}/-/healthy").status_code == 200
        except Exception:
            pass

    grafana_internal = os.getenv("GRAFANA_URL", "http://grafana:3000")
    grafana_external = os.getenv("GRAFANA_EXTERNAL_URL", "http://localhost:3000")
    grafana_healthy = False
    try:
        with httpx.Client(timeout=2.0) as hc:
            grafana_healthy = hc.get(f"{grafana_internal}/api/health").status_code == 200
    except Exception:
        pass

    return {
        "timestamp": now_iso(),
        "docker":     {"connected": docker_connected, "containers": containers, "count": len(containers)},
        "prometheus": {"configured": bool(PROMETHEUS_URL), "healthy": prom_healthy, "url": PROMETHEUS_URL or None},
        "grafana":    {"healthy": grafana_healthy, "external_url": grafana_external},
        "kubernetes": {"connected": _get_workload_replicas()[1] in ("kubernetes", "kubectl"),
                       "source": _get_workload_replicas()[1]},
    }


@app.get("/api/v1/infrastructure/recommendations", dependencies=[Depends(get_current_user)])
def infrastructure_recommendations():
    """Accurate HPA-style recommendations using full metric history for TREND detection."""
    if not PROMETHEUS_URL:
        raise HTTPException(503, "Prometheus not configured")
    keys = ("cpu_percent", "p95_latency_ms", "request_rate")
    values = query_prometheus_metrics(keys)
    missing = [key for key in keys if values[key] is None]
    if missing:
        raise HTTPException(503, detail={"message": "Incomplete Prometheus telemetry", "missing_metrics": missing})
    cpu, latency, rate = (values[key] for key in keys)

    # Real workload replica count (K8s → kubectl → Docker workload → 1)
    replicas, replica_source = _get_workload_replicas()

    # Build rate history from rolling window for accurate TREND detection
    with _history_lock:
        rate_history = list(_metric_history)
    rate_history.append(rate)  # include current reading

    recs: dict = {}
    for strategy in STRATEGIES:
        desired, reason = choose_replicas(
            strategy, cpu, latency, rate_history, replicas, 1, 20, 65.0, 250.0)
        recs[strategy] = {
            "recommended_replicas": desired,
            "reason": reason,
            "current": replicas,
        }

    # Emit scaling events when recommendations change
    new_events = 0
    with _advisor_lock:
        for strategy, rec in recs.items():
            desired = rec["recommended_replicas"]
            last_desired = _last_advisor_recs.get(strategy)
            if last_desired is not None and desired != last_desired:
                with db() as c:
                    c.execute(
                        "INSERT INTO scaling_events(experiment_id,timestamp,strategy,"
                        "previous_replicas,desired_replicas,reason,source) VALUES(?,?,?,?,?,?,?)",
                        (None, now_iso(), strategy, last_desired, desired,
                         rec["reason"], "PROMETHEUS_ADVISOR"),
                    )
                new_events += 1
            _last_advisor_recs[strategy] = desired

    return {
        "timestamp": now_iso(),
        "replica_source": replica_source,
        "history_points": len(rate_history),
        "new_scaling_events": new_events,
        "current_metrics": {
            "cpu_percent":  round(cpu,     2),
            "latency_ms":   round(latency, 2),
            "request_rate": round(rate,    4),
            "containers":   replicas,
        },
        "recommendations": recs,
    }


PROMETHEUS_QUERIES = {
    "cpu_percent": '100 * sum(rate(container_cpu_usage_seconds_total{name!=""}[1m])) / scalar(machine_cpu_cores)',
    "memory_percent": '100 * sum(container_memory_working_set_bytes{name!=""}) / scalar(machine_memory_bytes)',
    "p95_latency_ms": 'histogram_quantile(0.95, sum(rate(http_request_duration_seconds_bucket[2m])) by (le)) * 1000',
    "request_rate": 'sum(rate(http_requests_total[1m]))',
    "running_containers": 'count(count by (name)(container_last_seen{name!=""}))',
    "network_rx_bytes": 'sum(rate(container_network_receive_bytes_total{name!=""}[1m]))',
    "network_tx_bytes": 'sum(rate(container_network_transmit_bytes_total{name!=""}[1m]))',
}

@app.get("/api/v1/prometheus/status")
def prometheus_status():
    return {"configured": bool(PROMETHEUS_URL), "base_url": PROMETHEUS_URL or None,
            "query_names": list(PROMETHEUS_QUERIES),
            "message": "Prometheus queries are enabled." if PROMETHEUS_URL else "Set PROMETHEUS_URL to enable live Prometheus queries. Metric names depend on your exporter/instrumentation."}

@app.get("/api/v1/prometheus/snapshot")
def prometheus_snapshot():
    """Query common Prometheus/node-exporter/Kubernetes/application metrics.

    Missing metric families are returned as null rather than fabricated. The queries are
    conventions, not universal names; adapt them to the metrics your exporters expose.
    """
    if not PROMETHEUS_URL:
        raise HTTPException(503, "Prometheus is not configured. Set PROMETHEUS_URL (for example http://localhost:9090) and restart the API.")
    values = query_prometheus_metrics(tuple(PROMETHEUS_QUERIES))
    missing = [key for key, value in values.items() if value is None]
    return {"timestamp": now_iso(), "source": "PROMETHEUS", "status": "ok" if not missing else "partial",
            "values": values, "missing_metrics": missing,
            "note": "Null values mean the configured query returned no usable sample. Verify exporter names and scrape targets; values are never filled with mock numbers."}
