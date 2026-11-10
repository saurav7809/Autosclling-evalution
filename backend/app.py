from __future__ import annotations
import asyncio, json, os, re, shutil, sqlite3, subprocess, time, uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Literal, Optional

import psutil
import docker
from fastapi import FastAPI, HTTPException, Header, Query
from prometheus_fastapi_instrumentator import Instrumentator
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field, field_validator
from jose import jwt as jose_jwt, JWTError
from passlib.context import CryptContext

DATA_DIR = Path(os.getenv("DATA_DIR", "/data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = DATA_DIR / "autoscaling.db"
PROMETHEUS_URL = os.getenv("PROMETHEUS_URL", "").rstrip("/")
STRATEGIES = ("CPU", "TREND", "LATENCY")

JWT_SECRET = os.getenv("JWT_SECRET", "aegiscloud-dev-secret-change-in-prod")
JWT_ALGORITHM = "HS256"
JWT_EXPIRY_HOURS = 24

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

import math as _math, threading as _threading
from collections import deque as _deque

# Rolling metric history (updated by prometheus_scrape_loop, consumed by advisor)
_metric_history: _deque = _deque(maxlen=15)  # ~75 s at 5 s interval
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


def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
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
        """)


async def prometheus_scrape_loop():
    """Background task: scrape Prometheus every 5 s and persist samples with source=PROMETHEUS."""
    if not PROMETHEUS_URL:
        return
    import httpx
    while True:
        try:
            values: dict = {}
            with httpx.Client(timeout=4.0) as client:
                for key, query in PROMETHEUS_QUERIES.items():
                    try:
                        r = client.get(f"{PROMETHEUS_URL}/api/v1/query", params={"query": query})
                        r.raise_for_status()
                        result = r.json().get("data", {}).get("result", [])
                        nums = [
                            float(item["value"][1]) for item in result
                            if len(item.get("value", [])) > 1
                            and item["value"][1] not in ("NaN", "+Inf", "-Inf")
                        ]
                        values[key] = round(sum(nums) / len(nums), 3) if nums else None
                    except Exception:
                        values[key] = None
            cpu  = values.get("cpu_percent")
            mem  = values.get("memory_percent")
            lat  = values.get("p95_latency_ms")
            rate = values.get("request_rate")
            reps = values.get("running_containers")
            # Only persist when we have at least one real value
            if cpu is not None or mem is not None:
                # Feed rolling history for advisor TREND detection
                with _history_lock:
                    _metric_history.append({
                        "cpu":     round(cpu  or 0.0, 2),
                        "rate":    round(rate or 0.0, 4),
                        "latency": round(lat  or 0.0, 2),
                    })
                with db() as c:
                    c.execute(
                        "INSERT INTO samples(experiment_id,timestamp,cpu_percent,memory_percent,"
                        "latency_ms,request_rate,replicas,source,note) VALUES(?,?,?,?,?,?,?,?,?)",
                        (None, now_iso(),
                         round(cpu  or 0.0, 2),
                         round(mem  or 0.0, 2),
                         round(lat  or 0.0, 2),
                         round(rate or 0.0, 4),
                         int(reps   or 0),
                         "PROMETHEUS",
                         "Live Prometheus scrape"))
        except Exception:
            pass
        await asyncio.sleep(5)


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    if PROMETHEUS_URL:
        asyncio.create_task(prometheus_scrape_loop())
    yield

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

class ExperimentCreate(BaseModel):
    name: str = Field(min_length=3, max_length=100)
    image_ref: str = Field(min_length=3, max_length=255)
    strategies: list[Literal["CPU", "TREND", "LATENCY"]] = ["CPU", "TREND", "LATENCY"]
    workload: Literal["RAMP", "SPIKE", "STEADY", "WAVE"] = "RAMP"
    duration_seconds: int = Field(default=60, ge=15, le=3600)
    min_replicas: int = Field(default=1, ge=1, le=100)
    max_replicas: int = Field(default=8, ge=1, le=100)
    target_cpu: float = Field(default=65, ge=10, le=95)
    target_latency: float = Field(default=250, ge=10, le=10000)
    sample_interval_seconds: int = Field(default=2, ge=1, le=10)

    @field_validator("max_replicas")
    @classmethod
    def max_gte_min(cls, v, info):
        mn = info.data.get("min_replicas", 1)
        if v < mn:
            raise ValueError("max_replicas must be >= min_replicas")
        return v

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


def get_current_user(authorization: str = Header(None)) -> dict:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing or invalid Authorization header")
    token = authorization.removeprefix("Bearer ").strip()
    try:
        payload = jose_jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        user_id = payload.get("sub")
        if not user_id:
            raise HTTPException(status_code=401, detail="Invalid token")
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid or expired token")
    with db() as c:
        user = c.execute("SELECT * FROM users WHERE id=?", (user_id,)).fetchone()
    if not user:
        raise HTTPException(status_code=401, detail="User not found")
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
def auth_me(authorization: str = Header(None)):
    user = get_current_user(authorization)
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

@app.post("/api/v1/images", status_code=201)
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

@app.post("/api/v1/images/discover-docker")
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

@app.delete("/api/v1/images/{image_id}", status_code=204)
def delete_image(image_id: str):
    with db() as c:
        cur = c.execute("DELETE FROM images WHERE id=?", (image_id,))
        if not cur.rowcount:
            raise HTTPException(404, "Image registration not found")

@app.get("/api/v1/experiments")
def list_experiments():
    with db() as c:
        rows = c.execute("SELECT id,name,image_ref,strategy,workload,duration_seconds,min_replicas,max_replicas,target_cpu,target_latency,status,source,created_at,completed_at,results_json FROM experiments ORDER BY created_at DESC").fetchall()
    result = []
    for r in rows:
        item = rowdict(r)
        if item.get("results_json"):
            item["results"] = json.loads(item.pop("results_json"))
        else:
            item.pop("results_json", None)
        result.append(item)
    return result

@app.get("/api/v1/experiments/{experiment_id}")
def get_experiment(experiment_id: str):
    with db() as c:
        exp = c.execute("SELECT * FROM experiments WHERE id=?", (experiment_id,)).fetchone()
        if not exp: raise HTTPException(404, "Experiment not found")
        samples = c.execute("SELECT * FROM samples WHERE experiment_id=? ORDER BY id", (experiment_id,)).fetchall()
        events = c.execute("SELECT * FROM scaling_events WHERE experiment_id=? ORDER BY id", (experiment_id,)).fetchall()
    item = rowdict(exp)
    item["results"] = json.loads(item["results_json"]) if item["results_json"] else None
    item.pop("results_json", None)
    item["samples"] = [rowdict(x) for x in samples]
    item["scaling_events"] = [rowdict(x) for x in events]
    return item


def workload_rate(kind: str, pct: float) -> float:
    if kind == "STEADY": return 80
    if kind == "SPIKE": return 35 if pct < .25 else (220 if pct < .65 else 55)
    if kind == "WAVE": return 45 + 125 * (0.5 + 0.5 * __import__('math').sin(pct * 6.283 * 2))
    return 25 + 175 * pct


def choose_replicas(strategy: str, cpu: float, latency: float, rates: list[float], current: int,
                    mn: int, mx: int, target_cpu: float, target_latency: float) -> tuple[int, str]:
    """Deterministic policies so identical workload inputs are comparable."""
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
        desired = current + (1 if latency > target_latency else -1 if latency < target_latency * .55 else 0)
        reason = f"Latency {latency:.1f} ms versus target {target_latency:.1f} ms"
    return max(mn, min(mx, desired)), reason

async def run_experiment(experiment_id: str, config: dict):
    """Simulated controlled workload: clearly marked, not a real cloud cluster."""
    with db() as c:
        c.execute("UPDATE experiments SET status='RUNNING' WHERE id=?", (experiment_id,))
    all_result = {}
    start = time.monotonic()
    for strategy in config["strategies"]:
        replica = config["min_replicas"]
        rates: list[float] = []
        cpu_vals, mem_vals, lat_vals, rep_vals = [], [], [], []
        previous = replica
        duration = config["duration_seconds"]
        interval = config["sample_interval_seconds"]
        steps = max(5, min(180, duration // interval))
        for step in range(steps):
            pct = step / max(steps - 1, 1)
            rate = workload_rate(config["workload"], pct)
            # Controlled model: demand per replica drives pressure; deterministic jitter preserves reproducibility.
            import math
            jitter = 3 * math.sin(step * 1.71 + len(strategy))
            cpu = max(4, min(100, rate * 0.72 / max(replica, 1) + jitter + 8))
            latency = max(15, (26 + rate * 1.20 / max(replica, 1) + max(0, cpu - 60) * 6 + (20 if config["workload"] == "SPIKE" and .25 < pct < .65 else 0)))
            memory = max(12, min(96, 24 + replica * 5 + rate * .05))
            desired, reason = choose_replicas(strategy, cpu, latency, rates, replica,
                                               config["min_replicas"], config["max_replicas"],
                                               config["target_cpu"], config["target_latency"])
            scaling = None
            if desired != replica:
                scaling = "SCALE_UP" if desired > replica else "SCALE_DOWN"
                with db() as c:
                    c.execute("INSERT INTO scaling_events(experiment_id,timestamp,strategy,previous_replicas,desired_replicas,reason,source) VALUES(?,?,?,?,?,?,?)",
                              (experiment_id, now_iso(), strategy, replica, desired, reason, "SIMULATED_WORKLOAD"))
                replica = desired
            sample = (experiment_id, now_iso(), round(cpu, 2), round(memory, 2), round(latency, 2), round(rate, 2), replica, scaling, "SIMULATED_WORKLOAD", f"{strategy} policy; controlled workload")
            with db() as c:
                c.execute("INSERT INTO samples(experiment_id,timestamp,cpu_percent,memory_percent,latency_ms,request_rate,replicas,scaling_event,source,note) VALUES(?,?,?,?,?,?,?,?,?,?)", sample)
            cpu_vals.append(cpu); mem_vals.append(memory); lat_vals.append(latency); rep_vals.append(replica); rates.append(rate)
            # The evaluation runs fast for a presentation; metric timestamps still reflect each actual sample.
            await asyncio.sleep(min(0.05, interval / 100))
        all_result[strategy] = {
            "samples": len(cpu_vals), "avg_cpu_percent": round(sum(cpu_vals)/len(cpu_vals),2),
            "avg_memory_percent": round(sum(mem_vals)/len(mem_vals),2),
            "avg_latency_ms": round(sum(lat_vals)/len(lat_vals),2),
            "p95_latency_ms": round(sorted(lat_vals)[min(len(lat_vals)-1, int(len(lat_vals)*.95))],2),
            "avg_replicas": round(sum(rep_vals)/len(rep_vals),2),
            "max_replicas_used": max(rep_vals), "scaling_events": sum(1 for a,b in zip(rep_vals, rep_vals[1:]) if a != b),
            "target_latency_ms": config["target_latency"], "source": "SIMULATED_WORKLOAD"
        }
        # Store the same transparent comparison heuristic exposed by the comparison endpoint,
        # so the list/detail UI can render scores without calculating or guessing them client-side.
        row = all_result[strategy]
        row["evaluation_score"] = round(max(0, min(100, 100 -
            row["avg_latency_ms"] / max(row["target_latency_ms"], 1) * 35 -
            row["avg_cpu_percent"] * 0.25 - row["scaling_events"] * 0.7)), 2)
    with db() as c:
        c.execute("UPDATE experiments SET status='COMPLETED', completed_at=?, results_json=? WHERE id=?",
                  (now_iso(), json.dumps(all_result), experiment_id))

async def run_experiment_real(experiment_id: str, config: dict):
    """Live experiment: polls Prometheus for real metrics, then replays each strategy
    against the identical observation sequence so comparisons are fair."""
    import httpx
    with db() as c:
        c.execute("UPDATE experiments SET status='RUNNING' WHERE id=?", (experiment_id,))

    duration = config["duration_seconds"]
    interval = config["sample_interval_seconds"]
    steps = max(5, min(180, duration // interval))

    # ── Phase 1: collect real Prometheus snapshots ──────────────────────────
    snapshots: list[dict] = []
    for _ in range(steps):
        snap: dict = {"timestamp": now_iso(), "cpu": 0.0, "memory": 0.0,
                      "latency": 0.0, "rate": 0.0, "containers": 0}
        try:
            with httpx.Client(timeout=3.0) as client:
                for key, query in PROMETHEUS_QUERIES.items():
                    try:
                        r = client.get(f"{PROMETHEUS_URL}/api/v1/query", params={"query": query})
                        r.raise_for_status()
                        result = r.json().get("data", {}).get("result", [])
                        nums = [float(item["value"][1]) for item in result
                                if len(item.get("value", [])) > 1
                                and item["value"][1] not in ("NaN", "+Inf", "-Inf")]
                        val = round(sum(nums) / len(nums), 3) if nums else None
                        if   key == "cpu_percent":        snap["cpu"]        = round(val or 0.0, 2)
                        elif key == "memory_percent":     snap["memory"]     = round(val or 0.0, 2)
                        elif key == "p95_latency_ms":     snap["latency"]    = round(val or 0.0, 2)
                        elif key == "request_rate":       snap["rate"]       = round(val or 0.0, 4)
                        elif key == "running_containers": snap["containers"] = int(val or 0)
                    except Exception:
                        pass
        except Exception:
            pass
        snapshots.append(snap)
        await asyncio.sleep(interval)

    # ── Phase 2: replay each strategy against the same observations ──────────
    all_result: dict = {}
    for strategy in config["strategies"]:
        replica = config["min_replicas"]
        rates: list[float] = []
        cpu_vals, mem_vals, lat_vals, rep_vals = [], [], [], []
        for snap in snapshots:
            cpu, memory, latency, rate = snap["cpu"], snap["memory"], snap["latency"], snap["rate"]
            rates.append(rate)
            desired, reason = choose_replicas(
                strategy, cpu, latency, rates, replica,
                config["min_replicas"], config["max_replicas"],
                config["target_cpu"], config["target_latency"])
            scaling = None
            if desired != replica:
                scaling = "SCALE_UP" if desired > replica else "SCALE_DOWN"
                with db() as c:
                    c.execute("INSERT INTO scaling_events(experiment_id,timestamp,strategy,"
                              "previous_replicas,desired_replicas,reason,source) VALUES(?,?,?,?,?,?,?)",
                              (experiment_id, snap["timestamp"], strategy, replica, desired, reason, "PROMETHEUS"))
                replica = desired
            with db() as c:
                c.execute("INSERT INTO samples(experiment_id,timestamp,cpu_percent,memory_percent,"
                          "latency_ms,request_rate,replicas,scaling_event,source,note) VALUES(?,?,?,?,?,?,?,?,?,?)",
                          (experiment_id, snap["timestamp"], cpu, memory, latency, rate,
                           replica, scaling, "PROMETHEUS", f"{strategy} policy; live Prometheus observation"))
            cpu_vals.append(cpu); mem_vals.append(memory); lat_vals.append(latency); rep_vals.append(replica)

        if cpu_vals:
            all_result[strategy] = {
                "samples": len(cpu_vals),
                "avg_cpu_percent":    round(sum(cpu_vals) / len(cpu_vals), 2),
                "avg_memory_percent": round(sum(mem_vals) / len(mem_vals), 2),
                "avg_latency_ms":     round(sum(lat_vals) / len(lat_vals), 2),
                "p95_latency_ms":     round(sorted(lat_vals)[min(len(lat_vals) - 1, int(len(lat_vals) * .95))], 2),
                "avg_replicas":       round(sum(rep_vals) / len(rep_vals), 2),
                "max_replicas_used":  max(rep_vals),
                "scaling_events":     sum(1 for a, b in zip(rep_vals, rep_vals[1:]) if a != b),
                "target_latency_ms":  config["target_latency"],
                "source": "PROMETHEUS",
            }
            row = all_result[strategy]
            row["evaluation_score"] = round(max(0, min(100, 100 -
                row["avg_latency_ms"] / max(row["target_latency_ms"], 1) * 35 -
                row["avg_cpu_percent"] * 0.25 - row["scaling_events"] * 0.7)), 2)

    with db() as c:
        c.execute("UPDATE experiments SET status='COMPLETED', completed_at=?, results_json=? WHERE id=?",
                  (now_iso(), json.dumps(all_result), experiment_id))


@app.post("/api/v1/experiments", status_code=202)
async def create_experiment(payload: ExperimentCreate):
    if not payload.strategies:
        raise HTTPException(422, "Select at least one scaling strategy")
    src = "PROMETHEUS" if PROMETHEUS_URL else "SIMULATED_WORKLOAD"
    with db() as c:
        registered = c.execute("SELECT id FROM images WHERE image_ref=?", (payload.image_ref,)).fetchone()
        if not registered:
            raise HTTPException(400, "Register the container image before creating an experiment")
        eid = str(uuid.uuid4())
        cfg = payload.model_dump()
        base_strategy = ",".join(cfg["strategies"])
        c.execute("INSERT INTO experiments(id,name,image_ref,strategy,workload,duration_seconds,min_replicas,max_replicas,target_cpu,target_latency,status,source,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                  (eid, cfg["name"], cfg["image_ref"], base_strategy, cfg["workload"], cfg["duration_seconds"],
                   cfg["min_replicas"], cfg["max_replicas"], cfg["target_cpu"], cfg["target_latency"],
                   "QUEUED", src, now_iso()))
    if PROMETHEUS_URL:
        asyncio.create_task(run_experiment_real(eid, cfg))
    else:
        asyncio.create_task(run_experiment(eid, cfg))
    msg = ("Experiment queued using live Prometheus metrics. Samples will stream in real time."
           if PROMETHEUS_URL else
           "Experiment queued. Samples and strategy results will appear as they are collected.")
    return {"id": eid, "status": "QUEUED", "source": src, "message": msg}

@app.post("/api/v1/metrics/ingest", status_code=201)
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

@app.get("/api/v1/experiments/{experiment_id}/compare")
def compare_experiment(experiment_id: str):
    with db() as c:
        row = c.execute("SELECT results_json,status FROM experiments WHERE id=?", (experiment_id,)).fetchone()
    if not row: raise HTTPException(404, "Experiment not found")
    if row["status"] != "COMPLETED": return {"status": row["status"], "strategies": []}
    results = json.loads(row["results_json"])
    def score(x):
        # Transparent objective score (0-100); not a measured benchmark. Lower latency/cpu/events is preferred.
        return round(max(0, min(100, 100 - x["avg_latency_ms"] / max(x["target_latency_ms"], 1) * 35 - x["avg_cpu_percent"] * .25 - x["scaling_events"] * .7)), 2)
    ranked = sorted([{"strategy": k, **v, "evaluation_score": score(v)} for k,v in results.items()], key=lambda x:x["evaluation_score"], reverse=True)
    derived_source = next(iter(results.values()), {}).get("source", "SIMULATED_WORKLOAD")
    return {"experiment_id": experiment_id, "status": row["status"], "source": derived_source, "score_formula": "100 - 35*(avg_latency/latency_target) - 0.25*avg_cpu - 0.7*scaling_events; clamped 0..100", "strategies": ranked}

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
    import httpx
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


@app.get("/api/v1/infrastructure/recommendations")
def infrastructure_recommendations():
    """Accurate HPA-style recommendations using full metric history for TREND detection."""
    import httpx
    if not PROMETHEUS_URL:
        raise HTTPException(503, "Prometheus not configured")
    values: dict = {}
    try:
        with httpx.Client(timeout=3.0) as client:
            for key, query in PROMETHEUS_QUERIES.items():
                try:
                    r = client.get(f"{PROMETHEUS_URL}/api/v1/query", params={"query": query})
                    r.raise_for_status()
                    result = r.json().get("data", {}).get("result", [])
                    nums = [float(item["value"][1]) for item in result
                            if len(item.get("value", [])) > 1
                            and item["value"][1] not in ("NaN", "+Inf", "-Inf")]
                    values[key] = round(sum(nums) / len(nums), 3) if nums else None
                except Exception:
                    values[key] = None
    except Exception as exc:
        raise HTTPException(502, f"Cannot reach Prometheus: {exc}")

    cpu     = values.get("cpu_percent")    or 0.0
    latency = values.get("p95_latency_ms") or 0.0
    rate    = values.get("request_rate")   or 0.0

    # Real workload replica count (K8s → kubectl → Docker workload → 1)
    replicas, replica_source = _get_workload_replicas()

    # Build rate history from rolling window for accurate TREND detection
    with _history_lock:
        history = list(_metric_history)
    rate_history = [h["rate"] for h in history]
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
    import httpx
    values = {}
    missing = []
    try:
        with httpx.Client(timeout=3.0) as client:
            for key, query in PROMETHEUS_QUERIES.items():
                try:
                    response = client.get(f"{PROMETHEUS_URL}/api/v1/query", params={"query": query})
                    response.raise_for_status()
                    body = response.json()
                    result = body.get("data", {}).get("result", [])
                    if not result:
                        values[key] = None
                        missing.append(key)
                        continue
                    # Aggregate queries should return one row; if not, average returned series.
                    nums = [float(item["value"][1]) for item in result if len(item.get("value", [])) > 1 and item["value"][1] not in ("NaN", "+Inf", "-Inf")]
                    values[key] = round(sum(nums) / len(nums), 3) if nums else None
                    if values[key] is None:
                        missing.append(key)
                except (httpx.HTTPError, ValueError, KeyError, TypeError):
                    values[key] = None
                    missing.append(key)
    except httpx.HTTPError as exc:
        raise HTTPException(502, f"Could not reach configured Prometheus endpoint: {exc}")
    return {"timestamp": now_iso(), "source": "PROMETHEUS", "status": "ok" if not missing else "partial",
            "values": values, "missing_metrics": missing,
            "note": "Null values mean the configured query returned no usable sample. Verify exporter names and scrape targets; values are never filled with mock numbers."}
