# Autoscaling Evaluation

This FastAPI service compares CPU, request-trend, and latency policies for containerized workloads. Simulations compare each policy with a fixed-replica baseline. Prometheus replay shows the scaling decisions each policy would make from the same observed telemetry.

## Quickstart

Clone this repository and run the following from its root with Python 3.12.

### Windows PowerShell

~~~powershell
py -3.12 -m venv .venv
$projectPython = ".\.venv\Scripts\python.exe"
& $projectPython -m pip install -r backend/requirements.txt
$env:JWT_SECRET = & $projectPython -c "import secrets; print(secrets.token_urlsafe(32))"
$env:DATA_DIR = Join-Path (Get-Location) ".data"
& $projectPython -m uvicorn app:app --app-dir backend --port 8080
~~~

### macOS or Linux

~~~sh
python3.12 -m venv .venv
. .venv/bin/activate
python -m pip install -r backend/requirements.txt
export JWT_SECRET="$(python -c 'import secrets; print(secrets.token_urlsafe(32))')"
export DATA_DIR="$PWD/.data"
python -m uvicorn app:app --app-dir backend --port 8080
~~~

### Docker

Set a random JWT_SECRET of at least 32 bytes in your shell, then run these commands. The named volume stores the SQLite database.

~~~sh
docker build -t autoscaling-evaluation backend
docker run --rm -p 127.0.0.1:8080:8080 -e JWT_SECRET -v autoscaling-data:/data autoscaling-evaluation
~~~

Use the same signing secret across restarts if existing tokens should stay valid. Use the same data directory or volume to retain accounts and experiment history.

## Run a spike benchmark

1. Open the [interactive API docs](http://localhost:8080/docs).
2. Call **POST /api/v1/auth/register** with a username, email, display name, password, and **accepted_tos** set to true. Use **POST /api/v1/auth/login** on subsequent visits.
3. Copy **access_token** from the response. Select **Authorize** in the API docs and paste the token.
4. Call **POST /api/v1/images** with **{"image_ref":"nginx:alpine"}**. This registers an image reference for the experiment.
5. Call **POST /api/v1/experiments** with the following body.

   ~~~json
   {
     "name": "Traffic spike benchmark",
     "image_ref": "nginx:alpine",
     "mode": "SIMULATION",
     "workload": "SPIKE",
     "duration_seconds": 60,
     "target_latency": 250,
     "baseline_replicas": 2
   }
   ~~~

6. Copy the returned experiment ID. Check **GET /api/v1/experiments/{id}** for status, samples, and scaling events. Once the status is **COMPLETED**, open **GET /api/v1/experiments/{id}/compare** for the report.

Each simulated policy starts at the configured minimum. All policies and the baseline see the same traffic and modeled noise. The fixed baseline defaults to two replicas within the selected bounds. The simulation advances through equally spaced windows over the configured duration, with replica changes taking effect in the next window.

| Report field | Meaning |
| --- | --- |
| **seconds_above_latency_target** | Simulated time in windows where latency exceeded the target |
| **replica_minutes** | Total capacity used. Two replicas serving for one minute use two replica-minutes |
| **spike_recovery_seconds** | Time from the first target breach during the spike to the first window back within target. Zero means no breach during the spike. Null means no recovery was observed or the workload had no spike |
| **scaling_events** | Number of scale-up or scale-down decisions |

Among policies that met the latency target throughout the run, compare replica-minutes to find the lowest capacity use. The separate **baseline** row shows how fixed capacity performed. The simulation score is a heuristic based on average latency, average CPU, and scaling events.

## Replay Prometheus telemetry

Set **PROMETHEUS_URL** to a reachable Prometheus base URL before starting the API. Pass **-e PROMETHEUS_URL** when running with Docker. Check **GET /api/v1/prometheus/snapshot** for the available metrics. Replay needs CPU, memory, p95 response latency, and request rate for at least 80% of sampled windows.

Use **mode: TELEMETRY_REPLAY** when creating an experiment, or **mode: SIMULATION** to simulate even with Prometheus configured. Requests that omit **mode** use replay when Prometheus is configured and simulation otherwise.

Replay reports include data coverage and the missing-window count for each metric. Insufficient coverage produces a **FAILED** run with a reason. Replay results report observed metrics and recommended replica counts, with a null evaluation score. They use the same observations for every policy. Pre-upgrade replay reports have **UNVERIFIED** coverage.

## Recover interrupted or failed runs

Check **error** in the experiment details. On graceful shutdown the API marks unfinished runs as **INTERRUPTED**. After a hard stop, an expired worker lease is detected within about 50 seconds while the API is running. Existing SQLite databases are upgraded on startup.

After resolving the cause, call **POST /api/v1/experiments/{id}/retry** with a bearer token. The API reuses the stored settings and returns a new run ID. Repeating the request returns the same retry attempt. To retry again after another failure, use that attempt's ID. Legacy records use the default sample interval because the previous schema stored the other inputs only.

## Tests

With the runtime dependencies installed, install pytest and run the suite from the repository root.

~~~sh
python -m pip install pytest
python -m pytest backend
~~~

The suite uses temporary SQLite databases and mocked Prometheus responses.
