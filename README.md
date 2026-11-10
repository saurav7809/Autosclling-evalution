# Autoscaling Evaluation

This FastAPI service compares CPU, request-trend, and latency policies for containerized workloads. A live staging benchmark applies each policy to a Kubernetes Deployment and streams measurements from real HTTP requests. Simulation compares policies under a model, and Prometheus replay shows the decisions each policy would make from the same observed telemetry.

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

## Run a live Kubernetes benchmark

Use a staging Kubernetes context with Metrics Server and a default StorageClass for the evaluator's SQLite volume. The [staging manifest](deploy/staging.yaml) sets up a CPU-workload demo app, an internal Service, and a single evaluator with permission to scale that Deployment. The target must carry the **autoscaling-evaluation/staging=true** label on both the Deployment and its matching ClusterIP Service. A HorizontalPodAutoscaler on the target blocks the benchmark.

Build the evaluator image and make it available to the staging cluster. For a kind cluster, load the local image with the second command. For a registry-backed cluster, publish the image to the registry and set the evaluator image in the manifest.

~~~sh
docker build -t autoscaling-evaluation:staging backend
kind load docker-image autoscaling-evaluation:staging --name YOUR_STAGING_CLUSTER
~~~

Create the namespace and the evaluator Secret once in the staging context. Generate distinct random values for the two keys.

~~~sh
kubectl config current-context
kubectl create namespace autoscaling-staging
export JWT_SECRET="$(python -c 'import secrets; print(secrets.token_urlsafe(32))')"
export STAGING_OPERATOR_TOKEN="$(python -c 'import secrets; print(secrets.token_urlsafe(32))')"
kubectl -n autoscaling-staging create secret generic autoscaling-evaluator \
  --from-literal="JWT_SECRET=$JWT_SECRET" \
  --from-literal="STAGING_OPERATOR_TOKEN=$STAGING_OPERATOR_TOKEN"
kubectl apply -f deploy/staging.yaml
kubectl -n autoscaling-staging rollout status deployment/benchmark-app
kubectl -n autoscaling-staging rollout status deployment/autoscaling-evaluator
kubectl -n autoscaling-staging port-forward deployment/autoscaling-evaluator 8080:8080
~~~

Sign in through the [interactive API docs](http://localhost:8080/docs) and register **registry.k8s.io/hpa-example** as the image reference. Submit the following body to **POST /api/v1/experiments** with the bearer token and **X-Staging-Token** set to the staging operator token.

~~~json
{
  "name": "Live staging spike",
  "image_ref": "registry.k8s.io/hpa-example",
  "mode": "STAGING_BENCHMARK",
  "workload": "SPIKE",
  "duration_seconds": 60,
  "sample_interval_seconds": 5,
  "min_replicas": 1,
  "max_replicas": 4,
  "baseline_replicas": 2,
  "target_latency": 250,
  "staging": {
    "base_rps": 2,
    "peak_rps": 10,
    "warmup_seconds": 20,
    "cooldown_seconds": 10,
    "target_error_percent": 1
  }
}
~~~

The evaluator runs each policy and the fixed-replica baseline sequentially. Each trial sets its starting replica count, waits for ready replicas, warms up, and sends the same paced spike to the Service. The live mode supports up to 20 requests per second, eight replicas, and 120 seconds of measured traffic per trial. For a different staging workload, configure **STAGING_NAMESPACE**, **STAGING_DEPLOYMENT**, **STAGING_SERVICE**, and, if needed, **STAGING_SERVICE_PORT** and **STAGING_REQUEST_PATH**. Update the Role to match and run a single evaluator per target with durable storage. The image reference in the request must match the Deployment.

Follow the experiment with **GET /api/v1/experiments/{id}/stream** using the bearer token. The stream emits **sample** events with measured p95 latency, HTTP failures, CPU, and requested, ready, and active replica counts. **report** events show the current phase, finished trials, and restoration status. Reconnect with **Last-Event-ID** to resume the samples.

~~~sh
curl -N -H "Authorization: Bearer $ACCESS_TOKEN" \
  "http://localhost:8080/api/v1/experiments/$EXPERIMENT_ID/stream"
~~~

**GET /api/v1/experiments/{id}/compare** updates as trials finish. The final report identifies the policies or baseline that met both latency and error targets with the lowest replica-minutes. Latency is measured from successful requests, and errors include unsuccessful responses and timeouts. The trend policy uses the rate of observed HTTP responses. Replica-minutes are estimated from sampled active replicas. CPU and trend trials need fresh workload CPU metrics in at least 80% of windows to qualify for a recommendation. A missing measurement is represented by null.

The evaluator saves the original desired replica count before making a scale change. It restores that count after the run or resumes restoration from SQLite after a restart. A pending restoration keeps the target reserved. If another controller or operator has changed the target, resolve that conflict and restore the original count shown in the report. Retry a failed live run with the original target configuration and the staging operator token.

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
