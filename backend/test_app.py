import os
import tempfile
from pathlib import Path
from fastapi.testclient import TestClient

_TMP = tempfile.TemporaryDirectory()
os.environ['DATA_DIR'] = _TMP.name
import app

client = TestClient(app.app)
client.__enter__()
app.init_db()

def test_health():
    r = client.get('/healthz')
    assert r.status_code == 200
    assert r.json()['status'] == 'ok'

def test_image_registration_and_duplicate():
    ref = f'test.local/service:{os.getpid()}'
    first = client.post('/api/v1/images', json={'image_ref': ref, 'display_name': 'Test service'})
    assert first.status_code == 201, first.text
    image_id = first.json()['id']
    assert first.json()['image_ref'] == ref
    second = client.post('/api/v1/images', json={'image_ref': ref})
    assert second.status_code == 201
    assert second.json()['id'] == image_id
    assert 'already registered' in second.json()['message']

def test_validation_of_image_ref():
    r = client.post('/api/v1/images', json={'image_ref': 'bad ref with spaces'})
    assert r.status_code == 422

def test_experiment_requires_registered_image():
    r = client.post('/api/v1/experiments', json={'name':'Missing image test','image_ref':'missing/service:latest'})
    assert r.status_code == 400

def test_metric_ingestion_round_trip():
    payload = {'cpu_percent': 42.5, 'memory_percent': 50, 'latency_ms': 120, 'request_rate': 65, 'replicas': 2, 'source':'MANUAL'}
    r = client.post('/api/v1/metrics/ingest', json=payload)
    assert r.status_code == 201, r.text
    assert r.json()['source'] == 'MANUAL'
    samples = client.get('/api/v1/metrics').json()
    assert any(s['cpu_percent'] == 42.5 for s in samples)

def test_completed_experiment_and_comparison():
    ref = f'test.local/eval:{os.getpid()}'
    assert client.post('/api/v1/images', json={'image_ref':ref}).status_code == 201
    response = client.post('/api/v1/experiments', json={'name':'Quick strategy test','image_ref':ref,'strategies':['CPU','TREND','LATENCY'],'workload':'STEADY','duration_seconds':15,'sample_interval_seconds':10})
    assert response.status_code == 202, response.text
    eid = response.json()['id']
    # Wait for the task to finish; no external services or Docker needed.
    import time
    deadline = time.time() + 5
    while time.time() < deadline:
        detail = client.get(f'/api/v1/experiments/{eid}').json()
        if detail['status'] == 'COMPLETED': break
        time.sleep(.05)
    assert detail['status'] == 'COMPLETED', detail['status']
    assert len(detail['samples']) >= 15
    comparison = client.get(f'/api/v1/experiments/{eid}/compare')
    assert comparison.status_code == 200
    assert {x['strategy'] for x in comparison.json()['strategies']} == {'CPU','TREND','LATENCY'}
    assert all(x['source'] == 'SIMULATED_WORKLOAD' for x in comparison.json()['strategies'])
    detail_results = detail['results']
    assert all('evaluation_score' in detail_results[s] for s in ['CPU', 'TREND', 'LATENCY'])
    assert all(0 <= detail_results[s]['evaluation_score'] <= 100 for s in ['CPU', 'TREND', 'LATENCY'])

def test_prometheus_status_is_explicit_when_unconfigured():
    r = client.get('/api/v1/prometheus/status')
    assert r.status_code == 200
    assert r.json()['configured'] is False
    r = client.get('/api/v1/prometheus/snapshot')
    assert r.status_code == 503
    assert 'PROMETHEUS_URL' in r.json()['detail']
