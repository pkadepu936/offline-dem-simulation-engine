from __future__ import annotations

from fastapi.testclient import TestClient

from dem_sim.web import create_app


def test_health_endpoint() -> None:
    client = TestClient(create_app())
    res = client.get("/health")
    assert res.status_code == 200
    assert res.json()["status"] == "ok"


def test_sample_and_run_endpoint() -> None:
    client = TestClient(create_app())
    sample = client.get("/api/sample")
    assert sample.status_code == 200
    payload = sample.json()

    validate = client.post("/api/validate", json=payload)
    assert validate.status_code == 200
    assert validate.json()["valid"] is True

    run = client.post("/api/run", json=payload)
    assert run.status_code == 200
    data = run.json()
    assert data["total_discharged_mass_kg"] > 0
