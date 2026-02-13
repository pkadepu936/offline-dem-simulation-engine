from __future__ import annotations

from fastapi.testclient import TestClient

from dem_sim.web import create_app


def test_health_endpoint() -> None:
    client = TestClient(create_app())
    res = client.get("/health")
    assert res.status_code == 200
    assert res.json()["status"] == "ok"


def test_root_ui_served() -> None:
    client = TestClient(create_app())
    res = client.get("/")
    assert res.status_code == 200
    assert "DEM Blend Studio" in res.text


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


def test_optimize_endpoint() -> None:
    client = TestClient(create_app())
    payload = client.get("/api/sample").json()
    payload["target_params"] = {
        "moisture_pct": 4.5,
        "fine_extract_db_pct": 81.8,
    }
    payload["iterations"] = 5
    payload["seed"] = 7

    res = client.post("/api/optimize", json=payload)
    assert res.status_code == 200
    data = res.json()
    assert "recommended_discharge" in data
    assert data["objective_method"] == "normalized_weighted_l2_hybrid_search"
    assert len(data["top_candidates"]) >= 1
    assert data["best_run"]["total_discharged_mass_kg"] > 0
