from __future__ import annotations

import argparse
from io import StringIO
from pathlib import Path
from typing import Any

import pandas as pd
import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

from .reporting import validate_inputs_shape
from .sample_data import DISCHARGE_CSV, LAYERS_CSV, SILOS_CSV, SUPPLIERS_CSV
from .service import RunConfig, run_blend


class RunRequest(BaseModel):
    silos: list[dict[str, Any]] = Field(default_factory=list)
    layers: list[dict[str, Any]] = Field(default_factory=list)
    suppliers: list[dict[str, Any]] = Field(default_factory=list)
    discharge: list[dict[str, Any]] = Field(default_factory=list)
    config: dict[str, Any] = Field(default_factory=dict)


def _sample_payload() -> dict[str, Any]:
    return {
        "silos": pd.read_csv(StringIO(SILOS_CSV)).to_dict(orient="records"),
        "layers": pd.read_csv(StringIO(LAYERS_CSV)).to_dict(orient="records"),
        "suppliers": pd.read_csv(StringIO(SUPPLIERS_CSV)).to_dict(orient="records"),
        "discharge": pd.read_csv(StringIO(DISCHARGE_CSV)).to_dict(orient="records"),
        "config": {
            "rho_bulk_kg_m3": 610.0,
            "grain_diameter_m": 0.004,
            "beverloo_c": 0.58,
            "beverloo_k": 1.4,
            "gravity_m_s2": 9.81,
            "sigma_m": 0.12,
            "steps": 2000,
            "auto_adjust": True,
        },
    }


def create_app() -> FastAPI:
    app = FastAPI(title="DEM Simulation API", version="0.1.0")

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/api/sample")
    def sample() -> dict[str, Any]:
        return _sample_payload()

    @app.post("/api/validate")
    def validate(req: RunRequest) -> dict[str, Any]:
        inputs = {
            "silos": pd.DataFrame(req.silos),
            "layers": pd.DataFrame(req.layers),
            "suppliers": pd.DataFrame(req.suppliers),
            "discharge": pd.DataFrame(req.discharge),
        }
        errors = validate_inputs_shape(inputs)
        return {"valid": len(errors) == 0, "errors": errors}

    @app.post("/api/run")
    def run(req: RunRequest) -> dict[str, Any]:
        inputs = {
            "silos": pd.DataFrame(req.silos),
            "layers": pd.DataFrame(req.layers),
            "suppliers": pd.DataFrame(req.suppliers),
            "discharge": pd.DataFrame(req.discharge),
        }
        errors = validate_inputs_shape(inputs)
        if errors:
            raise HTTPException(status_code=422, detail=errors)

        cfg = RunConfig(**req.config)
        result = run_blend(inputs, cfg)
        return {
            "total_discharged_mass_kg": float(result["total_discharged_mass_kg"]),
            "total_blended_params": {
                k: float(v) for k, v in result["total_blended_params"].items()
            },
            "per_silo": {
                silo_id: {
                    "discharged_mass_kg": float(r["discharged_mass_kg"]),
                    "mass_flow_rate_kg_s": float(r["mass_flow_rate_kg_s"]),
                    "discharge_time_s": float(r["discharge_time_s"]),
                    "sigma_m": float(r["sigma_m"]),
                    "blended_params_per_silo": {
                        k: float(v) for k, v in r["blended_params_per_silo"].items()
                    },
                }
                for silo_id, r in result["per_silo"].items()
            },
        }

    @app.get("/", response_class=HTMLResponse)
    def index() -> str:
        return """<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>DEM Simulation UI</title>
  <style>
    :root { --bg:#f5f7fb; --card:#ffffff; --ink:#1f2937; --accent:#0f766e; --muted:#6b7280; }
    body { margin:0; font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, sans-serif; background:linear-gradient(160deg,#edf2ff,#f8fafc); color:var(--ink); }
    .wrap { max-width:1100px; margin:24px auto; padding:0 16px; }
    .card { background:var(--card); border:1px solid #e5e7eb; border-radius:12px; padding:16px; box-shadow:0 8px 24px rgba(17,24,39,0.05); }
    h1 { margin:0 0 12px; font-size:24px; }
    p { color:var(--muted); margin:0 0 12px; }
    textarea { width:100%; min-height:320px; font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size:12px; border:1px solid #d1d5db; border-radius:8px; padding:10px; }
    button { background:var(--accent); color:#fff; border:0; border-radius:8px; padding:10px 14px; cursor:pointer; margin-right:8px; }
    pre { white-space:pre-wrap; background:#0b1020; color:#d1fae5; padding:12px; border-radius:8px; min-height:160px; }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="card">
      <h1>DEM Simulation Web UI</h1>
      <p>Edit JSON payload, then validate or run simulation.</p>
      <div>
        <button onclick="loadSample()">Load Sample</button>
        <button onclick="validatePayload()">Validate</button>
        <button onclick="runSimulation()">Run Simulation</button>
      </div>
      <br />
      <textarea id="payload"></textarea>
      <h3>Response</h3>
      <pre id="out"></pre>
    </div>
  </div>
  <script>
    const payloadEl = document.getElementById('payload');
    const outEl = document.getElementById('out');
    function print(x) { outEl.textContent = typeof x === 'string' ? x : JSON.stringify(x, null, 2); }
    async function loadSample() {
      const r = await fetch('/api/sample');
      payloadEl.value = JSON.stringify(await r.json(), null, 2);
      print('Sample loaded.');
    }
    async function validatePayload() {
      try {
        const payload = JSON.parse(payloadEl.value);
        const r = await fetch('/api/validate', { method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(payload) });
        print(await r.json());
      } catch (e) { print(String(e)); }
    }
    async function runSimulation() {
      try {
        const payload = JSON.parse(payloadEl.value);
        const r = await fetch('/api/run', { method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(payload) });
        print(await r.json());
      } catch (e) { print(String(e)); }
    }
    loadSample();
  </script>
</body>
</html>"""

    return app


def run() -> None:
    parser = argparse.ArgumentParser(description="Run DEM simulation FastAPI server.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--reload", action="store_true", default=False)
    args = parser.parse_args()

    uvicorn.run(
        "dem_sim.web:create_app",
        host=args.host,
        port=args.port,
        reload=args.reload,
        factory=True,
    )


if __name__ == "__main__":
    run()
