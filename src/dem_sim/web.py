from __future__ import annotations

import argparse
import random
from io import StringIO
from math import isnan
from pathlib import Path
from typing import Any

import pandas as pd
import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
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


class OptimizeRequest(RunRequest):
    target_params: dict[str, float] = Field(default_factory=dict)
    iterations: int = 120
    seed: int = 42


def _records_json_safe(df: pd.DataFrame) -> list[dict[str, Any]]:
    records = df.to_dict(orient="records")
    out: list[dict[str, Any]] = []
    for row in records:
        cleaned: dict[str, Any] = {}
        for key, value in row.items():
            if isinstance(value, float) and isnan(value):
                cleaned[key] = None
            else:
                cleaned[key] = value
        out.append(cleaned)
    return out


def _sample_payload() -> dict[str, Any]:
    return {
        "silos": _records_json_safe(pd.read_csv(StringIO(SILOS_CSV))),
        "layers": _records_json_safe(pd.read_csv(StringIO(LAYERS_CSV))),
        "suppliers": _records_json_safe(pd.read_csv(StringIO(SUPPLIERS_CSV))),
        "discharge": _records_json_safe(pd.read_csv(StringIO(DISCHARGE_CSV))),
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


def _result_to_api_payload(result: dict[str, Any]) -> dict[str, Any]:
    return {
        "total_discharged_mass_kg": float(result["total_discharged_mass_kg"]),
        "total_remaining_mass_kg": float(result["total_remaining_mass_kg"]),
        "total_blended_params": {
            k: float(v) for k, v in result["total_blended_params"].items()
        },
        "silo_state_ledger": result["df_silo_state_ledger"].to_dict(orient="records"),
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


DEFAULT_PARAM_RANGES = {
    "moisture_pct": 5.0 - 0.0,
    "fine_extract_db_pct": 83.0 - 81.0,
    "wort_pH": 6.0 - 5.8,
    "diastatic_power_WK": 360.0 - 300.0,
    "total_protein_pct": 11.2 - 10.2,
    "wort_colour_EBC": 4.7 - 4.3,
}


def _score_blend(
    actual: dict[str, float], target: dict[str, float], param_ranges: dict[str, float]
) -> float:
    if not target:
        return float("inf")
    score = 0.0
    for key, t in target.items():
        a = float(actual.get(key, 0.0))
        scale = float(param_ranges.get(key, 1.0))
        if scale <= 0:
            scale = 1.0
        score += ((a - float(t)) / scale) ** 2
    return score


def create_app() -> FastAPI:
    app = FastAPI(title="DEM Simulation API", version="0.1.0")
    ui_dir = Path(__file__).resolve().parent / "ui"
    app.mount("/ui", StaticFiles(directory=ui_dir), name="ui")

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
        return _result_to_api_payload(result)

    @app.post("/api/optimize")
    def optimize(req: OptimizeRequest) -> dict[str, Any]:
        inputs = {
            "silos": pd.DataFrame(req.silos),
            "layers": pd.DataFrame(req.layers),
            "suppliers": pd.DataFrame(req.suppliers),
            "discharge": pd.DataFrame(req.discharge),
        }
        errors = validate_inputs_shape(inputs)
        if errors:
            raise HTTPException(status_code=422, detail=errors)
        if not req.target_params:
            raise HTTPException(status_code=422, detail="target_params must be provided.")

        cfg = RunConfig(**req.config)
        silos_df = inputs["silos"].copy()
        rng = random.Random(req.seed)
        best_score = float("inf")
        best_result: dict[str, Any] | None = None
        best_discharge: list[dict[str, Any]] = []
        top_candidates: list[dict[str, Any]] = []

        for _ in range(max(1, req.iterations)):
            candidate_rows = []
            for silo_id in silos_df["silo_id"].astype(str):
                candidate_rows.append(
                    {
                        "silo_id": silo_id,
                        "discharge_mass_kg": None,
                        "discharge_fraction": round(rng.uniform(0.2, 0.8), 4),
                    }
                )
            candidate_inputs = dict(inputs)
            candidate_inputs["discharge"] = pd.DataFrame(candidate_rows)
            result = run_blend(candidate_inputs, cfg)
            score = _score_blend(
                actual=result["total_blended_params"],
                target=req.target_params,
                param_ranges=DEFAULT_PARAM_RANGES,
            )
            candidate_record = {
                "objective_score": score,
                "recommended_discharge": candidate_rows,
                "blended_params": {
                    k: float(v) for k, v in result["total_blended_params"].items()
                },
                "total_discharged_mass_kg": float(result["total_discharged_mass_kg"]),
            }
            top_candidates.append(candidate_record)
            if score < best_score:
                best_score = score
                best_result = result
                best_discharge = candidate_rows

        if best_result is None:
            raise HTTPException(status_code=500, detail="Optimization failed.")

        top_candidates = sorted(top_candidates, key=lambda x: x["objective_score"])[:5]
        return {
            "objective_score": best_score,
            "recommended_discharge": best_discharge,
            "best_run": _result_to_api_payload(best_result),
            "target_params": req.target_params,
            "iterations": req.iterations,
            "objective_method": "normalized_weighted_l2",
            "param_ranges": DEFAULT_PARAM_RANGES,
            "top_candidates": top_candidates,
        }

    @app.get("/", include_in_schema=False)
    def index() -> FileResponse:
        return FileResponse(ui_dir / "index.html")

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
