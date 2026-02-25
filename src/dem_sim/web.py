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
from fastapi.responses import FileResponse, Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from .reporting import validate_inputs_shape
from .sample_data import (
    DISCHARGE_CSV,
    LAYERS_CSV,
    LOT_SIZE_KG,
    SILO_CAPACITY_KG,
    SILO_COUNT,
    SILO_SLOT_COUNT,
    SILOS_CSV,
    SUPPLIERS_CSV,
)
from .service import RunConfig, run_blend
from .state import (
    add_stage,
    apply_discharge_to_state,
    get_state,
    reset_state,
    run_fill_only_simulation,
    set_state,
    summarize_state,
)
from .storage import get_storage

_STORAGE = get_storage()
_STORAGE_READY = False


def _ensure_storage_ready() -> None:
    global _STORAGE_READY
    if _STORAGE_READY:
        return
    try:
        _STORAGE.ensure_schema()
        _STORAGE_READY = True
    except Exception:
        return


def _persist_state_bundle(event_type: str, payload: dict[str, Any] | None = None) -> None:
    try:
        _ensure_storage_ready()
        state = get_state()
        summary = summarize_state()
        _STORAGE.write_snapshot(
            event_type=event_type,
            action=str(state.get("last_action", "")),
            state=state,
            summary=summary,
            payload=payload or {},
        )
        _STORAGE.write_stages(state.get("stages", []))
        _STORAGE.write_history(state.get("history", []))
    except Exception:
        return


def _persist_result(event_type: str, result: dict[str, Any], payload: dict[str, Any] | None = None) -> None:
    try:
        _ensure_storage_ready()
        _STORAGE.write_result(event_type=event_type, result=result, payload=payload or {})
    except Exception:
        return


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


class ProcessRunSimulationRequest(BaseModel):
    silos: list[dict[str, Any]] = Field(default_factory=list)
    layers: list[dict[str, Any]] = Field(default_factory=list)
    suppliers: list[dict[str, Any]] = Field(default_factory=list)
    incoming_queue: list[dict[str, Any]] = Field(default_factory=list)


class ProcessOptimizeRequest(BaseModel):
    target_params: dict[str, float] = Field(default_factory=dict)
    iterations: int = 120
    seed: int = 42
    config: dict[str, Any] = Field(default_factory=dict)


class ProcessApplyDischargeRequest(BaseModel):
    discharge: list[dict[str, Any]] = Field(default_factory=list)
    config: dict[str, Any] = Field(default_factory=dict)


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
    layers = _records_json_safe(pd.read_csv(StringIO(LAYERS_CSV)))
    placed_lot_ids = {str(x.get("lot_id", "")) for x in layers}
    queue: list[dict[str, Any]] = []
    # Keep deterministic queue extension for UI demonstration after initial 12 filled lots.
    for i in range(1013, 1021):
        lot_id = f"L{i}"
        if lot_id in placed_lot_ids:
            continue
        supplier = "BBM" if i % 3 == 1 else ("COFCO" if i % 3 == 2 else "Malteurop")
        queue.append({"lot_id": lot_id, "supplier": supplier, "mass_kg": float(LOT_SIZE_KG)})
    return {
        "silos": _records_json_safe(pd.read_csv(StringIO(SILOS_CSV))),
        "layers": layers,
        "suppliers": _records_json_safe(pd.read_csv(StringIO(SUPPLIERS_CSV))),
        "discharge": _records_json_safe(pd.read_csv(StringIO(DISCHARGE_CSV))),
        "assumptions": {
            "lot_size_kg": LOT_SIZE_KG,
            "silo_slot_count": SILO_SLOT_COUNT,
            "silo_count": SILO_COUNT,
            "silo_capacity_kg": SILO_CAPACITY_KG,
            "charging_policy": "strict_whole_lot_no_split_block_fill",
        },
        "incoming_queue": queue,
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


def _ensure_state_initialized() -> None:
    state = get_state()
    if state.get("silos") and state.get("layers"):
        return
    payload = _sample_payload()
    set_state(
        silos=payload["silos"],
        layers=payload["layers"],
        suppliers=payload["suppliers"],
        incoming_queue=payload.get("incoming_queue", []),
        action="bootstrap_sample_state",
        meta={"source": "sample_payload"},
    )


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
DISCHARGE_FRACTION_MIN = 0.2
DISCHARGE_FRACTION_MAX = 0.8
FIXED_DISCHARGE_TARGET_KG = 12000.0
FIXED_DISCHARGE_TOL_KG = 1e-3


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


def _clip_fraction(v: float) -> float:
    return max(DISCHARGE_FRACTION_MIN, min(DISCHARGE_FRACTION_MAX, float(v)))


def _candidate_rows_from_fractions(
    silo_ids: list[str], fractions: list[float]
) -> list[dict[str, Any]]:
    return [
        {
            "silo_id": silo_id,
            "discharge_mass_kg": None,
            "discharge_fraction": round(_clip_fraction(frac), 4),
        }
        for silo_id, frac in zip(silo_ids, fractions)
    ]


def _ensure_discharge_has_silo_ids(inputs: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    discharge_df = inputs.get("discharge", pd.DataFrame())
    if not discharge_df.empty and "silo_id" in discharge_df.columns:
        return inputs
    silos_df = inputs["silos"]
    inputs["discharge"] = pd.DataFrame({"silo_id": silos_df["silo_id"].astype(str).tolist()})
    return inputs


def _available_mass_by_silo(layers_df: pd.DataFrame) -> dict[str, float]:
    if layers_df.empty:
        return {}
    grouped = (
        layers_df.groupby(layers_df["silo_id"].astype(str))["segment_mass_kg"]
        .sum()
        .astype(float)
    )
    return {str(k): float(v) for k, v in grouped.to_dict().items()}


def _normalize_discharge_to_target(
    rows: list[dict[str, Any]],
    available_by_silo: dict[str, float],
    target_total_kg: float,
) -> list[dict[str, Any]]:
    available_total = float(sum(available_by_silo.values()))
    masses = {str(r["silo_id"]): max(0.0, float(r.get("discharge_mass_kg", 0.0))) for r in rows}
    if available_total + 1e-12 < target_total_kg:
        raise HTTPException(
            status_code=422,
            detail=(
                f"Insufficient available mass for fixed discharge target {target_total_kg:.3f} kg. "
                f"Currently available: {available_total:.3f} kg."
            ),
        )
    total = sum(masses.values())
    if total <= 1e-12:
        # deterministic equal split over silos with available mass
        active = [sid for sid, m in available_by_silo.items() if m > 1e-12]
        if not active:
            raise HTTPException(status_code=422, detail="No available mass in silos.")
        share = target_total_kg / len(active)
        masses = {sid: share if sid in active else 0.0 for sid in available_by_silo}
    else:
        scale = target_total_kg / total
        masses = {sid: m * scale for sid, m in masses.items()}

    # Respect silo caps and redistribute overflow iteratively.
    capped = {sid: min(masses.get(sid, 0.0), available_by_silo.get(sid, 0.0)) for sid in available_by_silo}
    deficit = target_total_kg - sum(capped.values())
    for _ in range(10):
        if deficit <= 1e-9:
            break
        room = {sid: available_by_silo[sid] - capped[sid] for sid in capped}
        total_room = sum(max(0.0, v) for v in room.values())
        if total_room <= 1e-12:
            break
        for sid in capped:
            r = max(0.0, room[sid])
            if r <= 0:
                continue
            add = deficit * (r / total_room)
            capped[sid] += min(add, r)
        deficit = target_total_kg - sum(capped.values())

    if abs(target_total_kg - sum(capped.values())) > 1e-6:
        raise HTTPException(
            status_code=422,
            detail=f"Could not satisfy exact fixed discharge target {target_total_kg:.3f} kg.",
        )

    out: list[dict[str, Any]] = []
    for sid in sorted(available_by_silo.keys()):
        avail = available_by_silo[sid]
        mass = capped.get(sid, 0.0)
        out.append(
            {
                "silo_id": sid,
                "discharge_mass_kg": round(mass, 6),
                "discharge_fraction": round((mass / avail) if avail > 1e-12 else 0.0, 6),
            }
        )
    return out


def create_app() -> FastAPI:
    app = FastAPI(title="DEM Simulation API", version="0.1.0")
    ui_dir = Path(__file__).resolve().parent / "ui"
    app.mount("/ui", StaticFiles(directory=ui_dir), name="ui")

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/favicon.ico", include_in_schema=False)
    def favicon() -> Response:
        # Avoid noisy 404 logs when browsers auto-request favicon.
        return Response(status_code=204)

    @app.get("/api/sample")
    def sample() -> dict[str, Any]:
        return _sample_payload()

    @app.get("/api/state")
    def state() -> dict[str, Any]:
        _ensure_state_initialized()
        return {"state": get_state(), "summary": summarize_state()}

    @app.post("/api/state/reset")
    def state_reset() -> dict[str, Any]:
        payload = _sample_payload()
        reset_state()
        set_state(
            silos=payload["silos"],
            layers=payload["layers"],
            suppliers=payload["suppliers"],
            incoming_queue=payload.get("incoming_queue", []),
            action="state_reset_to_sample",
            meta={},
        )
        out = {"state": get_state(), "summary": summarize_state()}
        _persist_state_bundle("state_reset", payload=out)
        return out

    @app.post("/api/process/run_simulation")
    def process_run_simulation(req: ProcessRunSimulationRequest) -> dict[str, Any]:
        _ensure_state_initialized()
        # If payload state is provided, use it as the current authoritative state for fill-only simulation.
        if req.silos and req.layers:
            set_state(
                silos=req.silos,
                layers=req.layers,
                suppliers=req.suppliers,
                incoming_queue=req.incoming_queue,
                action="run_simulation_payload_state",
                meta={"source": "run_simulation_request"},
            )
        out = run_fill_only_simulation()
        _persist_state_bundle("run_simulation_fill_only", payload={"request": req.model_dump(), "summary": out.get("summary", {})})
        return out

    @app.get("/api/process/stages")
    def process_stages() -> dict[str, Any]:
        _ensure_state_initialized()
        return {"stages": get_state().get("stages", [])}

    @app.post("/api/process/optimize")
    def process_optimize(req: ProcessOptimizeRequest) -> dict[str, Any]:
        _ensure_state_initialized()
        state = get_state()
        opt_req = OptimizeRequest(
            silos=state.get("silos", []),
            layers=state.get("layers", []),
            suppliers=state.get("suppliers", []),
            discharge=[],
            config=req.config,
            target_params=req.target_params,
            iterations=req.iterations,
            seed=req.seed,
        )
        return optimize(opt_req)

    @app.post("/api/process/apply_discharge")
    def process_apply_discharge(req: ProcessApplyDischargeRequest) -> dict[str, Any]:
        _ensure_state_initialized()
        state = get_state()
        if not req.discharge:
            raise HTTPException(status_code=422, detail="discharge plan is required.")
        discharge_df = pd.DataFrame(req.discharge)
        if "silo_id" not in discharge_df.columns:
            raise HTTPException(status_code=422, detail="discharge rows need silo_id.")
        discharge_by_silo: dict[str, float] = {}
        for _, row in discharge_df.iterrows():
            sid = str(row["silo_id"])
            if pd.notna(row.get("discharge_mass_kg")):
                discharge_by_silo[sid] = max(0.0, float(row["discharge_mass_kg"]))
            elif pd.notna(row.get("discharge_fraction")):
                frac = float(row["discharge_fraction"])
                if frac < 0 or frac > 1:
                    raise HTTPException(status_code=422, detail=f"{sid} discharge_fraction must be in [0,1]")
                mass_total = sum(
                    float(x.get("remaining_mass_kg", x.get("segment_mass_kg", 0.0)))
                    for x in state.get("layers", [])
                    if str(x.get("silo_id", "")) == sid
                )
                discharge_by_silo[sid] = frac * mass_total
            else:
                discharge_by_silo[sid] = 0.0
        available_by_silo = {
            str(s["silo_id"]): sum(
                float(x.get("remaining_mass_kg", x.get("segment_mass_kg", 0.0)))
                for x in state.get("layers", [])
                if str(x.get("silo_id", "")) == str(s["silo_id"])
            )
            for s in state.get("silos", [])
        }
        normalized_rows = _normalize_discharge_to_target(
            rows=[{"silo_id": sid, "discharge_mass_kg": m} for sid, m in discharge_by_silo.items()],
            available_by_silo=available_by_silo,
            target_total_kg=FIXED_DISCHARGE_TARGET_KG,
        )
        discharge_by_silo = {str(r["silo_id"]): float(r["discharge_mass_kg"]) for r in normalized_rows}

        # Predict blend using existing physics core before mutation.
        run_req = RunRequest(
            silos=state.get("silos", []),
            layers=state.get("layers", []),
            suppliers=state.get("suppliers", []),
            discharge=[{"silo_id": k, "discharge_mass_kg": v} for k, v in discharge_by_silo.items()],
            config=req.config,
        )
        predicted = run(run_req)
        predicted_total = float(predicted.get("total_discharged_mass_kg", 0.0))
        if abs(predicted_total - FIXED_DISCHARGE_TARGET_KG) > FIXED_DISCHARGE_TOL_KG:
            raise HTTPException(
                status_code=422,
                detail=(
                    f"Predicted discharge is {predicted_total:.3f} kg, expected fixed target "
                    f"{FIXED_DISCHARGE_TARGET_KG:.3f} kg. Adjust config (steps/auto_adjust) and retry."
                ),
            )
        before = summarize_state()
        updated = apply_discharge_to_state(discharge_by_silo)
        after = summarize_state()
        add_stage(
            action="apply_discharge",
            before=before,
            after=after,
            meta={"discharge_by_silo": discharge_by_silo},
        )
        out = {"state": updated, "summary": after, "predicted_run": predicted}
        _persist_result("apply_discharge_predicted", predicted, payload={"discharge_by_silo": discharge_by_silo})
        _persist_state_bundle("apply_discharge", payload=out)
        return out

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
        out = _result_to_api_payload(result)
        _persist_result("run", out, payload=req.model_dump())
        return out

    @app.post("/api/optimize")
    def optimize(req: OptimizeRequest) -> dict[str, Any]:
        inputs = {
            "silos": pd.DataFrame(req.silos),
            "layers": pd.DataFrame(req.layers),
            "suppliers": pd.DataFrame(req.suppliers),
            "discharge": pd.DataFrame(req.discharge),
        }
        inputs = _ensure_discharge_has_silo_ids(inputs)
        errors = validate_inputs_shape(inputs)
        if errors:
            raise HTTPException(status_code=422, detail=errors)
        if not req.target_params:
            raise HTTPException(status_code=422, detail="target_params must be provided.")

        cfg = RunConfig(**req.config)
        silos_df = inputs["silos"].copy()
        layers_df = inputs["layers"].copy()
        available_by_silo = _available_mass_by_silo(layers_df)
        available_total = float(sum(available_by_silo.values()))
        if available_total + 1e-12 < FIXED_DISCHARGE_TARGET_KG:
            raise HTTPException(
                status_code=422,
                detail=(
                    f"Insufficient available mass for fixed optimization target {FIXED_DISCHARGE_TARGET_KG:.3f} kg. "
                    f"Currently available: {available_total:.3f} kg."
                ),
            )
        silo_ids = silos_df["silo_id"].astype(str).tolist()
        rng = random.Random(req.seed)
        total_iter = max(1, req.iterations)
        explore_iters = max(1, int(total_iter * 0.6))
        exploit_iters = total_iter - explore_iters
        best_score = float("inf")
        best_result: dict[str, Any] | None = None
        best_discharge: list[dict[str, Any]] = []
        top_candidates: list[dict[str, Any]] = []
        best_fractions: list[float] = []

        def evaluate_fractions(fracs: list[float]) -> None:
            nonlocal best_score, best_result, best_discharge, best_fractions
            candidate_rows = _candidate_rows_from_fractions(silo_ids, fracs)
            candidate_rows = _normalize_discharge_to_target(
                rows=[
                    {
                        "silo_id": str(r["silo_id"]),
                        "discharge_mass_kg": float(r["discharge_fraction"]) * float(available_by_silo.get(str(r["silo_id"]), 0.0)),
                    }
                    for r in candidate_rows
                ],
                available_by_silo=available_by_silo,
                target_total_kg=FIXED_DISCHARGE_TARGET_KG,
            )
            candidate_inputs = dict(inputs)
            candidate_inputs["discharge"] = pd.DataFrame(candidate_rows)
            result = run_blend(candidate_inputs, cfg)
            discharged_total = float(result["total_discharged_mass_kg"])
            if abs(discharged_total - FIXED_DISCHARGE_TARGET_KG) > FIXED_DISCHARGE_TOL_KG:
                # Reject candidates that cannot physically meet the fixed-target discharge.
                return
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
                "total_discharged_mass_kg": discharged_total,
            }
            top_candidates.append(candidate_record)
            if score < best_score:
                best_score = score
                best_result = result
                best_discharge = candidate_rows
                best_fractions = [float(c["discharge_fraction"]) for c in candidate_rows]

        # Explore: stratified random sampling in [0.2, 0.8] to improve coverage.
        for i in range(explore_iters):
            band_lo = DISCHARGE_FRACTION_MIN + (
                (DISCHARGE_FRACTION_MAX - DISCHARGE_FRACTION_MIN) * i / explore_iters
            )
            band_hi = DISCHARGE_FRACTION_MIN + (
                (DISCHARGE_FRACTION_MAX - DISCHARGE_FRACTION_MIN) * (i + 1) / explore_iters
            )
            fractions = [rng.uniform(band_lo, band_hi) for _ in silo_ids]
            rng.shuffle(fractions)
            evaluate_fractions(fractions)

        # Exploit: local perturbation around the current best solution.
        if not best_fractions:
            best_fractions = [0.5 for _ in silo_ids]
        for i in range(exploit_iters):
            anneal = 1.0 - (i / max(1, exploit_iters))
            step = 0.12 * anneal + 0.01
            trial = [_clip_fraction(f + rng.uniform(-step, step)) for f in best_fractions]
            evaluate_fractions(trial)

        if best_result is None:
            raise HTTPException(
                status_code=422,
                detail=(
                    f"No feasible optimization candidate can achieve fixed discharge target "
                    f"{FIXED_DISCHARGE_TARGET_KG:.3f} kg with current state/config."
                ),
            )

        top_candidates = sorted(top_candidates, key=lambda x: x["objective_score"])[:5]
        out = {
            "objective_score": best_score,
            "recommended_discharge": best_discharge,
            "best_run": _result_to_api_payload(best_result),
            "target_params": req.target_params,
            "fixed_discharge_target_kg": FIXED_DISCHARGE_TARGET_KG,
            "iterations": req.iterations,
            "objective_method": "normalized_weighted_l2_hybrid_search",
            "param_ranges": DEFAULT_PARAM_RANGES,
            "top_candidates": top_candidates,
        }
        _persist_result("optimize", out, payload=req.model_dump())
        return out

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
