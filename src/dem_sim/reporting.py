from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

import pandas as pd


def _jsonable_dict(d: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in d.items():
        if isinstance(v, (float, int, str, bool)) or v is None:
            out[k] = v
        else:
            out[k] = str(v)
    return out


def write_outputs(result: Dict[str, Any], output_dir: str | Path) -> Dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    segment_csv = out / "segment_contributions.csv"
    lot_csv = out / "lot_contributions.csv"
    summary_json = out / "summary.json"

    result["df_segment_contrib_all"].to_csv(segment_csv, index=False)
    result["df_lot_contrib_all"].to_csv(lot_csv, index=False)

    per_silo_summary = {}
    for silo_id, silo_result in result["per_silo"].items():
        per_silo_summary[silo_id] = {
            "discharged_mass_kg": float(silo_result["discharged_mass_kg"]),
            "mass_flow_rate_kg_s": float(silo_result["mass_flow_rate_kg_s"]),
            "discharge_time_s": float(silo_result["discharge_time_s"]),
            "sigma_m": float(silo_result["sigma_m"]),
            "blended_params_per_silo": _jsonable_dict(
                silo_result["blended_params_per_silo"]
            ),
        }

    payload = {
        "total_discharged_mass_kg": float(result["total_discharged_mass_kg"]),
        "total_blended_params": _jsonable_dict(result["total_blended_params"]),
        "per_silo": per_silo_summary,
    }
    summary_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    return {
        "segment_contributions_csv": segment_csv,
        "lot_contributions_csv": lot_csv,
        "summary_json": summary_json,
    }


def terminal_summary(result: Dict[str, Any]) -> str:
    lines = []
    lines.append("Simulation complete")
    lines.append(f"Total discharged mass: {result['total_discharged_mass_kg']:.3f} kg")
    lines.append("Total blended parameters:")
    for k, v in result["total_blended_params"].items():
        lines.append(f"- {k}: {v:.4f}")

    lines.append("Per-silo:")
    for silo_id, silo_result in result["per_silo"].items():
        lines.append(
            f"- {silo_id}: discharge={silo_result['discharged_mass_kg']:.3f} kg, "
            f"flow={silo_result['mass_flow_rate_kg_s']:.3f} kg/s, "
            f"time={silo_result['discharge_time_s']:.3f} s, sigma={silo_result['sigma_m']:.4f} m"
        )
    return "\n".join(lines)


def validate_inputs_shape(inputs: Dict[str, pd.DataFrame]) -> list[str]:
    errors: list[str] = []

    silos = inputs["silos"]
    layers = inputs["layers"]
    suppliers = inputs["suppliers"]
    discharge = inputs["discharge"]

    required_silos = {"silo_id", "capacity_kg", "body_diameter_m", "outlet_diameter_m"}
    required_layers = {"silo_id", "layer_index", "lot_id", "supplier", "segment_mass_kg"}
    required_suppliers = {"supplier"}
    required_discharge = {"silo_id"}

    if not required_silos.issubset(silos.columns):
        errors.append(f"silos.csv missing: {required_silos - set(silos.columns)}")
    if not required_layers.issubset(layers.columns):
        errors.append(f"layers.csv missing: {required_layers - set(layers.columns)}")
    if not required_suppliers.issubset(suppliers.columns):
        errors.append(f"suppliers.csv missing: {required_suppliers - set(suppliers.columns)}")
    if not required_discharge.issubset(discharge.columns):
        errors.append(f"discharge.csv missing: {required_discharge - set(discharge.columns)}")

    if "silo_id" in silos.columns and silos["silo_id"].astype(str).duplicated().any():
        errors.append("silos.csv has duplicate silo_id values.")

    if "supplier" in layers.columns and "supplier" in suppliers.columns:
        unknown = set(layers["supplier"].astype(str)) - set(suppliers["supplier"].astype(str))
        if unknown:
            errors.append(f"layers.csv references unknown suppliers: {sorted(unknown)}")

    return errors
