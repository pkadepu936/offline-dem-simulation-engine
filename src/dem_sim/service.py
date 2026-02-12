from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict

from .model import BeverlooParams, Material, run_multi_silo_blend


@dataclass(frozen=True)
class RunConfig:
    rho_bulk_kg_m3: float = 610.0
    grain_diameter_m: float = 0.004
    beverloo_c: float = 0.58
    beverloo_k: float = 1.4
    gravity_m_s2: float = 9.81
    sigma_m: float = 0.12
    steps: int = 2000
    auto_adjust: bool = True


def run_blend(inputs: Dict[str, Any], cfg: RunConfig) -> Dict[str, Any]:
    material = Material(
        rho_bulk_kg_m3=cfg.rho_bulk_kg_m3,
        grain_diameter_m=cfg.grain_diameter_m,
    )
    bev = BeverlooParams(
        C=cfg.beverloo_c,
        k=cfg.beverloo_k,
        g_m_s2=cfg.gravity_m_s2,
    )
    return run_multi_silo_blend(
        df_silos=inputs["silos"],
        df_layers=inputs["layers"],
        df_suppliers=inputs["suppliers"],
        df_discharge=inputs["discharge"],
        material=material,
        bev=bev,
        sigma_m=cfg.sigma_m,
        steps=cfg.steps,
        auto_adjust=cfg.auto_adjust,
    )
