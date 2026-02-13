from __future__ import annotations

import pandas as pd
import pytest

from dem_sim.model import (
    Material,
    Silo,
    _simulate_for_sigma,
    layer_probabilities,
)


def _simulate_reference(
    silo: Silo,
    intervals_df: pd.DataFrame,
    total_height_m: float,
    discharge_mass_kg: float,
    m_dot_kg_s: float,
    material: Material,
    sigma_m: float,
    steps: int,
) -> pd.DataFrame:
    seg = intervals_df.copy()
    seg["discharged_mass_kg"] = 0.0
    if discharge_mass_kg == 0:
        return seg
    discharge_time_s = discharge_mass_kg / m_dot_kg_s
    dt = discharge_time_s / steps
    dm = m_dot_kg_s * dt
    area = silo.cross_section_area_m2
    for i in range(steps):
        t_mid = (i + 0.5) * dt
        m_removed = min(discharge_mass_kg, m_dot_kg_s * t_mid)
        z_front = m_removed / (material.rho_bulk_kg_m3 * area)
        p = layer_probabilities(z_front, sigma_m, seg, total_height_m)
        seg["discharged_mass_kg"] += dm * p.values
    total_sim = float(seg["discharged_mass_kg"].sum())
    if total_sim > 0:
        seg["discharged_mass_kg"] *= discharge_mass_kg / total_sim
    return seg


def test_simulate_for_sigma_matches_reference_with_tolerance() -> None:
    silo = Silo(
        silo_id="S1",
        capacity_kg=5000.0,
        body_diameter_m=3.0,
        outlet_diameter_m=0.2,
    )
    material = Material(rho_bulk_kg_m3=610.0, grain_diameter_m=0.004)
    intervals = pd.DataFrame(
        {
            "silo_id": ["S1"] * 5,
            "layer_index": [1, 2, 3, 4, 5],
            "lot_id": ["L1", "L2", "L3", "L4", "L5"],
            "supplier": ["A", "B", "C", "A", "B"],
            "segment_mass_kg": [900.0, 850.0, 700.0, 650.0, 600.0],
            "z0_m": [0.0, 0.2, 0.4, 0.6, 0.8],
            "z1_m": [0.2, 0.4, 0.6, 0.8, 1.0],
        }
    )

    kwargs = {
        "silo": silo,
        "intervals_df": intervals,
        "total_height_m": 1.0,
        "discharge_mass_kg": 1300.0,
        "m_dot_kg_s": 110.0,
        "material": material,
        "sigma_m": 0.13,
        "steps": 800,
    }
    ref = _simulate_reference(**kwargs)
    fast = _simulate_for_sigma(**kwargs)

    ref_vals = ref["discharged_mass_kg"].to_numpy()
    fast_vals = fast["discharged_mass_kg"].to_numpy()
    assert float(fast_vals.sum()) == pytest.approx(float(ref_vals.sum()), rel=1e-8, abs=1e-8)
    assert fast_vals == pytest.approx(ref_vals, rel=2e-3, abs=1e-3)
