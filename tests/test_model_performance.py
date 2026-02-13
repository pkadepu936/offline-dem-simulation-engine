from __future__ import annotations

from time import perf_counter

import pandas as pd

from dem_sim.model import Material, Silo, _simulate_for_sigma, layer_probabilities


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


def test_simulate_for_sigma_is_not_slower_than_reference() -> None:
    silo = Silo(
        silo_id="S1",
        capacity_kg=5000.0,
        body_diameter_m=3.1,
        outlet_diameter_m=0.2,
    )
    material = Material(rho_bulk_kg_m3=610.0, grain_diameter_m=0.004)
    layers = 30
    height = 1.8
    dz = height / layers
    intervals = pd.DataFrame(
        {
            "silo_id": ["S1"] * layers,
            "layer_index": list(range(1, layers + 1)),
            "lot_id": [f"L{i}" for i in range(1, layers + 1)],
            "supplier": ["A"] * layers,
            "segment_mass_kg": [120.0] * layers,
            "z0_m": [i * dz for i in range(layers)],
            "z1_m": [(i + 1) * dz for i in range(layers)],
        }
    )
    kwargs = {
        "silo": silo,
        "intervals_df": intervals,
        "total_height_m": height,
        "discharge_mass_kg": 1500.0,
        "m_dot_kg_s": 95.0,
        "material": material,
        "sigma_m": 0.12,
        "steps": 1200,
    }

    t0 = perf_counter()
    _simulate_reference(**kwargs)
    ref_time = perf_counter() - t0

    t1 = perf_counter()
    _simulate_for_sigma(**kwargs)
    fast_time = perf_counter() - t1

    # Keep a loose guardrail to avoid flaky failures across environments.
    assert fast_time <= ref_time * 1.2
