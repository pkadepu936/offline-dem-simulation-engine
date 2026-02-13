from __future__ import annotations

from io import StringIO
import pandas as pd
import pytest

from dem_sim.model import (
    BeverlooParams,
    Material,
    Silo,
    _resolve_discharge_mass_kg,
    beverloo_mass_flow_rate_kg_s,
    layer_probabilities,
    run_multi_silo_blend,
)
from dem_sim.reporting import validate_inputs_shape
from dem_sim.sample_data import DISCHARGE_CSV, LAYERS_CSV, SILOS_CSV, SUPPLIERS_CSV


def _sample_inputs() -> dict[str, pd.DataFrame]:
    return {
        "silos": pd.read_csv(StringIO(SILOS_CSV)),
        "layers": pd.read_csv(StringIO(LAYERS_CSV)),
        "suppliers": pd.read_csv(StringIO(SUPPLIERS_CSV)),
        "discharge": pd.read_csv(StringIO(DISCHARGE_CSV)),
    }


def test_layer_probabilities_sum_to_one() -> None:
    intervals = pd.DataFrame({"z0_m": [0.0, 1.0], "z1_m": [1.0, 2.0]})
    probs = layer_probabilities(
        z_front_m=0.75,
        sigma_m=0.2,
        intervals_df=intervals,
        total_height_m=2.0,
    )
    assert pytest.approx(float(probs.sum()), rel=1e-8, abs=1e-8) == 1.0
    assert (probs >= 0).all()


def test_resolve_discharge_fraction_out_of_range_raises() -> None:
    discharge = pd.DataFrame(
        [{"silo_id": "S1", "discharge_mass_kg": None, "discharge_fraction": 1.2}]
    )
    with pytest.raises(ValueError, match="discharge_fraction must be between 0 and 1"):
        _resolve_discharge_mass_kg("S1", discharge, total_mass_kg=1000.0)


def test_beverloo_invalid_effective_diameter_raises() -> None:
    silo = Silo(
        silo_id="S1",
        capacity_kg=1000.0,
        body_diameter_m=2.0,
        outlet_diameter_m=0.005,
    )
    material = Material(rho_bulk_kg_m3=610.0, grain_diameter_m=0.004)
    bev = BeverlooParams(C=0.58, k=1.4, g_m_s2=9.81)
    with pytest.raises(ValueError, match="invalid Beverloo term"):
        beverloo_mass_flow_rate_kg_s(silo, material, bev)


def test_validate_inputs_shape_detects_duplicate_layer_index_and_bad_fraction() -> None:
    inputs = _sample_inputs()
    dup = inputs["layers"].iloc[[0]].copy()
    inputs["layers"] = pd.concat([inputs["layers"], dup], ignore_index=True)
    inputs["discharge"].loc[0, "discharge_fraction"] = 1.1
    errors = validate_inputs_shape(inputs)
    assert any("duplicate (silo_id, layer_index)" in e for e in errors)
    assert any("discharge_fraction between 0 and 1" in e for e in errors)


def test_run_multi_silo_blend_rejects_nonpositive_sigma() -> None:
    inputs = _sample_inputs()
    material = Material(rho_bulk_kg_m3=610.0, grain_diameter_m=0.004)
    bev = BeverlooParams(C=0.58, k=1.4, g_m_s2=9.81)
    with pytest.raises(ValueError, match="sigma_m must be > 0"):
        run_multi_silo_blend(
            df_silos=inputs["silos"],
            df_layers=inputs["layers"],
            df_suppliers=inputs["suppliers"],
            df_discharge=inputs["discharge"],
            material=material,
            bev=bev,
            sigma_m=0.0,
            steps=100,
        )
