from __future__ import annotations

import tempfile
from pathlib import Path

import pandas as pd

from dem_sim.io import load_inputs
from dem_sim.sample_data import write_sample_data
from dem_sim.service import RunConfig, run_blend


def test_sample_data_and_run_smoke() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        base = Path(tmp)
        write_sample_data(base)
        inputs = load_inputs(base)

        result = run_blend(inputs, RunConfig())
        assert result["total_discharged_mass_kg"] > 0
        assert result["total_remaining_mass_kg"] >= 0
        assert not pd.isna(result["df_lot_contrib_all"]["discharged_mass_kg"]).any()
        assert "remaining_mass_kg" in result["df_silo_state_ledger"].columns
