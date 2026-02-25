from __future__ import annotations

from .db import get_conn


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS silos (
    silo_id TEXT PRIMARY KEY,
    capacity_kg DOUBLE PRECISION NOT NULL,
    body_diameter_m DOUBLE PRECISION NOT NULL,
    outlet_diameter_m DOUBLE PRECISION NOT NULL,
    initial_mass_kg DOUBLE PRECISION NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS layers (
    id BIGSERIAL PRIMARY KEY,
    silo_id TEXT NOT NULL REFERENCES silos(silo_id) ON DELETE CASCADE,
    layer_index INTEGER NOT NULL,
    lot_id TEXT NOT NULL,
    supplier TEXT NOT NULL,
    remaining_mass_kg DOUBLE PRECISION NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (silo_id, layer_index)
);

CREATE TABLE IF NOT EXISTS suppliers (
    supplier_id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    moisture_pct DOUBLE PRECISION,
    fine_extract_db_pct DOUBLE PRECISION,
    wort_pH DOUBLE PRECISION,
    diastatic_power_WK DOUBLE PRECISION,
    total_protein_pct DOUBLE PRECISION,
    wort_colour_EBC DOUBLE PRECISION,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS incoming_queue (
    id BIGSERIAL PRIMARY KEY,
    lot_id TEXT NOT NULL,
    supplier TEXT NOT NULL,
    mass_kg DOUBLE PRECISION NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS lot_ledger (
    lot_id TEXT PRIMARY KEY,
    supplier TEXT NOT NULL,
    total_mass_kg DOUBLE PRECISION NOT NULL DEFAULT 0,
    discharged_kg DOUBLE PRECISION NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS history (
    id BIGSERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    action TEXT NOT NULL,
    meta JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS stages (
    id BIGSERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    action TEXT NOT NULL,
    before JSONB NOT NULL DEFAULT '{}'::jsonb,
    after JSONB NOT NULL DEFAULT '{}'::jsonb,
    meta JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS results_run (
    id BIGSERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    action TEXT NOT NULL,
    inputs_hash TEXT,
    total_discharged_mass_kg DOUBLE PRECISION NOT NULL,
    total_remaining_mass_kg DOUBLE PRECISION NOT NULL,
    total_blended_params JSONB NOT NULL DEFAULT '{}'::jsonb,
    per_silo JSONB NOT NULL DEFAULT '{}'::jsonb,
    silo_state_ledger JSONB NOT NULL DEFAULT '[]'::jsonb
);

CREATE TABLE IF NOT EXISTS results_optimize (
    id BIGSERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    objective_score DOUBLE PRECISION NOT NULL,
    recommended_discharge JSONB NOT NULL DEFAULT '[]'::jsonb,
    target_params JSONB NOT NULL DEFAULT '{}'::jsonb,
    top_candidates JSONB NOT NULL DEFAULT '[]'::jsonb,
    best_run JSONB NOT NULL DEFAULT '{}'::jsonb
);
"""


def ensure_schema() -> None:
    with get_conn() as conn:
        with conn.transaction():
            conn.execute(SCHEMA_SQL)
            conn.execute("ALTER TABLE suppliers ADD COLUMN IF NOT EXISTS moisture_pct DOUBLE PRECISION")
            conn.execute("ALTER TABLE suppliers ADD COLUMN IF NOT EXISTS fine_extract_db_pct DOUBLE PRECISION")
            conn.execute("ALTER TABLE suppliers ADD COLUMN IF NOT EXISTS wort_pH DOUBLE PRECISION")
            conn.execute("ALTER TABLE suppliers ADD COLUMN IF NOT EXISTS diastatic_power_WK DOUBLE PRECISION")
            conn.execute("ALTER TABLE suppliers ADD COLUMN IF NOT EXISTS total_protein_pct DOUBLE PRECISION")
            conn.execute("ALTER TABLE suppliers ADD COLUMN IF NOT EXISTS wort_colour_EBC DOUBLE PRECISION")
