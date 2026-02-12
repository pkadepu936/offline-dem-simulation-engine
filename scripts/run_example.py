from __future__ import annotations

import pandas as pd

from dem_sim.model import BeverlooParams, Material, run_multi_silo_blend


def main() -> None:
    df_suppliers = pd.DataFrame(
        [
            {
                "supplier": "BBM",
                "moisture_pct": 4.2,
                "fine_extract_db_pct": 82.0,
                "wort_pH": 5.98,
                "diastatic_power_WK": 342.1,
                "total_protein_pct": 10.12,
                "wort_colour_EBC": 3.8,
            },
            {
                "supplier": "COFCO",
                "moisture_pct": 4.4,
                "fine_extract_db_pct": 81.8,
                "wort_pH": 5.93,
                "diastatic_power_WK": 317.4,
                "total_protein_pct": 11.1,
                "wort_colour_EBC": 4.0,
            },
            {
                "supplier": "Malteurop",
                "moisture_pct": 4.3,
                "fine_extract_db_pct": 81.2,
                "wort_pH": 5.97,
                "diastatic_power_WK": 336.9,
                "total_protein_pct": 10.5,
                "wort_colour_EBC": 3.8,
            },
        ]
    )

    df_silos = pd.DataFrame(
        [
            {
                "silo_id": "S1",
                "capacity_kg": 4000.0,
                "body_diameter_m": 3.0,
                "outlet_diameter_m": 0.20,
            },
            {
                "silo_id": "S2",
                "capacity_kg": 4000.0,
                "body_diameter_m": 3.2,
                "outlet_diameter_m": 0.20,
            },
            {
                "silo_id": "S3",
                "capacity_kg": 4000.0,
                "body_diameter_m": 3.1,
                "outlet_diameter_m": 0.21,
            },
        ]
    )

    df_layers = pd.DataFrame(
        [
            {"silo_id": "S1", "layer_index": 1, "lot_id": "L1001", "supplier": "BBM", "segment_mass_kg": 1200.0},
            {"silo_id": "S1", "layer_index": 2, "lot_id": "L1002", "supplier": "COFCO", "segment_mass_kg": 900.0},
            {"silo_id": "S1", "layer_index": 3, "lot_id": "L1003", "supplier": "Malteurop", "segment_mass_kg": 700.0},
            {"silo_id": "S2", "layer_index": 1, "lot_id": "L1001", "supplier": "BBM", "segment_mass_kg": 1400.0},
            {"silo_id": "S2", "layer_index": 2, "lot_id": "L1003", "supplier": "Malteurop", "segment_mass_kg": 1000.0},
            {"silo_id": "S2", "layer_index": 3, "lot_id": "L1002", "supplier": "COFCO", "segment_mass_kg": 600.0},
            {"silo_id": "S3", "layer_index": 1, "lot_id": "L1002", "supplier": "COFCO", "segment_mass_kg": 700.0},
            {"silo_id": "S3", "layer_index": 2, "lot_id": "L1003", "supplier": "Malteurop", "segment_mass_kg": 700.0},
        ]
    )

    df_discharge = pd.DataFrame(
        [
            {"silo_id": "S1", "discharge_mass_kg": 1600.0},
            {"silo_id": "S2", "discharge_fraction": 0.50},
            {"silo_id": "S3", "discharge_mass_kg": 800.0},
        ]
    )

    material = Material(rho_bulk_kg_m3=610.0, grain_diameter_m=0.004)
    bev = BeverlooParams(C=0.58, k=1.4, g_m_s2=9.81)

    result = run_multi_silo_blend(
        df_silos=df_silos,
        df_layers=df_layers,
        df_suppliers=df_suppliers,
        df_discharge=df_discharge,
        material=material,
        bev=bev,
        sigma_m=0.12,
        steps=2000,
        auto_adjust=True,
    )

    for silo_id, r in result["per_silo"].items():
        print(f"\n=== Silo {silo_id} ===")
        print(f"discharged_mass_kg: {r['discharged_mass_kg']:.3f}")
        print(f"mass_flow_rate_kg_s: {r['mass_flow_rate_kg_s']:.3f}")
        print(f"discharge_time_s: {r['discharge_time_s']:.3f}")
        print(f"sigma_m used: {r['sigma_m']:.4f}")
        print(r["df_lot_contrib"][["lot_id", "supplier", "discharged_mass_kg"]].to_string(index=False))

    print("\n=== TOTAL COLLECTED BLEND ===")
    print(f"total_discharged_mass_kg: {result['total_discharged_mass_kg']:.3f}")
    for key, value in result["total_blended_params"].items():
        print(f"{key}: {value:.4f}")


if __name__ == "__main__":
    main()
