from __future__ import annotations

from dataclasses import dataclass
from math import erf, pi, sqrt
from typing import Dict
import warnings

import pandas as pd


@dataclass(frozen=True)
class Material:
    rho_bulk_kg_m3: float
    grain_diameter_m: float


@dataclass(frozen=True)
class BeverlooParams:
    C: float = 0.58
    k: float = 1.4
    g_m_s2: float = 9.81


@dataclass(frozen=True)
class Silo:
    silo_id: str
    capacity_kg: float
    body_diameter_m: float
    outlet_diameter_m: float
    initial_mass_kg: float = 0.0

    @property
    def cross_section_area_m2(self) -> float:
        return pi * (self.body_diameter_m / 2.0) ** 2


def normal_cdf(x: float) -> float:
    return 0.5 * (1.0 + erf(x / sqrt(2.0)))


def _build_silo_map(df_silos: pd.DataFrame) -> Dict[str, Silo]:
    required = {"silo_id", "capacity_kg", "body_diameter_m", "outlet_diameter_m"}
    missing = required - set(df_silos.columns)
    if missing:
        raise ValueError(f"df_silos missing columns: {missing}")

    d = df_silos.copy()
    if "initial_mass_kg" not in d.columns:
        d["initial_mass_kg"] = 0.0

    silos: Dict[str, Silo] = {}
    for _, r in d.iterrows():
        silo_id = str(r["silo_id"])
        silos[silo_id] = Silo(
            silo_id=silo_id,
            capacity_kg=float(r["capacity_kg"]),
            body_diameter_m=float(r["body_diameter_m"]),
            outlet_diameter_m=float(r["outlet_diameter_m"]),
            initial_mass_kg=float(r["initial_mass_kg"]),
        )
    return silos


def _validate_suppliers(df_layers: pd.DataFrame, df_suppliers: pd.DataFrame) -> None:
    if "supplier" not in df_suppliers.columns:
        raise ValueError("df_suppliers must include 'supplier' column.")
    missing = set(df_layers["supplier"].astype(str).unique()) - set(
        df_suppliers["supplier"].astype(str).unique()
    )
    if missing:
        raise ValueError(f"Suppliers in df_layers not found in df_suppliers: {missing}")


def build_intervals_from_df_layers(
    silo_id: str,
    df_layers: pd.DataFrame,
    silo: Silo,
    material: Material,
) -> tuple[pd.DataFrame, float]:
    required = {"silo_id", "layer_index", "lot_id", "supplier", "segment_mass_kg"}
    missing = required - set(df_layers.columns)
    if missing:
        raise ValueError(f"df_layers missing columns: {missing}")

    d = df_layers[df_layers["silo_id"].astype(str) == str(silo_id)].copy()
    if d.empty:
        raise ValueError(f"No layers found for silo_id={silo_id}")

    d["layer_index"] = d["layer_index"].astype(int)
    d = d.sort_values(["layer_index"], kind="mergesort").reset_index(drop=True)

    expected = list(range(1, len(d) + 1))
    actual = d["layer_index"].tolist()
    if actual != expected:
        raise ValueError(
            f"layer_index for silo {silo_id} must be contiguous bottom->top 1..N. Got {actual}"
        )

    total_mass = float(d["segment_mass_kg"].sum())
    if total_mass > silo.capacity_kg + 1e-9:
        warnings.warn(
            f"Silo {silo_id}: segment mass sum ({total_mass:.2f}) exceeds capacity ({silo.capacity_kg:.2f})."
        )

    area = silo.cross_section_area_m2
    z_cursor = 0.0
    z0_list = []
    z1_list = []
    for mass_kg in d["segment_mass_kg"].astype(float):
        if mass_kg < 0:
            raise ValueError(f"Silo {silo_id}: negative segment_mass_kg found.")
        h_m = mass_kg / (material.rho_bulk_kg_m3 * area)
        z0_list.append(z_cursor)
        z_cursor += h_m
        z1_list.append(z_cursor)

    d["z0_m"] = z0_list
    d["z1_m"] = z1_list
    return d, z_cursor


def layer_probabilities(
    z_front_m: float,
    sigma_m: float,
    intervals_df: pd.DataFrame,
    total_height_m: float,
) -> pd.Series:
    if sigma_m <= 0:
        raise ValueError("sigma_m must be > 0.")

    denom = normal_cdf((total_height_m - z_front_m) / sigma_m) - normal_cdf(
        (0.0 - z_front_m) / sigma_m
    )
    if denom <= 1e-15:
        return pd.Series(0.0, index=intervals_df.index)

    z0 = intervals_df["z0_m"].astype(float)
    z1 = intervals_df["z1_m"].astype(float)
    p_raw = (
        z1.map(lambda v: normal_cdf((v - z_front_m) / sigma_m))
        - z0.map(lambda v: normal_cdf((v - z_front_m) / sigma_m))
    ) / denom
    p_raw = p_raw.clip(lower=0.0)
    s = float(p_raw.sum())
    return (p_raw / s) if s > 0 else pd.Series(0.0, index=intervals_df.index)


def beverloo_mass_flow_rate_kg_s(
    silo: Silo, material: Material, bev: BeverlooParams
) -> float:
    d_eff = silo.outlet_diameter_m - bev.k * material.grain_diameter_m
    if d_eff <= 0:
        raise ValueError(
            f"Silo {silo.silo_id}: invalid Beverloo term D-k*d <= 0 "
            f"({silo.outlet_diameter_m:.6f} - {bev.k:.6f}*{material.grain_diameter_m:.6f})."
        )
    return bev.C * material.rho_bulk_kg_m3 * sqrt(bev.g_m_s2) * (d_eff**2.5)


def _resolve_discharge_mass_kg(
    silo_id: str, df_discharge: pd.DataFrame, total_mass_kg: float
) -> float:
    row = df_discharge[df_discharge["silo_id"].astype(str) == str(silo_id)]
    if row.empty:
        raise ValueError(f"No discharge row found for silo_id={silo_id}")

    row = row.iloc[0]
    has_mass = "discharge_mass_kg" in row.index and pd.notna(row["discharge_mass_kg"])
    has_frac = "discharge_fraction" in row.index and pd.notna(row["discharge_fraction"])

    if has_mass:
        m_kg = float(row["discharge_mass_kg"])
    elif has_frac:
        m_kg = float(row["discharge_fraction"]) * total_mass_kg
    else:
        raise ValueError(f"Silo {silo_id}: provide discharge_mass_kg or discharge_fraction.")

    if m_kg < 0:
        raise ValueError(f"Silo {silo_id}: discharge mass cannot be negative.")
    if m_kg > total_mass_kg + 1e-9:
        raise ValueError(
            f"Silo {silo_id}: discharge_mass_kg ({m_kg:.2f}) exceeds total mass in silo ({total_mass_kg:.2f})."
        )
    return m_kg


def _simulate_for_sigma(
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


def estimate_discharge_contrib_for_silo(
    silo: Silo,
    df_layers: pd.DataFrame,
    df_discharge: pd.DataFrame,
    material: Material,
    bev: BeverlooParams,
    sigma_m: float,
    steps: int = 2000,
    auto_adjust: bool = False,
    min_nonzero_mass_kg: float = 1e-3,
) -> Dict[str, object]:
    intervals_df, total_height_m = build_intervals_from_df_layers(
        silo.silo_id, df_layers, silo, material
    )
    total_mass_kg = float(intervals_df["segment_mass_kg"].sum())
    discharge_mass_kg = _resolve_discharge_mass_kg(
        silo.silo_id, df_discharge, total_mass_kg
    )
    m_dot = beverloo_mass_flow_rate_kg_s(silo, material, bev)
    discharge_time_s = discharge_mass_kg / m_dot if m_dot > 0 else 0.0

    used_sigma_m = sigma_m
    seg_contrib = _simulate_for_sigma(
        silo=silo,
        intervals_df=intervals_df,
        total_height_m=total_height_m,
        discharge_mass_kg=discharge_mass_kg,
        m_dot_kg_s=m_dot,
        material=material,
        sigma_m=used_sigma_m,
        steps=steps,
    )

    if auto_adjust:
        growth = 1.35
        for _ in range(12):
            lot_nonzero = (
                seg_contrib.groupby("lot_id")["discharged_mass_kg"].sum()
                > min_nonzero_mass_kg
            ).sum()
            if lot_nonzero >= 2:
                break
            used_sigma_m *= growth
            seg_contrib = _simulate_for_sigma(
                silo=silo,
                intervals_df=intervals_df,
                total_height_m=total_height_m,
                discharge_mass_kg=discharge_mass_kg,
                m_dot_kg_s=m_dot,
                material=material,
                sigma_m=used_sigma_m,
                steps=steps,
            )

    lot_contrib = (
        seg_contrib.groupby(["silo_id", "lot_id", "supplier"], as_index=False)[
            "discharged_mass_kg"
        ]
        .sum()
        .sort_values(["silo_id", "lot_id"])
        .reset_index(drop=True)
    )

    return {
        "silo_id": silo.silo_id,
        "discharged_mass_kg": discharge_mass_kg,
        "mass_flow_rate_kg_s": m_dot,
        "discharge_time_s": discharge_time_s,
        "sigma_m": used_sigma_m,
        "df_segment_contrib": seg_contrib[
            [
                "silo_id",
                "layer_index",
                "lot_id",
                "supplier",
                "segment_mass_kg",
                "discharged_mass_kg",
            ]
        ].copy(),
        "df_lot_contrib": lot_contrib,
    }


def blend_params_from_contrib(
    df_contrib: pd.DataFrame, df_suppliers: pd.DataFrame
) -> Dict[str, float]:
    required = {"supplier", "discharged_mass_kg"}
    missing = required - set(df_contrib.columns)
    if missing:
        raise ValueError(f"df_contrib missing columns: {missing}")

    param_cols = [c for c in df_suppliers.columns if c != "supplier"]
    if not param_cols:
        raise ValueError(
            "df_suppliers must have at least one parameter column besides 'supplier'."
        )

    merged = df_contrib.merge(df_suppliers, on="supplier", how="left")
    if merged[param_cols].isna().any().any():
        raise ValueError(
            "Some suppliers in contributions do not have complete specs in df_suppliers."
        )

    total_mass = float(merged["discharged_mass_kg"].sum())
    if total_mass <= 0:
        return {p: float("nan") for p in param_cols}

    out: Dict[str, float] = {}
    w = merged["discharged_mass_kg"].astype(float)
    for p in param_cols:
        out[p] = float((w * merged[p].astype(float)).sum() / total_mass)
    return out


def run_multi_silo_blend(
    df_silos: pd.DataFrame,
    df_layers: pd.DataFrame,
    df_suppliers: pd.DataFrame,
    df_discharge: pd.DataFrame,
    material: Material,
    bev: BeverlooParams,
    sigma_m: float,
    steps: int = 2000,
    auto_adjust: bool = False,
) -> Dict[str, object]:
    if "silo_id" not in df_discharge.columns:
        raise ValueError("df_discharge must include 'silo_id' column.")

    _validate_suppliers(df_layers, df_suppliers)
    silos = _build_silo_map(df_silos)
    if len(silos) != 3:
        warnings.warn(f"Expected 3 silos; got {len(silos)}. Running on provided silos.")

    per_silo_results: Dict[str, Dict[str, object]] = {}
    all_segment_contrib = []
    all_lot_contrib = []
    total_discharged = 0.0

    for silo_id, silo in silos.items():
        res = estimate_discharge_contrib_for_silo(
            silo=silo,
            df_layers=df_layers,
            df_discharge=df_discharge,
            material=material,
            bev=bev,
            sigma_m=sigma_m,
            steps=steps,
            auto_adjust=auto_adjust,
        )
        res["blended_params_per_silo"] = blend_params_from_contrib(
            res["df_lot_contrib"], df_suppliers
        )
        per_silo_results[silo_id] = res
        all_segment_contrib.append(res["df_segment_contrib"])
        all_lot_contrib.append(res["df_lot_contrib"])
        total_discharged += float(res["discharged_mass_kg"])

    df_segment_contrib_all = pd.concat(all_segment_contrib, ignore_index=True)
    df_lot_contrib_all = pd.concat(all_lot_contrib, ignore_index=True)
    total_blended_params = blend_params_from_contrib(df_lot_contrib_all, df_suppliers)

    return {
        "per_silo": per_silo_results,
        "df_segment_contrib_all": df_segment_contrib_all,
        "df_lot_contrib_all": df_lot_contrib_all,
        "total_discharged_mass_kg": total_discharged,
        "total_blended_params": total_blended_params,
    }
