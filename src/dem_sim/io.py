from __future__ import annotations

from pathlib import Path
from typing import Dict

import pandas as pd

REQUIRED_INPUT_FILES = {
    "silos": "silos.csv",
    "layers": "layers.csv",
    "suppliers": "suppliers.csv",
    "discharge": "discharge.csv",
}


def load_inputs(input_dir: str | Path) -> Dict[str, pd.DataFrame]:
    base = Path(input_dir)
    if not base.exists():
        raise FileNotFoundError(f"Input directory not found: {base}")

    out: Dict[str, pd.DataFrame] = {}
    for key, filename in REQUIRED_INPUT_FILES.items():
        path = base / filename
        if not path.exists():
            raise FileNotFoundError(f"Missing input file: {path}")
        out[key] = pd.read_csv(path)
    return out


def ensure_output_dir(output_dir: str | Path) -> Path:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    return out
