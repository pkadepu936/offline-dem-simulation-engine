# Offline DEM Simulation Engine

Production-style application for estimating discharged blend composition from layered malt lots in silos.

## Features
- Beverloo mass-flow model for each silo.
- Layer-to-height conversion for lot segments.
- Gaussian mixing sampler over moving discharge front.
- Multi-silo discharge simulation and total blend parameter calculation.
- CLI workflow for generating sample data, validating inputs, running simulation, and exporting artifacts.

## Project structure
- `src/dem_sim/model.py`: physics + blending engine.
- `src/dem_sim/service.py`: run orchestration and config object.
- `src/dem_sim/io.py`: CSV input loading and output directory helpers.
- `src/dem_sim/reporting.py`: summaries and output artifact writers.
- `src/dem_sim/sample_data.py`: built-in sample dataset generator.
- `src/dem_sim/synthetic.py`: synthetic dataset generation within malt COA ranges.
- `src/dem_sim/cli.py`: command-line interface.
- `src/dem_sim/web.py`: FastAPI app and API endpoints.
- `src/dem_sim/ui/index.html`: web app shell.
- `src/dem_sim/ui/styles.css`: responsive UI styling.
- `src/dem_sim/ui/app.js`: frontend behavior and API integration.
- `scripts/run_example.py`: script entrypoint using in-code example dataframes.
- `tests/test_smoke.py`: smoke test over sample-data flow.
- `tests/test_web_api.py`: web/API smoke coverage.
- `Silo_discharge.ipynb`: original R&D notebook.

## Install
```bash
python -m pip install -e .
```

Or install dependencies only:
```bash
python -m pip install -r requirements.txt
```

## CLI usage
Create sample input files:
```bash
dem-sim init-sample --out data/sample
```

Create synthetic input files:
```bash
dem-sim init-synthetic --out data/synthetic --seed 42 --silos 3 --suppliers 3 --lots 8
```

Validate input files:
```bash
dem-sim validate --in data/sample
```

Run simulation:
```bash
dem-sim run --in data/sample --out outputs/latest --auto-adjust
```

Run without installing as a script:
```bash
PYTHONPATH=src python -m dem_sim run --in data/sample --out outputs/latest --auto-adjust
```

## Web UI / API
Start the server:
```bash
dem-sim-web --host 127.0.0.1 --port 8000
```

Or without script install:
```bash
PYTHONPATH=src python -m dem_sim.web --host 127.0.0.1 --port 8000
```

Open:
- `http://127.0.0.1:8000/` (full web UI)
- `http://127.0.0.1:8000/docs` (Swagger UI)

Main endpoints:
- `GET /health`
- `GET /api/sample`
- `POST /api/validate`
- `POST /api/run`
- `POST /api/optimize` (searches discharge fractions toward target COA)
  - Uses normalized weighted L2 objective (error scaled by COA parameter ranges)
  - Returns best plan plus Top-5 candidate plans

## Input files
Place these CSV files in the input directory:
- `silos.csv`
- `layers.csv`
- `suppliers.csv`
- `discharge.csv`

## Output artifacts
Generated in output directory:
- `segment_contributions.csv`
- `lot_contributions.csv`
- `segment_state_ledger.csv` (initial/discharged/remaining per segment)
- `lot_state_ledger.csv` (initial/discharged/remaining per lot per silo)
- `silo_state_ledger.csv` (initial/discharged/remaining per silo)
- `summary.json`
