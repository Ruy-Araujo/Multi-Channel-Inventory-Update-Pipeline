# Multi-Channel Inventory Update Pipeline

Python pipeline to integrate datasets through the DataMission API, validate raw formats (CSV/JSON/Parquet), normalize data in pandas, and persist artifacts with execution metadata.

## Requirements

- Python 3.10+
- On Debian/Ubuntu, ensure `python3-venv` is installed to create a virtual environment

## Installation

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Install the local package so the CLI module can be resolved from the project root:

```bash
pip install -e .
```

Copy `.env.example` to `.env` and set your token:

```bash
cp .env.example .env
```

## Single Run (CLI)

From project root, using installed package:

```bash
python -m datamission_pipeline.cli run --project-id <your_project_id> --format parquet
```

If you do not want to install with `-e`, run with `PYTHONPATH=src`:

```bash
PYTHONPATH=src python -m datamission_pipeline.cli run --project-id <your_project_id> --format parquet
```

Example without `--project-id` (uses `DATAMISSION_PROJECT_ID` from `.env`):

```bash
PYTHONPATH=src python -m datamission_pipeline.cli run --format parquet
```

or through the installed script:

```bash
datamission-pipeline run --project-id <your_project_id> --format parquet
```

## Generated Outputs

- Validated raw data: `data/raw/`
- Processed data (Parquet): `data/processed/`
- Execution metadata: `data/logs/`

Each execution writes a metadata file with:

- `run_id`
- `downloaded_at`
- `checksum_sha256`
- validation results
- total rows before/after normalization
- final execution status

## Scheduling with cron

Ready-to-use script in `scheduler/cron_run.sh`.

Crontab example (daily at 02:00):

```bash
0 2 * * * cd /path/to/project && bash scheduler/cron_run.sh >> data/logs/cron.log 2>&1
```

## Scheduling with Airflow

DAG file: `scheduler/airflow_dag.py`.

The DAG executes the same CLI command with retries.

## Tests

```bash
pytest -q
```