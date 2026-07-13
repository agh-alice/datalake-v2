# tools/

Standalone, consumer-facing scripts. Nothing here is part of the
`alice-ingest` package or its container image -- these are meant to be
copied out and run on a laptop, a PLGrid Athena SLURM node, or any machine
outside the cluster, with the smallest possible dependency footprint.

## `extract_training_data.py`

Bulk-extracts ALICE datalake tables (`job_info`, `trace`,
`mon_jdls_parsed` by default) to Parquet, using DuckDB's Iceberg
REST-catalog reader against Lakekeeper. Replaces the ML consumer's
`alice_data_downloader.py` (Arrow Flight through Dremio, `job_id`
percentile sharding, bisection retries, hardcoded admin credentials -- see
the redesign repo's `research/2026-07-12_ml-consumer-data-contract.md`).

**Only dependency: `duckdb`.** No pandas, no pyarrow, no Trino client.

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install duckdb==1.5.4
python extract_training_data.py \
  --output-dir ./training-data \
  --catalog-uri http://<lakekeeper-host>:8181/catalog \
  --token <bearer-token> \
  --warehouse default
```

Full quickstart (including the port-forward commands for a kind dev
cluster, the S3-endpoint override flags and why they're needed there, the
Athena/SLURM `sbatch` mapping, and the manifest's provenance fields):
see `docs/runbooks/ml-extraction.md`.

Unit tests (pure logic -- arg parsing/validation, S3-endpoint-string
parsing, SQL-literal escaping, manifest assembly; no cluster needed):

```bash
python3 -m unittest discover -s tools/tests
```
