# Runbook: ML bulk-extraction (`tools/extract_training_data.py`)

How to pull `job_info`/`trace`/`mon_jdls_parsed` out of the datalake as
Parquet for offline ML training -- the replacement for
`alice_data_downloader.py`, the ML consumer's current script (Apache Arrow
Flight through Dremio, `job_id`-percentile shard/bisection retries,
hardcoded admin credentials in the script itself; see the redesign repo's
`research/2026-07-12_ml-consumer-data-contract.md` for the full picture of
what it does and why it's flaky). No Dremio, no Flight, no bisection, no
credentials baked into a script -- one DuckDB `COPY` per table against
Lakekeeper's Iceberg REST catalog.

## 1. Quickstart (PhD-student persona)

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install duckdb==1.5.4          # only dependency -- no pandas, no pyarrow
```

```bash
python tools/extract_training_data.py \
  --output-dir ./training-data \
  --catalog-uri http://<lakekeeper-host>:8181/catalog \
  --token <bearer-token> \
  --warehouse default
```

Produces, in `./training-data/`:

- `job_info.parquet`, `trace.parquet`, `mon_jdls_parsed.parquet`
- `manifest.json` -- row counts, Iceberg snapshot IDs, extraction
  timestamp (see section 5)

That's the whole tool. `--tables` (default
`job_info,trace,mon_jdls_parsed`) lets you extract a subset; every other
flag has a sane default except `--catalog-uri` and `--token`, which are
deployment-specific and always required.

## 2. Interim story: kind/MinIO today, cyfronet at Plan 4 cutover

**This recipe is real and runnable today, but only against the kind dev
cluster.** There is no production cyfronet deployment yet (Plan 4). Until
cutover:

- The catalog/warehouse/data all live on this repo's kind cluster + MinIO,
  not on any consumer-reachable production endpoint.
- Getting to the catalog/storage from outside the cluster requires
  `kubectl port-forward` (section 3) -- something a real off-cluster
  consumer (a laptop, an Athena SLURM node) will never need to do against
  cyfronet, where the endpoints are meant to be directly reachable.
- The `--s3-endpoint`/`--s3-access-key`/`--s3-secret-key` override flags
  (section 3) exist specifically to work around kind's Kubernetes-internal
  DNS names being unreachable from outside the cluster. At cyfronet
  cutover, if the storage endpoint Lakekeeper vends is externally routable
  (the expectation), these flags should simply be **omitted** -- the
  script's default (`ACCESS_DELEGATION_MODE 'vended_credentials'`, DuckDB's
  own default) is the simpler, intended long-term path. Treat needing
  `--s3-endpoint` as a kind-only workaround, not a permanent part of the
  recipe.

Update this section at Plan 4 cutover with the real `--catalog-uri` and
confirmation of whether the override flags are still needed.

## 3. Running against kind (dev / this repo's acceptance proof)

Port-forward both Lakekeeper and MinIO from outside the cluster (two
separate terminals, or background them):

```bash
kubectl -n lakekeeper port-forward svc/lakekeeper 18181:8181 &
kubectl -n minio port-forward svc/minio 19000:9000 &
```

Fetch the MinIO root credentials this kind cluster's `ingest-env` Secret
also uses (see `hack/kind-up.sh`):

```bash
S3_ACCESS_KEY=$(kubectl -n minio get secret minio-creds -o jsonpath='{.data.rootUser}' | base64 -d)
S3_SECRET_KEY=$(kubectl -n minio get secret minio-creds -o jsonpath='{.data.rootPassword}' | base64 -d)
```

Run the extraction:

```bash
python tools/extract_training_data.py \
  --output-dir ./training-data \
  --catalog-uri http://127.0.0.1:18181/catalog \
  --token dummy \
  --warehouse default \
  --s3-endpoint http://127.0.0.1:19000 \
  --s3-access-key "$S3_ACCESS_KEY" \
  --s3-secret-key "$S3_SECRET_KEY" \
  --s3-region local-01
```

`--token dummy`: this kind cluster's Lakekeeper accepts any non-empty
Bearer token ("Bearer-anything" -- no real OAuth2 configured). A real
deployment needs a real Bearer token issued by whatever's guarding
Lakekeeper.

### Why `--s3-endpoint` is needed here (verified live, 2026-07-13)

Without it, DuckDB uses Lakekeeper's vended per-table storage credentials
by default (`ACCESS_DELEGATION_MODE 'vended_credentials'`) -- and those
credentials carry the **Kubernetes-internal** MinIO endpoint
(`http://minio.minio.svc:9000/`), which is not reachable from outside the
cluster even with the catalog itself port-forwarded. Verified: without the
override, extraction fails with
`IOException: Could not resolve hostname error for HTTP GET to
'http://minio.minio.svc:9000/warehouse/...'`. Passing `--s3-endpoint`
switches the script to `ACCESS_DELEGATION_MODE 'none'` and a manually
configured S3 secret pointed at the port-forwarded address instead -- see
`tools/extract_training_data.py`'s module docstring for the full discovery
trail (every DuckDB Iceberg-REST-catalog ATTACH option was checked; there
is no "keep vended credentials, just override the endpoint" option --
disabling delegation and supplying your own S3 secret is the only working
combination).

### Known gap: contract column spellings are not yet served (verified live, 2026-07-13)

The design keeps `alice-ingest apply-views` publishing ML-consumer
dtypes-contract column spellings (`LPMPassName`, `TTL`, `Packages`, ...) as
Iceberg REST-catalog **views** under `lake.contract.*` (see
`ingest/src/alice_ingest/views.py`). This script tries that schema first
for every table -- but as of duckdb 1.5.4 / duckdb-iceberg 75726455,
**DuckDB's Iceberg REST-catalog client cannot read Iceberg views at all**
(only tables; no view-read support is documented or was observed). This is
not "the views are missing" -- Trino reads them fine
(`hack/kind-verify.sh`'s Trino probes), and `SHOW SCHEMAS FROM lake` /
`duckdb_schemas()` list `contract` right alongside `alice` -- it's a client
capability gap in DuckDB's own Iceberg extension.

**Practical effect today: every extraction falls back to `lake.alice.*`,**
the raw dlt-normalized column names (`jdl__lpm_pass_name` instead of
`LPMPassName`, `jdl__packages` instead of `Packages`, etc.), with a
`WARNING` printed per table and recorded in `manifest.json`'s
`fallback_reason` field. If your downstream loader expects the exact
dtypes-contract spellings, you currently need to rename columns yourself
after extraction (or wait for DuckDB to add Iceberg view support, at which
point this script picks up the contract spellings automatically -- the
fallback logic is generic "try, catch, fall back", not a special case, so
no script change will be needed when that happens).

## 4. Athena/SLURM mapping (old sbatch pattern -> new)

The old script ran via `alice_data_downloader.sh` on PLGrid Athena's A100
partition (128 GB RAM, 16 CPU, 6h walltime), doing the Flight
shard-and-bisect dance and writing chunked CSVs
(`--to_csv --chunked_csv 10`) because large scans over Flight were flaky
enough that production kept the chunked-CSV path rather than the
single-Parquet merge (which also had a `list.extend(str)` bug).

None of that machinery is needed anymore -- a DuckDB `COPY` per table is a
single bulk columnar read, not thousands of small shard queries, so there
is nothing to bisect and no chunking workaround to carry forward. The
`sbatch` wrapper itself maps 1:1, just running a much simpler command:

```bash
#!/bin/bash
#SBATCH --job-name=alice-extract
#SBATCH --partition=plgrid-gpu-a100
#SBATCH --time=6:00:00
#SBATCH --cpus-per-task=16
#SBATCH --mem=128G

source /path/to/.venv/bin/activate
python tools/extract_training_data.py \
  --output-dir "$SCRATCH/alice-training-data/$(date +%Y%m%d)" \
  --catalog-uri "$ALICE_CATALOG_URI" \
  --token "$ALICE_CATALOG_TOKEN" \
  --warehouse default
```

The resource request (A100/128GB/16CPU/6h) is carried over unchanged for
parity with the old job's footprint, but a bulk Parquet `COPY` is far
lighter than the old Flight/bisection/pandas-merge path -- expect this to
finish well inside the old walltime budget once running against a real
(non-kind) deployment; re-tune the SLURM request down once real timings are
observed at cyfronet cutover rather than carrying the old numbers forever.

## 5. The manifest: provenance the old script never had

`manifest.json` (written next to the Parquet files) records, per table:

- `source` -- the fully-qualified table/view actually read (e.g.
  `lake.alice.job_info` or, once DuckDB supports Iceberg views,
  `lake.contract.job_info`)
- `source_schema` -- `"alice"` or `"contract"`
- `row_count` -- rows written to that table's Parquet file (from DuckDB's
  own `COPY` result, not a second scan)
- `snapshot_id` -- the **physical `alice.<table>`** Iceberg table's current
  snapshot ID at extraction time, via DuckDB's `iceberg_snapshots(...)`
  table function (no Trino round-trip, no direct REST catalog call needed
  for this). Always the physical table's snapshot, even when reading
  through a (future) `contract` view, because a view has no snapshot of
  its own and the view selects from that same base table.
- `fallback_reason` -- present only when the `contract` schema wasn't
  readable for that table (today: always, per section 3's known gap);
  explains exactly why and what column-naming implication that has
- one shared top-level `extracted_at` (UTC, `YYYY-MM-DDTHH:MM:SSZ`)

Old script: no equivalent record existed at all -- a training run's input
data had no way to answer "which snapshot of the lake produced this file"
after the fact. Keep `manifest.json` alongside any Parquet files you use
for a training run; it's the answer to "can I reproduce this exact input
later" (via Iceberg time-travel on the recorded `snapshot_id`, subject to
the maintenance CronWorkflow's snapshot-expiry window -- see
`docs/runbooks/backfill.md`'s "Snapshot retention" section for how long a
given snapshot ID stays queryable).

## 6. kind-verify integration: decided NOT to wire in (recorded)

The brief left kind-verify.sh integration optional, with an explicit
steer: don't add `duckdb` to the ingest image just to run a probe cheaply.
**Decision: skip it.** The ingest image has no `duckdb` today and this
recipe is deliberately standalone (`tools/README.md`) -- bloating the
image with a dependency the pipeline itself never uses, just to satisfy an
optional smoke probe, would cut against that. The live proof instead lives
here and in `tools/tests/`:

- **Pure logic** (arg parsing/validation, S3-endpoint-string parsing,
  SQL-literal escaping, manifest assembly): `python3 -m unittest discover
  -s tools/tests`, no cluster needed.
- **End-to-end**: the manual verification in section 3 above, run against
  this repo's kind cluster -- Parquet row counts matching the fixture's
  1000/1000/1000, snapshot IDs recorded, files re-opening correctly in a
  fresh DuckDB session (`SELECT count(*) FROM read_parquet(...)`).
  Re-run this manually whenever the recipe or the underlying catalog/view
  setup changes; there is no automated CI gate for it.

## 7. Troubleshooting

- **`IOException: Could not resolve hostname ... minio.minio.svc:9000`**:
  you're on kind and forgot `--s3-endpoint`/`--s3-access-key`/
  `--s3-secret-key` (section 3).
- **`CatalogException: Table with name <table> does not exist!` mentioning
  `lake.contract.<table>`**: expected today, not an error -- this is the
  known gap in section 3; the script catches this itself and falls back
  automatically, printing a `WARNING`. If you see this exception escape
  the script (rather than a `WARNING` line), something else changed;
  re-check `lake.alice.<table>` is queryable at all first.
- **`ParserException` around `ATTACH`**: DuckDB's `ATTACH` statement does
  not accept `?` placeholders in its option list -- if you're modifying
  `connect_catalog()`, keep credential values in `CREATE SECRET` (which
  does support parameter binding) and only hand-escape the `ATTACH`
  statement's own literals (warehouse name, catalog URI), same as the
  existing code.
