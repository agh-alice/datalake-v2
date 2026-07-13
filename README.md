# datalake-v2

GitOps platform for the AGH–ALICE datalake v2 (design: alice-datalake-pepeline-redesign
deliverables/2026-07-12-datalake-v2-design.md). Pattern: rendered manifests via ArgoCD
Source Hydrator + umbrella chart + ESO (per the GAUGE upgrade of the Sano vht template).

## Layout
- `chart/` — umbrella Helm chart: OUR authored resources (DRY source; hydrated per env)
- `apps/project.yaml` — the single scoped AppProject
- `apps/infra/` — upstream operators as plain-Helm Applications (version-pinned)
- `environments/<env>/apps/datalake.yaml` — per-env Source Hydrator Application
- `environments/<env>/` — env bootstrap values + (cyfronet) SOPS secrets
- `ingest/` — the `alice-ingest` pipeline image (dlt + pyiceberg; nightly/sitesonar/retention/maintenance/views/freshness subcommands)
- `tools/` — standalone consumer-side scripts, not part of the ingest image (e.g. the DuckDB extraction recipe)
- `docs/runbooks/` — operator guides (ingestion, backfill, cyfronet bootstrap, ML extraction, XID wraparound, adding a component)
- Rendered branches `environments/kind`, `environments/cyfronet(-next)` are machine-owned.

## Environments
| env | purpose | bootstrap |
|-----|---------|-----------|
| kind | local/CI verification | `make kind-up && make kind-verify` |
| cyfronet | production | `docs/runbooks/bootstrap-cyfronet.md` |

## Components

Full platform surface as of `v0.3.0` (Plan 1: bootstrap/GitOps; Plan 2:
ingestion/lakehouse; Plan 3: query layer). Pins live in each Application's
own file — this table is a map, not the source of truth; a version listed
here can drift if the linked file isn't updated in lockstep, though CI's
`make lint` only validates the manifests render, not that this table stays
current.

| Component | Role | Pin | Defined in |
|---|---|---|---|
| ArgoCD (Source Hydrator) | GitOps controller: renders `chart/` + per-env values, pushes to the `environments/<env>` branches this cluster actually syncs from | chart 10.1.3 / image `v3.5.0-rc2` (RC — no 3.5 GA exists yet; re-pin before cyfronet, tracked in `docs/runbooks/bootstrap-cyfronet.md`) | `hack/kind-up.sh`, `environments/kind/argocd-values.yaml` |
| CloudNativePG | Postgres operator; runs the `lakekeeper-db` and `landing-db` Clusters (digest-pinned `postgresql:16.6`) | chart 0.28.0 | `apps/infra/cloudnative-pg.yaml` |
| External Secrets Operator | ExternalSecret/ClusterSecretStore CRDs for cyfronet credential sourcing; unused on kind, which harness-provisions Secrets directly (`hack/kind-up.sh`) | chart 2.7.0 | `apps/infra/external-secrets.yaml` |
| kube-prometheus-stack | Prometheus + Alertmanager; loads the `datalake`/`datalake-pipeline` PrometheusRule groups (XID age, workflow failures, retention, Iceberg snapshot staleness) | chart 83.4.0 | `apps/infra/monitoring.yaml` |
| MinIO | S3-compatible object store backing Lakekeeper's storage-profile | chart 5.4.0 — **kind-only** (Cyfronet S3 replaces it at Plan 4 cutover) | `environments/kind/apps/minio.yaml` |
| Lakekeeper | Iceberg REST catalog: owns the `default` warehouse and (Plan 3) the `lake.contract` view schema | chart 0.11.0 / app 0.12.2 | `apps/infra/lakekeeper.yaml` |
| Argo Workflows | Workflow/CronWorkflow engine; every `alice-ingest` subcommand runs as a Workflow under this | chart 1.0.19 / app v4.0.7 (`spec.schedules` list schema, not legacy `schedule`) | `apps/infra/argo-workflows.yaml` |
| Trino | SQL query layer; catalogs `lake` (Iceberg REST, vended S3 creds) and `landing` (PostgreSQL federation, read-only `trino_ro` role) | chart 1.40.0 / app 476 — **kind-only pin** (476+ builds need x86-64-v3; cyfronet's real hardware should re-evaluate the newest chart) | `apps/infra/trino.yaml` |
| `chart/` (this repo's umbrella chart) | Our authored resources: `lakekeeper-db`/`landing-db` Clusters, namespaces, `ingest-nightly`/`ingest-sitesonar`/`ingest-maintenance` CronWorkflows, the `hello` workflow-execution canary (kind-only, values-gated), RBAC, alert rules | hydrated per env by the Source Hydrator | `chart/templates/`, `chart/values*.yaml` |
| `alice-ingest` (ingest image) | `run-nightly`, `run-sitesonar`, `run-retention`, `check-freshness`, `run-maintenance`, `run-trino-maintenance`, `apply-views` | digest-pinned; `chart/values.yaml`'s `images.ingest` | `ingest/`, `docs/runbooks/ingestion-pipeline.md` |
| `tools/extract_training_data.py` | Consumer-side DuckDB extraction recipe: reads `lake.contract.*` (falls back to `lake.alice.*` with a warning — DuckDB's Iceberg extension cannot read REST-catalog views on the pinned version), writes Parquet + a provenance manifest. Replaces the old Dremio Flight script | standalone; not part of the ingest image | `tools/`, `docs/runbooks/ml-extraction.md` |
