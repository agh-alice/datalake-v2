# datalake-v2

GitOps platform for the AGH–ALICE datalake v2 (design:
`alice-datalake-pepeline-redesign/deliverables/2026-07-12-datalake-v2-design.md`).
We are a three-namespace **tenant** on `alice-k8s-datalake`, a cluster the
AGH-ALICE platform team owns and runs — not a cluster we install or manage
ourselves. Their Argo CD (Source Hydrator) renders this repo's `main` into a
rendered branch and syncs from there; we never touch their cluster or their
ArgoCD directly except via `kubectl` reads. See
`docs/runbooks/bootstrap-cyfronet.md` for the full deployment/rollback
runbook (name kept for link stability; the content is the tenant runbook).

## Layout

- `envs/prod/{storage,compute,orchestration}/` — the three tier Helm charts,
  one per namespace (`datalake-storage`, `datalake-compute`,
  `datalake-orchestration`). Each renders standalone (`helm template
  envs/prod/<tier>`, remote chart dependencies only, no `file://`) and is
  the platform's `sourceHydrator.drySource.path` for that tier. `Chart.lock`
  is committed (vendored, exact pins); `charts/*.tgz` is gitignored, fetched
  fresh by `helm dependency build` on every clone/CI run/hydration.
- `environments/kind/` — the local/CI mirror: three Applications pointing at
  the *same* `envs/prod/<tier>` charts with `values-kind.yaml` layered on,
  plus `infra/` (the platform-owned operators kind has to stand up for
  itself — CNPG, ESO, Argo Workflows, kube-prometheus-stack, Dex, MinIO —
  and the kind-only ArgoCD instance that syncs all of it).
- `ingest/` — the `alice-ingest` pipeline image (dlt + pyiceberg;
  nightly/sitesonar/retention/maintenance/views/freshness subcommands).
- `tools/` — standalone consumer-side scripts, not part of the ingest image
  (the DuckDB extraction recipe).
- `docs/runbooks/` — operator guides (ingestion, backfill, tenant
  deployment, ML extraction, XID wraparound, adding a component).
- `hack/` — `make lint`/`make kind-up`/`make kind-verify` and their
  supporting scripts, including `check-seam.sh` (the cluster-scoped-kind
  gate, reused locally, in CI, and conceptually by the platform's
  `clusterResourceWhitelist: []` at sync time).
- Rendered branches: `environments/kind` (this repo's own kind ArgoCD syncs
  from it) and, once the platform's ApplicationSet switches to
  `sourceHydrator`, `environments/prod`/`environments/prod-next`
  (machine-rendered by *their* hub, not pushed by us — see the runbook).

## Environments

| env | purpose | how it's driven |
|-----|---------|-----------|
| kind | local/CI verification, mirrors the tenant topology | `make kind-up && make kind-verify` |
| prod (`alice-k8s-datalake`) | production, platform-managed | commit to `main` → platform's Argo CD hydrates → PR `environments/prod-next` → `environments/prod` → their ArgoCD syncs. `docs/runbooks/bootstrap-cyfronet.md` for the full flow, rollback, and current open dependencies. |

## Ownership — the seam

The platform's AppProject bounds us with `clusterResourceWhitelist: []` and
three namespace destinations — every object every tier chart renders must
be namespaced. This table is the map of who runs what; `docs/runbooks/
bootstrap-cyfronet.md` §1 has the full reasoning and `hack/check-seam.sh` is
the enforceable version of the same rule (local + CI + the platform's own
admission at sync time).

**Platform-owned** (their cluster, applied for real, not in this repo):

| Component | Role |
|---|---|
| Node pools, labels, placement | `pool={db,avx2,workers}`, `cpu=x86-64-v{2,3}`, `datalake.agh.edu.pl/role=database` on the db pool |
| CloudNativePG 1.30 | Postgres operator our `Cluster` CRs (below) run under |
| External Secrets Operator 2.8 | The `platform` `ClusterSecretStore` (Kubernetes provider, `remoteNamespace: eso-secret-source`) our `ExternalSecret` objects target |
| Argo Workflows v4.0.8 | Scoped to `datalake-orchestration`; runs our CronWorkflows (below) |
| kube-prometheus-stack | Their Prometheus adopts our `PrometheusRule`/`PodMonitor` objects via label `release: monitoring`, any namespace (`ruleNamespaceSelector: {}`) |
| Dex | Standalone OIDC bridge — not deployed from this repo in prod at all |
| cert-manager, Traefik, Cinder CSI | Cluster plumbing we consume (StorageClasses, ingress) but never define |
| The tenant AppProject + ApplicationSet | Generates our three Applications from `envs/prod/<tier>` on branch `main` (drySource) |

**We own** (this repo, three tier charts):

| Component | Role | Defined in |
|---|---|---|
| `datalake-storage` chart | `landing-db`/`lakekeeper-db` CNPG `Cluster` CRs, hand-authored `PodMonitor`s (CNPG's own toggle can't carry `release: monitoring`), storage-tier alert rules, `ExternalSecret`s | `envs/prod/storage/` |
| Lakekeeper | Iceberg REST catalog, chart **dependency** of `datalake-storage` (not a standalone Application — the AppProject shape doesn't allow one) | `envs/prod/storage/Chart.yaml` |
| `datalake-compute` chart | Trino placement, resources, catalog config, `ExternalSecret`s | `envs/prod/compute/` |
| Trino | SQL query layer, chart **dependency** of `datalake-compute`; catalogs `lake` (Iceberg REST, conditional on `values-lake.yaml` pending S3 creds) and `landing` (PostgreSQL federation, read-only `trino_ro` role) | `envs/prod/compute/Chart.yaml` |
| `datalake-orchestration` chart | Ingest CronWorkflows (nightly/sitesonar/maintenance), workflow RBAC, orchestration-tier alert rules, kind-only `hello` canary and `echoReceiver`, `ExternalSecret`s | `envs/prod/orchestration/` |
| `alice-ingest` (ingest image) | `run-nightly`, `run-sitesonar`, `run-retention`, `check-freshness`, `run-maintenance`, `run-trino-maintenance`, `apply-views` | `ingest/`, `docs/runbooks/ingestion-pipeline.md` |
| `tools/extract_training_data.py` | Consumer-side DuckDB extraction recipe: reads `lake.contract.*` (falls back to `lake.alice.*` with a disambiguated reason — DuckDB's Iceberg extension can't read REST-catalog views), writes Parquet + a provenance manifest | `tools/`, `docs/runbooks/ml-extraction.md` |

**kind-only** (never reaches prod — `environments/kind/infra/`, applied
directly by `hack/kind-up.sh`, not hydrated): the operators the platform
already runs for real (CNPG, ESO, Argo Workflows, kube-prometheus-stack),
MinIO (Cyfronet S3 replaces it once G2 credentials land), and a
mock-connector Dex (proves the OIDC issuer + a relying party, Grafana, work
end-to-end — the real GitHub-org connector is entirely the platform's
concern in prod, not something this repo configures).

## Current status

Three tier charts render standalone and pass `make lint` (helm dependency
build + `hack/check-seam.sh` + helm lint + helm template/kubeconform,
prod and kind values) from a clean clone. The kind harness mirrors the
tenant topology (three Applications, same charts as prod, `values-kind.yaml`
overrides) and passes `make kind-verify` end to end. **Not yet live in
prod**: the platform's ApplicationSet still needs to switch to
`spec.sourceHydrator`, gated on a deploy key handoff, plus several other
open dependencies (S3 credentials, Slack webhook, ResourceQuotas, a missing
database node taint). Full list, live-verified: `docs/runbooks/
bootstrap-cyfronet.md` §5.
