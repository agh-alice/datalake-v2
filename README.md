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
- Rendered branches `environments/kind`, `environments/cyfronet(-next)` are machine-owned.

## Environments
| env | purpose | bootstrap |
|-----|---------|-----------|
| kind | local/CI verification | `make kind-up && make kind-verify` |
| cyfronet | production | `docs/runbooks/bootstrap-cyfronet.md` |
