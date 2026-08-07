# Runbook: adding a component

Two recipes, depending on what you're adding. Both are additive — nothing
in either recipe requires touching an unrelated tier's files. Pick a tier
first regardless of recipe: `envs/prod/storage` (landing/catalog databases,
Lakekeeper), `envs/prod/compute` (Trino), or `envs/prod/orchestration`
(CronWorkflows, RBAC, alert rules). See `README.md`'s Ownership table for
which tier a given piece of functionality belongs to.

**Global constraint, every recipe, no exceptions:** the platform's
AppProject bounds us with `clusterResourceWhitelist: []` — every rendered
object must be namespaced. No CRDs, ClusterRoles, ClusterRoleBindings,
StorageClasses, Namespaces, PriorityClasses, ValidatingAdmissionPolicies. If
what you're adding needs a cluster-scoped resource, stop — that's a platform
conversation (`alice-k8s-gitops` issue #3 is the live thread), not something
to work around from this repo.

## Recipe A: an authored resource (our own manifest)

Use this when the resource is *ours* — something a tier chart should own
and render directly, the way `envs/prod/storage/templates/landing-db.yaml`
owns the `mon-data` CNPG `Cluster` or
`envs/prod/orchestration/templates/ingest-cronworkflows.yaml` owns the
ingest CronWorkflows. No new chart dependency, no new Application — it
rides on whichever of the three tier Applications already syncs that
namespace.

1. **Write the template.** New file under `envs/prod/<tier>/templates/`.
   Reference tunables through `.Values`, not hardcoded — follow
   `landing-db.yaml` or `lakekeeper-db.yaml` for the values-driven style
   already in use. Hardcode the tier's own namespace in `metadata.namespace`
   (the convention every hand-authored template in this repo follows,
   deliberately not relying on Helm's `.Release.Namespace` — see §2's note
   below on why that matters for chart *dependencies*, though it doesn't
   affect an authored template either way since it self-declares).
2. **Add the values keys.** Base defaults in `envs/prod/<tier>/values.yaml`;
   kind-only overrides in that tier's `values-kind.yaml` only where they
   actually differ (see `helloCron.enabled`: `false` base / `true` on kind,
   `envs/prod/orchestration/values.yaml` + `values-kind.yaml`, as one
   existing example). Every container needs `resources.requests` — the
   platform withholds ResourceQuotas until we hold this line consistently
   (`docs/runbooks/bootstrap-cyfronet.md` §5).
3. **Never render a `kind: Secret`.** If the resource needs a credential,
   follow the existing pattern: an `ExternalSecret` against the platform's
   `platform` `ClusterSecretStore`, referenced by name/key only from the
   chart (see the `lakekeeper-pg-encryption` `ExternalSecret`,
   `envs/prod/storage/templates/external-secrets.yaml`, and its comment for
   the reasoning — ArgoCD has pruned chart-rendered secrets before, and a
   `helm lookup`-based generation approach is incompatible with GitOps
   render, argoproj/argo-cd#5202). On kind, `hack/kind-up.sh` provisions the
   backing remote Secret directly; on the tenant cluster it's the platform's
   or owner's to provision in `eso-secret-source`
   (`docs/runbooks/bootstrap-cyfronet.md` §5) — same `ExternalSecret`
   object either way, only who populates the remote Secret differs.
4. **If it needs `release: monitoring`:** any `PrometheusRule` or
   `ServiceMonitor`/`PodMonitor` must carry that label or the platform's
   Prometheus (`ruleSelector`/`podMonitorSelector: {matchLabels: {release:
   monitoring}}`) never adopts it — see
   `envs/prod/storage/templates/monitoring.yaml` for CNPG's hand-authored
   `PodMonitor` pattern (CNPG's own `enablePodMonitor` toggle cannot carry
   this label, confirmed against cloudnative-pg/cloudnative-pg discussion
   #1617 — write the `PodMonitor` by hand if wrapping an operator-managed
   CR that has the same limitation).
5. **Push and let the hydrator do the rest** (once the platform's
   ApplicationSet is switched to `sourceHydrator` —
   `docs/runbooks/bootstrap-cyfronet.md` §2 for current status). `git push`
   to `main`; the hub's commit-server renders each tier's chart + its
   `values.yaml` and pushes to `environments/prod-next`; a PR into
   `environments/prod` is the review gate; merging lets the tier's
   Application sync. No manual `kubectl apply` for the new resource itself.

## Recipe B: an upstream chart you don't own

Use this when the component ships as its own Helm chart from a third-party
repo — the way `envs/prod/storage/Chart.yaml` vendors Lakekeeper and
`envs/prod/compute/Chart.yaml` vendors Trino, as `dependencies:` inside the
*tier's own* `Chart.yaml`. This is **not** a new ArgoCD Application — the
three-Application, `clusterResourceWhitelist: []` shape doesn't allow a
fourth for a third-party chart the way the old `apps/infra/*.yaml` pattern
did. Everything upstream renders as part of whichever tier chart declares
the dependency.

1. **Verify first, before touching values — this step blocks.** Pull the
   chart at the version you intend to pin and render it standalone
   (`helm template <chart-repo>/<chart> --version <x> --include-crds`, or
   pull it into a scratch dir and run `hack/check-seam.sh` against it once
   vendored). `hack/check-seam.sh`'s real gate (independent-review finding,
   2026-08-07) is that EVERY rendered object must carry `metadata.namespace`
   -- a cluster-scoped kind (CRD, ClusterRole, StorageClass, or anything
   else, named or not) always fails this, no list to consult. If the chart
   emits a *namespaced* object that simply omits `metadata.namespace` in
   its own rendered manifest (some charts' helm-test hooks do this,
   relying on apply-time namespace) that also fails here and needs adding
   to the script's small, commented `ALLOWED_NAMESPACELESS` array -- not
   automatically a stop-and-report case the way a genuinely cluster-scoped
   kind is. If it's the latter -- stop and report it. That's a
   platform-side decision (does the platform install this operator
   instead, does it get a whitelist exception), not something to route
   around by hand-editing the rendered output. Lakekeeper
   0.11.0 and Trino 1.42.2 were both verified namespace-only this way before
   being vendored (task-2-report.md has the full verification transcript).
2. **Add it to the tier's `Chart.yaml` `dependencies:`, pinned, with a
   verified-on-date comment** — no `latest`, no floating ranges, the same
   convention every existing pin in this repo follows. Confirm the version
   exists in the repo's index first (`helm repo add <name> <url> && helm
   search repo <name>/<chart> --versions`). Commit the regenerated
   `Chart.lock` (vendored, exact pin) — `charts/*.tgz` itself is gitignored
   (`**/charts/*.tgz`), fetched fresh by `helm dependency build` on every
   clone/CI run/hydration, never committed.
3. **Port values under the dependency's own key** — no alias unless the
   `Chart.yaml` entry sets one, so the values key is just the dependency
   name (see `envs/prod/compute/values.yaml`'s top-level `trino:` block).
   Two things every dependency in this repo has needed:
   - **`fullnameOverride`**, so the in-cluster Service name is short and
     predictable regardless of the Helm release name the platform's
     ApplicationSet ends up using — see `trino.fullnameOverride: trino` and
     `lakekeeper.fullnameOverride: lakekeeper`. Anything referencing this
     component's Service DNS (another tier's catalog config, an env var)
     depends on this being stable.
   - **`helm.namespace` if this Application's `sourceHydrator` is
     ArgoCD-hydrated.** Live finding, Task 4: `destination.namespace` alone
     does not make a dependency chart's own unnamespaced templates (a
     migration Job, e.g.) land in the right namespace during hydration on
     this ArgoCD version — `sourceHydrator.drySource.helm.namespace` has to
     be set explicitly per tier. See
     `docs/runbooks/bootstrap-cyfronet.md` §2 for the full reproduction;
     this is set on the Application side (`environments/kind/apps/*.yaml`
     on kind), not in the tier chart itself, but know it exists before
     debugging a hung sync.
4. **Placement, resources, secrets, monitoring** — same requirements as
   Recipe A steps 2-4, applied through the dependency's own values schema
   rather than a template you write (`nodeSelector`/`tolerations`,
   `resources.requests`, `envFrom`/`ExternalSecret` for credentials,
   `release: monitoring` on anything Prometheus-facing the chart exposes a
   toggle for).
5. **If the platform already provides this operator** (CloudNativePG,
   External Secrets, Argo Workflows, kube-prometheus-stack, Dex — see
   `README.md`'s Ownership table), you don't need this recipe at all in
   prod. It's still something kind has to stand up for itself to mirror
   what the platform gives us for free — add it under
   `environments/kind/infra/` following the existing pattern there
   (`hack/kind-up.sh` applies everything in that directory before the three
   tier Applications sync). Prod never sees it.

## Both recipes end the same way

1. **`make lint`** — `helm dependency build` (where a `Chart.lock` exists) +
   `hack/check-seam.sh` + `helm lint` + `helm template`/`kubeconform` (strict
   schema flags, `--include-crds`) for all three tier charts, prod values
   and kind values, plus every kind-only Application/AppProject manifest.
   Fix everything it flags before pushing — it's the exact gate CI runs on
   every push/PR (`.github/workflows/lint.yaml`), no drift between local and
   CI by construction (CI calls the same `make lint`).
2. **`make kind-up && make kind-verify`** — full acceptance against the kind
   mirror (three Applications, same charts, `values-kind.yaml` overrides):
   every Application Synced/Healthy, plus whatever component-specific
   hard-gate probe already exists. If the new component needs its own
   liveness proof (the way Lakekeeper gets a REST probe and Argo Workflows
   gets a manual workflow run), add a hard-gate block to
   `hack/kind-verify.sh` following the existing pattern: a real command, an
   explicit `if`/`else` with `exit 1` on failure — never a bare
   `cmd && echo OK` under `set -e`, which silently falls through on a
   non-zero exit instead of failing the script (a review finding fixed more
   than once in this repo's history).
3. **Do not run step 2 against a kind cluster someone else is actively using
   for a review or debugging session** — `make kind-down`/`make kind-up`
   destroys and rebuilds it. Coordinate first.
