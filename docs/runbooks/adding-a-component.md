# Runbook: adding a component

Two recipes, depending on what you're adding. Both are additive — nothing
in either recipe requires touching an unrelated component's files.

## Recipe A: an authored resource (our own manifest)

Use this when the resource is *ours* — something this chart should own and
render directly, the way `chart/templates/landing-db.yaml` owns the
`mon-data` CNPG `Cluster` or `chart/templates/hello-cronworkflow.yaml` owns
the `hello` CronWorkflow. No new ArgoCD Application, no new Helm repo — it
rides on the existing `datalake-kind` / `datalake-cyfronet` Source Hydrator
Applications.

1. **Write the template.** New file under `chart/templates/`. Reference any
   tunables through `.Values`, not hardcoded — follow
   `chart/templates/landing-db.yaml` or `chart/templates/lakekeeper-db.yaml`
   as examples of the values-driven style already in use.
2. **Add the values keys.** Base defaults in `chart/values.yaml`; per-env
   overrides only where they actually differ, in `chart/values-kind.yaml` /
   `chart/values-cyfronet.yaml` (see `landingDb.instances`: 1 on kind vs. 3 on
   cyfronet's base `values.yaml`, as one existing example of this pattern).
   If the resource lives in a namespace that doesn't already exist, add it to
   the `namespaces` list in `chart/values.yaml` — `chart/templates/
   namespaces.yaml` renders every entry.
3. **Never render a `kind: Secret`.** If the resource needs a credential,
   follow the existing pattern: harness-provisioned on kind
   (`hack/kind-up.sh`), `ExternalSecret` on cyfronet
   (`docs/runbooks/bootstrap-cyfronet.md` §5), referenced by name/key only
   from the chart — never templated as a Secret object in this repo (see the
   `lakekeeper-pg-encryption` comment block in `chart/values.yaml` for the
   reasoning: ArgoCD has pruned chart-rendered secrets before, and Argo's own
   `helm lookup`-based secret generation is incompatible with GitOps render).
4. **Push and let the hydrator do the rest.** `git push` to `main`; the
   commit-server renders `chart/` + each env's values and pushes to the
   env's hydrated branch (`environments/kind`, `environments/cyfronet-next`
   -> promoted to `environments/cyfronet` per the PR gate). No manual
   `kubectl apply` step for the new resource itself — it arrives through the
   Application that's already syncing `chart/`.

## Recipe B: an upstream operator (a Helm chart you don't own)

Use this when the component ships as its own Helm chart from a third-party
repo — the way `apps/infra/cloudnative-pg.yaml`, `apps/infra/lakekeeper.yaml`,
and `apps/infra/argo-workflows.yaml` each wrap one upstream chart. This gets
its own ArgoCD Application, separate from the hydrated `chart/`.

1. **Find the chart's Helm repo and pin an exact version.** No `latest`, no
   floating ranges — `targetRevision` is a literal version string, the same
   convention every existing `apps/infra/*.yaml` follows. Confirm the
   version actually exists in the repo's index before writing it down
   (`helm search repo <repo>/<chart> --versions`).
2. **Write `apps/infra/<name>.yaml`** — an ArgoCD `Application`, `project:
   datalake`, `source.repoURL`/`chart`/`targetRevision` pinned, a `sync-wave`
   annotation if this component provides CRDs another app's chart consumes
   (see the Task 8 runbook note in `chart/templates/argocd-servicemonitor.
   yaml` and `environments/kind/argocd-values.yaml`: **sync-wave does not
   mechanically enforce this ordering across standalone Applications** — it's
   documentation of intent, not a guarantee; `syncPolicy.automated.selfHeal:
   true` is what actually makes a CRD-dependent render converge once the CRD
   exists). `destination.namespace` your own; `syncPolicy.automated:
   {prune: true, selfHeal: true}` and `syncOptions: [CreateNamespace=true]`
   (add `ServerSideApply=true` if the chart's CRDs are large — CNPG and
   kube-prometheus-stack both need it; `apiextensions.k8s.io` client-side
   apply hits the annotation size limit on big CRDs).
3. **Extend the AppProject's `sourceRepos`.** Add the new chart's repo URL to
   `apps/project.yaml`'s `spec.sourceRepos` — byte-identical to the URL used
   in `apps/infra/<name>.yaml`'s `source.repoURL` (every existing infra app
   does this; a mismatch here is a common copy-paste bug and ArgoCD will
   refuse to sync with a project-policy violation).
4. **Apply it.**

   ```bash
   kubectl apply -f apps/project.yaml
   kubectl apply -f apps/infra/
   ```

## Both recipes end the same way

1. **Extend `EXPECTED_APPS`** in `hack/kind-verify.sh` — add the new
   Application's `metadata.name`. If it provides CRDs another app consumes,
   place it before that consumer in the array (dependency-first order, per
   the Task 8 comment at the top of that file) — this doesn't change
   correctness (selfHeal converges regardless of check order) but keeps the
   verify loop's early failures meaningful instead of racing a dependency
   that just hasn't synced yet.
2. **`make lint`** — helm lint + template render + kubeconform for both
   envs, plus kubeconform over every `apps/`/`environments/*/apps/`
   manifest. Fix everything it flags before syncing; it's the same gate CI
   runs on every push/PR (`.github/workflows/lint.yaml`).
3. **`make kind-verify`** — full acceptance: every `EXPECTED_APPS` entry
   Synced/Healthy, plus whatever component-specific hard-gate probe already
   exists. If the new component needs its own liveness proof (the way
   Lakekeeper gets a REST probe and Argo Workflows gets a manual workflow
   run), add a hard-gate block following the existing pattern: real command,
   `if`/`else` with an explicit `exit 1` on failure — never a bare `A && echo
   OK` under `set -e`, which silently falls through to the next line on a
   non-zero exit without failing the script (a review finding fixed more
   than once in this repo's history; see the comments in `hack/kind-verify.sh`
   itself).
