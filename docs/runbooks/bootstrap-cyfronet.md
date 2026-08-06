# Runbook: tenant deployment on `alice-k8s-datalake`

Historical name (`bootstrap-cyfronet.md`) kept so existing links don't
break. The content is not a bootstrap anymore — the platform team owns and
already runs the cluster. This is now the tenant runbook: what they own,
what we own, how a commit becomes a live change, how to roll one back, and
what to do when a sync goes wrong. Read `README.md`'s Layout/Ownership
tables first if this is unfamiliar territory.

Everything under "Current open dependencies" (§5) was live-verified against
the real `alice-k8s-datalake` cluster on 2026-08-06. Re-check before relying
on any of it — this is a moving target while the platform side is mid-setup.

## 1. The seam: what the platform owns, what we own

The platform runs Argo CD on a separate **hub** cluster and manages
`alice-k8s-datalake` remotely (repo `github.com/agh-alice/alice-k8s-gitops`,
private). We never get hub access — no ArgoCD UI, no CLI, no credentials.
What we get is:

- **kubectl to the tenant cluster itself** (`alice-k8s-datalake`), scoped to
  observing our own three namespaces and whatever the platform's RBAC
  allows — enough to read pods, events, `Cluster`/`ExternalSecret` status,
  logs. Not enough to force a sync, pause auto-sync, or see anything
  ArgoCD-side.
- **git**, the only real control surface: what's live is *only* ever a
  function of what's committed (§2).

**Platform-owned** (their cluster, their operators, applied for real —
verified live 2026-08-06 unless noted): node pools and placement labels;
CloudNativePG 1.30, External Secrets 2.8, Argo Workflows v4.0.8 (scoped to
`datalake-orchestration` via `controller.workflowNamespaces` — the chart's
own default is `["default"]`, which would have silently never run our
CronWorkflows had the platform not overridden it); kube-prometheus-stack;
Dex; cert-manager; Traefik; Cinder CSI and its StorageClasses; the three
tenant namespaces (`datalake-storage`, `datalake-compute`,
`datalake-orchestration`, labels `tenant: datalake` +
`datalake.agh.edu.pl/{tier,placement}`); the `platform` `ClusterSecretStore`
(ESO Kubernetes provider, `remoteNamespace: eso-secret-source`); their
Alertmanager (adopts our `PrometheusRule`s via `release: monitoring`,
`ruleNamespaceSelector: {}` — label-only, any namespace); the AppProject and
the `platform/datalake/applicationset.yaml` that generates our three
Applications.

**We own**: the three tier Helm charts (`envs/prod/{storage,compute,
orchestration}`); Lakekeeper and Trino as chart *dependencies* inside the
storage/compute charts (not standalone Applications — see below for why);
the `landing-db`/`lakekeeper-db` CNPG `Cluster` CRs; the ingest CronWorkflows
and their namespaced RBAC; hand-authored `PrometheusRule`/`PodMonitor`
objects; `ExternalSecret` objects (the *pointer*, not the credential
material behind it — see §5's per-item split of who provisions what);
Iceberg contract views (applied at runtime by `alice-ingest apply-views`,
not a Kubernetes object at all).

**The seam's enforceable test**: `clusterResourceWhitelist: []` on the
platform's AppProject. Every object every tier chart renders must be
namespaced — no CRDs, ClusterRoles, ClusterRoleBindings, StorageClasses,
Namespaces, PriorityClasses, ValidatingAdmissionPolicies, or any other
cluster-scoped kind. This is why Lakekeeper and Trino had to become chart
*dependencies* (`envs/prod/{storage,compute}/Chart.yaml`'s `dependencies:`,
vendored via `Chart.lock`) instead of their own ArgoCD Applications the way
`apps/infra/*.yaml` used to wrap them — the three-Application, zero-extra-
cluster-resource shape doesn't allow a fourth. Three independent things
enforce the same rule, on purpose: `hack/check-seam.sh` locally, the same
script as a required PR check in CI (`.github/workflows/lint.yaml` →
`make lint`), and the AppProject itself at sync time on the real cluster.
The first two catch a violation before merge; the third is what actually
protects the shared cluster if the first two are ever wrong.

## 2. How a change reaches the cluster

```
commit to main (dry source)
  -> platform's Argo CD hub, Source Hydrator commit-server
     renders `helm template` per tier (values.yaml only -- no
     values-kind.yaml, that's kind-only)
  -> pushes rendered manifests to environments/prod-next  (hydrateTo)
  -> a human opens a PR: environments/prod-next -> environments/prod
     (review the RENDERED manifest diff, not the Helm template source)
  -> PR merges
  -> datalake-{storage,compute,orchestration} Applications
     (automated selfHeal, on the hub) sync from environments/prod HEAD
```

Three tier charts, three Applications, one dry source path each:
`envs/prod/storage`, `envs/prod/compute`, `envs/prod/orchestration` on
branch `main` — exactly what the platform's ApplicationSet expects
(confirmed against our own kind mirror, `environments/kind/apps/*.yaml`,
which uses the identical `drySource.path`/`targetRevision: main` shape,
differing only in `syncSource.targetBranch` and the extra
`values-kind.yaml`). **This repo's layout does not need to change** for
their ApplicationSet to work as designed.

**This is not live yet.** `environments/prod` doesn't exist as a branch
(checked 2026-08-06, GitHub API branch list: only `main` and
`environments/kind`), and the three tenant namespaces on the live cluster
are empty (`kubectl get all -n datalake-storage` → nothing) — the platform's
ApplicationSet still generates plain `spec.source` Applications, not
`spec.sourceHydrator`. Switching that is blocked on their side by their
issue #7 (§5, deploy key). Nothing in this repo is broken or waiting on us;
the pipe is built and unverified end-to-end because the other end isn't
connected yet.

**Two things the platform team needs to get right on their side, that we
can't fix from here** (surface both before they template the
`sourceHydrator` block, not after a mysterious first-sync failure):

1. **`drySource.helm.namespace` must be set explicitly per tier**, not left
   to default from `destination.namespace`. Live-verified on kind (Task 4,
   same ArgoCD version, same Source Hydrator rc2/beta channel): the
   Application CRD's schema documents `sourceHydrator.drySource.helm.
   namespace` as defaulting to the destination namespace when empty, but
   that fallback does not actually work at this ArgoCD version — the
   hydrator's own rendered `manifest.yaml` had `namespace: argocd` (the
   Application's *own* namespace) baked into Lakekeeper's `db-migration`
   Job, and the sync hung forever waiting on a hook pod that could never
   start (it referenced Secrets living in `datalake-storage`, not
   `argocd`). Every hand-authored template in our charts hardcodes its own
   namespace and is unaffected, but Lakekeeper's and Trino's *upstream*
   templates rely on normal Helm release-namespace semantics for anything
   that doesn't self-declare one — see `environments/kind/apps/
   datalake-storage.yaml`'s header comment for the full reproduction. Our
   kind mirror sets `drySource.helm.namespace` explicitly per tier as the
   fix; the real ApplicationSet needs the same.
2. **Chart repo egress + AppProject `sourceRepos`.** `envs/prod/storage`
   and `envs/prod/compute` declare remote Helm dependencies
   (`https://lakekeeper.github.io/lakekeeper-charts/`,
   `https://trinodb.github.io/charts`) that `helm dependency build` must
   fetch at render time — `charts/*.tgz` is gitignored, not vendored in
   Git. The hub's repo-server needs network egress to both, and if their
   tenant AppProject enforces a `sourceRepos` allowlist the way our own
   kind-only Project does (`environments/kind/project.yaml`), both URLs
   need to be on it. **Unverified from our side** — `alice-k8s-gitops` is
   private and we could not inspect their AppProject this session. Confirm
   before the first real sync rather than debugging a dependency-fetch
   failure blind.

## 3. How to roll back

Git is the only control surface (§1) — there is no `kubectl apply` shortcut
and no hub access to force or pause a sync.

1. **Revert in both places, in this order.** The hydrator continuously
   re-renders `environments/prod-next` from whatever `main` says, on every
   push that touches a watched path — reverting only the merge on
   `environments/prod` gets silently re-proposed on the very next hydration
   cycle. Revert (or fix forward) the offending commit **on `main` first**,
   then revert the corresponding merge commit **on `environments/prod`**
   (a second PR, same review gate as any other change reaching prod).
   Skipping the `main` revert means the bad state keeps coming back.
2. **Check what the revert deletes before merging it — prune is armed.**
   `syncPolicy.automated: {prune: true, selfHeal: true}` with
   `ServerSideApply=true`: anything that disappears from the rendered
   manifest is deleted from the live cluster automatically, no confirmation
   step. A revert that removes a resource — most dangerously, the storage
   tier's `lakekeeperDb`/`landingDb` CNPG `Cluster` blocks — deletes that
   `Cluster` and its PVCs live. **Never blind-revert the storage tier.**
   Diff what the rollback PR actually removes; if it touches a `Cluster`
   object, treat it as a data-loss event and coordinate before merging, not
   after.
3. **Don't hand-fix live objects with kubectl.** selfHeal reconciles on a
   short loop (minutes) and reverts any manual edit back to whatever the
   synced branch says — a kubectl edit doesn't fix anything, it just adds a
   race you'll lose.

## 4. When a sync goes wrong

No hub access means no ArgoCD UI/CLI to read sync state directly — infer it
from the tenant cluster's own object state and from GitHub.

- **General triage:**
  `kubectl get externalsecret,cluster.postgresql.cnpg.io,deployment,
  cronworkflow -n datalake-<tier>`, then `kubectl describe` on anything not
  Ready/Synced, then `kubectl get events -n datalake-<tier>
  --sort-by=.lastTimestamp`.
- **`ExternalSecret` never produces its target `Secret`:** check the remote
  Secret actually exists in `eso-secret-source`
  (`kubectl -n eso-secret-source get secret <name>`) and that its keys match
  our `remoteRef.property` values exactly. ESO fails loud here — no
  `optional: true` anywhere in this repo's `ExternalSecret`s — so a missing
  remote key means the target `Secret` never appears at all (pods relying on
  it sit in `CreateContainerConfigError`), not a silently-empty one.
- **CronWorkflow pod stuck `Pending`:** almost always placement. Storage's
  `pool: db` `nodeSelector` needs that label present on a schedulable node;
  compute's coordinator carries an explicit `nodeAffinity` exclusion on
  `datalake.agh.edu.pl/role=database` (§5, the missing-taint workaround) —
  if that label ever moves to different nodes without updating the chart,
  the coordinator has nowhere to schedule.
- **Trino serves `landing`/`system`/`tpch`/`tpcds` but no `lake` catalog:**
  expected, not a bug, until G2 (S3 credentials, §5) lands — the `lake`
  catalog only exists once `values-lake.yaml` is layered on top of
  `values.yaml` (`envs/prod/compute/values.yaml`'s Decision 2 comment: a
  Trino that's honestly missing one catalog is safer than one serving a
  catalog pointed at placeholder S3 values that would fail every query).
- **Nothing syncs at all, `environments/prod` doesn't exist:** the
  platform's ApplicationSet hasn't been switched to `sourceHydrator` yet
  (§2) — this is their side, not ours to debug further.
- **Escalation:** `alice-k8s-gitops` issue #3 is the live coordination
  thread this whole plan was negotiated over (node facts, quotas, storage
  class trade-offs, the taint regression) — continue there, or open a new
  issue referencing it. There is no direct channel to the hub cluster.

## 5. Current open dependencies (live-verified 2026-08-06)

- **Deploy key — the one thing blocking everything else (their issue #7).**
  The owner generates an SSH keypair, adds the public half as a
  **write-capable** GitHub deploy key on `agh-alice/datalake-v2` (write
  access is required: the hydrator pushes rendered manifests into
  `environments/prod-next` in *this* repo), and hands the private key to
  the platform team out-of-band for them to register as their
  ApplicationSet's `sourceHydrator` credential. Nothing in §2 can go live
  until this lands.

  **Also flag before relying on the PR gate in §2 as real:** this repo's
  branch rulesets (`gh api repos/agh-alice/datalake-v2/rulesets`, checked
  2026-08-06) still carry a PR-required rule named `cyfronet-pr-gate`
  targeting `refs/heads/environments/cyfronet` — a branch name this plan
  retired. **`environments/prod` and `environments/prod-next` currently
  have no ruleset coverage at all** (only `main` has a bare
  non-fast-forward/no-deletion rule, no PR requirement). Until the owner
  repoints or replaces that ruleset to target `environments/prod`, a push
  with write access — including the hydrator's own deploy key once it
  exists — could land directly on `environments/prod` with no PR at all,
  silently defeating the human-review gate this whole design is built
  around.

- **S3 credentials (Plan 4 gate G2).** Endpoint, region, access key, secret
  key, and bucket+key-prefix for Cyfronet/platform object storage are all
  still TO VERIFY (`envs/prod/orchestration/values.yaml`'s `ingest-env`
  comment block; `envs/prod/compute/values-lake.yaml`). Confirmed live
  2026-08-06: `eso-secret-source` has zero Secrets provisioned on the
  tenant cluster. Until these land: Trino has no `lake` catalog (§4), and
  nightly/sitesonar ingestion cannot write to Iceberg in prod at all.

- **Slack webhook (gate G3).** Alertmanager's `slack-datalake` receiver
  needs a real incoming-webhook URL, delivered the same `ExternalSecret`
  way as everything else here, once the owner provides one. The kind-only
  `echoReceiver` stand-in (`envs/prod/orchestration/values-kind.yaml`)
  proves the whole route/group/inhibit/receiver pipeline end-to-end without
  it; prod inherits the base `echoReceiver.enabled: false` and simply has no
  working receiver until this lands.

- **ResourceQuotas (their issue #8).** Not yet applied — confirmed live
  2026-08-06, `kubectl get resourcequota -A` shows none in any `datalake-*`
  namespace. Our per-tier requests are sized and were reported against the
  agreed figures and fit comfortably (all figures requests.cpu /
  requests.memory / limits.memory): storage 2.2 CPU/7.25Gi/14.5Gi against a
  12 CPU/48Gi ask, compute 2 CPU/8Gi/10Gi against 12 CPU/28Gi, orchestration
  1.75 CPU/3.5Gi/7Gi (worst-case concurrent DAG overlap) against 8 CPU/16Gi
  (full arithmetic: `.superpowers/sdd/progress.md`, Plan 5 Task 3 entries).
  Nothing in this repo enforces those totals until the platform applies the
  actual quota objects — a values change that pushes real usage above the
  agreed figures today would only be caught by manual review.

- **The missing database taint.** `db-0`/`db-1`/`db-2` carry the
  `pool=db`/`cpu=x86-64-v3`/`datalake.agh.edu.pl/role=database` labels but
  — confirmed live 2026-08-06,
  `kubectl get nodes -l pool=db -o jsonpath='{.items[*].spec.taints}'` →
  empty on all three — **not** the `dedicated=database:NoSchedule` taint
  the original platform brief described as already present. Reported as a
  platform-side regression on issue #3 (present 2026-08-05, gone
  2026-08-06). The storage tier's `tolerations` block is a harmless no-op
  until the taint returns. The compute tier compensates with an explicit
  `nodeAffinity` exclusion on `datalake.agh.edu.pl/role=database`
  (`envs/prod/compute/values.yaml`) so a Trino coordinator can't land on a
  database node and compete with Postgres for memory/IO in the meantime.
  Remove that compensating affinity once the platform confirms the taint is
  back — leaving it is harmless, but it's a workaround for a gap that
  shouldn't exist.

- **Storage class trade-off, unresolved.** No live StorageClass is both the
  fast default and expandable — confirmed live 2026-08-06: `cinder-perf`
  (default, fastest, `n.performance`) has `allowVolumeExpansion: false`;
  `csi-cinder-sc-retain` is expandable but is the untyped/slow cloud-default
  tier, and volumes keep their provisioned type forever (no migrating a live
  PVC to a different class later). We chose to size the full working set up
  front on `cinder-perf` rather than trade away IOPS for future
  expandability: landing DB 3×40Gi (120Gi), catalog DB 2×5Gi (10Gi) — see
  `envs/prod/storage/values.yaml`'s comments for the reasoning. Revisit the
  sizing if the platform ever flips `allowVolumeExpansion` on `cinder-perf`.

## 6. Retired — don't go looking for these

- **`make sops-setup` / `.sops.yaml` / the SOPS-encrypted deploy-key flow.**
  This existed because we used to run our *own* ArgoCD on the target
  cluster and had to get a repo-write credential into it ourselves. The
  platform's hub-side ArgoCD makes that the platform's problem now — a real
  deploy key still gets generated (§5), it's just handed over out-of-band
  instead of encrypted into this repo. `make sops-setup` still runs if
  invoked but nothing in the current flow calls for it.
- **`apps/project.yaml` / `apps/infra/*.yaml`.** Retired in Task 4 — the
  platform owns the real AppProject and every operator that used to live
  under `apps/infra/` (CNPG, ESO, kube-prometheus-stack, Dex, Argo
  Workflows). Lakekeeper and Trino moved to chart dependencies (§1), not to
  a platform-owned operator.
- **Dex GitHub-org OIDC configuration.** Dex is platform-owned in prod —
  not deployed from this repo at all (`environments/kind/infra/dex.yaml` is
  kind-only, standing up a mock connector purely so kind's own OIDC-relying
  parties, i.e. Grafana, have something to test against). **Lakekeeper
  currently runs open-auth in every environment including prod**
  (`envs/prod/storage/values.yaml`'s `authz.backend`/`auth.oauth2` comment —
  both unset, ported unchanged from the retired standalone Application).
  This is a known, explicitly-flagged gap, not an oversight — Task 2 ported
  existing values and did not redesign the security posture; it's out of
  Plan 5's scope. If Lakekeeper ever needs to authenticate against the
  platform's real Dex, that's new work, not a resumption of this section.

## 7. Verify

There is no tenant equivalent of `make kind-verify` yet. Options, same as
before: extend `hack/kind-verify.sh` into a script parameterized by
kubeconfig/namespace-prefix so it can point at either environment, or run
its component probes by hand against the tenant cluster's context. At
minimum, after a sync:

```bash
kubectl get externalsecret,cluster.postgresql.cnpg.io -n datalake-storage
kubectl get deployment -n datalake-compute
kubectl get cronworkflow -n datalake-orchestration
```

## Trino sizing note (resolved, kept for history)

The kind harness pins Trino chart 1.40.0/appVersion 476 because the kind
host's virtualized CPU lacks AVX2/BMI2 (x86-64-v3). Real compute-tier nodes
(`ccs1.large`) are confirmed x86-64-v3, so `envs/prod/compute` pins the
newest chart (1.42.2/appVersion 480) directly — this was resolved during
Plan 5 Task 2, not deferred. No further re-evaluation needed unless the
platform changes the compute node flavor.
