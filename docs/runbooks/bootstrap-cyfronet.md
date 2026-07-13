# Runbook: bootstrap ArgoCD on Cyfronet (production)

Mirrors `hack/kind-up.sh`, adapted for a real cluster: real secrets instead
of harness-generated throwaway ones, a write-capable SSH deploy key instead
of a GitHub PAT, and a PR gate on rendered changes instead of direct sync.
Read `README.md`'s Layout/Environments tables first if this is unfamiliar
territory.

## 0. Prerequisites

- `kubectl` context pointed at the target Cyfronet cluster.
- `helm`, `age`, `sops` installed locally.
- Repo cloned, on `main`, working tree clean.
- A GCP service-account key (JSON) with `roles/secretmanager.secretAccessor`
  on project `tensile-ethos-474915-f7`, for ESO's `ClusterSecretStore`.

## 1. Install ArgoCD (hydrator + commit-server on)

`environments/cyfronet/argocd-values.yaml` is the kind bootstrap values
(`environments/kind/argocd-values.yaml`) minus `server.insecure` (a real
cluster terminates TLS deliberately, not via ArgoCD's insecure flag), plus
baseline resource requests for `controller`/`repoServer`/`server`/
`commitServer`. It keeps the same `hydrator.enabled`, `commitServer.enabled`,
and `controller.metrics.enabled` as kind — the ArgoAppOutOfSync drift alert
needs metrics on here exactly like it does on kind.

```bash
ARGOCD_CHART_VERSION="10.1.3"   # confirm current pin: `helm search repo argo/argo-cd --versions | head -1`
                                  # and cross-check against environments/kind/argocd-values.yaml,
                                  # which records the same chart/appVersion deviation (see §5 below)
helm repo add argo https://argoproj.github.io/argo-helm
helm upgrade --install argocd argo/argo-cd --version "$ARGOCD_CHART_VERSION" \
  -n argocd --create-namespace -f environments/cyfronet/argocd-values.yaml --wait --timeout 10m
```

## 2. Repo access: write-capable SSH deploy key, SOPS-encrypted

Unlike kind's PAT-in-a-harness-created-Secret, cyfronet's repo credential is
committed to Git, SOPS-encrypted, so the deploy key survives a
re-bootstrap without re-issuing a new key each time.

One-time, if `.sops.yaml` still has the placeholder recipient:

```bash
make sops-setup
```

This generates an age keypair at `./age.key` (gitignored — back it up out of
band, e.g. a password manager or the team's secret store) and writes the
public key into `.sops.yaml` as the SOPS recipient, replacing
`REPLACE_WITH_AGE_PUBLIC_KEY_FROM_make_sops-setup`. Commit the updated
`.sops.yaml`; never commit `age.key` itself.

Generate the deploy key and encrypt it:

```bash
ssh-keygen -t ed25519 -f deploy-key -N "" -C "datalake-v2 commit-server (write)"
# Add deploy-key.pub as a write-enabled deploy key on the GitHub repo
# (Settings -> Deploy keys -> Allow write access).
cat > /tmp/argocd-repo-key.yaml <<EOF
apiVersion: v1
kind: Secret
metadata:
  name: datalake-v2-repo-write
  namespace: argocd
  labels:
    argocd.argoproj.io/secret-type: repository-write
stringData:
  type: git
  url: git@github.com:agh-alice/datalake-v2.git
  sshPrivateKey: |
$(sed 's/^/    /' deploy-key)
EOF
SOPS_AGE_KEY_FILE=./age.key sops --encrypt /tmp/argocd-repo-key.yaml \
  > environments/cyfronet/secrets/argocd-repo-key.enc.yaml
rm -f /tmp/argocd-repo-key.yaml deploy-key deploy-key.pub
```

Apply it directly (SOPS-encrypted files are decrypted at apply time, not by
ArgoCD itself — this repo has no SOPS operator wired in):

```bash
SOPS_AGE_KEY_FILE=./age.key sops --decrypt environments/cyfronet/secrets/argocd-repo-key.enc.yaml \
  | kubectl apply -f -
```

**Two secret objects are needed, same as kind** (`hack/kind-up.sh`'s
comment on this explains why): the Source Hydrator's commit-server looks up
push credentials under the `repository-write` secret-type label,
independently of the `repository` (pull) label the repo-server uses. Create
a second, read-only `repository`-labeled secret for pulling (an HTTPS
deploy token or the same SSH key re-labeled works; it just needs read
access) — a single `repository-write`-only secret leaves the repo-server
unable to pull. Follow the same encrypt-then-apply pattern for it, or reuse
the pull credential ArgoCD's repo-server already has if one exists for this
GitHub org.

## 3. Apply the AppProject, infra apps, and the cyfronet Application

```bash
kubectl apply -f apps/project.yaml -f apps/infra/ -f environments/cyfronet/apps/
```

Same three-tier structure as kind: `apps/project.yaml` (the scoped
AppProject), `apps/infra/*.yaml` (upstream operators — CNPG, ESO,
kube-prometheus-stack, Lakekeeper, Argo Workflows — version-pinned
identically across environments), then `environments/cyfronet/apps/
datalake.yaml` (the `datalake-cyfronet` Source Hydrator Application, values
`values.yaml` + `values-cyfronet.yaml`).

## 4. ESO `ClusterSecretStore` for GCP Secret Manager (out-of-band)

Not part of any Application sync — it carries the credential ESO itself
needs to reach GCP, so it is applied directly, once, outside GitOps (same
reasoning as kind's harness-provisioned `lakekeeper-pg-encryption`: a
secret-of-secrets can't be sourced from the thing it bootstraps).

```bash
kubectl -n external-secrets create secret generic gcpsm-credentials \
  --from-file=secret-access-credentials=/path/to/gcp-service-account-key.json
kubectl apply -f - <<EOF
apiVersion: external-secrets.io/v1
kind: ClusterSecretStore
metadata:
  name: gcp-secret-manager
spec:
  provider:
    gcpsm:
      projectID: tensile-ethos-474915-f7
      auth:
        secretRef:
          secretAccessKeySecretRef:
            name: gcpsm-credentials
            namespace: external-secrets
            key: secret-access-credentials
EOF
```

(If Cyfronet's cluster supports GCP Workload Identity Federation for
non-GKE clusters, `auth.workloadIdentityFederation` avoids storing a
long-lived key — not set up as of this writing; the secretRef form above is
the documented default.)

## 5. Lakekeeper secret-store encryption key via ExternalSecret

On kind, `hack/kind-up.sh` creates `lakekeeper/lakekeeper-pg-encryption`
directly (`openssl rand -hex 32`, thrown away on every `kind-down`). On
cyfronet this must survive a re-bootstrap, so it is sourced from GCP Secret
Manager via an `ExternalSecret` instead:

```bash
# One-time: put a real value in Secret Manager (generate once, keep forever —
# rotating it re-encrypts nothing existing but breaks decrypting old rows).
echo -n "$(openssl rand -hex 32)" | gcloud secrets create lakekeeper-pg-encryption-key \
  --project=tensile-ethos-474915-f7 --data-file=-

kubectl apply -f - <<EOF
apiVersion: external-secrets.io/v1
kind: ExternalSecret
metadata:
  name: lakekeeper-pg-encryption
  namespace: lakekeeper
spec:
  refreshInterval: 1h
  secretStoreRef:
    name: gcp-secret-manager
    kind: ClusterSecretStore
  target:
    name: lakekeeper-pg-encryption
  data:
    - secretKey: encryptionKey
      remoteRef:
        key: lakekeeper-pg-encryption-key
EOF
```

This produces the same `lakekeeper/lakekeeper-pg-encryption` Secret
(name/key) that `chart/templates/lakekeeper-db.yaml`'s
`secretBackend.postgres.encryptionKeySecret` already expects — no chart
change needed between environments, only how the Secret is provisioned.

Any other credential a cyfronet consumer needs that can't come from the
in-cluster CNPG-generated app secrets (e.g. an external system's database
connection URL, once a Plan 4 consumer is wired to `landing-db` from outside
the cluster) follows this same pattern: value in GCP Secret Manager under
project `tensile-ethos-474915-f7`, pulled in via an `ExternalSecret` against
the `gcp-secret-manager` `ClusterSecretStore` above, never hardcoded into
the chart or its values files.

## 6. Lakekeeper warehouse creation against Cyfronet S3

**TO VERIFY with real creds when provided -- everything in this section is
unverified until then.** `hack/lakekeeper-warehouse.sh` (used as-is on
kind, invoked automatically by `hack/kind-up.sh`) creates the `default`
Lakekeeper warehouse with a storage-profile pointed at MinIO -- it
hardcodes MinIO's in-cluster endpoint (`http://minio.minio.svc:9000`) and
pulls credentials from the `minio` namespace's `minio-creds` Secret, and
`chart/values.yaml`'s `namespaces:` list confirms there is no `minio`
namespace on cyfronet at all (kind-only, per `hack/kind-up.sh`'s own
comment: "MinIO itself is kind-only -- no cyfronet equivalent"). The script
cannot be reused unmodified against real Cyfronet S3; there is no
`hack/lakekeeper-warehouse-cyfronet.sh` yet (deliberately not written here
-- untestable without real Cyfronet S3 credentials, and writing an
unverified variant would just move the risk into code instead of a
runbook's TO VERIFY marker).

Same script, different storage-profile, once credentials exist: run the
same create/poll/logs/delete throwaway-pod pattern
`hack/lakekeeper-warehouse.sh` uses, POSTing to `http://lakekeeper.
lakekeeper.svc:8181/management/v1/warehouse` (the Lakekeeper Service DNS
itself is identical across environments -- only the storage-profile body
changes) with:

```json
{
  "warehouse-name": "default",
  "storage-profile": {
    "type": "s3",
    "bucket": "<cyfronet-bucket>",
    "key-prefix": "<cyfronet-key-prefix>",
    "endpoint": "<cyfronet-s3-endpoint>",
    "region": "<cyfronet-region>",
    "path-style-access": true,
    "flavor": "s3-compat",
    "sts-enabled": true
  },
  "storage-credential": {
    "type": "s3",
    "credential-type": "access-key",
    "access-key-id": "<cyfronet-s3-access-key-id>",
    "secret-access-key": "<cyfronet-s3-secret-access-key>"
  },
  "delete-profile": {"type": "hard"}
}
```

Fields that need live verification against real Cyfronet S3 before this
ships, none of them assumed from kind's MinIO behavior:

- **`flavor`**: `"s3-compat"` is what MinIO needs (verified working, Task
  1). Lakekeeper 0.12.2's schema also accepts `"aws"` for genuine AWS S3 --
  Cyfronet's actual S3 implementation and its compatibility class are
  unknown as of this writing. Try `"s3-compat"` first (most non-AWS S3
  implementations are Ceph/MinIO-alike); if warehouse creation or the
  first table create fails in an S3-flavor-specific way, `"aws"` is the
  fallback to try next.
- **`sts-enabled`**: `true` + MinIO's root user AssumeRole worked
  end-to-end on kind with zero extra IAM setup (Task 1). Whether Cyfronet's
  S3 supports STS AssumeRole at all is unverified -- if it doesn't,
  warehouse creation or the first vended-credentials request will fail;
  the fallback is `sts-enabled: false`, which makes Lakekeeper vend the
  static `storage-credential` pair directly instead of assuming a
  short-lived role (confirm this fallback path actually works on Cyfronet
  S3 too -- do not assume it from the Lakekeeper docs alone, same
  verify-live discipline as every other placeholder in this runbook).
- **`path-style-access`**: `true` matches MinIO; virtual-hosted-style
  buckets would need `false`. Depends on how Cyfronet's S3 endpoint is set
  up -- verify which style it actually serves before setting this.
- **field names** (`access-key-id`/`secret-access-key`, not
  `aws-access-key-id`/`aws-secret-access-key`): these are Lakekeeper
  0.12.2's own schema, not S3-provider-specific, so they should carry over
  unchanged -- but re-confirm against this cluster's actual Lakekeeper
  chart pin's `/api-docs/management/v1/openapi.json` if the chart version
  has moved since (Task 4's lesson: chart version vs. newest upstream docs
  can and do disagree).

`S3_BUCKET` in the `ingest-env` ExternalSecret (`chart/values-cyfronet.yaml`'s
comment block) must be `<cyfronet-bucket>/<cyfronet-key-prefix>` -- bucket
AND key-prefix together, exactly the same "CRITICAL FINDING" as kind's
`hack/kind-up.sh` documents (dlt computes the Iceberg table location
entirely client-side from `destination.filesystem.bucket_url`, it never
asks Lakekeeper for the warehouse's real base path).

## 7. The PR gate: `environments/cyfronet-next` -> `environments/cyfronet`

`environments/cyfronet/apps/datalake.yaml` sets `sourceHydrator.hydrateTo.
targetBranch: environments/cyfronet-next`, distinct from `syncSource.
targetBranch: environments/cyfronet`. Every push to `main` that changes
`chart/` makes the commit-server render and push to `environments/cyfronet-
next` automatically, but `datalake-cyfronet` only ever syncs from
`environments/cyfronet` — the rendered change does not reach the live
cluster until someone:

1. Opens a PR from `environments/cyfronet-next` into `environments/cyfronet`
   (review the actual rendered manifest diff, not the Helm template source).
2. Merges it.
3. `datalake-cyfronet`'s automated selfHeal picks up the new
   `environments/cyfronet` head and syncs.

This is deliberately the only environment with this extra hop — kind syncs
directly from its hydrated branch for fast local iteration; production gets
a human in the loop on the exact bytes about to apply.

## 8. Verify

Same probes as `hack/kind-verify.sh`, run by hand against the cyfronet
context (there is no `make cyfronet-verify` yet — extend `hack/kind-verify.sh`
into a shared script parameterized by context/`EXPECTED_APPS`, or duplicate
it, if this becomes routine):

```bash
kubectl -n argocd get applications
kubectl -n lakekeeper get secret lakekeeper-pg-encryption
kubectl -n lakekeeper get cluster lakekeeper-db
kubectl -n landing-db get cluster mon-data
```

## Notes on the ArgoCD version pin

`environments/cyfronet/argocd-values.yaml` carries the same
`global.image.tag: v3.5.0-rc2` override as kind, and for the same reason: no
`argo-cd` Helm chart bundles ArgoCD v3.5.0 GA yet (only rc1/rc2 exist
upstream as of this writing), and v3.5's Source Hydrator is materially ahead
of the v3.4.5 the chart itself bundles. **Owner decision, 2026-07-12: the
newest hydrator matters more than GA status — RCs are acceptable in every
environment including production.** Moving to v3.5.0 GA once a chart bundles
it (or once `global.image.tag` is bumped to it directly) is a routine version
upgrade like any other, not a gate that blocks cyfronet activation. Track it
opportunistically; do not delay bootstrap waiting for it.

## Trino on cyfronet — Plan 4 notes

- **Chart and CPU re-evaluation:** The kind cluster pins chart 1.40.0 / Trino 476 because the kind host's virtualized CPU lacks x86-64-v3 support (AVX2/BMI2 flags); Trino ≥477 inherits RHEL 10's x86-64-v3 baseline and refuses to boot. Before re-pinning to the newest chart on cyfronet, check that all worker nodes support AVX2 and BMI2: `grep -m1 -oE 'avx2|bmi2' /proc/cpuinfo` on a node. Once verified, bump to the newest available chart and appVersion in apps/infra/trino.yaml. **Re-resolve the image digest at the same time** (final review R3): `apps/infra/trino.yaml`'s `values:` block pins `image.digest` to trinodb/trino:476's manifest-list digest (E2 founding lesson — no mutable tags); a chart/appVersion bump without re-resolving this digest would either still deploy 476 (digest wins over tag) or fail to pull (digest belongs to a different appVersion), silently or loudly wrong either way. Re-resolve via `docker buildx imagetools inspect trinodb/trino:<new-tag>` (or the Docker Hub registry v2 API's `docker-content-digest` header, per apps/infra/trino.yaml's comment) and update the `image.digest` value.

- **Kind-only values to override:** `apps/infra/trino.yaml` is applied verbatim by bootstrap (no per-environment overlay exists for this Application yet) — three values in its `values:` block are hardcoded for the kind cluster and MUST change for cyfronet: `s3.endpoint=http://minio.minio.svc:9000` (real S3 endpoint, per the cyfronet S3 credentials once provided — see §5-style ExternalSecret pattern), `s3.region=local-01` (real cyfronet region), and `server.workers: 0` (coordinator-only sizing for kind's resource-frugality constraint; cyfronet needs real worker capacity sized per `deliverables/2026-07-12-cyfronet-cluster-requirements.md`'s Trino sizing table, plus removing the `nodeScheduler.includeCoordinator` coordinator-only workaround if dedicated workers are added). If any of the three is left at its kind value, the failure is loud, not silent: §8's Trino query probes (`hack/kind-verify.sh`'s `trino-query-probe`, or its `cyfronet-verify` equivalent once it exists) hit the live coordinator with real SQL (`SHOW CATALOGS`, a `lake.alice.job_info` count, contract-view sampling) and fail outright if S3/region are wrong or if capacity is too thin to answer.

- **Property-name version-lock:** Trino 476 uses `fs.native-s3.enabled` for S3 connectivity, not `fs.s3.enabled`. The latter became the property name in later releases (≥477). When re-pinning the chart/appVersion, re-verify the exact property name against the Trino version's own docs (`https://trino.io/docs/<version>/object-storage/file-system-s3.html`), not the `/current/` doc version — Trino docs are version-specific and property names have changed. See apps/infra/trino.yaml comments for the root-cause.

- **Landing-RO credentials on cyfronet:** The `landing-ro` Secret is currently harness-provisioned on kind by `hack/kind-up.sh`. On cyfronet, it is sourced via `ExternalSecret` from GCP Secret Manager (analogous to section §5's `lakekeeper-pg-encryption` pattern). Store the read-only Trino user's password in Secret Manager at project `tensile-ethos-474915-f7` under key `trino-landing-ro-password`, then apply an `ExternalSecret` to pull it into namespace `trino` as Secret name `landing-ro` (same Secret name, different provisioning). The chart's existing `envFrom.secretRef.name: landing-ro` requires no change — only how the Secret is created differs between environments.

## Dex (GitHub-org OIDC) — Plan 4 notes

Design decision D-Dex (`deliverables/2026-07-12-datalake-v2-design.md`, OIDC
provider row) + migration plan Task 1 Step 4: standalone Dex
(`apps/infra/dex.yaml`) bridges GitHub (OAuth2-only, not a full OIDC IdP) to
every OIDC-speaking consumer in this platform (Lakekeeper, Grafana; Trino
deferred — see below), gated on `agh-alice` org membership. **On kind**,
`apps/infra/dex.yaml` deploys with the zero-setup `mockCallback` connector
instead — a real GitHub OAuth App requires owner action in the GitHub org
UI that cannot be done from this session (or any automated one). Everything
below is what changes to go from kind's mock-connector proof to cyfronet's
real GitHub-org gate.

### 1. Create the GitHub OAuth App (owner action, cannot be scripted)

In the `agh-alice` GitHub org: **Settings → Developer settings → OAuth
Apps → New OAuth App**.

- Homepage URL / Authorization callback URL: `https://<dex-hostname>/dex/callback`
  (the real ingress hostname once cyfronet has one — TO VERIFY, no ingress
  controller is chosen yet as of this writing).
- Record the generated **Client ID** and **Client Secret** — the secret is
  shown once only.

### 2. Client secret via ESO, NOT a Git-tracked value

Unlike Grafana's `staticClients[].secretEnv` (kind: `apps/infra/dex.yaml`'s
comment — `idEnv`/`secretEnv` are dedicated per-field env-var references,
verified against `dexidp/dex`'s `storage/storage.go` `Client` struct,
shipped since Dex v2.35.0), **connector** config (the `github` connector's
`clientSecret`) has NO equivalent env-var-reference field — verified
against `connector/github/github.go`'s `Config` struct and `Open()`
method: no `os.ExpandEnv`/env-var substitution is applied to any connector
field, so a bare `clientSecret: $GITHUB_CLIENT_SECRET` string (the kind of
snippet some Dex docs pages show) is NOT expanded by Dex itself and would
leak the literal string if a plaintext secret isn't substituted some other
way. The only secret-safe path with this chart (`configSecret.create: true`
renders `.Values.config` verbatim into a Secret) is to render the **entire**
`config.yaml` outside Helm/Git, via an ExternalSecret's own templating:

```yaml
apiVersion: external-secrets.io/v1
kind: ExternalSecret
metadata:
  name: dex-config
  namespace: dex
spec:
  refreshInterval: 1h
  secretStoreRef: {name: gcp-secret-manager, kind: ClusterSecretStore}
  target:
    name: dex-config
    template:
      data:
        config.yaml: |
          issuer: https://<dex-hostname>/dex
          storage:
            type: memory   # or a persistent backend -- TO VERIFY whether this matters for the real GitHub connector
          connectors:
            - type: github
              id: github
              name: GitHub
              config:
                clientID: "{{ .clientID }}"
                clientSecret: "{{ .clientSecret }}"
                redirectURI: https://<dex-hostname>/dex/callback
                orgs:
                  - name: agh-alice
          staticClients:
            - id: grafana
              name: Grafana
              secretEnv: GRAFANA_OIDC_CLIENT_SECRET
              redirectURIs:
                - https://<grafana-hostname>/login/generic_oauth
  data:
    - secretKey: clientID
      remoteRef: {key: dex-github-oauth-client-id}
    - secretKey: clientSecret
      remoteRef: {key: dex-github-oauth-client-secret}
```

Then set `apps/infra/dex.yaml`'s values to `configSecret: {create: false,
name: dex-config}` and drop the inline `config:`/`connectors:`/
`staticClients:` block this file currently carries for kind's mock
connector — the ExternalSecret above becomes the sole source of Dex's
config on cyfronet. Store `dex-github-oauth-client-id`/
`dex-github-oauth-client-secret` in GCP Secret Manager under project
`tensile-ethos-474915-f7` (same project every other cyfronet secret in
this repo uses), populated from Step 1's OAuth App values. Grafana's own
`dex-grafana-oauth` Secret (kind: harness-generated by `hack/kind-up.sh`)
becomes an ExternalSecret too, same §5 pattern, sourcing a
`grafana-oidc-client-secret` key — the value must equal whatever the
`staticClients[].secretEnv`-referenced env var resolves to, same
shared-value constraint as kind's two-namespace Secret pair.

**TO VERIFY**: whether `storage.type: memory` is acceptable for the real
GitHub connector in production (kind only ever proved memory storage works
for `mockCallback` + one static client) — a Dex pod restart drops all
in-flight auth-code state, which mostly just means an in-progress login
has to restart, not a security issue, but re-evaluate once real user load
exists.

### 3. Lakekeeper OIDC (cyfronet only — NOT applied on kind)

**On kind, Lakekeeper's auth stays open** (`apps/infra/lakekeeper.yaml`
sets no `auth.oauth2` block at all) — deliberately: open-auth is
load-bearing for every ingest/Trino/verify probe this platform's own
harness runs against Lakekeeper's REST catalog (`hack/kind-verify.sh`'s
`rest-probe`/`warehouse-probe`, `hack/lakekeeper-warehouse.sh`, every
ingest CronWorkflow's `LAKEKEEPER_URI` client) — flipping it on kind would
break the platform this task was explicitly told not to touch. Real schema
below, verified against `lakekeeper/lakekeeper` chart 0.11.0's own
`values.yaml` (`auth.oauth2.*` block, lines ~491-518) rather than assumed
from the brief's URI-based invariant (task-4-report.md's earlier finding
that the chart's real schema differs from that invariant applies here too):

```yaml
# apps/infra/lakekeeper.yaml addition, cyfronet only:
auth:
  oauth2:
    providerUri: https://<dex-hostname>/dex
    audience: lakekeeper
    ui:
      clientID: lakekeeper-ui
      scopes: "openid profile email groups"
```

Lakekeeper acts as an OIDC **resource server** here (validates Bearer
tokens Dex issues; no `clientSecret` field exists in this chart's
`auth.oauth2` block at all — confirmed against the chart's real schema,
not assumed) except for `ui.clientID`, the UI's own browser-side
authorization-code client, which is `secretEnv`-style secretless at the
Lakekeeper end too (PKCE-based, native/public client — TO VERIFY once
implemented, this chart's own docs don't spell out whether Lakekeeper's UI
flow uses PKCE explicitly). This block needs a matching `staticClients`
entry in Dex's config (Step 2's ExternalSecret template) with
`id: lakekeeper-ui`, an `audience`-matching claim, and no `clientSecret` (a
public client). **This ends Lakekeeper's open-auth mode on cyfronet** —
every ingest Workflow's Lakekeeper REST client and every Trino query
against the `lake` catalog will need a real Bearer token once this lands;
re-verify the whole ingestion+query acceptance sequence
(`docs/runbooks/ingestion-pipeline.md`) against an authenticated Lakekeeper
before calling cyfronet's Plan 4 Task 1 complete.

### 4. Trino OIDC — explicitly deferred

Migration plan (`deliverables/plans/2026-07-13-datalake-v2-migration-plan.md`,
Task 1 Step 4): **"Trino auth deferred until multi-user need"** — Trino's
own `landing`/`lake` catalog access stays behind the existing `trino_ro`
Postgres role + Lakekeeper's vended-credentials flow, not OIDC, until a
real multi-user access-control need materializes. Record this as a
deliberate deferral, not an oversight, if a future review asks why Trino
has no `http.authentication.type=OAUTH2` block here.

## Alerting — Plan 4 Task T2S1: G3 = replace webhook secret

On kind, `monitoring/alertmanager-slack-webhook` (harness-provisioned by
`hack/kind-up.sh`) carries a placeholder URL pointing at the in-cluster
`echo-receiver` (`chart/templates/echo-receiver.yaml`, kind-only) so the
whole route/group/inhibit/`slack-datalake`-receiver pipeline
(`apps/infra/monitoring.yaml`) is provably wired end-to-end without a real
Slack workspace. **G3 = replace webhook secret**: once the owner provides a
real Slack incoming-webhook URL, source it via the same §5-style
ExternalSecret pattern —

```yaml
apiVersion: external-secrets.io/v1
kind: ExternalSecret
metadata: {name: alertmanager-slack-webhook, namespace: monitoring}
spec:
  refreshInterval: 1h
  secretStoreRef: {name: gcp-secret-manager, kind: ClusterSecretStore}
  target: {name: alertmanager-slack-webhook}
  data:
    - secretKey: webhook-url
      remoteRef: {key: alertmanager-slack-webhook-url}
```

— same Secret name and key (`webhook-url`) the `slack_configs[].api_url_file`
path in `apps/infra/monitoring.yaml` already expects, so no chart/values
change is needed for the swap, only how the Secret is provisioned. `chart/
values.yaml`'s `echoReceiver.enabled` stays `false` on cyfronet (base
default; only `values-kind.yaml` turns it on) — nothing extra to disable.
