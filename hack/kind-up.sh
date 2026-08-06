#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
# shellcheck source=lib/warehouse-config.sh
. hack/lib/warehouse-config.sh
ARGOCD_CHART_VERSION="10.1.3"   # from Step 1 (Plan 4) -- unchanged, kind's own bootstrap ArgoCD, not one of the platform-mirrored operators
kind get clusters | grep -q '^datalake-v2$' || kind create cluster --config hack/kind-config.yaml

# Placement (Plan 5 Task 4 -- brief: "prefer labelling the kind nodes...
# placement is actually tested for real"). Labels every schedulable
# (non control-plane) kind node `pool=db` so the storage tier's real
# `nodeSelector: {pool: db}` (envs/prod/storage/values-kind.yaml) matches
# for real, instead of being neutralised through values. Deliberately does
# NOT also label `cpu: x86-64-v3` -- this host's actual CPU lacks AVX2
# (verified live, Task 4: no avx2/bmi1/bmi2/fma in /proc/cpuinfo, and the
# prod-pinned Trino 480 image SIGILLs here) -- see envs/prod/compute/
# values-kind.yaml's comment for why that selector is neutralised instead
# of labelled. No taint is applied either: a 2-node kind cluster (control-
# plane + one worker) has nowhere else for every other workload to run if
# its only worker were tainted.
for node in $(kubectl get nodes -l '!node-role.kubernetes.io/control-plane' -o name); do
  kubectl label "$node" pool=db --overwrite
done

# StorageClass `local-path` (Plan 5 Task 4 -- brief: "kind has no
# cinder-perf; map the storage class through values-kind.yaml (local-path
# on kind)"). Kind's own bundled local-path-provisioner ships a
# StorageClass named `standard` by default (not `local-path`) -- this adds
# a SECOND StorageClass object using the exact same provisioner
# (rancher.io/local-path, already running as part of kind's default
# node-image config) under the literal name both tier charts'
# values-kind.yaml overrides reference. Idempotent apply.
kubectl apply -f - <<'YAML'
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: local-path
provisioner: rancher.io/local-path
volumeBindingMode: WaitForFirstConsumer
reclaimPolicy: Delete
YAML

helm repo add argo https://argoproj.github.io/argo-helm >/dev/null
# 10m timeout (Task 8 clean-room finding): on a fresh kind cluster every ArgoCD
# image is a cold pull; 5m timed out mid-pull, leaving the release status=failed
# and no ArgoCD installed. The install itself is idempotent (upgrade --install
# recovers on rerun), but the first attempt should just succeed.
helm upgrade --install argocd argo/argo-cd --version "$ARGOCD_CHART_VERSION" \
  -n argocd --create-namespace -f environments/kind/argocd-values.yaml --wait --timeout 10m
# Private repo, read+write (commit-server pushes rendered branches). Owner decision 2026-07-12.
# Two secret objects, same credential: the Source Hydrator's commit-server looks up push
# credentials under the `repository-write` secret-type label, separately from the
# `repository` (pull) label used by repo-server -- confirmed against
# docs/user-guide/source-hydrator.md at tag v3.5.0-rc2 ("Argo CD requires different
# secrets for pushing and pulling to provide better isolation"). A single secret with
# only the `repository` label leaves the commit-server unauthenticated on push
# (git fetch/push fails with "could not read Username ... terminal prompts disabled").
GH_TOKEN="$(gh auth token)"
kubectl -n argocd create secret generic datalake-v2-repo \
  --from-literal=type=git \
  --from-literal=url=https://github.com/agh-alice/datalake-v2.git \
  --from-literal=username=x-access-token \
  --from-literal=password="$GH_TOKEN" \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl -n argocd label secret datalake-v2-repo argocd.argoproj.io/secret-type=repository --overwrite
kubectl -n argocd create secret generic datalake-v2-repo-write \
  --from-literal=type=git \
  --from-literal=url=https://github.com/agh-alice/datalake-v2.git \
  --from-literal=username=x-access-token \
  --from-literal=password="$GH_TOKEN" \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl -n argocd label secret datalake-v2-repo-write argocd.argoproj.io/secret-type=repository-write --overwrite

# Plan 5 Task 4: AppProject + the five platform-owned operator Applications
# (CloudNativePG, External Secrets, Argo Workflows, Dex, kube-prometheus-
# stack) + MinIO, all moved to environments/kind/{project.yaml,infra/}.
kubectl apply -f environments/kind/project.yaml
kubectl apply -f environments/kind/infra/

# The three tenant namespaces -- platform-owned in prod (created with the
# platform's own labels before this repo's Applications ever sync); kind
# mirrors that division of responsibility by pre-creating them itself.
# FINDING (Task 4, unverified): this session has no access to the real
# alice-k8s-datalake cluster, so the platform's actual label set on these
# namespaces could not be confirmed -- only the label Kubernetes applies
# automatically (kubernetes.io/metadata.name) is guaranteed to match.
# Open item for whoever next has cluster access to close (does not block
# this task: none of the three tier charts' selectors/policies depend on a
# namespace label, only on nodeSelectors and the ClusterSecretStore name).
for ns in datalake-storage datalake-compute datalake-orchestration; do
  kubectl create namespace "$ns" --dry-run=client -o yaml | kubectl apply -f -
done

# `platform` ClusterSecretStore (Plan 5 Task 4 -- brief: "same name as
# production deliberately"). Two live findings from the Task 4 clean-room,
# both fixed by waiting on the external-secrets Application's own ArgoCD
# health rather than a narrower proxy signal:
#   1. `kubectl get crd` succeeding is NOT enough: a CRD object can exist
#      while the API server's aggregated discovery document -- what
#      `kubectl apply` actually consults to resolve "ClusterSecretStore"
#      to a REST endpoint -- hasn't caught up yet, reproduced live as
#      `no matches for kind "ClusterSecretStore" in version
#      "external-secrets.io/v1"`.
#   2. Once the CRD resolves, applying a ClusterSecretStore also triggers
#      ESO's OWN validating webhook (`validate.clustersecretstore.
#      external-secrets.io`) -- reproduced live as a `connection refused`
#      against `external-secrets-webhook.external-secrets.svc` when the
#      webhook Service exists (Service objects come up fast) but its
#      backing Deployment isn't Ready yet (cold image pull).
# ArgoCD's own Healthy rollup for a Deployment already means "available
# replicas" -- waiting on it covers both the CRD-establishment and the
# webhook-readiness gaps in one signal, rather than chasing each
# individually.
i=0
while true; do
  sync=$(kubectl -n argocd get application external-secrets -o jsonpath='{.status.sync.status}' 2>/dev/null || echo "")
  health=$(kubectl -n argocd get application external-secrets -o jsonpath='{.status.health.status}' 2>/dev/null || echo "")
  [ "$sync" = "Synced" ] && [ "$health" = "Healthy" ] && { echo "OK: external-secrets Application Healthy (CRDs + webhook ready)"; break; }
  i=$((i + 1))
  [ "$i" -gt 60 ] && { echo "FAIL: external-secrets Application not Healthy after 5m (sync=$sync health=$health)"; exit 1; }
  sleep 5
done
# Still retried for a short window as a second layer: a client-side
# discovery-cache staleness gap on the machine running kubectl can
# outlast even the CRD's own Established condition on some kubectl
# versions.
i=0
until kubectl apply -f environments/kind/infra/cluster-secret-store.yaml; do
  i=$((i + 1))
  [ "$i" -gt 18 ] && { echo "FAIL: cluster-secret-store.yaml apply kept failing after 3m"; exit 1; }
  sleep 10
done
i=0
while [ "$(kubectl get clustersecretstore platform -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null)" != "True" ]; do
  i=$((i + 1))
  [ "$i" -gt 60 ] && { echo "FAIL: ClusterSecretStore platform not Ready after 5m"; exit 1; }
  sleep 5
done
echo "ClusterSecretStore platform: Ready"

# Lakekeeper's Postgres secret-store encryption key: harness-generated
# throwaway, random per kind cluster (never in Git; the prod tenant
# cluster gets it via a real ExternalSecret against the platform's own
# store -- same mechanism, different backing value). Created as the
# REMOTE secret in eso-secret-source (Plan 5 Task 3's ExternalSecret,
# envs/prod/storage/templates/external-secrets.yaml, resolves it from
# there) rather than directly in datalake-storage -- independent of
# everything else, so it's created early, before Lakekeeper's own
# Application first syncs, avoiding an ExternalSecret resolve race.
kubectl -n eso-secret-source get secret lakekeeper-pg-encryption >/dev/null 2>&1 || \
  kubectl -n eso-secret-source create secret generic lakekeeper-pg-encryption \
    --from-literal=encryptionKey="$(openssl rand -hex 32)"

# MinIO root credentials: harness-generated throwaway, random per kind cluster
# (never in Git; MinIO itself is kind-only -- no prod-tenant equivalent). Keys
# rootUser/rootPassword are the chart's existingSecret contract (Task 1,
# Plan 2). Plain kubectl Secret, NOT routed through ESO -- MinIO is kind-only
# scaffolding, not one of the three tier charts' ExternalSecret consumers.
kubectl create namespace minio --dry-run=client -o yaml | kubectl apply -f -
kubectl -n minio get secret minio-creds >/dev/null 2>&1 || \
  kubectl -n minio create secret generic minio-creds \
    --from-literal=rootUser="$(openssl rand -hex 16)" \
    --from-literal=rootPassword="$(openssl rand -hex 16)"

# Dex <-> Grafana OAuth client secret (Plan 4 Task T1S4, design D-Dex):
# harness-generated, random per kind cluster, never in Git -- the SAME
# value is written into two namespaces because the two consumers
# (environments/kind/infra/dex.yaml's staticClients[0].secretEnv,
# environments/kind/infra/monitoring.yaml's grafana.envFromSecret) each
# read a same-namespace Secret and Kubernetes Secrets don't cross
# namespaces. Same generate-once-reuse-after idempotency pattern as this
# script's `landing-ro`/trino_ro password further down: whichever copy
# already exists (kind-up.sh rerun) wins, so both namespaces always agree.
# `monitoring` and `dex` are pre-created here (not left to their
# Applications' own CreateNamespace=true) so the Secret can exist before
# `kubectl apply -f environments/kind/infra/` above -- same reasoning as
# minio above. Plain kubectl Secrets throughout -- Dex/Grafana are
# kind-only, not ESO consumers.
kubectl create namespace dex --dry-run=client -o yaml | kubectl apply -f -
kubectl create namespace monitoring --dry-run=client -o yaml | kubectl apply -f -
if kubectl -n monitoring get secret dex-grafana-oauth >/dev/null 2>&1; then
  GRAFANA_OIDC_CLIENT_SECRET=$(kubectl -n monitoring get secret dex-grafana-oauth -o jsonpath='{.data.GRAFANA_OIDC_CLIENT_SECRET}' | base64 -d)
elif kubectl -n dex get secret dex-grafana-oauth >/dev/null 2>&1; then
  GRAFANA_OIDC_CLIENT_SECRET=$(kubectl -n dex get secret dex-grafana-oauth -o jsonpath='{.data.GRAFANA_OIDC_CLIENT_SECRET}' | base64 -d)
else
  GRAFANA_OIDC_CLIENT_SECRET="$(openssl rand -hex 32)"
fi
kubectl -n dex create secret generic dex-grafana-oauth \
  --from-literal=GRAFANA_OIDC_CLIENT_SECRET="$GRAFANA_OIDC_CLIENT_SECRET" \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl -n monitoring create secret generic dex-grafana-oauth \
  --from-literal=GRAFANA_OIDC_CLIENT_SECRET="$GRAFANA_OIDC_CLIENT_SECRET" \
  --dry-run=client -o yaml | kubectl apply -f -

# Alertmanager Slack-webhook stand-in (Plan 4 Task T2S1 prework): on kind
# this is a PLACEHOLDER URL pointing at the in-cluster echo-receiver.
# Plan 5 Task 1 retargeted echo-receiver from namespace `monitoring` to
# `datalake-orchestration` (envs/prod/orchestration/templates/
# echo-receiver.yaml, values-gated `echoReceiver.enabled` -- true on kind
# per values-kind.yaml) -- this URL updated to match. Key name
# `webhook-url` matches environments/kind/infra/monitoring.yaml's
# `api_url_file` path verbatim.
kubectl -n monitoring get secret alertmanager-slack-webhook >/dev/null 2>&1 || \
  kubectl -n monitoring create secret generic alertmanager-slack-webhook \
    --from-literal=webhook-url="http://echo-receiver.datalake-orchestration.svc.cluster.local:8080/webhook"

# Plan 5 Task 4: the three tier Applications, mirroring the tenant
# topology -- same charts prod uses (envs/prod/<tier>), values-kind.yaml
# only. Namespaces already exist (above); no CreateNamespace=true on these
# (prod parity -- the platform owns namespace creation there too).
kubectl apply -f environments/kind/apps/

# Warehouse bootstrap: idempotent, self-guards on lakekeeper Service
# readiness internally (Task 1) -- no separate wait needed here.
hack/lakekeeper-warehouse.sh

# Landing read-only role for Trino's `landing` catalog (Plan 3 Task 1):
# role `trino_ro`, LOGIN + SELECT-only against mon_data's public schema.
# Plan 5 Task 1 retargeted the landing DB from namespace `landing-db` to
# `datalake-storage`; Plan 5 Task 3 moved the `landing-ro` Secret from a
# hack/kind-up.sh-provisioned plain Secret (namespace `trino`) to a real
# ExternalSecret (namespace `datalake-compute`, envs/prod/compute/
# templates/external-secrets.yaml) resolving `username`/`password`
# properties off a remote Secret named `landing-ro` in eso-secret-source
# -- created here instead of directly in the target namespace.
if kubectl -n eso-secret-source get secret landing-ro >/dev/null 2>&1; then
  LANDING_RO_PASSWORD=$(kubectl -n eso-secret-source get secret landing-ro -o jsonpath='{.data.password}' | base64 -d)
  echo "landing-ro remote secret already exists; reusing its password to keep the DB role in sync"
else
  LANDING_RO_PASSWORD="$(openssl rand -hex 16)"
fi
i=0
MD_PRIMARY=""
while [ -z "$MD_PRIMARY" ]; do
  MD_PRIMARY=$(kubectl -n datalake-storage get cluster mon-data -o jsonpath='{.status.currentPrimary}' 2>/dev/null || echo "")
  [ -n "$MD_PRIMARY" ] && break
  i=$((i + 1))
  [ "$i" -gt 60 ] && { echo "FAIL: mon-data currentPrimary not set after 5m"; exit 1; }
  sleep 5
done
kubectl -n datalake-storage exec "$MD_PRIMARY" -- psql -U postgres -d mon_data -v ON_ERROR_STOP=1 -c "
DO \$\$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'trino_ro') THEN
    CREATE ROLE trino_ro LOGIN PASSWORD '${LANDING_RO_PASSWORD}';
  ELSE
    ALTER ROLE trino_ro LOGIN PASSWORD '${LANDING_RO_PASSWORD}';
  END IF;
END
\$\$;
GRANT CONNECT ON DATABASE mon_data TO trino_ro;
GRANT USAGE ON SCHEMA public TO trino_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO trino_ro;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO trino_ro;
"
kubectl -n eso-secret-source create secret generic landing-ro \
  --from-literal=username=trino_ro \
  --from-literal=password="$LANDING_RO_PASSWORD" \
  --dry-run=client -o yaml | kubectl apply -f -
echo "landing-ro role + remote secret ready"

# Ingest job env Secret (Plan 2 Task 3): the env-var contract consumed by
# ingest/src/alice_ingest/pipeline.py (_REQUIRED_ENV_VARS). PG_URL is read
# verbatim from the CNPG "-app" secret's `uri` key, now in namespace
# `datalake-storage` (Plan 5 Task 1). S3 creds come from minio-creds; the
# rest are static values matching hack/lakekeeper-warehouse.sh's
# endpoints, DNS names updated to the three-tier namespaces (Plan 5 Task
# 1). Plan 5 Task 3 moved this Secret onto a real ExternalSecret
# (namespace `datalake-orchestration`, envs/prod/orchestration/templates/
# external-secrets.yaml) resolving 10 named properties off a remote Secret
# `ingest-env` in eso-secret-source -- created here instead of directly in
# the target namespace. NOTE: SITESONAR_LIMIT is deliberately NOT one of
# the 10 keys that ExternalSecret requests (see envs/prod/orchestration/
# values-kind.yaml's comment for why, and how kind compensates by NOT
# tightening the sitesonar schedule instead).
if kubectl -n eso-secret-source get secret ingest-env >/dev/null 2>&1; then
  echo "ingest-env remote secret already exists"
else
  i=0
  while ! kubectl -n datalake-storage get secret mon-data-app >/dev/null 2>&1; do
    i=$((i + 1))
    [ "$i" -gt 60 ] && { echo "FAIL: mon-data-app secret not found after 5m"; exit 1; }
    sleep 5
  done
  PG_URL=$(kubectl -n datalake-storage get secret mon-data-app -o jsonpath='{.data.uri}' | base64 -d)
  S3_ACCESS_KEY=$(kubectl -n minio get secret minio-creds -o jsonpath='{.data.rootUser}' | base64 -d)
  S3_SECRET_KEY=$(kubectl -n minio get secret minio-creds -o jsonpath='{.data.rootPassword}' | base64 -d)
  kubectl -n eso-secret-source create secret generic ingest-env \
    --from-literal=PG_URL="$PG_URL" \
    --from-literal=S3_ENDPOINT="http://minio.minio.svc:9000" \
    --from-literal=S3_ACCESS_KEY="$S3_ACCESS_KEY" \
    --from-literal=S3_SECRET_KEY="$S3_SECRET_KEY" \
    --from-literal=S3_BUCKET="${WAREHOUSE_BUCKET}/${WAREHOUSE_KEY_PREFIX}" \
    --from-literal=S3_REGION="local-01" \
    --from-literal=LAKEKEEPER_URI="http://lakekeeper.datalake-storage.svc:8181" \
    --from-literal=LAKEKEEPER_WAREHOUSE="default" \
    --from-literal=RETENTION_DAYS="14" \
    --from-literal=TRINO_URI="http://trino.datalake-compute.svc:8080"
  echo "ingest-env remote secret created"
fi

# Force an immediate ESO reconcile on the three ExternalSecrets rather than
# waiting out refreshInterval: 1h (review guidance, Task 4) -- any
# annotation change on the ExternalSecret object bumps its resourceVersion,
# which is what actually triggers controller-runtime's watch-based
# reconcile; "force-sync" is not a special ESO-recognized keyword, just a
# self-documenting annotation name for this purpose. Then poll each
# resulting target Secret for existence before anything downstream
# (lakekeeper-warehouse.sh already ran above and self-guards on
# Lakekeeper's own Service readiness independently of this).
for pair in "datalake-storage:lakekeeper-pg-encryption" "datalake-compute:landing-ro" "datalake-orchestration:ingest-env"; do
  ns=${pair%%:*}
  name=${pair##*:}
  kubectl -n "$ns" annotate externalsecret "$name" force-sync="$(date +%s)" --overwrite >/dev/null 2>&1 || true
done
for pair in "datalake-storage:lakekeeper-pg-encryption" "datalake-compute:landing-ro" "datalake-orchestration:ingest-env"; do
  ns=${pair%%:*}
  name=${pair##*:}
  i=0
  until kubectl -n "$ns" get secret "$name" >/dev/null 2>&1; do
    i=$((i + 1))
    [ "$i" -gt 60 ] && { echo "FAIL: Secret $ns/$name never materialized from ExternalSecret $name after 5m"; exit 1; }
    sleep 5
  done
  echo "OK: Secret $ns/$name materialized via ExternalSecret"
done

echo "kind + ArgoCD (hydrator) + operators + platform ClusterSecretStore + 3 tier apps ready"
