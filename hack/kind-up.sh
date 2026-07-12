#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
ARGOCD_CHART_VERSION="10.1.3"   # from Step 1
kind get clusters | grep -q '^datalake-v2$' || kind create cluster --config hack/kind-config.yaml
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
# Lakekeeper secret-store encryption key: harness-generated throwaway, random per
# kind cluster (never in Git; cyfronet gets it via ExternalSecret). Namespace may
# not exist yet (hydrator creates it later) — pre-create idempotently; SSA adopts it.
kubectl create namespace lakekeeper --dry-run=client -o yaml | kubectl apply -f -
kubectl -n lakekeeper get secret lakekeeper-pg-encryption >/dev/null 2>&1 || \
  kubectl -n lakekeeper create secret generic lakekeeper-pg-encryption \
    --from-literal=encryptionKey="$(openssl rand -hex 32)"
# MinIO root credentials: harness-generated throwaway, random per kind cluster
# (never in Git; MinIO itself is kind-only -- no cyfronet equivalent). Keys
# rootUser/rootPassword are the chart's existingSecret contract (Task 1).
kubectl create namespace minio --dry-run=client -o yaml | kubectl apply -f -
kubectl -n minio get secret minio-creds >/dev/null 2>&1 || \
  kubectl -n minio create secret generic minio-creds \
    --from-literal=rootUser="$(openssl rand -hex 16)" \
    --from-literal=rootPassword="$(openssl rand -hex 16)"
kubectl apply -f apps/project.yaml
[ -d apps/infra ] && ls apps/infra/*.yaml >/dev/null 2>&1 && kubectl apply -f apps/infra/
kubectl apply -f environments/kind/apps/
# Warehouse bootstrap: idempotent, self-guards on lakekeeper Service
# readiness internally (Task 1) -- no separate wait needed here.
hack/lakekeeper-warehouse.sh
# Ingest job env Secret (Plan 2 Task 3): the env-var contract consumed by
# ingest/src/alice_ingest/pipeline.py (_REQUIRED_ENV_VARS). PG_URL is read
# verbatim from the CNPG "-app" secret's `uri` key
# (postgresql://mon_user:...@mon-data-rw.landing-db:5432/mon_data) -- the
# 2-label host `mon-data-rw.landing-db` resolves cross-namespace via the k8s
# resolver's search-domain expansion (ndots:5 default -> tries
# mon-data-rw.landing-db.svc.cluster.local, which matches the Service's real
# DNS record), verified working end-to-end from argo-workflows-namespace
# workflow pods in Task 3. S3 creds come from minio-creds; the rest are
# static values matching lakekeeper-warehouse.sh's endpoints. mon-data-app is
# CNPG-generated only after the mon-data Cluster goes Ready (async, via the
# datalake-kind ArgoCD app applied above) -- guarded poll before reading it,
# same shape as lakekeeper-warehouse.sh's own readiness wait. Idempotent:
# same create-if-absent pattern as the other harness secrets above.
#
# CRITICAL FINDING (Task 3, first real run against this cluster): S3_BUCKET
# must be the Lakekeeper storage-profile's FULL base path, bucket name PLUS
# key-prefix -- NOT just the bucket name. dlt's Iceberg table location is
# computed entirely client-side from `destination.filesystem.bucket_url` +
# dataset_name + table_name (dlt/destinations/impl/filesystem/filesystem.py
# get_open_table_location() -> get_table_prefix() + make_remote_url(), read
# from the shipped image's site-packages to confirm) -- it never asks
# Lakekeeper for a server-assigned default location. hack/lakekeeper-
# warehouse.sh creates the `default` warehouse with
# storage-profile.key-prefix="lakekeeper-warehouse", so its real base is
# s3://warehouse/lakekeeper-warehouse, not s3://warehouse. Passing plain
# "warehouse" here reproduced, verbatim:
#   pyiceberg.exceptions.BadRequestError: InvalidLocation: Provided location
#   s3://warehouse/alice/trace is not a valid sublocation of the storage
#   profile s3://warehouse/lakekeeper-warehouse.
# on every CREATE TABLE. The research doc's `bucket_url = "s3://warehouse"`
# example (research/2026-07-12_dlt-iceberg-lakekeeper-api-verification.md)
# is therefore only correct for a warehouse created with NO key-prefix; ours
# has one, so bucket_url must include it too.
kubectl create namespace argo-workflows --dry-run=client -o yaml | kubectl apply -f -
if kubectl -n argo-workflows get secret ingest-env >/dev/null 2>&1; then
  echo "ingest-env secret already exists"
else
  i=0
  while ! kubectl -n landing-db get secret mon-data-app >/dev/null 2>&1; do
    i=$((i + 1))
    [ "$i" -gt 60 ] && { echo "FAIL: mon-data-app secret not found after 5m"; exit 1; }
    sleep 5
  done
  PG_URL=$(kubectl -n landing-db get secret mon-data-app -o jsonpath='{.data.uri}' | base64 -d)
  S3_ACCESS_KEY=$(kubectl -n minio get secret minio-creds -o jsonpath='{.data.rootUser}' | base64 -d)
  S3_SECRET_KEY=$(kubectl -n minio get secret minio-creds -o jsonpath='{.data.rootPassword}' | base64 -d)
  kubectl -n argo-workflows create secret generic ingest-env \
    --from-literal=PG_URL="$PG_URL" \
    --from-literal=S3_ENDPOINT="http://minio.minio.svc:9000" \
    --from-literal=S3_ACCESS_KEY="$S3_ACCESS_KEY" \
    --from-literal=S3_SECRET_KEY="$S3_SECRET_KEY" \
    --from-literal=S3_BUCKET="warehouse/lakekeeper-warehouse" \
    --from-literal=S3_REGION="local-01" \
    --from-literal=LAKEKEEPER_URI="http://lakekeeper.lakekeeper.svc:8181" \
    --from-literal=LAKEKEEPER_WAREHOUSE="default" \
    --from-literal=RETENTION_DAYS="14"
  echo "ingest-env secret created"
fi
echo "kind + ArgoCD (hydrator) + apps ready"
