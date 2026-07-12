#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
ARGOCD_CHART_VERSION="10.1.3"   # from Step 1
kind get clusters | grep -q '^datalake-v2$' || kind create cluster --config hack/kind-config.yaml
helm repo add argo https://argoproj.github.io/argo-helm >/dev/null
helm upgrade --install argocd argo/argo-cd --version "$ARGOCD_CHART_VERSION" \
  -n argocd --create-namespace -f environments/kind/argocd-values.yaml --wait --timeout 5m
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
kubectl apply -f apps/project.yaml
[ -d apps/infra ] && ls apps/infra/*.yaml >/dev/null 2>&1 && kubectl apply -f apps/infra/
kubectl apply -f environments/kind/apps/
echo "kind + ArgoCD (hydrator) + apps ready"
