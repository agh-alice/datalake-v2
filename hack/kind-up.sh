#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
ARGOCD_CHART_VERSION="10.1.3"   # from Step 1
kind get clusters | grep -q '^datalake-v2$' || kind create cluster --config hack/kind-config.yaml
helm repo add argo https://argoproj.github.io/argo-helm >/dev/null
helm upgrade --install argocd argo/argo-cd --version "$ARGOCD_CHART_VERSION" \
  -n argocd --create-namespace -f environments/kind/argocd-values.yaml --wait --timeout 5m
# Private repo, read+write (commit-server pushes rendered branches). Owner decision 2026-07-12.
kubectl -n argocd create secret generic datalake-v2-repo \
  --from-literal=type=git \
  --from-literal=url=https://github.com/agh-alice/datalake-v2.git \
  --from-literal=username=x-access-token \
  --from-literal=password="$(gh auth token)" \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl -n argocd label secret datalake-v2-repo argocd.argoproj.io/secret-type=repository --overwrite
kubectl apply -f apps/project.yaml
[ -d apps/infra ] && ls apps/infra/*.yaml >/dev/null 2>&1 && kubectl apply -f apps/infra/
kubectl apply -f environments/kind/apps/
echo "kind + ArgoCD (hydrator) + apps ready"
