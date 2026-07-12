#!/usr/bin/env bash
set -euo pipefail
EXPECTED_APPS=(datalake-kind cloudnative-pg lakekeeper external-secrets monitoring)   # extended by later tasks
for app in "${EXPECTED_APPS[@]}"; do
  for i in $(seq 1 60); do
    sync=$(kubectl -n argocd get application "$app" -o jsonpath='{.status.sync.status}' 2>/dev/null || echo "")
    health=$(kubectl -n argocd get application "$app" -o jsonpath='{.status.health.status}' 2>/dev/null || echo "")
    [ "$sync" = "Synced" ] && [ "$health" = "Healthy" ] && { echo "OK: $app"; break; }
    [ "$i" = 60 ] && { echo "FAIL: $app sync=$sync health=$health"; exit 1; }
    sleep 10
  done
done
# Hard gate (probe pattern per Task 2/3 reviews: soft `A && echo` falls through under set -e)
# Primary resolved via currentPrimary (Task 3 review Minor: never pin -1; failover breaks it)
LK_PRIMARY=$(kubectl -n lakekeeper get cluster lakekeeper-db -o jsonpath='{.status.currentPrimary}')
if kubectl -n lakekeeper wait cluster/lakekeeper-db --for=condition=Ready --timeout=300s \
   && kubectl -n lakekeeper exec "$LK_PRIMARY" -- psql -U postgres -Atc "SELECT 1" | grep -qx 1; then
  echo "lakekeeper-db OK"
else
  echo "FAIL: lakekeeper-db not Ready or not answering"; exit 1
fi
# Hard gate + primary-resolved pod (Task 3 review: never pin -1; failover breaks it)
MD_PRIMARY=$(kubectl -n landing-db get cluster mon-data -o jsonpath='{.status.currentPrimary}')
if kubectl -n landing-db wait cluster/mon-data --for=condition=Ready --timeout=300s \
   && kubectl -n landing-db exec "$MD_PRIMARY" -- psql -U postgres -d mon_data -Atc "SHOW max_connections" | grep -qx 60; then
  echo "landing-db OK"
else
  echo "FAIL: mon-data not Ready or max_connections wrong"; exit 1
fi
git fetch origin 'refs/heads/environments/*:refs/remotes/origin/environments/*' 2>/dev/null || true
# Hard gate (Task 2 review finding): a bare `A && B` under set -e falls through
# on non-match, and the final success echo would still run.
if git ls-remote --heads origin | grep -q 'refs/heads/environments/kind$'; then
  echo "hydrated branch environments/kind exists"
else
  echo "FAIL: hydrated branch environments/kind missing on origin"; exit 1
fi
# Hard gate (Task 4 review Critical): harness-provisioned secret must exist —
# ArgoCD once pruned it after it left the chart; verify it survives reconciliation.
if kubectl -n lakekeeper get secret lakekeeper-pg-encryption >/dev/null 2>&1; then
  echo "lakekeeper-pg-encryption secret present"
else
  echo "FAIL: lakekeeper-pg-encryption secret missing"; exit 1
fi
# Hard gate: the probe must actually gate (Task 2/3 review pattern). A 4xx on the
# unconfigured-warehouse query is acceptable proof of liveness; connection failure is not.
# `kubectl run --rm -i` attaches container stdout to the client over the same session
# used for stdin; in a non-TTY runner the attach can silently fail to relay output (only
# the `--rm` "pod deleted" message reaches stdout, dropping the actual curl result) --
# create/poll/logs/delete avoids the attach path entirely.
kubectl -n lakekeeper delete pod rest-probe --ignore-not-found >/dev/null 2>&1
kubectl -n lakekeeper run rest-probe --restart=Never --image=curlimages/curl -- \
  sh -c 'curl -s -o /dev/null -w "%{http_code}" http://lakekeeper.lakekeeper.svc:8181/catalog/v1/config?warehouse=none' >/dev/null
for i in $(seq 1 30); do
  phase=$(kubectl -n lakekeeper get pod rest-probe -o jsonpath='{.status.phase}' 2>/dev/null || echo "")
  { [ "$phase" = "Succeeded" ] || [ "$phase" = "Failed" ]; } && break
  sleep 2
done
REST_CODE=$(kubectl -n lakekeeper logs rest-probe 2>/dev/null || echo "")
kubectl -n lakekeeper delete pod rest-probe --ignore-not-found >/dev/null 2>&1
if echo "$REST_CODE" | grep -qE "^[234]"; then
  echo "lakekeeper REST endpoint reachable"
else
  echo "FAIL: lakekeeper REST endpoint unreachable (got: '$REST_CODE')"; exit 1
fi
# Hard gate (probe pattern per Task 2/3 reviews) — Prometheus STS name is
# discovered by label, never hardcoded (chart-generated name can change).
PROM_STS=$(kubectl -n monitoring get sts -l app.kubernetes.io/name=prometheus -o jsonpath='{.items[0].metadata.name}')
if kubectl -n monitoring exec "sts/$PROM_STS" -c prometheus -- \
     wget -qO- 'http://localhost:9090/api/v1/rules' | grep -q LandingDBXidAgeHigh; then
  echo "alert rules loaded"
else
  echo "FAIL: datalake alert rules not loaded in Prometheus"; exit 1
fi
echo "kind-verify: all applications Synced/Healthy"
