#!/usr/bin/env bash
set -euo pipefail
EXPECTED_APPS=(datalake-kind cloudnative-pg lakekeeper)   # extended by later tasks
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
if kubectl -n lakekeeper wait cluster/lakekeeper-db --for=condition=Ready --timeout=300s \
   && kubectl -n lakekeeper exec lakekeeper-db-1 -- psql -U postgres -Atc "SELECT 1" | grep -qx 1; then
  echo "lakekeeper-db OK"
else
  echo "FAIL: lakekeeper-db not Ready or not answering"; exit 1
fi
git fetch origin 'refs/heads/environments/*:refs/remotes/origin/environments/*' 2>/dev/null || true
# Hard gate (Task 2 review finding): a bare `A && B` under set -e falls through
# on non-match, and the final success echo would still run.
if git ls-remote --heads origin | grep -q 'refs/heads/environments/kind$'; then
  echo "hydrated branch environments/kind exists"
else
  echo "FAIL: hydrated branch environments/kind missing on origin"; exit 1
fi
# Hard gate: the probe must actually gate (Task 2/3 review pattern). A 4xx on the
# unconfigured-warehouse query is acceptable proof of liveness; connection failure is not.
if kubectl -n lakekeeper run rest-probe --rm -i --restart=Never --image=curlimages/curl -- \
     sh -c 'curl -s -o /dev/null -w "%{http_code}" http://lakekeeper.lakekeeper.svc:8181/catalog/v1/config?warehouse=none' \
     | grep -qE "^[234]"; then
  echo "lakekeeper REST endpoint reachable"
else
  echo "FAIL: lakekeeper REST endpoint unreachable"; exit 1
fi
echo "kind-verify: all applications Synced/Healthy"
