#!/usr/bin/env bash
set -euo pipefail
# Dependency-first order (Task 8 review finding): CRD-providing apps verify
# before their consumers. This ordering is NOT what makes convergence
# succeed -- ArgoCD's automated selfHeal retries each Application
# independently until its CRDs exist, regardless of the order this script
# checks them in. It only makes the *verify loop* deterministic-ish: a
# CRD-provider is far more likely to already be Synced/Healthy by the time
# we poll it, so an early consumer poll doesn't spend its own 60x10s budget
# waiting on a dependency that was going to converge anyway.
# cloudnative-pg / external-secrets / monitoring provide CRDs (Cluster,
# ExternalSecret/ClusterSecretStore, ServiceMonitor/PodMonitor/PrometheusRule)
# consumed by lakekeeper, argo-workflows and datalake-kind's chart templates.
# datalake-kind is checked last -- it renders resources (PrometheusRule,
# ServiceMonitor, CNPG Clusters, ExternalSecrets) that depend on every other
# app's CRDs. workflows-rbac is a chart template (SA/Role/RoleBinding), not
# an Application, so it has no separate entry here.
EXPECTED_APPS=(cloudnative-pg external-secrets monitoring lakekeeper argo-workflows datalake-kind)   # extended by later tasks
for app in "${EXPECTED_APPS[@]}"; do
  for i in $(seq 1 60); do
    sync=$(kubectl -n argocd get application "$app" -o jsonpath='{.status.sync.status}' 2>/dev/null || echo "")
    health=$(kubectl -n argocd get application "$app" -o jsonpath='{.status.health.status}' 2>/dev/null || echo "")
    [ "$sync" = "Synced" ] && [ "$health" = "Healthy" ] && { echo "OK: $app"; break; }
    [ "$i" = 60 ] && { echo "FAIL: $app sync=$sync health=$health"; exit 1; }
    sleep 10
  done
done
# Hard gate (final review R1): "Synced" can be true against a STALE rendered
# branch if the commit-server stops pushing (e.g. push-credential loss) --
# every probe above and below would still pass while chart changes silently
# stop deploying. Assert the hydrator's last successful dry SHA matches
# origin/main's HEAD. Right after a push, hydration takes ~1-2 min to catch
# up, so poll (every 15s, up to 300s) rather than checking once.
MAIN_SHA=$(git ls-remote origin refs/heads/main | cut -f1)
for i in $(seq 1 20); do
  DRY_SHA=$(kubectl -n argocd get application datalake-kind -o jsonpath='{.status.sourceHydrator.lastSuccessfulOperation.drySHA}' 2>/dev/null || echo "")
  if [ -n "$MAIN_SHA" ] && [ "$DRY_SHA" = "$MAIN_SHA" ]; then
    echo "hydration current (drySHA == origin/main)"
    break
  fi
  [ "$i" = 20 ] && { echo "FAIL: hydrator stale or dead (drySHA=$DRY_SHA main=$MAIN_SHA)"; exit 1; }
  sleep 15
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
# Manual Workflow run using the same image the CronWorkflow uses (Task 7) --
# the CronWorkflow itself ticks every 5m; this proves the pipeline-runner SA
# + RBAC + digest-pinned image actually execute a workflow, without waiting
# for a scheduled tick.
# Task 8 review finding (Task 7 concern #1): querying items[-1] sorted by
# creationTimestamp is racy -- a cron tick landing inside the 30s sleep below
# can make the newest workflow the scheduled one (possibly still Running),
# producing a spurious FAIL. Capture the created object's own name via
# `create -o name` and query exactly that workflow instead.
WF_NAME=$(kubectl -n argo-workflows create -o name -f - <<EOF
apiVersion: argoproj.io/v1alpha1
kind: Workflow
metadata: {generateName: hello-manual-}
spec:
  serviceAccountName: pipeline-runner
  entrypoint: main
  ttlStrategy: {secondsAfterCompletion: 3600}
  templates:
    - name: main
      container: {image: $(kubectl -n argo-workflows get cronworkflow hello -o jsonpath='{.spec.workflowSpec.templates[0].container.image}'), command: [sh, -c, "echo verify"]}
EOF
)
sleep 30
# Hard gate (probe pattern per Task 2/3 reviews) -- queries the exact
# workflow created above, immune to concurrent cron ticks.
if kubectl -n argo-workflows get "$WF_NAME" -o jsonpath='{.status.phase}' | grep -q Succeeded; then
  echo "workflow execution OK"
else
  echo "FAIL: manual verification workflow did not succeed"; exit 1
fi
# Banner moved here (final review R1): this must be the LAST line of the
# script. It used to print before the workflow probe above, so a log reader
# scanning for this line would see "success" on a run that later failed.
echo "kind-verify: all applications Synced/Healthy"
