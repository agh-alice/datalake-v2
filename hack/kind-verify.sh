#!/usr/bin/env bash
set -euo pipefail
# Dependency-first order (Task 8 review finding, carried forward through
# Plan 5 Task 4's rework): CRD-providing apps verify before their
# consumers. This ordering is NOT what makes convergence succeed --
# ArgoCD's automated selfHeal retries each Application independently
# until its CRDs exist, regardless of the order this script checks them
# in. It only makes the *verify loop* deterministic-ish: a CRD-provider is
# far more likely to already be Synced/Healthy by the time we poll it, so
# an early consumer poll doesn't spend its own 60x10s budget waiting on a
# dependency that was going to converge anyway.
#
# Plan 5 Task 4 rework: cloudnative-pg / external-secrets / dex /
# monitoring / minio / argo-workflows are the six kind-only Applications
# (environments/kind/infra/) that install what the platform provides for
# real in prod -- they provide CRDs (Cluster, ExternalSecret/
# ClusterSecretStore, ServiceMonitor/PodMonitor/PrometheusRule,
# CronWorkflow/Workflow) consumed by the three tier Applications
# (environments/kind/apps/) that replace the old single `datalake-kind`
# umbrella Application: datalake-storage, datalake-compute,
# datalake-orchestration -- checked last, in that dependency order
# (compute's Trino consumes storage's Lakekeeper + landing DB; both
# consume CNPG/External Secrets; orchestration consumes Argo Workflows'
# CRDs + external-secrets).
EXPECTED_APPS=(cloudnative-pg external-secrets dex monitoring minio argo-workflows datalake-storage datalake-compute datalake-orchestration)
for app in "${EXPECTED_APPS[@]}"; do
  for i in $(seq 1 60); do
    sync=$(kubectl -n argocd get application "$app" -o jsonpath='{.status.sync.status}' 2>/dev/null || echo "")
    health=$(kubectl -n argocd get application "$app" -o jsonpath='{.status.health.status}' 2>/dev/null || echo "")
    [ "$sync" = "Synced" ] && [ "$health" = "Healthy" ] && { echo "OK: $app"; break; }
    [ "$i" = 60 ] && { echo "FAIL: $app sync=$sync health=$health"; exit 1; }
    sleep 10
  done
done

# Hard gate (Plan 5 Task 4): the `platform` ClusterSecretStore
# (environments/kind/infra/cluster-secret-store.yaml) must be Ready --
# every one of the three tier Applications' ExternalSecret objects depends
# on it. hack/kind-up.sh already polls for this once at cluster-up time;
# re-asserted here as part of the acceptance run itself, not just the
# bootstrap script.
CSS_READY=$(kubectl get clustersecretstore platform -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || echo "")
if [ "$CSS_READY" = "True" ]; then
  echo "OK: ClusterSecretStore platform Ready"
else
  echo "FAIL: ClusterSecretStore platform not Ready (status=$CSS_READY)"; exit 1
fi

# Hard gate (final review R1): "Synced" can be true against a STALE rendered
# branch if the commit-server stops pushing (e.g. push-credential loss) --
# every probe above and below would still pass while chart changes silently
# stop deploying. Assert the hydrator's last successful dry SHA matches
# origin/main's HEAD, for ALL THREE tier Applications (Plan 5 Task 4 --
# each tier hydrates its own envs/prod/<tier> path independently; a stale
# hydration on any one of the three is the same silent-drift risk the
# single-Application version of this gate caught). Right after a push,
# hydration takes ~1-2 min to catch up, so poll (every 15s, up to 300s)
# rather than checking once.
MAIN_SHA=$(git ls-remote origin refs/heads/main | cut -f1)
for app in datalake-storage datalake-compute datalake-orchestration; do
  for i in $(seq 1 20); do
    DRY_SHA=$(kubectl -n argocd get application "$app" -o jsonpath='{.status.sourceHydrator.lastSuccessfulOperation.drySHA}' 2>/dev/null || echo "")
    if [ -n "$MAIN_SHA" ] && [ "$DRY_SHA" = "$MAIN_SHA" ]; then
      echo "hydration current for $app (drySHA == origin/main)"
      break
    fi
    [ "$i" = 20 ] && { echo "FAIL: $app hydrator stale or dead (drySHA=$DRY_SHA main=$MAIN_SHA)"; exit 1; }
    sleep 15
  done
done
# Hard gate (Plan 4 Task T1S4, design D-Dex): the OIDC discovery endpoint
# must actually resolve -- proves Dex is up and correctly serving its
# issuer, not just that the Application rolled up Healthy (a Dex pod can be
# Running/Ready per its own k8s probes yet still be misconfigured in a way
# that only shows up when something actually queries the OIDC surface).
# Same create/poll/logs/delete throwaway-pod pattern as every other probe
# in this file (Task 2/3 review: a non-TTY `kubectl run --rm -i` can drop
# the attached stdout).
kubectl -n dex delete pod oidc-discovery-probe --ignore-not-found >/dev/null 2>&1
kubectl -n dex run oidc-discovery-probe --restart=Never --image=curlimages/curl -- \
  sh -c 'curl -s http://dex.dex.svc.cluster.local:5556/dex/.well-known/openid-configuration' >/dev/null
for i in $(seq 1 30); do
  phase=$(kubectl -n dex get pod oidc-discovery-probe -o jsonpath='{.status.phase}' 2>/dev/null || echo "")
  { [ "$phase" = "Succeeded" ] || [ "$phase" = "Failed" ]; } && break
  sleep 2
done
OIDC_DISCOVERY=$(kubectl -n dex logs oidc-discovery-probe 2>/dev/null || echo "")
kubectl -n dex delete pod oidc-discovery-probe --ignore-not-found >/dev/null 2>&1
if echo "$OIDC_DISCOVERY" | grep -q '"issuer"' && echo "$OIDC_DISCOVERY" | grep -q '"authorization_endpoint"'; then
  echo "dex OIDC discovery endpoint resolves"
else
  echo "FAIL: dex OIDC discovery endpoint did not resolve (got: '$OIDC_DISCOVERY')"; exit 1
fi
# Hard gate (Plan 4 Task T1S4): Grafana must offer the Dex OAuth login
# option AND local-admin login/API access must keep working -- OIDC is
# additive on kind, not a replacement (see environments/kind/infra/
# monitoring.yaml's comment). Grafana's `/login` HTML embeds a bootData
# script tag containing `"oauth":{"generic_oauth":{...}}` once
# auth.generic_oauth.enabled is true (verified live against this cluster's
# actual rendered page, not assumed from docs) -- grep for the provider
# name set in that Application's grafana.ini (`"name":"Dex"`) rather than
# the bare string "generic_oauth", which also appears in unrelated static
# JS bundle references on the same page. Local-admin proof: the same
# Secret <release>-grafana admin-user/admin-password this Application
# already generates, hit against the real HTTP Basic auth `/api/user`
# endpoint.
GRAFANA_ADMIN_USER=$(kubectl -n monitoring get secret monitoring-grafana -o jsonpath='{.data.admin-user}' | base64 -d)
GRAFANA_ADMIN_PASSWORD=$(kubectl -n monitoring get secret monitoring-grafana -o jsonpath='{.data.admin-password}' | base64 -d)
kubectl -n monitoring delete pod grafana-oidc-probe --ignore-not-found >/dev/null 2>&1
cat <<PODYAML | kubectl -n monitoring apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: grafana-oidc-probe
spec:
  restartPolicy: Never
  containers:
    - name: probe
      image: curlimages/curl
      command:
        - sh
        - -c
        - |
          echo "===LOGIN_PAGE==="
          curl -s http://monitoring-grafana.monitoring.svc/login
          echo "===LOCAL_ADMIN_API==="
          curl -s -u '$GRAFANA_ADMIN_USER:$GRAFANA_ADMIN_PASSWORD' http://monitoring-grafana.monitoring.svc/api/user
PODYAML
phase=""
for i in $(seq 1 30); do
  phase=$(kubectl -n monitoring get pod grafana-oidc-probe -o jsonpath='{.status.phase}' 2>/dev/null || echo "")
  { [ "$phase" = "Succeeded" ] || [ "$phase" = "Failed" ]; } && break
  sleep 2
done
GRAFANA_PROBE_LOG=$(kubectl -n monitoring logs grafana-oidc-probe 2>/dev/null || echo "")
kubectl -n monitoring delete pod grafana-oidc-probe --ignore-not-found >/dev/null 2>&1
if echo "$GRAFANA_PROBE_LOG" | grep -q '"name":"Dex"' && echo "$GRAFANA_PROBE_LOG" | grep -q '"login":"admin"'; then
  echo "grafana OIDC login option present + local-admin login/API still works"
else
  echo "FAIL: grafana OIDC/local-admin proof failed (got: '$GRAFANA_PROBE_LOG')"; exit 1
fi
# Hard gate (probe pattern per Task 2/3 reviews: soft `A && echo` falls through under set -e)
# Primary resolved via currentPrimary (Task 3 review Minor: never pin -1; failover breaks it)
# Namespace `datalake-storage` (Plan 5 Task 1 merged the old dedicated
# `lakekeeper`/`landing-db` namespaces into the storage tier's single
# namespace; Task 4 updated every probe below to follow).
LK_PRIMARY=$(kubectl -n datalake-storage get cluster lakekeeper-db -o jsonpath='{.status.currentPrimary}')
if kubectl -n datalake-storage wait cluster/lakekeeper-db --for=condition=Ready --timeout=300s \
   && kubectl -n datalake-storage exec "$LK_PRIMARY" -- psql -U postgres -Atc "SELECT 1" | grep -qx 1; then
  echo "lakekeeper-db OK"
else
  echo "FAIL: lakekeeper-db not Ready or not answering"; exit 1
fi
# Hard gate + primary-resolved pod (Task 3 review: never pin -1; failover breaks it)
MD_PRIMARY=$(kubectl -n datalake-storage get cluster mon-data -o jsonpath='{.status.currentPrimary}')
if kubectl -n datalake-storage wait cluster/mon-data --for=condition=Ready --timeout=300s \
   && kubectl -n datalake-storage exec "$MD_PRIMARY" -- psql -U postgres -d mon_data -Atc "SHOW max_connections" | grep -qx 60; then
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
# Hard gate (Task 4 review Critical, carried forward): harness-provisioned
# secrets must exist -- ArgoCD once pruned one after it left the chart;
# verify they survive reconciliation. Plan 5 Task 3/4: all three are now
# ExternalSecret-sourced (envs/prod/<tier>/templates/external-secrets.yaml)
# against the `platform` ClusterSecretStore, not hack/kind-up.sh-created
# plain Secrets -- checking for the resulting TARGET Secret in each tier's
# namespace proves the whole ESO round-trip (remote Secret in
# eso-secret-source -> ClusterSecretStore -> ExternalSecret -> target
# Secret) actually worked, not just that the harness wrote the remote
# side.
for pair in "datalake-storage:lakekeeper-pg-encryption" "datalake-compute:landing-ro" "datalake-orchestration:ingest-env"; do
  ns=${pair%%:*}
  name=${pair##*:}
  if kubectl -n "$ns" get secret "$name" >/dev/null 2>&1; then
    echo "$ns/$name secret present (ExternalSecret round-trip OK)"
  else
    echo "FAIL: $ns/$name secret missing"; exit 1
  fi
done
# Hard gate: the probe must actually gate (Task 2/3 review pattern). A 4xx on the
# unconfigured-warehouse query is acceptable proof of liveness; connection failure is not.
# `kubectl run --rm -i` attaches container stdout to the client over the same session
# used for stdin; in a non-TTY runner the attach can silently fail to relay output (only
# the `--rm` "pod deleted" message reaches stdout, dropping the actual curl result) --
# create/poll/logs/delete avoids the attach path entirely.
kubectl -n datalake-storage delete pod rest-probe --ignore-not-found >/dev/null 2>&1
kubectl -n datalake-storage run rest-probe --restart=Never --image=curlimages/curl -- \
  sh -c 'curl -s -o /dev/null -w "%{http_code}" http://lakekeeper.datalake-storage.svc:8181/catalog/v1/config?warehouse=none' >/dev/null
for i in $(seq 1 30); do
  phase=$(kubectl -n datalake-storage get pod rest-probe -o jsonpath='{.status.phase}' 2>/dev/null || echo "")
  { [ "$phase" = "Succeeded" ] || [ "$phase" = "Failed" ]; } && break
  sleep 2
done
REST_CODE=$(kubectl -n datalake-storage logs rest-probe 2>/dev/null || echo "")
kubectl -n datalake-storage delete pod rest-probe --ignore-not-found >/dev/null 2>&1
if echo "$REST_CODE" | grep -qE "^[234]"; then
  echo "lakekeeper REST endpoint reachable"
else
  echo "FAIL: lakekeeper REST endpoint unreachable (got: '$REST_CODE')"; exit 1
fi
# Hard gate (Task 1, Plan 2): the `default` warehouse must exist -- proves
# hack/lakekeeper-warehouse.sh actually ran and Lakekeeper accepted the
# MinIO-backed storage profile, not just that the REST endpoint answers.
# Same create/poll/logs/delete pattern as the probe above.
kubectl -n datalake-storage delete pod warehouse-probe --ignore-not-found >/dev/null 2>&1
kubectl -n datalake-storage run warehouse-probe --restart=Never --image=curlimages/curl -- \
  sh -c 'curl -s -H "Authorization: Bearer dummy" http://lakekeeper.datalake-storage.svc:8181/management/v1/warehouse' >/dev/null
for i in $(seq 1 30); do
  phase=$(kubectl -n datalake-storage get pod warehouse-probe -o jsonpath='{.status.phase}' 2>/dev/null || echo "")
  { [ "$phase" = "Succeeded" ] || [ "$phase" = "Failed" ]; } && break
  sleep 2
done
WH_LIST=$(kubectl -n datalake-storage logs warehouse-probe 2>/dev/null || echo "")
kubectl -n datalake-storage delete pod warehouse-probe --ignore-not-found >/dev/null 2>&1
if echo "$WH_LIST" | grep -q '"name":"default"'; then
  echo "lakekeeper warehouse 'default' present"
else
  echo "FAIL: lakekeeper warehouse 'default' not found (got: '$WH_LIST')"; exit 1
fi
# Hard gate (probe pattern per Task 2/3 reviews) — Prometheus STS name is
# discovered by label, never hardcoded (chart-generated name can change).
# Extended (Plan 2 Task 5): also asserts WorkflowFailed (the `datalake-
# pipeline` PrometheusRule group, envs/prod/orchestration/templates/
# datalake-alerts.yaml) loaded alongside the original `datalake` group's
# LandingDBXidAgeHigh (envs/prod/storage/templates/datalake-alerts.yaml,
# Plan 5 Task 1 split these into two PrometheusRule objects, one per
# tier) -- proves BOTH tiers' rules hydrated, not just whichever happened
# to already be present.
PROM_STS=$(kubectl -n monitoring get sts -l app.kubernetes.io/name=prometheus -o jsonpath='{.items[0].metadata.name}')
RULES_JSON=$(kubectl -n monitoring exec "sts/$PROM_STS" -c prometheus -- \
     wget -qO- 'http://localhost:9090/api/v1/rules')
if echo "$RULES_JSON" | grep -q LandingDBXidAgeHigh && echo "$RULES_JSON" | grep -q WorkflowFailed \
   && echo "$RULES_JSON" | grep -q IcebergSnapshotStale && echo "$RULES_JSON" | grep -q WorkflowChronicFailure \
   && echo "$RULES_JSON" | grep -q SiteSonarStale && echo "$RULES_JSON" | grep -q IcebergMaintenanceStale; then
  echo "alert rules loaded (datalake + datalake-pipeline groups, incl. IcebergSnapshotStale/WorkflowChronicFailure/SiteSonarStale/IcebergMaintenanceStale)"
else
  echo "FAIL: datalake alert rules not loaded in Prometheus (one or more of LandingDBXidAgeHigh/WorkflowFailed/IcebergSnapshotStale/WorkflowChronicFailure/SiteSonarStale/IcebergMaintenanceStale missing)"; exit 1
fi

# Hard gate (Plan 5 Task 4 -- brief: "Live-verify the CNPG `job` label
# claim... query Prometheus for cnpg_pg_replication_streaming_replicas and
# confirm the job label really has that shape. Report the actual label
# values."). envs/prod/storage/templates/datalake-alerts.yaml's
# LandingDBXidAgeHigh/CNPGClusterUnhealthy exprs hardcode
# job="datalake-storage/mon-data" / job="datalake-storage/lakekeeper-db",
# documented there as "not re-verified live against the prod tenant
# cluster... flagging as a Task 3/5 follow-up". This is that
# verification, run against kind's real CNPG + prometheus-operator stack
# (both database clusters are up by this point in the script). Both
# clusters' PodMonitors are named after the CNPG cluster
# (envs/prod/storage/templates/monitoring.yaml: `name: mon-data` / `name:
# lakekeeper-db`) -- prometheus-operator's default job-relabeling for a
# PodMonitor-derived target is `<namespace>/<podmonitor-name>` (NOT, as
# the alert file's own comment implies, an identity the CNPG exporter
# itself sets), so the "datalake-storage/mon-data" shape holds ONLY
# because the PodMonitor happens to be named identically to the CNPG
# Cluster it selects -- load-bearing on that naming choice, not a
# CNPG-exporter guarantee. Reporting the actual observed values either way.
CNPG_METRICS_JSON=$(kubectl -n monitoring exec "sts/$PROM_STS" -c prometheus -- \
     wget -qO- 'http://localhost:9090/api/v1/query?query=cnpg_pg_replication_streaming_replicas')
OBSERVED_JOBS=$(echo "$CNPG_METRICS_JSON" | grep -oE '"job":"[^"]*"' | sort -u | tr '\n' ' ')
echo "Task 4 CNPG job-label live-verification -- cnpg_pg_replication_streaming_replicas observed job label values: ${OBSERVED_JOBS:-<none>}"
if echo "$CNPG_METRICS_JSON" | grep -q '"job":"datalake-storage/mon-data"' \
   && echo "$CNPG_METRICS_JSON" | grep -q '"job":"datalake-storage/lakekeeper-db"'; then
  echo "CONFIRMED: job label shape is <namespace>/<podmonitor-name> == datalake-storage/mon-data and datalake-storage/lakekeeper-db for both CNPG clusters -- envs/prod/storage/templates/datalake-alerts.yaml's job= selectors match live metrics, load-bearing on the PodMonitor/Cluster name match (see comment above)."
else
  echo "FAIL: CNPG job label does NOT match the shape envs/prod/storage/templates/datalake-alerts.yaml's alert exprs assume (job=\"datalake-storage/mon-data\" / job=\"datalake-storage/lakekeeper-db\") -- observed instead: ${OBSERVED_JOBS:-<none>}. LandingDBXidAgeHigh/CNPGClusterUnhealthy are broken as committed -- this is a live finding to report, not paper over."
  exit 1
fi

# Hard gate (Plan 4 Task T2S1 prework): fire a synthetic always-firing
# PrometheusRule end-to-end through the real pipeline -- Prometheus evals it
# -> Alertmanager groups/routes it to the `slack-datalake` receiver -> the
# receiver POSTs to echo-receiver -> the POST body lands in `kubectl logs`.
# Proves the whole chain environments/kind/infra/monitoring.yaml wires
# (route group_by/group_wait, the `slack-datalake` receiver, the
# Secret-mounted api_url_file) actually works, not just that each piece's
# config parses. echo-receiver itself now lives in namespace
# `datalake-orchestration` (Plan 5 Task 1 retargeted it out of
# `monitoring`, envs/prod/orchestration/templates/echo-receiver.yaml) --
# the PrometheusRule/Alertmanager side of this probe stays in `monitoring`
# (still kind-only, unmoved). Throwaway PrometheusRule + probe pod, same
# create/verify/delete pattern as every other probe in this file; removed
# unconditionally on the way out (trap) so a failed gate doesn't leave a
# permanent always-firing alert behind.
SYNTH_RULE_CLEANUP() { kubectl -n monitoring delete prometheusrule kind-verify-synthetic-alert --ignore-not-found >/dev/null 2>&1; }
trap SYNTH_RULE_CLEANUP EXIT
kubectl -n monitoring apply -f - <<'RULEYAML'
apiVersion: monitoring.coreos.com/v1
kind: PrometheusRule
metadata:
  name: kind-verify-synthetic-alert
  namespace: monitoring
spec:
  groups:
    - name: kind-verify-synthetic
      rules:
        - alert: KindVerifySyntheticAlert
          expr: vector(1) > 0
          labels: {severity: warning}
          annotations: {summary: "kind-verify synthetic alert -- always firing, removed at the end of this gate"}
RULEYAML
# group_wait (30s) + a Prometheus eval cycle + Alertmanager's own dispatch
# need real wall-clock time before the receiver actually POSTs -- poll the
# echo-receiver pod's logs rather than a single fixed sleep.
ECHO_POD=""
for i in $(seq 1 30); do
  ECHO_POD=$(kubectl -n datalake-orchestration get pod -l app=echo-receiver -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
  [ -n "$ECHO_POD" ] && [ "$(kubectl -n datalake-orchestration get pod "$ECHO_POD" -o jsonpath='{.status.phase}' 2>/dev/null)" = "Running" ] && break
  sleep 5
done
[ -z "$ECHO_POD" ] && { echo "FAIL: echo-receiver pod not found/Running"; exit 1; }
SYNTH_OK=""
for i in $(seq 1 24); do
  ECHO_LOG=$(kubectl -n datalake-orchestration logs "$ECHO_POD" 2>/dev/null || echo "")
  if echo "$ECHO_LOG" | grep -q KindVerifySyntheticAlert; then
    SYNTH_OK=1
    break
  fi
  sleep 10
done
if [ -n "$SYNTH_OK" ] && echo "$ECHO_LOG" | grep -q 'datalake-alerts'; then
  echo "synthetic alert reached echo-receiver end-to-end (route/group/receiver pipeline OK)"
else
  echo "FAIL: synthetic alert never reached echo-receiver (Alertmanager route/receiver pipeline broken) -- last echo-receiver log:"
  echo "$ECHO_LOG"
  exit 1
fi
SYNTH_RULE_CLEANUP
trap - EXIT
# Manual Workflow run using the same image the CronWorkflow uses (Task 7) --
# the CronWorkflow itself ticks every 6h on kind (values-kind.yaml); this
# proves the pipeline-runner SA + RBAC + digest-pinned image actually
# execute a workflow, without waiting for a scheduled tick. Namespace
# `datalake-orchestration` (Plan 5 Task 1 retargeted every CronWorkflow/
# Workflow object + the pipeline-runner SA/RBAC out of the old umbrella
# chart's `argo-workflows` namespace).
# Task 8 review finding (Task 7 concern #1): querying items[-1] sorted by
# creationTimestamp is racy -- a cron tick landing inside the 30s sleep below
# can make the newest workflow the scheduled one (possibly still Running),
# producing a spurious FAIL. Capture the created object's own name via
# `create -o name` and query exactly that workflow instead.
WF_NAME=$(kubectl -n datalake-orchestration create -o name -f - <<EOF
apiVersion: argoproj.io/v1alpha1
kind: Workflow
metadata: {generateName: hello-manual-}
spec:
  serviceAccountName: pipeline-runner
  entrypoint: main
  ttlStrategy: {secondsAfterCompletion: 3600}
  templates:
    - name: main
      container: {image: $(kubectl -n datalake-orchestration get cronworkflow hello -o jsonpath='{.spec.workflowSpec.templates[0].container.image}'), command: [sh, -c, "echo verify"]}
EOF
)
sleep 30
# Hard gate (probe pattern per Task 2/3 reviews) -- queries the exact
# workflow created above, immune to concurrent cron ticks.
if kubectl -n datalake-orchestration get "$WF_NAME" -o jsonpath='{.status.phase}' | grep -q Succeeded; then
  echo "workflow execution OK"
else
  echo "FAIL: manual verification workflow did not succeed"; exit 1
fi
# Hard gate (Plan 2 Task 3): Iceberg contents probe. Everything above proves
# infrastructure is up; this proves data actually flowed fixture-PostgreSQL
# -> dlt -> Lakekeeper/Iceberg/MinIO end to end. Depends on hack/seed-
# fixture.sh + hack/run-ingest-once.sh having been run first (Task 3's
# acceptance sequence: seed -> run-ingest-once (x2) -> kind-verify) -- this
# gate does not run them itself, it only asserts their result persisted.
#
# Runs the ingest image's own `python -c` (brief Step 4) as a throwaway pod,
# envFrom Secret ingest-env so it authenticates to Lakekeeper/MinIO exactly
# like the real ingestion Workflow (same flat-key iceberg_catalog_config
# dict trap applies here -- see ingest/src/alice_ingest/pipeline.py's
# configure_dlt() docstring and research/2026-07-12_dlt-iceberg-lakekeeper-
# api-verification.md).
#
# Verified empirically against this cluster in Task 3, not from memory/docs:
#   `catalog.load_table(...).scan().count()` IS a valid direct call on the
#   pinned pyiceberg (0.11.1, resolved via dlt[pyiceberg]==1.28.2) -- no
#   to_arrow().num_rows fallback needed.
#
# LPM casing assertion (review fix, Task 3 -- design spec section 4,
# deliverables/2026-07-12-datalake-v2-design.md: "Fixed at ingestion rather
# than in the consumer: ... LPMPassName/LPMPASSNAME casing"). Before the
# fix, dlt's naming convention did NOT collapse the two casings on its own:
# the mixed-case fixture value `LPMPassName` has a detectable camelCase
# boundary and normalized to `jdl__lpm_pass_name`, while all-caps
# `LPMPASSNAME` has no boundary to split and normalized to
# `jdl__lpmpassname` -- two real, distinct columns, which is the split-key
# regression the spec mandates fixing. ingest/src/alice_ingest/jdl.py now
# coalesces both casings into the canonical `LPMPassName` key BEFORE dlt
# ever sees the record, so post-fix only `jdl__lpm_pass_name` should exist.
# This gate now asserts BOTH sides: the merged column present AND the
# split-casing column absent, so a regression in either direction (merge
# stops working, or a future dlt/schema change reintroduces the split)
# fails the gate rather than passing silently.
# Plan 5 Task 1/4: image source moved from the deleted `chart/values.yaml`
# to `envs/prod/orchestration/values.yaml`.
INGEST_IMAGE=$(yq -r '.images.ingest' envs/prod/orchestration/values.yaml)
kubectl -n datalake-orchestration delete pod iceberg-contents-probe --ignore-not-found >/dev/null 2>&1
cat <<PODYAML | kubectl -n datalake-orchestration apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: iceberg-contents-probe
spec:
  restartPolicy: Never
  containers:
    - name: probe
      image: $INGEST_IMAGE
      envFrom:
        - secretRef: {name: ingest-env}
      command:
        - python
        - -c
        - |
          import os, sys
          from pyiceberg.catalog import load_catalog
          warehouse = os.environ["LAKEKEEPER_WAREHOUSE"]
          catalog = load_catalog(
              warehouse,
              **{
                  "uri": os.environ["LAKEKEEPER_URI"].rstrip("/") + "/catalog",
                  "type": "rest",
                  "warehouse": warehouse,
                  "header.X-Iceberg-Access-Delegation": "vended-credentials",
                  "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
                  "s3.endpoint": os.environ["S3_ENDPOINT"],
                  "s3.access-key-id": os.environ["S3_ACCESS_KEY"],
                  "s3.secret-access-key": os.environ["S3_SECRET_KEY"],
                  "s3.region": os.environ.get("S3_REGION", "local-01"),
              },
          )
          n = catalog.load_table("alice.job_info").scan().count()
          print(f"job_info count={n}")
          if n < 900:
              print(f"FAIL: job_info count {n} < 900")
              sys.exit(1)
          cols = [f.name for f in catalog.load_table("alice.mon_jdls_parsed").schema().fields]
          print("mon_jdls_parsed columns:", cols)
          if "jdl__lpm_pass_name" not in cols:
              print("FAIL: missing merged JDL column jdl__lpm_pass_name")
              sys.exit(1)
          if "jdl__lpmpassname" in cols:
              print("FAIL: split-casing column jdl__lpmpassname present (LPM casing merge regression)")
              sys.exit(1)
          # Final-review N3: mon_jdls's JDL list fields (e.g. Packages) must
          # land as VALUE columns, not spin off dlt child tables -- the ML
          # consumer's data contract expects Packages as a column
          # (pipeline.py's build_mon_jdls_resource(), max_table_nesting=1).
          # Assert BOTH sides, same regression-proof shape as the LPM
          # casing check above: the value column present, AND no child
          # table of mon_jdls_parsed exists at all in the catalog.
          if "jdl__packages" not in cols:
              print("FAIL: missing jdl__packages value column (max_table_nesting regression)")
              sys.exit(1)
          alice_tables = [".".join(t) for t in catalog.list_tables("alice")]
          child_tables = [t for t in alice_tables if t.startswith("alice.mon_jdls_parsed__")]
          if child_tables:
              print(f"FAIL: mon_jdls_parsed child table(s) present (max_table_nesting regression): {child_tables}")
              sys.exit(1)
          print("iceberg-contents-probe: OK")
PODYAML
phase=""
for i in $(seq 1 30); do
  phase=$(kubectl -n datalake-orchestration get pod iceberg-contents-probe -o jsonpath='{.status.phase}' 2>/dev/null || echo "")
  { [ "$phase" = "Succeeded" ] || [ "$phase" = "Failed" ]; } && break
  sleep 5
done
PROBE_LOG=$(kubectl -n datalake-orchestration logs iceberg-contents-probe 2>/dev/null || echo "")
kubectl -n datalake-orchestration delete pod iceberg-contents-probe --ignore-not-found >/dev/null 2>&1
echo "$PROBE_LOG"
if [ "$phase" = "Succeeded" ] && echo "$PROBE_LOG" | grep -q "iceberg-contents-probe: OK"; then
  echo "iceberg contents OK (job_info >=900 rows, mon_jdls_parsed jdl__lpm_pass_name present and jdl__lpmpassname absent, jdl__packages column present and no mon_jdls_parsed__* child tables)"
else
  echo "FAIL: iceberg-contents-probe phase=$phase"; exit 1
fi
# Hard gate (Plan 3 Task 1): Trino query probes. Everything above proves the
# lakehouse (Iceberg/Lakekeeper/MinIO) and the landing DB (mon-data) are
# each independently correct; this proves Trino's SQL layer actually
# federates both -- `lake.alice.job_info` (Iceberg REST catalog, vended
# creds) and `landing.public.job_info` (PostgreSQL connector, the
# `trino_ro` read-only role from hack/kind-up.sh). Namespace
# `datalake-compute` (Plan 5 Task 1 retargeted Trino out of the old
# umbrella chart's dedicated `trino` namespace).
#
# Reuses the ingest image (already resolved above as $INGEST_IMAGE) rather
# than curlimages/curl: the Trino REST client protocol
# (https://trino.io/docs/current/develop/client-protocol.html) requires
# following `nextUri` across possibly-several polls before `data` appears
# (state QUEUED/RUNNING -> FINISHED), which is impractical to parse
# reliably with grep/sed the way the simpler single-shot lakekeeper probes
# above do -- the ingest image already ships `requests` (pinned, no new
# dep, same library Task 2's Trino client will reuse). No envFrom secret
# needed: the probe is a plain Trino SQL client hitting the coordinator's
# HTTP API with an arbitrary X-Trino-User header -- Trino itself holds the
# landing-ro credentials server-side via envFrom + `${ENV:...}` (see
# envs/prod/compute/values.yaml's `envFrom`), the querying client never
# sees them.
kubectl -n datalake-compute delete pod trino-query-probe --ignore-not-found >/dev/null 2>&1
cat <<PODYAML | kubectl -n datalake-compute apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: trino-query-probe
spec:
  restartPolicy: Never
  containers:
    - name: probe
      image: $INGEST_IMAGE
      command:
        - python
        - -c
        - |
          import sys, time
          import requests

          TRINO = "http://trino.datalake-compute.svc:8080"
          HEADERS = {"X-Trino-User": "kind-verify", "Content-Type": "text/plain"}

          def run_query(sql, attempts=20, delay=15):
              last_err = None
              for attempt in range(1, attempts + 1):
                  try:
                      resp = requests.post(f"{TRINO}/v1/statement", data=sql, headers=HEADERS, timeout=30)
                      resp.raise_for_status()
                      result = resp.json()
                      rows = []
                      while True:
                          if "error" in result:
                              raise RuntimeError(f"query error: {result['error']}")
                          rows.extend(result.get("data") or [])
                          next_uri = result.get("nextUri")
                          if not next_uri:
                              return rows
                          time.sleep(1)
                          resp = requests.get(next_uri, timeout=30)
                          resp.raise_for_status()
                          result = resp.json()
                  except Exception as exc:  # noqa: BLE001 -- coordinator may still be warming up
                      last_err = exc
                      print(f"query attempt {attempt}/{attempts} failed: {exc}; retrying in {delay}s")
                      time.sleep(delay)
              raise RuntimeError(f"query never succeeded after {attempts} attempts: {last_err}")

          # Hard gate (Plan 5 Task 4 -- brief: relax from an exact set to
          # "lake and landing are present, AND nothing appears outside the
          # allowlist {lake, landing, system, tpch, tpcds}"). Reason
          # (Task 2/3 review, relayed to Task 4): Helm cannot delete
          # subchart values keys through the `dependencies:` path (a
          # confirmed upstream limitation, helm/helm#9027 and friends) --
          # UNLESS a second `-f` values file also touches a key under
          # `trino:`, in which case the null-deletion DOES propagate
          # (empirically confirmed by the reviewer against this exact
          # chart+dependency shape). envs/prod/compute/values-kind.yaml
          # necessarily touches `trino:` (image, coordinator, catalogs.lake)
          # for its own unrelated reasons, so kind's actual rendered
          # catalog set is expected to be {lake, landing, system} -- but
          # this gate does not assume that outcome, it verifies the
          # allowlist + required-pair shape either way (accepted, not
          # fought: tpch/tpcds are synthetic connectors with no external
          # I/O and no credentials, if they do appear).
          ALLOWED_CATALOGS = {"lake", "landing", "system", "tpch", "tpcds"}
          REQUIRED_CATALOGS = {"lake", "landing"}
          catalog_rows = run_query("SHOW CATALOGS")
          catalogs = sorted(row[0] for row in catalog_rows)
          catalogs_set = set(catalogs)
          print(f"SHOW CATALOGS -> {catalogs}")
          missing = REQUIRED_CATALOGS - catalogs_set
          unexpected = catalogs_set - ALLOWED_CATALOGS
          if missing or unexpected:
              print(
                  f"FAIL: SHOW CATALOGS returned {catalogs} -- missing required "
                  f"{sorted(missing)}, unexpected outside the allowlist "
                  f"{sorted(unexpected)} (allowlist: {sorted(ALLOWED_CATALOGS)})"
              )
              sys.exit(1)

          lake_rows = run_query("SELECT count(*) FROM lake.alice.job_info")
          lake_count = lake_rows[0][0]
          print(f"lake.alice.job_info count={lake_count}")
          if lake_count < 900:
              print(f"FAIL: lake.alice.job_info count {lake_count} < 900")
              sys.exit(1)

          landing_rows = run_query("SELECT count(*) FROM landing.public.job_info")
          print(f"landing.public.job_info count={landing_rows[0][0]}")

          # Hard gate (Plan 3 Task 2): the lake.contract schema's views
          # actually select -- proves alice-ingest apply-views ran and the
          # dtypes-contract column spellings (contract_columns.py) resolve
          # against the live dlt-normalized columns, not just that the DDL
          # parsed. Named columns per the brief's acceptance line; count
          # parity against job_info (the same representative table the
          # probe above already counted) proves the view is not silently
          # dropping/duplicating rows relative to its base table.
          # NOTE: no backtick characters in this comment block -- this whole
          # pod spec lives inside an UNQUOTED cat <<PODYAML heredoc (needed
          # for $INGEST_IMAGE to expand), so a backtick here would be
          # evaluated by bash as command substitution before the YAML is
          # ever emitted (verified live: caused spurious "command not
          # found" stderr noise in this script's own output -- harmless to
          # the JSON/YAML payload itself since it substitutes to empty
          # string inside a Python comment, but wrong and worth avoiding).
          contract_rows = run_query(
              'SELECT "LPMPassName", "TTL", "Packages" '
              'FROM lake.contract.mon_jdls_parsed LIMIT 5'
          )
          print(f"lake.contract.mon_jdls_parsed sample rows={len(contract_rows)}")
          if not contract_rows:
              print(
                  "FAIL: lake.contract.mon_jdls_parsed returned no rows for "
                  "LPMPassName/TTL/Packages -- has alice-ingest apply-views run?"
              )
              sys.exit(1)

          contract_job_info_count = run_query("SELECT count(*) FROM lake.contract.job_info")[0][0]
          print(f"lake.contract.job_info count={contract_job_info_count}")
          if contract_job_info_count != lake_count:
              print(
                  f"FAIL: lake.contract.job_info count {contract_job_info_count} "
                  f"!= lake.alice.job_info count {lake_count}"
              )
              sys.exit(1)

          print("trino-query-probe: OK")
PODYAML
phase=""
for i in $(seq 1 60); do
  phase=$(kubectl -n datalake-compute get pod trino-query-probe -o jsonpath='{.status.phase}' 2>/dev/null || echo "")
  { [ "$phase" = "Succeeded" ] || [ "$phase" = "Failed" ]; } && break
  sleep 10
done
TRINO_PROBE_LOG=$(kubectl -n datalake-compute logs trino-query-probe 2>/dev/null || echo "")
kubectl -n datalake-compute delete pod trino-query-probe --ignore-not-found >/dev/null 2>&1
echo "$TRINO_PROBE_LOG"
if [ "$phase" = "Succeeded" ] && echo "$TRINO_PROBE_LOG" | grep -q "trino-query-probe: OK"; then
  echo "trino query probes OK (SHOW CATALOGS within the allowlist and covering lake+landing, lake.alice.job_info >=900 rows, landing.public.job_info SELECT succeeds, lake.contract.mon_jdls_parsed LPMPassName/TTL/Packages return data, lake.contract.job_info count matches lake.alice.job_info)"
else
  echo "FAIL: trino-query-probe phase=$phase"; exit 1
fi
# Banner moved here (final review R1): this must be the LAST line of the
# script. It used to print before the workflow probe above, so a log reader
# scanning for this line would see "success" on a run that later failed.
echo "kind-verify: all applications Synced/Healthy"
