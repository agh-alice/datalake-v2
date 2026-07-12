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
# minio has no CRD relationship to anything -- it's placed before lakekeeper
# only because the warehouse hard-gate below (which needs lakekeeper AND
# minio up) reads more naturally right after both have been probed Healthy
# (Task 1, Plan 2). datalake-kind is checked last -- it renders resources
# (PrometheusRule, ServiceMonitor, CNPG Clusters, ExternalSecrets) that
# depend on every other app's CRDs. workflows-rbac is a chart template
# (SA/Role/RoleBinding), not an Application, so it has no separate entry here.
# trino (Plan 3 Task 1) inserted right before datalake-kind: it consumes
# lakekeeper's REST catalog and landing-db's mon-data Cluster (the latter is
# rendered BY datalake-kind, but that dependency is on the Cluster object
# existing, not on datalake-kind's Application being Synced/Healthy -- CNPG
# creates the Cluster as soon as the chart's manifests land, regardless of
# the parent Application's own health rollup) -- datalake-kind itself still
# has to stay last for the reason in the note above.
EXPECTED_APPS=(cloudnative-pg external-secrets monitoring minio lakekeeper argo-workflows trino datalake-kind)   # extended by later tasks
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
# Hard gate (Task 1, Plan 2): the `default` warehouse must exist -- proves
# hack/lakekeeper-warehouse.sh actually ran and Lakekeeper accepted the
# MinIO-backed storage profile, not just that the REST endpoint answers.
# Same create/poll/logs/delete pattern as the probe above.
kubectl -n lakekeeper delete pod warehouse-probe --ignore-not-found >/dev/null 2>&1
kubectl -n lakekeeper run warehouse-probe --restart=Never --image=curlimages/curl -- \
  sh -c 'curl -s -H "Authorization: Bearer dummy" http://lakekeeper.lakekeeper.svc:8181/management/v1/warehouse' >/dev/null
for i in $(seq 1 30); do
  phase=$(kubectl -n lakekeeper get pod warehouse-probe -o jsonpath='{.status.phase}' 2>/dev/null || echo "")
  { [ "$phase" = "Succeeded" ] || [ "$phase" = "Failed" ]; } && break
  sleep 2
done
WH_LIST=$(kubectl -n lakekeeper logs warehouse-probe 2>/dev/null || echo "")
kubectl -n lakekeeper delete pod warehouse-probe --ignore-not-found >/dev/null 2>&1
if echo "$WH_LIST" | grep -q '"name":"default"'; then
  echo "lakekeeper warehouse 'default' present"
else
  echo "FAIL: lakekeeper warehouse 'default' not found (got: '$WH_LIST')"; exit 1
fi
# Hard gate (probe pattern per Task 2/3 reviews) — Prometheus STS name is
# discovered by label, never hardcoded (chart-generated name can change).
# Extended (Plan 2 Task 5): also asserts WorkflowFailed (the new
# `datalake-pipeline` PrometheusRule group, chart/templates/datalake-alerts.yaml)
# loaded alongside the original `datalake` group's LandingDBXidAgeHigh --
# proves BOTH groups in the same PrometheusRule resource hydrated, not just
# whichever group happened to already be present before Task 5.
PROM_STS=$(kubectl -n monitoring get sts -l app.kubernetes.io/name=prometheus -o jsonpath='{.items[0].metadata.name}')
RULES_JSON=$(kubectl -n monitoring exec "sts/$PROM_STS" -c prometheus -- \
     wget -qO- 'http://localhost:9090/api/v1/rules')
if echo "$RULES_JSON" | grep -q LandingDBXidAgeHigh && echo "$RULES_JSON" | grep -q WorkflowFailed; then
  echo "alert rules loaded (datalake + datalake-pipeline groups)"
else
  echo "FAIL: datalake alert rules not loaded in Prometheus (LandingDBXidAgeHigh and/or WorkflowFailed missing)"; exit 1
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
INGEST_IMAGE=$(yq -r '.images.ingest' chart/values.yaml)
kubectl -n argo-workflows delete pod iceberg-contents-probe --ignore-not-found >/dev/null 2>&1
cat <<PODYAML | kubectl -n argo-workflows apply -f -
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
  phase=$(kubectl -n argo-workflows get pod iceberg-contents-probe -o jsonpath='{.status.phase}' 2>/dev/null || echo "")
  { [ "$phase" = "Succeeded" ] || [ "$phase" = "Failed" ]; } && break
  sleep 5
done
PROBE_LOG=$(kubectl -n argo-workflows logs iceberg-contents-probe 2>/dev/null || echo "")
kubectl -n argo-workflows delete pod iceberg-contents-probe --ignore-not-found >/dev/null 2>&1
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
# `trino_ro` read-only role from hack/kind-up.sh).
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
# apps/infra/trino.yaml), the querying client never sees them.
kubectl -n trino delete pod trino-query-probe --ignore-not-found >/dev/null 2>&1
cat <<PODYAML | kubectl -n trino apply -f -
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

          TRINO = "http://trino.trino.svc:8080"
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
  phase=$(kubectl -n trino get pod trino-query-probe -o jsonpath='{.status.phase}' 2>/dev/null || echo "")
  { [ "$phase" = "Succeeded" ] || [ "$phase" = "Failed" ]; } && break
  sleep 10
done
TRINO_PROBE_LOG=$(kubectl -n trino logs trino-query-probe 2>/dev/null || echo "")
kubectl -n trino delete pod trino-query-probe --ignore-not-found >/dev/null 2>&1
echo "$TRINO_PROBE_LOG"
if [ "$phase" = "Succeeded" ] && echo "$TRINO_PROBE_LOG" | grep -q "trino-query-probe: OK"; then
  echo "trino query probes OK (lake.alice.job_info >=900 rows, landing.public.job_info SELECT succeeds, lake.contract.mon_jdls_parsed LPMPassName/TTL/Packages return data, lake.contract.job_info count matches lake.alice.job_info)"
else
  echo "FAIL: trino-query-probe phase=$phase"; exit 1
fi
# Banner moved here (final review R1): this must be the LAST line of the
# script. It used to print before the workflow probe above, so a log reader
# scanning for this line would see "success" on a run that later failed.
echo "kind-verify: all applications Synced/Healthy"
