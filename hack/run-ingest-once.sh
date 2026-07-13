#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
# One-shot ingestion Workflow (Plan 2 Task 3): submits a Workflow running
# `alice-ingest run-nightly` (image ENTRYPOINT + this args list) under the
# pipeline-runner ServiceAccount, envFrom Secret ingest-env (hack/kind-up.sh
# Step 2), and hard-gates on it reaching Succeeded. Image comes from
# chart/values.yaml `images.ingest` via yq -- same digest-pinned image the
# CronWorkflow will use once Task 4 wires it up.
#
# create -o name / poll-that-name / hard-gate pattern, same shape as
# hack/kind-verify.sh's manual workflow probe: querying items[-1] sorted by
# creationTimestamp is racy against a concurrent hello CronWorkflow tick;
# capturing the created object's own name sidesteps that entirely.
#
# Plan 3 Task 2 handoff (2026-07-12, folded into Task 5): REST-catalog views
# (`lake.contract.*`) live in Lakekeeper's own Postgres, which `kind-down`
# destroys along with everything else -- a fresh cluster has ingested data
# but NO contract views until `alice-ingest apply-views` runs at least once.
# `hack/kind-verify.sh`'s trino-query-probe hard-gates on
# `lake.contract.mon_jdls_parsed`/`lake.contract.job_info` existing and
# matching row counts, so the clean-room sequence (`make kind-down && make
# kind-up && hack/seed-fixture.sh && hack/run-ingest-once.sh && make
# kind-verify`) would otherwise fail that gate on every fresh cluster. This
# script now runs `apply-views` as a SECOND Workflow, after `run-nightly`
# succeeds (views select from the `alice.*` tables run-nightly populates --
# running apply-views first would create views over tables that don't exist
# yet). `apply-views` needs `TRINO_URI` from the `ingest-env` Secret
# (hack/kind-up.sh) -- already present, no new env-var wiring required.
# Idempotent: `views.py`'s DDL is `CREATE OR REPLACE VIEW`, safe to rerun
# any number of times (matches this script's own existing idempotency for
# run-nightly's merge/upsert tables).
#
# Submits one Workflow running `alice-ingest <args...>`, waits up to 600s
# for a terminal phase, dumps its pod logs, and hard-gates on Succeeded.
# Factored into a function once this script needed to submit a SECOND
# Workflow (apply-views, above) rather than duplicate the whole create/
# poll/logs/gate block a second time.
IMAGE=$(yq -r '.images.ingest' chart/values.yaml)

run_workflow() {
  local generate_name=$1
  local args_yaml=$2

  local wf_ref
  wf_ref=$(kubectl -n argo-workflows create -o name -f - <<EOF
apiVersion: argoproj.io/v1alpha1
kind: Workflow
metadata: {generateName: ${generate_name}-}
spec:
  serviceAccountName: pipeline-runner
  entrypoint: main
  ttlStrategy: {secondsAfterCompletion: 3600}
  templates:
    - name: main
      container:
        image: $IMAGE
        envFrom:
          - secretRef: {name: ingest-env}
        args: $args_yaml
EOF
)
  # wf_ref is "workflow.argoproj.io/<name>" (kubectl get/wait understand this
  # TYPE/NAME form generically via API discovery) -- but `kubectl logs` does
  # NOT: it resolves objects through a hardcoded client-go scheme that only
  # knows built-in pod-owning workloads, so `kubectl logs workflow.argoproj.io/x`
  # fails with "no kind Workflow is registered ... in scheme" even though the
  # workflow itself succeeded. Argo names the pod after the workflow (single-
  # template, no retries) -- fetch logs by that bare pod name instead.
  local wf_name=${wf_ref#*/}
  echo "submitted: $wf_name"

  local phase=""
  for _ in $(seq 1 120); do
    phase=$(kubectl -n argo-workflows get "$wf_ref" -o jsonpath='{.status.phase}' 2>/dev/null || echo "")
    { [ "$phase" = "Succeeded" ] || [ "$phase" = "Failed" ] || [ "$phase" = "Error" ]; } && break
    sleep 5
  done

  echo "=== $wf_name pod logs (container: main) ==="
  kubectl -n argo-workflows logs "$wf_name" -c main --timestamps 2>&1 || true

  if [ "$phase" != "Succeeded" ]; then
    echo "FAIL: $wf_name phase='$phase' (timeout 600s)"
    exit 1
  fi
  echo "run-ingest-once.sh: $wf_name Succeeded"
}

run_workflow "ingest-manual" '["run-nightly"]'
run_workflow "apply-views-manual" '["apply-views"]'
