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
IMAGE=$(yq -r '.images.ingest' chart/values.yaml)

WF_REF=$(kubectl -n argo-workflows create -o name -f - <<EOF
apiVersion: argoproj.io/v1alpha1
kind: Workflow
metadata: {generateName: ingest-manual-}
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
        args: ["run-nightly"]
EOF
)
# WF_REF is "workflow.argoproj.io/<name>" (kubectl get/wait understand this
# TYPE/NAME form generically via API discovery) -- but `kubectl logs` does
# NOT: it resolves objects through a hardcoded client-go scheme that only
# knows built-in pod-owning workloads, so `kubectl logs workflow.argoproj.io/x`
# fails with "no kind Workflow is registered ... in scheme" even though the
# workflow itself succeeded. Argo names the pod after the workflow (single-
# template, no retries) -- fetch logs by that bare pod name instead.
WF_NAME=${WF_REF#*/}
echo "submitted: $WF_NAME"

phase=""
for i in $(seq 1 120); do
  phase=$(kubectl -n argo-workflows get "$WF_REF" -o jsonpath='{.status.phase}' 2>/dev/null || echo "")
  { [ "$phase" = "Succeeded" ] || [ "$phase" = "Failed" ] || [ "$phase" = "Error" ]; } && break
  sleep 5
done

echo "=== $WF_NAME pod logs (container: main) ==="
kubectl -n argo-workflows logs "$WF_NAME" -c main --timestamps 2>&1 || true

if [ "$phase" != "Succeeded" ]; then
  echo "FAIL: $WF_NAME phase='$phase' (timeout 600s)"
  exit 1
fi
echo "run-ingest-once.sh: $WF_NAME Succeeded"
