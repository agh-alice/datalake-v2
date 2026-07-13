#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
# shellcheck source=lib/warehouse-config.sh
. hack/lib/warehouse-config.sh
# Idempotent bootstrap: bootstraps the Lakekeeper server (once, ever) and
# creates the `default` warehouse backed by the in-cluster MinIO stand-in
# (Task 1, Plan 2 -- every later ingestion task writes to this catalog).
#
# Runs entirely inside a throwaway pod in the `minio` namespace: the host has
# no route to pod-network ClusterIPs on kind, so the management API can only
# be reached from inside the cluster (same reason kind-verify.sh's REST probe
# uses a pod). Namespace `minio` (not `lakekeeper`) is deliberate: it lets the
# pod pull minio-creds via secretKeyRef without duplicating the Secret across
# namespaces. Cross-namespace Service DNS (lakekeeper.lakekeeper.svc) is
# unaffected by which namespace the pod runs in -- verified interactively.
# Credential values never touch this host script, its logs, or the terminal:
# they flow Secret -> pod env -> curl body entirely inside the cluster.
kubectl -n minio delete pod lakekeeper-warehouse-bootstrap --ignore-not-found >/dev/null 2>&1
# PODYAML stays single-quoted (fully literal) so every $var referenced by
# the pod's OWN embedded shell script ($ROOTUSER, $LK, $AUTH, $CODE, ...)
# is left for that script's runtime, not expanded by this host shell. The
# two host-side constants below (shared with hack/kind-up.sh via
# hack/lib/warehouse-config.sh -- review fix, Task 3) are injected instead
# via a sed pass over placeholder tokens, which keeps the heredoc's
# quoting/escaping simple and avoids touching the runtime pod variables.
cat <<'PODYAML' | sed "s|__WAREHOUSE_BUCKET__|${WAREHOUSE_BUCKET}|g; s|__WAREHOUSE_KEY_PREFIX__|${WAREHOUSE_KEY_PREFIX}|g" | kubectl -n minio apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: lakekeeper-warehouse-bootstrap
spec:
  restartPolicy: Never
  containers:
    - name: bootstrap
      image: curlimages/curl
      env:
        - name: ROOTUSER
          valueFrom: {secretKeyRef: {name: minio-creds, key: rootUser}}
        - name: ROOTPASS
          valueFrom: {secretKeyRef: {name: minio-creds, key: rootPassword}}
      command:
        - sh
        - -c
        - |
          set -e
          LK=http://lakekeeper.lakekeeper.svc:8181
          AUTH='Authorization: Bearer dummy'
          # Guarded wait (brief requirement): right after `kubectl apply`,
          # ArgoCD sync + pod startup latency means the Service may not answer
          # yet. Poll up to 5 minutes before giving up.
          i=0
          CODE=000
          while [ "$i" -lt 60 ]; do
            CODE=$(curl -s -o /tmp/info.json -w '%{http_code}' "$LK/management/v1/info" || echo 000)
            [ "$CODE" = "200" ] && break
            i=$((i+1))
            sleep 5
          done
          if [ "$CODE" != "200" ]; then
            echo "FAIL: lakekeeper /management/v1/info unreachable after 5m (last code: $CODE)"
            exit 1
          fi
          # Bootstrap is mandatory before warehouse creation and is a one-time,
          # server-lifetime operation (open-auth mode still requires a Bearer
          # header; convention `dummy` -- there is no real auth on kind).
          # Field name and idempotency check verified against the live
          # /management/v1/info response (`bootstrapped` field), not from docs.
          if grep -q '"bootstrapped":true' /tmp/info.json; then
            echo "lakekeeper already bootstrapped"
          else
            BCODE=$(curl -s -o /tmp/boot.json -w '%{http_code}' -X POST "$LK/management/v1/bootstrap" \
              -H "Content-Type: application/json" -H "$AUTH" -d '{"accept-terms-of-use":true}')
            if [ "$BCODE" != "204" ]; then
              echo "FAIL: bootstrap returned $BCODE: $(cat /tmp/boot.json)"
              exit 1
            fi
            echo "lakekeeper bootstrapped"
          fi
          # Skip cleanly if `default` already exists (list first).
          curl -s -H "$AUTH" "$LK/management/v1/warehouse" -o /tmp/wh.json
          if grep -q '"name":"default"' /tmp/wh.json; then
            echo "warehouse 'default' already exists"
          else
            # flavor s3-compat + path-style-access:true + sts-enabled:true
            # verified working end-to-end against this chart's MinIO (Task 1):
            # a real namespace+table create through the Iceberg REST catalog
            # succeeded and returned vended STS credentials -- MinIO's root
            # user can AssumeRole with no extra IAM setup. Field names are
            # access-key-id/secret-access-key (NOT aws-access-key-id/
            # aws-secret-access-key) on Lakekeeper 0.12.2's actual schema --
            # verified against the live /api-docs/management/v1/openapi.json
            # on this deployment, not from docs/memory (chart 0.11.0 vs
            # newest docs can and do disagree -- see Task 4 lesson).
            BODY=$(printf '{"warehouse-name":"default","storage-profile":{"type":"s3","bucket":"__WAREHOUSE_BUCKET__","key-prefix":"__WAREHOUSE_KEY_PREFIX__","endpoint":"http://minio.minio.svc:9000","region":"local-01","path-style-access":true,"flavor":"s3-compat","sts-enabled":true},"storage-credential":{"type":"s3","credential-type":"access-key","access-key-id":"%s","secret-access-key":"%s"},"delete-profile":{"type":"hard"}}' "$ROOTUSER" "$ROOTPASS")
            WCODE=$(curl -s -o /tmp/whcreate.json -w '%{http_code}' -X POST "$LK/management/v1/warehouse" \
              -H "Content-Type: application/json" -H "$AUTH" -d "$BODY")
            if [ "$WCODE" != "201" ]; then
              echo "FAIL: warehouse creation returned $WCODE: $(cat /tmp/whcreate.json)"
              exit 1
            fi
            echo "warehouse 'default' created"
          fi
          echo "lakekeeper-warehouse: OK"
PODYAML
# create/poll/logs/delete (kind-verify.sh pattern): `kubectl apply` doesn't
# attach, so the attach-race that pattern guards against doesn't apply here,
# but polling pod phase (rather than tailing) keeps the same reliable shape.
phase=""
for _ in $(seq 1 70); do
  phase=$(kubectl -n minio get pod lakekeeper-warehouse-bootstrap -o jsonpath='{.status.phase}' 2>/dev/null || echo "")
  { [ "$phase" = "Succeeded" ] || [ "$phase" = "Failed" ]; } && break
  sleep 5
done
LOGS=$(kubectl -n minio logs lakekeeper-warehouse-bootstrap 2>/dev/null || echo "")
kubectl -n minio delete pod lakekeeper-warehouse-bootstrap --ignore-not-found >/dev/null 2>&1
echo "$LOGS"
if [ "$phase" = "Succeeded" ] && echo "$LOGS" | grep -q "lakekeeper-warehouse: OK"; then
  echo "lakekeeper-warehouse.sh: OK"
else
  echo "FAIL: lakekeeper-warehouse bootstrap pod phase=$phase"
  exit 1
fi
