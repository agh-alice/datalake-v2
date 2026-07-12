#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
# Full reset of the pipeline's OUTPUT state (Iceberg catalog + the underlying
# S3 objects), for use before a from-scratch backfill or whenever the
# `alice` namespace's schema needs to be forced back to empty (docs/
# runbooks/ingestion-pipeline.md's "Reset procedure", docs/runbooks/
# backfill.md). Does NOT touch the landing PostgreSQL tables (job_info/
# trace/mon_jdls) -- those are source data (on kind, hack/seed-fixture.sh's
# fixture; reset that separately, via its own TRUNCATE, if its schema needs
# to change too).
#
# Filed follow-up from Plan 2 Task 3's fresh-slate finding (task-3-report.md,
# "Fix: parity + LPM merge", finding 3): dropping the Iceberg tables + the
# `alice` namespace via pyiceberg's catalog API is NOT enough to reset this
# pipeline. dlt's `destination="filesystem"` persists its own pipeline/
# schema bookkeeping (`_dlt_loads/`, `_dlt_pipeline_state/`, `_dlt_version/`,
# `init`) as plain objects directly in the S3 bucket, ENTIRELY OUTSIDE the
# Iceberg catalog (verified live again in Plan 2 Task 6, listing every
# object under s3://<S3_BUCKET>/alice/ against this cluster) -- catalog-side
# drops never touch them, so the next ingest run silently restores the OLD
# schema/watermarks from that leftover dlt state. This script wipes BOTH:
# the catalog tables/namespace AND the S3 key-prefix objects, in one step.
#
# Runs inside a throwaway pod (same reason every other hack/*.sh probe does:
# the host has no route to pod-network ClusterIPs on kind) using the exact
# ingest image + ingest-env Secret the real pipeline runs use, so catalog
# auth/location config can never drift from pipeline.py's own
# iceberg_catalog_properties().
#
# Per-object delete (task-3-report.md finding 3): s3fs's bulk
# `fs.rm(path, recursive=True)` -> S3 DeleteObjects batch call fails against
# this cluster's MinIO with `MissingContentMD5` (an aiobotocore/MinIO
# compatibility gap) -- delete each object individually via `fs.rm_file`
# instead. Cyfronet's real S3 may or may not hit the same gap; per-object
# delete is slower but safe everywhere, so it stays the only path here
# rather than special-casing by environment.
INGEST_IMAGE=$(yq -r '.images.ingest' chart/values.yaml)
kubectl -n argo-workflows delete pod reset-pipeline --ignore-not-found >/dev/null 2>&1
# Single-quoted delimiter (fully literal) + sed placeholder substitution --
# same pattern as hack/lakekeeper-warehouse.sh, and for the same reason: an
# unquoted heredoc lets the shell evaluate `$...`/backtick pairs anywhere in
# the body, including inside the embedded Python's own comments/docstrings
# (e.g. a markdown-style `` `alice` `` aside reads as command substitution
# to bash, not as a quoted word to Python -- caught live while testing this
# script: it silently ran `alice` as a shell command, "alice: command not
# found", harmless only because the result lands inside a Python comment).
# Quoting the delimiter turns the whole body inert to the shell; the one
# real host-side variable (the image ref) is injected via sed instead.
cat <<'PODYAML' | sed "s|__INGEST_IMAGE__|${INGEST_IMAGE}|" | kubectl -n argo-workflows apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: reset-pipeline
spec:
  restartPolicy: Never
  containers:
    - name: reset
      image: __INGEST_IMAGE__
      envFrom:
        - secretRef: {name: ingest-env}
      command:
        - python
        - -c
        - |
          import os, sys
          import fsspec
          from pyiceberg.catalog import load_catalog
          from pyiceberg.exceptions import NoSuchNamespaceError

          NAMESPACE = "alice"
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

          # --- 1. Catalog side: purge every table under `alice`, then the
          # namespace itself. purge_table (not drop_table) so underlying
          # data files are removed too, not just the catalog pointer
          # (confirmed present on pinned pyiceberg 0.11.1, task-3-report.md).
          try:
              tables = catalog.list_tables(NAMESPACE)
          except NoSuchNamespaceError:
              tables = []
          print(f"reset-pipeline: {len(tables)} table(s) under {NAMESPACE}: {tables}")
          for identifier in tables:
              catalog.purge_table(identifier)
              print(f"reset-pipeline: purged {identifier}")

          try:
              catalog.drop_namespace(NAMESPACE)
              print(f"reset-pipeline: dropped namespace {NAMESPACE}")
          except NoSuchNamespaceError:
              print(f"reset-pipeline: namespace {NAMESPACE} already absent")

          # --- 2. Bucket side: wipe every object dlt itself wrote under the
          # dataset prefix (data/metadata files the catalog drop above just
          # orphaned, PLUS the _dlt_*/init bookkeeping objects the catalog
          # never knew about in the first place).
          bucket = os.environ["S3_BUCKET"]
          prefix = f"{bucket}/{NAMESPACE}"
          fs = fsspec.filesystem(
              "s3",
              key=os.environ["S3_ACCESS_KEY"],
              secret=os.environ["S3_SECRET_KEY"],
              endpoint_url=os.environ["S3_ENDPOINT"],
              client_kwargs={"region_name": os.environ.get("S3_REGION", "local-01")},
          )
          try:
              objects = fs.find(prefix)
          except FileNotFoundError:
              objects = []
          print(f"reset-pipeline: {len(objects)} object(s) under s3://{prefix}")
          failed = []
          for obj in objects:
              try:
                  fs.rm_file(obj)
              except Exception as exc:
                  failed.append((obj, str(exc)))
          if failed:
              print(f"reset-pipeline: FAIL: {len(failed)} object(s) failed to delete: {failed[:5]}")
              sys.exit(1)
          print(f"reset-pipeline: deleted {len(objects)} object(s) under s3://{prefix}")
          print("reset-pipeline: OK")
PODYAML
phase=""
for i in $(seq 1 60); do
  phase=$(kubectl -n argo-workflows get pod reset-pipeline -o jsonpath='{.status.phase}' 2>/dev/null || echo "")
  { [ "$phase" = "Succeeded" ] || [ "$phase" = "Failed" ]; } && break
  sleep 5
done
LOGS=$(kubectl -n argo-workflows logs reset-pipeline 2>/dev/null || echo "")
kubectl -n argo-workflows delete pod reset-pipeline --ignore-not-found >/dev/null 2>&1
echo "$LOGS"
if [ "$phase" = "Succeeded" ] && echo "$LOGS" | grep -q "reset-pipeline: OK"; then
  echo "reset-pipeline.sh: OK -- alice namespace + S3 prefix wiped, safe to rerun ingestion from a clean slate"
else
  echo "FAIL: reset-pipeline pod phase=$phase"
  exit 1
fi
