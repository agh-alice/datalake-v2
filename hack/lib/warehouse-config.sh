# Shared source of truth for the Lakekeeper `default` warehouse's S3
# location (review fix, Task 3: hack/lakekeeper-warehouse.sh's
# storage-profile bucket/key-prefix and hack/kind-up.sh's ingest-env
# S3_BUCKET value were two independent hardcoded literals that had to be
# kept in sync by hand -- a future change to one without the other would
# reproduce the InvalidLocation bug documented in hack/kind-up.sh's Step 2
# comment). Sourced (not executed) by both scripts:
#   . "$(dirname "$0")/lib/warehouse-config.sh"
WAREHOUSE_BUCKET="warehouse"
WAREHOUSE_KEY_PREFIX="lakekeeper-warehouse"
