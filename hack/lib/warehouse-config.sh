# shellcheck shell=bash
# Shared source of truth for the Lakekeeper `default` warehouse's S3
# location (review fix, Task 3: hack/lakekeeper-warehouse.sh's
# storage-profile bucket/key-prefix and hack/kind-up.sh's ingest-env
# S3_BUCKET value were two independent hardcoded literals that had to be
# kept in sync by hand -- a future change to one without the other would
# reproduce the InvalidLocation bug documented in hack/kind-up.sh's Step 2
# comment). Sourced (not executed) by both scripts:
#   . "$(dirname "$0")/lib/warehouse-config.sh"
#
# This file has no shebang (never executed directly, only sourced) -- the
# `shellcheck shell=bash` directive above (Plan 3 Task 5, shellcheck CI job)
# replaces it so `shellcheck hack/lib/*.sh`, run standalone, knows which
# shell dialect to check against (SC2148: "target shell is unknown"
# otherwise) without making this file spuriously executable/shebang-bearing.
#
# shellcheck disable=SC2034 # consumed by the sourcing scripts
# (hack/kind-up.sh, hack/lakekeeper-warehouse.sh), not read within this file
# itself -- shellcheck's per-file unused-variable check cannot see across
# the `. hack/lib/warehouse-config.sh` source boundary when this file is
# linted on its own (as `hack/lib/*.sh` in the CI job does); the `-x -P hack
# -P hack/lib` flags on the sourcing scripts' own shellcheck invocation
# already prove real usage there.
WAREHOUSE_BUCKET="warehouse"
# shellcheck disable=SC2034 # see WAREHOUSE_BUCKET's disable comment above -- same reasoning
WAREHOUSE_KEY_PREFIX="lakekeeper-warehouse"
