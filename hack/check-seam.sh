#!/usr/bin/env bash
# Seam gate (Plan 5 Global Constraints, Task 1 Step 4): the platform's
# AppProject bounds us with `clusterResourceWhitelist: []` and three
# namespace destinations. Every object every tier chart renders MUST be
# namespaced -- no CRDs, ClusterRoles, ClusterRoleBindings, StorageClasses,
# Namespaces, PriorityClasses, ValidatingAdmissionPolicies, and the other
# cluster-scoped kinds below. This script enforces that locally, the same
# check Task 2 (Lakekeeper/Trino dependency verify-first step) and the
# Task 5 CI gate reuse -- one definition, not three copies.
#
# Usage: hack/check-seam.sh <chart-dir>[:extra-values.yaml] [<chart-dir> ...]
#   hack/check-seam.sh envs/prod/storage envs/prod/compute envs/prod/orchestration
#   hack/check-seam.sh "envs/prod/compute:values-lake.yaml"   # a distinct
#     values-layering STATE, not a different chart -- e.g. compute's
#     conditional `lake` catalog (values-lake.yaml, Plan 5 Task 3 Decision
#     2) only exists once that file is layered on top of values.yaml; the
#     base-values render checked by the bare "envs/prod/compute" arg above
#     never renders it, so it needs its own pass to actually prove the
#     state prod will run once G2 lands is seam-clean too.
#
# Plain grep, deliberately -- the task brief's own Step 4 says "grep the
# rendered output for cluster-scoped kinds"; a `kind:` line is always a
# top-level (column-0) mapping key in a valid Kubernetes manifest, so an
# anchored `^kind: <Name>$` match cannot false-positive on a nested field
# of the same name, with no yq/jq flavor dependency to pin.
set -euo pipefail

if [ "$#" -eq 0 ]; then
  echo "usage: $0 <chart-dir> [<chart-dir> ...]" >&2
  exit 2
fi

# Cluster-scoped kinds the AppProject's clusterResourceWhitelist: [] forbids.
CLUSTER_SCOPED_KINDS=(
  Namespace
  ClusterRole
  ClusterRoleBinding
  CustomResourceDefinition
  StorageClass
  PriorityClass
  ValidatingAdmissionPolicy
  ValidatingWebhookConfiguration
  MutatingWebhookConfiguration
  APIService
)
KIND_PATTERN="^kind: ($(IFS='|'; echo "${CLUSTER_SCOPED_KINDS[*]}"))\$"

fail=0
for arg in "$@"; do
  chart_dir="${arg%%:*}"
  extra_values=""
  helm_args=(template "$chart_dir" --include-crds)
  if [ "$arg" != "$chart_dir" ]; then
    extra_values="${arg#*:}"
    helm_args=(template "$chart_dir" -f "$chart_dir/values.yaml" -f "$chart_dir/$extra_values" --include-crds)
  fi
  echo "== seam gate: $chart_dir${extra_values:+ (+ $extra_values)} =="
  rendered="$(helm "${helm_args[@]}")"
  if [ -z "$rendered" ]; then
    echo "  (empty render -- nothing to check)"
    continue
  fi
  hits="$(echo "$rendered" | grep -E "$KIND_PATTERN" || true)"
  if [ -n "$hits" ]; then
    echo "  FAIL: cluster-scoped kind(s) found in $chart_dir's rendered output:"
    echo "$hits" | sort | uniq -c | sed 's/^/    /'
    fail=1
  fi
done

if [ "$fail" -ne 0 ]; then
  echo "seam gate FAILED: cluster-scoped resources would violate the AppProject's clusterResourceWhitelist: []" >&2
  exit 1
fi
echo "seam gate OK: no cluster-scoped kinds in $*"
