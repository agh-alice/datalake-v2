#!/usr/bin/env bash
# Seam gate (Plan 5 Global Constraints, Task 1 Step 4): the platform's
# AppProject bounds us with `clusterResourceWhitelist: []` and three
# namespace destinations. Every object every tier chart renders MUST be
# namespaced -- no CRDs, ClusterRoles, ClusterRoleBindings, StorageClasses,
# Namespaces, PriorityClasses, ValidatingAdmissionPolicies, and any other
# cluster-scoped kind. This script enforces that locally, the same check
# the Task 5 CI gate reuses -- one definition, not two copies.
#
# Independent-review finding (2026-08-07, controller-verified): a
# hand-maintained denylist of ~10 kind names can never be exhaustive --
# ClusterIssuer, IngressClass, RuntimeClass, PersistentVolume,
# VolumeSnapshotClass, CSIDriver, FlowSchema, Node, and any future
# CRD-defined cluster-scoped kind all passed the old grep-only version of
# this script silently. THE REAL GATE below inverts the question: instead
# of "is this one of N forbidden kinds", it asserts every rendered object
# carries `metadata.namespace` -- cluster-scoped objects cannot have one,
# so a brand-new cluster-scoped kind nobody has heard of yet is still
# caught, with nothing to maintain. The old denylist stays as a SECOND,
# named layer purely for a friendlier failure message on the common cases
# (it costs nothing to keep) -- it is no longer what stands between a
# cluster-scoped object and a passing gate.
#
# Usage: hack/check-seam.sh <chart-dir>[:extra-values.yaml] [<chart-dir> ...]
#   hack/check-seam.sh envs/prod/storage envs/prod/compute envs/prod/orchestration
#   hack/check-seam.sh "envs/prod/compute:values-lake.yaml"   # a distinct
#     values-layering STATE, not a different chart -- e.g. compute's
#     conditional `lake` catalog (values-lake.yaml, Plan 5 Task 3 Decision
#     2) only exists once that file is layered on top of values.yaml; the
#     base-values render checked by the bare "envs/prod/compute" arg above
#     never renders it, so it needs its own pass to actually prove the
#     state prod will run once G2 lands is seam-clean too. Same reasoning
#     is why hack/lint.sh now also passes every values-kind.yaml
#     combination -- every render state that is actually deployed anywhere
#     (prod defaults, the conditional lake state, and all three kind
#     combinations) must clear this gate, not just the prod defaults.
#
# Every render below passes `-f <chart-dir>/values.yaml` explicitly, even
# for the plain no-suffix case -- deliberately, not redundant with Helm's
# own auto-load of the chart's values.yaml. Live-verified (2026-08-06,
# review finding): `helm template envs/prod/compute` with NO -f at all
# renders Trino's default `tpch`/`tpcds` demo catalogs (the chart's own
# defaults, un-nulled); `helm template envs/prod/compute -f
# envs/prod/compute/values.yaml` -- same file, passed explicitly -- deletes
# them, because Helm's dependency-values null-key coalescing (helm/helm#9027
# territory) behaves differently for values supplied via an explicit -f than
# for the chart's own auto-loaded default. Every real render this repo's
# Applications produce (kind's `sourceHydrator.drySource.helm.valueFiles`,
# and prod's once it exists) passes an explicit valueFiles list -- never a
# bare `helm template <chart>` -- so a bare render here would validate a
# state nothing ever deploys. tpch/tpcds are harmless namespaced ConfigMap
# entries either way (not a seam violation in either render shape), but the
# principle applies regardless of what's at stake this time.
set -euo pipefail

if [ "$#" -eq 0 ]; then
  echo "usage: $0 <chart-dir> [<chart-dir> ...]" >&2
  exit 2
fi

# Denylist -- SECOND layer, kept for a legible, specific message on the
# common cases. Not exhaustive by design (see the header note above); the
# namespace-presence check further down is the layer that actually has to
# catch everything.
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

# Explicit, justified allowlist of namespaced-but-namespace-less rendered
# objects -- Helm/Kubernetes allow a namespaced object's manifest to omit
# `metadata.namespace` and let it be supplied at apply time (`kubectl apply
# -n`, or here Argo CD's Application destination namespace); the primary
# gate below cannot distinguish "cluster-scoped, must never have one" from
# "namespaced, deliberately left implicit" by field presence alone, so
# known cases of the latter are named here, each with why. Format:
# "Kind/Name" (the same identifier the gate below prints).
ALLOWED_NAMESPACELESS=(
  # Trino chart's `helm test` connection-check hook (values.yaml Step-1
  # verify-first finding: "ConfigMap, Deployment, Pod [helm-test hook],
  # Service only -- no cluster-scoped kind"). Namespaced (Pod is always a
  # namespaced kind) -- the upstream trino/trino chart's test-connection
  # template simply never sets an explicit `metadata.namespace`, same as
  # most Helm chart test hooks (relies on the release/apply-time
  # namespace). Live-verified 2026-08-07: this is the ONLY object across
  # every render state (3 prod defaults, compute:values-lake.yaml, and all
  # three values-kind.yaml combinations) that fails the namespace-presence
  # check.
  "Pod/trino-test-connection"
)

# Namespace-presence check (THE gate). Plain awk, deliberately -- no yq/jq
# flavor dependency to pin, matching this script's existing no-external-
# YAML-parser-dependency stance (see the KIND_PATTERN comment above, and
# hack/lint.sh's CI job, which never installs one). A rendered Kubernetes
# manifest's TOP-LEVEL `kind:` and `metadata:` are always column-0 mapping
# keys (never nested -- a nested field of the same name is indented). Only
# `name:`/`namespace:` lines seen WHILE inside that top-level `metadata:`
# block count -- tracked with an explicit `in_meta` flag, set on the
# column-0 `metadata:` line and cleared on the next column-0 line,
# whatever it is. Indentation alone is NOT enough to distinguish
# metadata's own children from a same-indent field belonging to something
# else: an RBAC `subjects:` list item's `namespace:` can render at
# EXACTLY 2 spaces too, depending on the chart author's list-indent style
# (verified live: this repo's own RoleBinding subjects render at 4
# spaces, but --include-crds also renders third-party dependency charts
# [Lakekeeper, Trino] whose list-indent conventions we do not control) --
# an earlier version of this check matched on indentation alone and would
# have silently accepted an unlisted cluster-scoped kind (e.g.
# ClusterRoleBinding) with a 2-space-indented subject namespace and no
# metadata.namespace of its own. The `in_meta` gate closes that.
# shellcheck disable=SC2016 # single-quoted deliberately -- this is a
# literal awk program with its own $0 references, not a bash string meant
# to expand; double-quoting would make the shell try to expand $0/$1/etc.
NS_CHECK_AWK='
function reset() { kind = ""; name = ""; hasns = 0; in_meta = 0 }
BEGIN { reset() }
/^---[ \t]*$/ {
  if (kind != "" && hasns == 0) print kind "/" (name == "" ? "<unnamed>" : name)
  reset()
  next
}
/^[^ \t]/ { in_meta = 0 }
/^kind: / { k = $0; sub(/^kind: /, "", k); kind = k; next }
/^metadata:[ \t]*$/ { in_meta = 1; next }
in_meta && /^  name: / { n = $0; sub(/^  name: /, "", n); gsub(/^"|"$/, "", n); if (name == "") name = n; next }
in_meta && /^  namespace: / { hasns = 1; next }
END { if (kind != "" && hasns == 0) print kind "/" (name == "" ? "<unnamed>" : name) }
'

is_allowed() {
  local candidate="$1"
  local entry
  for entry in "${ALLOWED_NAMESPACELESS[@]}"; do
    [ "$entry" = "$candidate" ] && return 0
  done
  return 1
}

fail=0
for arg in "$@"; do
  chart_dir="${arg%%:*}"
  extra_values=""
  helm_args=(template "$chart_dir" -f "$chart_dir/values.yaml" --include-crds)
  if [ "$arg" != "$chart_dir" ]; then
    extra_values="${arg#*:}"
    helm_args+=(-f "$chart_dir/$extra_values")
  fi
  echo "== seam gate: $chart_dir${extra_values:+ (+ $extra_values)} =="
  rendered="$(helm "${helm_args[@]}")"
  if [ -z "$rendered" ]; then
    echo "  (empty render -- nothing to check)"
    continue
  fi

  hits="$(echo "$rendered" | grep -E "$KIND_PATTERN" || true)"
  if [ -n "$hits" ]; then
    echo "  FAIL (denylist): cluster-scoped kind(s) found in $chart_dir's rendered output:"
    echo "$hits" | sort | uniq -c | sed 's/^/    /'
    fail=1
  fi

  namespaceless="$(echo "$rendered" | awk "$NS_CHECK_AWK" || true)"
  if [ -n "$namespaceless" ]; then
    unallowed=""
    while IFS= read -r obj; do
      [ -z "$obj" ] && continue
      if ! is_allowed "$obj"; then
        unallowed="${unallowed}${obj}"$'\n'
      fi
    done <<< "$namespaceless"
    if [ -n "$unallowed" ]; then
      echo "  FAIL (namespace-presence): object(s) in $chart_dir's rendered output have no metadata.namespace and are not on the allowlist:"
      echo -n "$unallowed" | sed 's/^/    /'
      fail=1
    fi
  fi
done

if [ "$fail" -ne 0 ]; then
  echo "seam gate FAILED: cluster-scoped (or unaccounted-for namespace-less) resources would violate the AppProject's clusterResourceWhitelist: []" >&2
  exit 1
fi
echo "seam gate OK: every rendered object is namespaced (or on the explicit allowlist) across $*"
