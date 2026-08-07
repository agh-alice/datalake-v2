#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
KC_FLAGS=(-strict -ignore-missing-schemas -summary
  -schema-location default
  -schema-location 'https://raw.githubusercontent.com/datreeio/CRDs-catalog/main/{{.Group}}/{{.ResourceKind}}_{{.ResourceAPIVersion}}.json')

# Plan 5 Task 4 review finding (TDD constraint applies to Python changes
# generally, not just ingest/**): hack/jdl_fixture_fields.py's pure
# functions, plus the HAND_AUTHORED-vs-seed-fixture.sh drift guard,
# hack/tests/test_jdl_fixture_fields.py -- runs with no cluster, stdlib
# unittest only (no pytest dependency, matching tools/tests' own
# precedent), so it belongs ahead of the cluster-dependent gates below.
echo "== hack/ unit tests =="
python3 -m unittest discover -s hack/tests

# Plan 5: chart/ no longer exists -- split into three standalone tier
# charts (Task 1), each with remote Helm dependencies (Task 2: Lakekeeper,
# Trino). Chart.lock is committed; the fetched charts/*.tgz is gitignored
# (envs/prod/{storage,compute}/Chart.yaml's own comment) -- `helm
# dependency build` must run before `helm template`/`helm lint` on either
# of those two, every time (a fresh clone/CI runner has no charts/ dir at
# all).
TIERS=(storage compute orchestration)
for tier in "${TIERS[@]}"; do
  chart_dir="envs/prod/$tier"
  if [ -f "$chart_dir/Chart.lock" ]; then
    echo "== helm dependency build ($tier) =="
    helm dependency build "$chart_dir" >/dev/null
  fi
done

echo "== seam gate: every namespaced object, every deployed render state (Global Constraints) =="
# compute:values-lake.yaml is a distinct RENDER STATE, not a fourth chart --
# the conditional `lake` catalog (values-lake.yaml, Task 3 Decision 2) only
# exists once that file is layered on top of values.yaml, and that's the
# state prod actually runs once G2 (S3 creds) lands. The bare
# envs/prod/compute pass above never renders it, so without this second
# pass the gate never proves the render prod will really use is seam-clean.
#
# Independent-review finding (2026-08-07): this gate used to run ONLY on
# the three prod defaults plus compute:values-lake.yaml -- never on any
# values-kind.yaml combination, even though kubeconform below DOES render
# those (kubeconform validates schemas; it has no opinion on
# clusterResourceWhitelist: [] and would happily accept a cluster-scoped
# object). Every render state actually deployed anywhere -- the three prod
# defaults, the conditional lake state, and all three kind combinations --
# now goes through the same gate.
hack/check-seam.sh envs/prod/storage envs/prod/compute envs/prod/orchestration \
  "envs/prod/compute:values-lake.yaml" \
  envs/prod/storage:values-kind.yaml \
  envs/prod/compute:values-kind.yaml \
  envs/prod/orchestration:values-kind.yaml

# 2026-08-07 finding: every render below used to omit -n/--namespace, so
# any object rendered via bare `.Release.Namespace` (a subchart
# dependency's own upstream templates, e.g.) landed in Helm's own default
# of literal `default` -- a render shape no real deployment ever produces.
# Argo CD's Source Hydrator always resolves a namespace for
# `.Release.Namespace` (`sourceHydrator.drySource.helm.namespace` defaults
# to the Application's `destination.namespace` per the Application CRD,
# confirmed against the platform's live cluster; kind sets the identical
# `datalake-<tier>` destination.namespace too -- environments/kind/
# apps/*.yaml), so every render here now passes `-n datalake-$tier` to
# match. See hack/check-seam.sh's header comment for the fuller writeup
# (same finding, same fix, applied there too).
for tier in "${TIERS[@]}"; do
  chart_dir="envs/prod/$tier"
  ns="datalake-$tier"
  echo "== helm lint ($tier) =="
  helm lint "$chart_dir" -f "$chart_dir/values.yaml" -n "$ns"
  echo "== helm template + kubeconform ($tier, prod values) =="
  helm template "$chart_dir" -f "$chart_dir/values.yaml" -n "$ns" --include-crds | kubeconform "${KC_FLAGS[@]}" -
  if [ -f "$chart_dir/values-kind.yaml" ]; then
    echo "== helm template + kubeconform ($tier, kind values) =="
    helm template "$chart_dir" -f "$chart_dir/values.yaml" -f "$chart_dir/values-kind.yaml" -n "$ns" --include-crds | kubeconform "${KC_FLAGS[@]}" -
  fi
  if [ -f "$chart_dir/values-lake.yaml" ]; then
    echo "== helm template + kubeconform ($tier, lake values -- the conditional G2 state) =="
    helm template "$chart_dir" -f "$chart_dir/values.yaml" -f "$chart_dir/values-lake.yaml" -n "$ns" --include-crds | kubeconform "${KC_FLAGS[@]}" -
  fi
done

echo "== kubeconform: Application/AppProject manifests =="
# Skip only directories that don't exist yet (git tracks no empty dirs);
# real validation errors MUST fail the lint (review finding, Task 1).
# Plan 5 Task 4: apps/ retired entirely (nothing there applies to prod any
# more); environments/cyfronet/ retired too (dead under the platform-hub
# hydration model -- Task 5 defines the real prod entry point). Kind-only
# Application/AppProject manifests now live under environments/kind/.
APP_DIRS=()
for d in environments/kind/apps environments/kind/infra; do
  [ -d "$d" ] && compgen -G "$d/*.yaml" >/dev/null && APP_DIRS+=("$d")
done
[ -f environments/kind/project.yaml ] && APP_DIRS+=(environments/kind/project.yaml)
if [ "${#APP_DIRS[@]}" -gt 0 ]; then
  kubeconform "${KC_FLAGS[@]}" "${APP_DIRS[@]}"
fi
echo "lint OK"
