#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
KC_FLAGS=(-strict -ignore-missing-schemas -summary
  -schema-location default
  -schema-location 'https://raw.githubusercontent.com/datreeio/CRDs-catalog/main/{{.Group}}/{{.ResourceKind}}_{{.ResourceAPIVersion}}.json')

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

echo "== seam gate: no cluster-scoped kinds (Global Constraints) =="
hack/check-seam.sh envs/prod/storage envs/prod/compute envs/prod/orchestration

for tier in "${TIERS[@]}"; do
  chart_dir="envs/prod/$tier"
  echo "== helm lint ($tier) =="
  helm lint "$chart_dir" -f "$chart_dir/values.yaml"
  echo "== helm template + kubeconform ($tier, prod values) =="
  helm template "$chart_dir" -f "$chart_dir/values.yaml" --include-crds | kubeconform "${KC_FLAGS[@]}" -
  if [ -f "$chart_dir/values-kind.yaml" ]; then
    echo "== helm template + kubeconform ($tier, kind values) =="
    helm template "$chart_dir" -f "$chart_dir/values.yaml" -f "$chart_dir/values-kind.yaml" --include-crds | kubeconform "${KC_FLAGS[@]}" -
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
