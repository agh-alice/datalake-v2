#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
KC_FLAGS=(-strict -ignore-missing-schemas -summary
  -schema-location default
  -schema-location 'https://raw.githubusercontent.com/datreeio/CRDs-catalog/main/{{.Group}}/{{.ResourceKind}}_{{.ResourceAPIVersion}}.json')
echo "== helm lint =="
helm lint chart/ -f chart/values.yaml
for env in kind cyfronet; do
  echo "== helm template + kubeconform ($env) =="
  helm template datalake chart/ -f chart/values.yaml -f "chart/values-$env.yaml" --include-crds | kubeconform "${KC_FLAGS[@]}" -
done
echo "== kubeconform: Application/AppProject manifests =="
# Skip only directories that don't exist yet (git tracks no empty dirs);
# real validation errors MUST fail the lint (review finding, Task 1).
APP_DIRS=()
for d in apps environments/kind/apps environments/cyfronet/apps; do
  [ -d "$d" ] && compgen -G "$d/*.yaml" >/dev/null && APP_DIRS+=("$d")
done
if [ "${#APP_DIRS[@]}" -gt 0 ]; then
  kubeconform "${KC_FLAGS[@]}" "${APP_DIRS[@]}"
fi
echo "lint OK"
