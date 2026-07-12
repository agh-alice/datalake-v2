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
kubeconform "${KC_FLAGS[@]}" apps/ environments/kind/apps/ environments/cyfronet/apps/ 2>/dev/null || true
echo "lint OK"
