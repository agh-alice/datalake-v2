# Manifest Hydration

To hydrate the manifests in this repository, run the following commands:

```shell
git clone https://github.com/agh-alice/datalake-v2.git
# cd into the cloned directory
git checkout fa23e24ff0852767c5a4b4ceedae53f9f8d9518a
helm template . --name-template datalake-kind --values ./chart/values.yaml --values ./chart/values-kind.yaml --include-crds
```
