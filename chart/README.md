# Manifest Hydration

To hydrate the manifests in this repository, run the following commands:

```shell
git clone https://github.com/agh-alice/datalake-v2.git
# cd into the cloned directory
git checkout a697e5c4f1e33a8f270ae7d3a0280f93c9bb59e5
helm template . --name-template datalake-kind --values ./chart/values.yaml --values ./chart/values-kind.yaml --include-crds
```
