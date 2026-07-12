# Manifest Hydration

To hydrate the manifests in this repository, run the following commands:

```shell
git clone https://github.com/agh-alice/datalake-v2.git
# cd into the cloned directory
git checkout 5560b40386b8e93e91d3b543cfec8c52d640177f
helm template . --name-template datalake-kind --values ./chart/values.yaml --values ./chart/values-kind.yaml --include-crds
```
