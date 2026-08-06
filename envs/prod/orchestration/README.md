# Manifest Hydration

To hydrate the manifests in this repository, run the following commands:

```shell
git clone https://github.com/agh-alice/datalake-v2.git
# cd into the cloned directory
git checkout 182500faa7f03d064f107950a1e8c99f8fa3f943
helm template . --name-template datalake-orchestration --values ./envs/prod/orchestration/values.yaml --values ./envs/prod/orchestration/values-kind.yaml --include-crds
```
