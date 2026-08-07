# Manifest Hydration

To hydrate the manifests in this repository, run the following commands:

```shell
git clone https://github.com/agh-alice/datalake-v2.git
# cd into the cloned directory
git checkout d140271df4380ce8e0cea5284ed730e32733eb3f
helm template . --name-template datalake-storage --namespace datalake-storage --values ./envs/prod/storage/values.yaml --values ./envs/prod/storage/values-kind.yaml --include-crds
```
