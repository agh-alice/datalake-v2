# Manifest Hydration

To hydrate the manifests in this repository, run the following commands:

```shell
git clone https://github.com/agh-alice/datalake-v2.git
# cd into the cloned directory
git checkout f84826f5c0f3a80dc95e040be442f1e8c51ff6a1
helm template . --name-template datalake-storage --values ./envs/prod/storage/values.yaml --values ./envs/prod/storage/values-kind.yaml --include-crds
```
