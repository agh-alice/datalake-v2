# Manifest Hydration

To hydrate the manifests in this repository, run the following commands:

```shell
git clone https://github.com/agh-alice/datalake-v2.git
# cd into the cloned directory
git checkout eb4162c1364e88ae8e5bf3bfa01ced7cb2636e70
helm template . --name-template datalake-storage --values ./envs/prod/storage/values.yaml --values ./envs/prod/storage/values-kind.yaml --include-crds
```
