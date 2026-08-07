# Manifest Hydration

To hydrate the manifests in this repository, run the following commands:

```shell
git clone https://github.com/agh-alice/datalake-v2.git
# cd into the cloned directory
git checkout 3af6d457ea5341163b57d4dd3ed12ce4556b16e0
helm template . --name-template datalake-compute --namespace datalake-compute --values ./envs/prod/compute/values.yaml --values ./envs/prod/compute/values-kind.yaml --include-crds
```
