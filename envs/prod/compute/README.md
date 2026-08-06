# Manifest Hydration

To hydrate the manifests in this repository, run the following commands:

```shell
git clone https://github.com/agh-alice/datalake-v2.git
# cd into the cloned directory
git checkout 5e6caaa7c0706db3ab8d17f5c4643100e08b4438
helm template . --name-template datalake-compute --namespace datalake-compute --values ./envs/prod/compute/values.yaml --values ./envs/prod/compute/values-kind.yaml --include-crds
```
