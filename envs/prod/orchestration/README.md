# Manifest Hydration

To hydrate the manifests in this repository, run the following commands:

```shell
git clone git@github.com:agh-alice/datalake-v2.git
# cd into the cloned directory
git checkout 2b04d37818c5eb01a5773d8840687ed4ad601df0
helm template . --name-template datalake-orchestration --namespace datalake-orchestration --include-crds
```
