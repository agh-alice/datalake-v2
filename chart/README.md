# Manifest Hydration

To hydrate the manifests in this repository, run the following commands:

```shell
git clone https://github.com/agh-alice/datalake-v2.git
# cd into the cloned directory
git checkout c225a696f459b98f617d77ec2298608e19d91c3b
helm template . --name-template datalake-kind --values ./chart/values.yaml --values ./chart/values-kind.yaml --include-crds
```
