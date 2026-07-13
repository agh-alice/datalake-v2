# Manifest Hydration

To hydrate the manifests in this repository, run the following commands:

```shell
git clone https://github.com/agh-alice/datalake-v2.git
# cd into the cloned directory
git checkout 7746f629363c23eb4e40288e8aaa08ea624df26e
helm template . --name-template datalake-kind --values ./chart/values.yaml --values ./chart/values-kind.yaml --include-crds
```
