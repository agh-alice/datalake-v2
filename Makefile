.DEFAULT_GOAL := help
.PHONY: lint kind-up kind-down kind-verify help
lint: ## helm lint + template render + kubeconform (both envs)
	hack/lint.sh
kind-up: ## kind cluster + ArgoCD (hydrator on) + apps
	hack/kind-up.sh
kind-down: ## delete kind cluster
	kind delete cluster --name datalake-v2
kind-verify: ## assert all Applications Synced/Healthy + component probes
	hack/kind-verify.sh
help:
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "%-12s %s\n", $$1, $$2}'
