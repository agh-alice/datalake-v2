.DEFAULT_GOAL := help
.PHONY: lint kind-up kind-down kind-verify sops-setup help
lint: ## helm lint + template render + kubeconform (both envs)
	hack/lint.sh
kind-up: ## kind cluster + ArgoCD (hydrator on) + apps
	hack/kind-up.sh
kind-down: ## delete kind cluster
	kind delete cluster --name datalake-v2
kind-verify: ## assert all Applications Synced/Healthy + component probes
	hack/kind-verify.sh
sops-setup: ## generate an age keypair and wire its public key into .sops.yaml (run once, before cyfronet bootstrap)
	@command -v age-keygen >/dev/null || { echo "age-keygen not found (install the 'age' package)"; exit 1; }
	@test -f age.key && { echo "age.key already exists -- refusing to overwrite (would break every *.enc.yaml already encrypted to the old key). Delete it first if a new keypair is really intended."; exit 1; } || true
	age-keygen -o age.key
	@pubkey=$$(grep '^# public key:' age.key | cut -d' ' -f4); \
	sed -i.bak "s/REPLACE_WITH_AGE_PUBLIC_KEY_FROM_make_sops-setup/$$pubkey/" .sops.yaml && rm -f .sops.yaml.bak; \
	echo "age keypair generated at ./age.key (gitignored -- back it up out of band, e.g. a password manager)."; \
	echo "Public key $$pubkey written into .sops.yaml as the SOPS recipient."; \
	echo "Export SOPS_AGE_KEY_FILE=\$$(pwd)/age.key before running sops encrypt/decrypt."
help:
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "%-12s %s\n", $$1, $$2}'
