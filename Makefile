help: ## Display help
	@awk 'BEGIN {FS = ":.*##"; printf "Usage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

presubmit: verify test licenses vulncheck ## Run all steps required for code to be checked in

test: ## Run tests
	go test ./... \
		-race \
		-timeout 10m \
		--ginkgo.focus="${FOCUS}" \
		--ginkgo.timeout=10m \
		--ginkgo.v \
		-cover -coverprofile=coverage.out -outputdir=. -coverpkg=./...

deflake: ## Run randomized, racing tests until the test fails to catch flakes
	ginkgo \
		--race \
		--focus="${FOCUS}" \
		--timeout=10m \
		--randomize-all \
		--until-it-fails \
		-v \
		./pkg/...

install-kwok: ## Install kwok into cluster
	UNINSTALL_KWOK=false ./hack/install-kwok.sh
uninstall-kwok: ## Install kwok provider
	UNINSTALL_KWOK=true ./hack/install-kwok.sh

vulncheck: ## Verify code vulnerabilities
	@govulncheck ./pkg/...

licenses: download ## Verifies dependency licenses
	! go-licenses csv ./... | grep -v -e 'MIT' -e 'Apache-2.0' -e 'BSD-3-Clause' -e 'BSD-2-Clause' -e 'ISC' -e 'MPL-2.0'

verify: ## Verify code. Includes codegen, dependencies, linting, formatting, etc
	go mod tidy
	go generate ./...
	hack/validation/kubelet.sh
	hack/validation/taint.sh
	hack/validation/requirements.sh
	hack/validation/labels.sh
	hack/dependabot.sh
	@# Use perl instead of sed due to https://stackoverflow.com/questions/4247068/sed-command-with-i-option-failing-on-mac-but-works-on-linux
	@# We need to do this "sed replace" until controller-tools fixes this parameterized types issue: https://github.com/kubernetes-sigs/controller-tools/issues/756
	@perl -i -pe 's/sets.Set/sets.Set[string]/g' pkg/scheduling/zz_generated.deepcopy.go
	hack/boilerplate.sh
	go vet ./...
	golangci-lint run
	@git diff --quiet ||\
		{ echo "New file modification detected in the Git working tree. Please check in before commit."; git --no-pager diff --name-only | uniq | awk '{print "  - " $$0}'; \
		if [ "${CI}" = true ]; then\
			exit 1;\
		fi;}
	actionlint -oneline

download: ## Recursively "go mod download" on all directories where go.mod exists
	$(foreach dir,$(MOD_DIRS),cd $(dir) && go mod download $(newline))

toolchain: ## Install developer toolchain
	./hack/toolchain.sh

.PHONY: help presubmit dev test verify toolchain
