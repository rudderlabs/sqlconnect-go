.PHONY: help default test test-run generate lint fmt

GO=go
LDFLAGS?=-s -w

# go tools versions
GOLANGCI=github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.9.0
gotestsum=gotest.tools/gotestsum@v1.13.0
gci=github.com/daixiang0/gci@v0.13.7
gofumpt=mvdan.cc/gofumpt@latest
mockgen=go.uber.org/mock/mockgen@v0.6.0

default: lint

generate: install-tools
	$(GO) generate ./...

test: install-tools test-run

test-run: ## Run all unit tests
ifdef package
ifdef exclude
	$(eval PACKAGES = `go list ./$(package)/... | egrep -iv '$(exclude)' | xargs`)
else
	$(eval PACKAGES = "./$(package)/...")
endif
else ifdef exclude
	$(eval PACKAGES = `go list ./... | egrep -iv '$(exclude)' | xargs`)
else
	$(eval PACKAGES = "./...")
endif

ifeq ($(filter 1,$(debug) $(RUNNER_DEBUG)),)
	$(eval TEST_CMD = gotestsum --rerun-fails --format pkgname-and-test-fails --packages="${PACKAGES}"  --)
	$(eval TEST_OPTIONS = -p=1 -v -shuffle=on -coverprofile=profile.out -coverpkg=./... -covermode=atomic -vet=all --timeout=60m)
	$(eval CMD = $(TEST_CMD) -count=1 $(TEST_OPTIONS) ${PACKAGES})
else
	$(eval TEST_CMD = go test)
	$(eval TEST_OPTIONS = -p=1 -v -shuffle=on -coverprofile=profile.out -coverpkg=./... -covermode=atomic -vet=all --timeout=60m)
	$(eval CMD = $(TEST_CMD) -count=1 $(TEST_OPTIONS) ${PACKAGES})
endif
	$(CMD)

help: ## Show the available commands
	@grep -E '^[0-9a-zA-Z_-]+:.*?## .*$$' ./Makefile | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

install-tools:
	$(GO) install $(mockgen)
	$(GO) install $(gotestsum)

.PHONY: lint
lint: fmt ## Run linters on all go files
	$(GO) run $(GOLANGCI) run -v --timeout 5m

.PHONY: fmt
fmt: install-tools ## Formats all go files
	$(GO) fix ./...
	$(GO) run $(gofumpt) -l -w -extra  .
		$(GO) run $(gci) write -s standard -s default -s "prefix(github.com/rudderlabs)" -s "prefix($(shell $(GO) list -m))" --skip-generated .