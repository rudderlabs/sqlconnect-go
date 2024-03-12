.PHONY: help default test test-run generate lint fmt

GO=go
LDFLAGS?=-s -w
TESTFILE=_testok

default: lint

generate: install-tools
	$(GO) generate ./...

test: install-tools test-run

test-run: ## Run all unit tests
ifeq ($(filter 1,$(debug) $(RUNNER_DEBUG)),)
	$(eval TEST_CMD = gotestsum --format pkgname-and-test-fails --)
	$(eval TEST_OPTIONS = -p=1 -v -failfast -shuffle=on -coverprofile=profile.out -covermode=atomic -coverpkg=./... -vet=all --timeout=30m)
else
	$(eval TEST_CMD = SLOW=0 go test)
	$(eval TEST_OPTIONS = -p=1 -v -failfast -shuffle=on -coverprofile=profile.out -covermode=atomic -coverpkg=./... -vet=all --timeout=30m)
endif
ifdef package
ifdef exclude
	$(eval FILES = `go list ./$(package)/... | egrep -iv '$(exclude)'`)
	$(TEST_CMD) -count=1 $(TEST_OPTIONS) $(FILES) && touch $(TESTFILE)
else
	$(TEST_CMD) $(TEST_OPTIONS) ./$(package)/... && touch $(TESTFILE)
endif
else ifdef exclude
	$(eval FILES = `go list ./... | egrep -iv '$(exclude)'`)
	$(TEST_CMD) -count=1 $(TEST_OPTIONS) $(FILES) && touch $(TESTFILE)
else
	$(TEST_CMD) -count=1 $(TEST_OPTIONS) ./... && touch $(TESTFILE)
endif

help: ## Show the available commands
	@grep -E '^[0-9a-zA-Z_-]+:.*?## .*$$' ./Makefile | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

install-tools:
	go install github.com/golang/mock/mockgen@v1.6.0
	go install mvdan.cc/gofumpt@latest
	go install gotest.tools/gotestsum@v1.8.2
	go install golang.org/x/tools/cmd/goimports@latest
	bash ./internal/scripts/install-golangci-lint.sh v1.56.2

.PHONY: lint
lint: fmt ## Run linters on all go files
	golangci-lint run -v --timeout 5m

.PHONY: fmt
fmt: install-tools ## Formats all go files
	gofumpt -l -w -extra  .
	find . -type f -name '*.go' -exec grep -L -E 'Code generated by .*\. DO NOT EDIT.' {} + | xargs goimports -format-only -w -local=github.com/rudderlabs