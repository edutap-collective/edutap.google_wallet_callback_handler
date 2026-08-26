# The entry points of this repository.
#
# The CI workflow calls these same targets rather than repeating the commands in
# YAML. Two lists of commands drift, and the drift is only noticed the day the
# pipeline lets something through that would have been red locally.

UV ?= uv
RUN := $(UV) run

.DEFAULT_GOAL := help

.PHONY: help
help:  ## show the available targets
	@grep -hE '^[a-zA-Z_-]+:.*?##' $(MAKEFILE_LIST) \
		| sort \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

.PHONY: sync
sync:  ## create or refresh the development environment
	$(UV) sync --group dev

.PHONY: lint
lint: sync  ## run every check that can fail the build
	$(RUN) ruff check src tests
	$(RUN) ruff format --check src tests
	$(RUN) ty check src tests

.PHONY: reformat
reformat: sync  ## apply the fixes the linter and formatter can make themselves
	$(RUN) ruff check --fix src tests
	$(RUN) ruff format src tests

.PHONY: typecheck
typecheck: sync  ## run the type checker on its own
	$(RUN) ty check src tests

.PHONY: test-local
test-local: sync  ## the fast suite: no broker, no network
	$(RUN) pytest

.PHONY: test-matrix
test-matrix:  ## the suite across every supported Python version
	uvx --with tox-uv tox

.PHONY: docker-build
docker-build:  ## build the service image the way the registry does
	docker build --tag edutap-wallet-google-callback-handler:local .

.PHONY: clean
clean:  ## remove build and tool caches
	rm -rf build dist .ruff_cache .pytest_cache .tox
	find src tests -name '__pycache__' -type d -prune -exec rm -rf {} +
