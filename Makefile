# The entry points of this repository.
#
# The CI workflow calls these same targets rather than repeating the commands in
# YAML. Two lists of commands drift, and the drift is only noticed the day the
# pipeline lets something through that would have been red locally.

UV ?= uv
# `--no-sync` on purpose: a bare `uv run` re-resolves the project on every
# invocation, which puts a network round trip in the middle of a lint. The
# `sync` prerequisite is what keeps the environment current.
RUN := $(UV) run --no-sync

COMPOSE_TEST := compose.test.yml
IMAGE := edutap-wallet-google-callback-handler:local

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

# Starts the broker, runs the opt-in tests, and takes the broker down again
# whether they passed or not -- while still failing the target if they did not.
.PHONY: test-integration
test-integration: sync  ## the suite against a real Kafka broker (starts one)
	docker compose -f $(COMPOSE_TEST) up -d --wait
	@status=0; $(RUN) pytest -m integration || status=$$?; \
		docker compose -f $(COMPOSE_TEST) down -v; \
		exit $$status

.PHONY: test-matrix
test-matrix:  ## the fast suite across every supported Python version
	uvx --with tox-uv tox

# The image is what reaches the cluster. `--platform` matters on an Apple
# Silicon machine: every cluster node is x86_64, and without it the container
# fails at start with "exec format error".
.PHONY: docker-build
docker-build:  ## build the container image for the cluster's architecture
	docker build --platform linux/amd64 --tag $(IMAGE) .

.PHONY: clean
clean:  ## remove build and tool caches
	rm -rf build dist .ruff_cache .pytest_cache .tox
	find src tests -name '__pycache__' -type d -prune -exec rm -rf {} +
