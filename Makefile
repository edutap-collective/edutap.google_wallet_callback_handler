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
RELEASE_IMAGE := ghcr.io/edutap-collective/edutap.wallet_google_callback_handler

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

# --- Releases --------------------------------------------------------------
#
# The release tag is a UTC timestamp, `YYYY-MM-DD_HHmm`, and it becomes the image
# tag verbatim. Not a semantic version: this is a service, not a library. It is
# not on an index, nothing imports it, and what it ships is a container -- so the
# artefact carries a state rather than a promise, and a number would claim
# something nobody can act on. The `lmu_edutap_*` services use the same form.
#
# `release` always reads the clock at the moment it runs, so the tag cannot be
# stale. Override it only to repeat a release that failed after tagging:
#
#     make release RELEASE_TAG=2026-08-27_0930
RELEASE_TAG ?= $(shell date -u +%Y-%m-%d_%H%M)
RELEASE_BRANCH ?= main

# Check before tagging, not after. A tag on a commit that is not on the server
# points into nothing for everyone else -- and the workflow that builds on the
# tag would check out a commit the runner cannot fetch.
.PHONY: release-check
release-check:  ## verify the checkout is clean, pushed, and not yet tagged
	@fail=0; \
	head=$$(git rev-parse --abbrev-ref HEAD); \
	if [ "$$head" != "$(RELEASE_BRANCH)" ]; then \
	  printf "on branch '%s', expected '%s'\n" "$$head" "$(RELEASE_BRANCH)"; fail=1; fi; \
	if [ -n "$$(git status --porcelain)" ]; then \
	  echo "the working tree has uncommitted changes"; fail=1; fi; \
	git fetch --quiet origin || true; \
	ahead=$$(git rev-list --count origin/$(RELEASE_BRANCH)..HEAD 2>/dev/null); \
	if [ "$$ahead" != "0" ]; then \
	  printf "%s commit(s) NOT PUSHED\n" "$$ahead"; fail=1; fi; \
	if git rev-parse -q --verify "refs/tags/$(RELEASE_TAG)" >/dev/null; then \
	  printf "tag %s already exists\n" "$(RELEASE_TAG)"; fail=1; fi; \
	[ $$fail -eq 0 ] || { echo ""; echo "release-check failed -- nothing was tagged."; exit 1; }; \
	echo "release-check passed: clean, pushed, no tag $(RELEASE_TAG) yet"

.PHONY: release
release: release-check  ## tag the current main with a UTC timestamp and push it
	git tag -a "$(RELEASE_TAG)" -m "Release $(RELEASE_TAG)"
	git push origin "$(RELEASE_TAG)"
	@echo ""
	@echo "Tag $(RELEASE_TAG) is pushed. The workflow now builds and publishes"
	@echo "ghcr.io/edutap-collective/edutap.wallet_google_callback_handler:$(RELEASE_TAG)"
	@echo "Follow it with: gh run list --branch $(RELEASE_TAG)"

# Which tag to look up: an explicit `RELEASE_TAG=` on the command line wins,
# otherwise the newest release tag in this checkout.
#
# Emphatically NOT the current minute. `release-digest` runs after `release`, and
# by then the clock has moved on -- which is exactly how this failed the first
# time anyone used it, one minute after tagging:
#
#     failed to resolve reference "...:2026-08-27_0130": not found
#
# while the tag that existed was 2026-08-27_0129.
LATEST_RELEASE_TAG = $(shell git tag --list '20[0-9][0-9]-[0-1][0-9]-[0-3][0-9]_[0-2][0-9][0-5][0-9]' --sort=-creatordate | head -n 1)
DIGEST_TAG = $(if $(filter command line,$(origin RELEASE_TAG)),$(RELEASE_TAG),$(LATEST_RELEASE_TAG))

.PHONY: release-digest
release-digest:  ## print the pin for ansible-app-server: <tag>@sha256:...
	@test -n "$(DIGEST_TAG)" || { \
	  echo "no release tag in this checkout -- run 'make release' first,"; \
	  echo "or name one: make release-digest RELEASE_TAG=2026-08-27_0129"; \
	  exit 1; }
	@docker pull --platform linux/amd64 -q $(RELEASE_IMAGE):$(DIGEST_TAG) >/dev/null
	@printf '%s@%s\n' "$(DIGEST_TAG)" \
	  "$$(docker inspect --format '{{index .RepoDigests 0}}' $(RELEASE_IMAGE):$(DIGEST_TAG) | cut -d@ -f2)"

.PHONY: clean
clean:  ## remove build and tool caches
	rm -rf build dist .ruff_cache .pytest_cache .tox
	find src tests -name '__pycache__' -type d -prune -exec rm -rf {} +
