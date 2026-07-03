# Reproducible entry points. `uv` manages the Python 3.11 venv and locks deps.
# Java 17 (OpenJDK) must be on PATH for local Spark; see README.

.DEFAULT_GOAL := help
PYTHON_VERSION := 3.12

.PHONY: help setup lint test run clean hooks

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

setup: ## Create the virtualenv and install project + dev dependencies
	uv python install $(PYTHON_VERSION)
	uv sync --extra dev

hooks: ## Install pre-commit git hooks
	uv run pre-commit install

lint: ## Run the ruff linter (PEP8)
	uv run ruff check src tests

test: ## Run the unit test suite
	uv run pytest -q

run: ## Run the full pipeline in dev for all tenants (override with ARGS=...)
	uv run saas-pipeline --env dev --tenant all $(ARGS)

clean: ## Remove generated data and caches
	rm -rf data/dev data/qa data/main .pytest_cache
	find . -type d -name __pycache__ -exec rm -rf {} +
