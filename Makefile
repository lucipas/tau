PYTHON ?= python3
VENV ?= .venv
BIN := $(VENV)/bin

.PHONY: venv install test test-all demo social-demo lint clean web-install web-build web-dev web-clean

venv:
	@test -d "$(VENV)" || $(PYTHON) -m venv "$(VENV)"
	$(BIN)/pip install --quiet --upgrade pip
	$(BIN)/pip install --quiet -e ".[dev]"

install: venv

test: venv
	$(BIN)/pytest -m "not integration"

test-all: venv
	TAU_INTEGRATION=1 $(BIN)/pytest

demo: venv
	$(BIN)/python scripts/demo.py

social-demo: venv
	$(BIN)/python scripts/social_demo.py

lint:
	$(BIN)/python -m compileall -q tau scripts tests

web-install:
	cd web && npm install --no-audit --no-fund

web-build: web-install
	cd web && npm run build

web-dev: web-install
	cd web && npm run dev

web-clean:
	rm -rf web/out web/.next

clean:
	rm -rf build dist .pytest_cache *.egg-info
	find . -name __pycache__ -type d -prune -exec rm -rf {} +
