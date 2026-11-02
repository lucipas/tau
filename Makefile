PYTHON ?= python3
VENV ?= .venv
BIN := $(VENV)/bin

.PHONY: venv install test test-all demo lint clean

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

lint:
	$(BIN)/python -m compileall -q tau scripts tests

clean:
	rm -rf build dist .pytest_cache *.egg-info
	find . -name __pycache__ -type d -prune -exec rm -rf {} +
