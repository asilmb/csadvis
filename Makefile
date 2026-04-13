# ─── CS2 Analytics — dev tooling ─────────────────────────────────────────────
# Requires: pip install -r requirements-dev.txt
#
# Usage:
#   make test      run pytest with coverage
#   make lint      check code with ruff
#   make fmt       format code with ruff
#   make fix       auto-fix + format  (Rector-style)
#   make types     run mypy type checker
#   make ci        full pipeline: fix → types → test

.PHONY: test lint fmt fix types ci e2e

test:
	pytest --cov=engine --cov=ingestion --cov=database \
	       --cov-report=term-missing --cov-report=html:htmlcov

lint:
	ruff check .

fmt:
	ruff format --check .

fix:
	ruff check --fix .
	ruff format .

types:
	mypy engine/ ingestion/ database/ scheduler/ api/

ci: fix types test

e2e:
	pytest tests/test_e2e_dashboard.py -v -m e2e --base-url http://localhost:8050
