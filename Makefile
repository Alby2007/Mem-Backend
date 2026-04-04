# Makefile — Trading Galaxy
# Usage: make <target>
# Requires: Python 3.11+, Docker, Ollama

.PHONY: help setup setup-models install dev test test-screenshot test-portfolio \
        test-all ingest seed push-seed docker-up docker-down lint

PYTHON   := python
PORT     := 5051
API_BASE := http://localhost:$(PORT)

help:
	@echo ""
	@echo "  make setup          Full first-run setup (install + pull models)"
	@echo "  make setup-models   Pull required Ollama models only"
	@echo "  make install        Install Python dependencies"
	@echo "  make dev            Start Flask dev server on port $(PORT)"
	@echo "  make test           Run full test suite"
	@echo "  make test-screenshot Run screenshot upload tests only"
	@echo "  make test-portfolio  Run portfolio unit tests only"
	@echo "  make ingest         Trigger a full ingest cycle"
	@echo "  make seed           Load seed into local DB"
	@echo "  make push-seed      Export + push KB seed to GitHub releases"
	@echo "  make docker-up      Start via docker-compose"
	@echo "  make docker-down    Stop docker-compose services"
	@echo "  make lint           Run ruff linter"
	@echo ""

# ── Setup ─────────────────────────────────────────────────────────────────────

setup: install setup-models
	@echo "Setup complete. Run 'make dev' to start the server."

install:
	$(PYTHON) -m pip install -r requirements.txt

setup-models:
	@echo "Pulling required Ollama models..."
	ollama pull llava
	ollama pull llama3.2
	@echo "Models ready."

# ── Development ───────────────────────────────────────────────────────────────

dev:
	PORT=$(PORT) $(PYTHON) api.py

# ── Tests ─────────────────────────────────────────────────────────────────────

test:
	$(PYTHON) -m pytest tests/ -v --tb=short -q

test-screenshot:
	$(PYTHON) -m pytest tests/test_screenshot_upload.py -v --tb=short

test-portfolio:
	$(PYTHON) -m pytest tests/test_portfolio.py -v --tb=short

test-all: test

# ── Data ──────────────────────────────────────────────────────────────────────

ingest:
	curl -s -X POST $(API_BASE)/ingest/run-all | $(PYTHON) -m json.tool

seed:
	$(PYTHON) scripts/load_seed.py

push-seed:
	$(PYTHON) scripts/push_seed.py

# ── Docker ────────────────────────────────────────────────────────────────────

docker-up:
	docker-compose up --build -d
	@echo "Pulling llava into the Ollama container..."
	docker-compose exec ollama ollama pull llava
	docker-compose exec ollama ollama pull llama3.2
	@echo "Done. App at http://localhost:5050"

docker-down:
	docker-compose down

# ── Lint ──────────────────────────────────────────────────────────────────────

lint:
	$(PYTHON) -m ruff check . --select E,W,F --ignore E501
