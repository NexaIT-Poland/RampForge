.PHONY: help install-backend install-client install test lint format clean seed run-backend run-client

help:
	@echo "RampForge - Dock Scheduling Application"
	@echo ""
	@echo "Available commands:"
	@echo "  make install-backend  - Install backend dependencies"
	@echo "  make install-client   - Install TUI client dependencies"
	@echo "  make install          - Install all dependencies"
	@echo "  make seed             - Seed database with demo data"
	@echo "  make run-backend      - Run backend API server"
	@echo "  make run-client       - Run TUI client (Phase 3)"
	@echo "  make test             - Run all tests"
	@echo "  make lint             - Run linters"
	@echo "  make format           - Format code"
	@echo "  make clean            - Clean generated files"

install-backend:
	cd backend && pip install -e ".[dev]"

install-client:
	cd client_tui && pip install -e ".[dev]"

install: install-backend install-client

seed:
	cd backend && python -m app.seed

run-backend:
	cd backend && python run.py

run-client:
	cd client_tui && python run.py

test:
	cd backend && pytest
	@echo "TUI tests not yet implemented (Phase 3)"

lint:
	cd backend && ruff check app/
	cd backend && mypy app/

format:
	cd backend && black app/
	cd backend && ruff check --fix app/

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".mypy_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".ruff_cache" -exec rm -rf {} + 2>/dev/null || true
	rm -f backend/rampforge.db
