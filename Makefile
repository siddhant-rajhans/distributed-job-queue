.PHONY: test lint format run bench clean

test:
	PYTHONPATH=src:. pytest tests/ -v --tb=short

lint:
	ruff check src/ tests/

format:
	ruff format src/ tests/

run:
	docker compose up -d db

bench:
	PYTHONPATH=src:. python scripts/run_benchmark.py

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null; true
	rm -rf .pytest_cache dist *.egg-info
