.PHONY: install lint test run replay compare docker-build docker-run ci

install:
	python3.11 -m pip install -r requirements.txt

lint:
	ruff check broker tests api.py

test:
	python -m pytest -q

run:
	python -m uvicorn --factory api:create_app --reload

replay:
	python -m broker.trace replay --trace data/01a_trace.csv --policy fair --seed 42 --out out
	python -m broker.trace replay --trace data/01a_trace.csv --policy fifo --seed 42 --out out

compare:
	python -m broker.trace compare --trace data/01a_trace.csv --seeds 20 --out out

docker-build:
	docker build -t job-broker:dev .

docker-run:
	docker compose up --build

ci: lint test
	python -m broker.trace replay --trace data/01a_trace.csv --policy fair --seed 42 --out /tmp/job-broker-ci >/dev/null
