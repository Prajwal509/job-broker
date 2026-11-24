.PHONY: install lint test run replay docker-build docker-run ci

install:
	python3.11 -m pip install -r requirements.txt

lint:
	ruff check broker tests api.py

test:
	python -m pytest -q

run:
	python -m uvicorn api:app --reload

replay:
	python -m broker.trace replay --trace data/01a_trace.csv --policy fair --seed 42 --out out

docker-build:
	docker build -t job-broker:dev .

docker-run:
	docker compose up --build

ci: lint test
	head -n 11 data/01a_trace.csv > /tmp/job-broker-slice.csv
	python -m broker.trace replay --trace /tmp/job-broker-slice.csv --policy fair --seed 42 --out out >/dev/null
	python -c "import json; d=json.load(open('out/fair-42-10m.json')); c=d['cost']; assert round(sum(t['total_cost'] for t in c['teams'].values()) + c['platform_cost'], 2) == c['total_cost']"
