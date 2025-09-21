# Intro to Prefect

This project is meant as an introduction to data engineering through Prefect.
This `main` branch is the starting point. For the completed version, see
the `complete` branch.

## Setup

```shell
python -m venv .venv
source .venv/bin/activate
pip install -e '.[dev]'
pre-commit install
# docker-compose up -d
prefect config set PREFECT_API_URL="http://127.0.0.1:4200/api"
prefect server start
```

## Teardown

```shell
# docker-compose down
```


> uv add prefect httpx psycopg2-binary

> uv pip install -e '.[dev]'

> uv run prefect config set PREFECT_API_URL="http://127.0.0.1:4200/api"

> uv run prefect server start

> uv run pipeline

> uv run pytest src/test/tasks.py