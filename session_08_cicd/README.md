# Session 08 — CI/CD for Data Pipeline

Automate quality gates (lint, security scan, test coverage) and continuous deployment of DAGs to Airflow using GitHub Actions.

## Stack

| Tool | Role |
|---|---|
| GitHub Actions | CI/CD platform |
| `ruff` | Lint + format |
| `bandit` | Security scan |
| `pytest` + `pytest-cov` | Unit test + 70% coverage gate |
| self-hosted runner + `git pull` | CD deploy to Airflow Docker Compose |

## Structure

```
session_08_cicd/
├── .github/workflows/   # ci.yml + cd.yml (copy to your repo root)
├── demo/                # working pipeline + full test suite
├── labs/                # pipeline jobs + test stubs
├── pyproject.toml       # pytest + bandit config
└── requirements-dev.txt
```

## Quick start (demo)

```bash
pip install -r requirements-dev.txt
pytest demo/tests/ --cov=demo/dags --cov=demo/jobs --cov-report=term-missing
```

> **Note:** PySpark requires Java 8 or 11. If your default Java is 17+, set `JAVA_HOME` before running:
> ```bash
> JAVA_HOME=$(/usr/libexec/java_home -v 1.8) pytest demo/tests/test_spark/
> ```

## Common errors

| Error | Cause | Fix |
|---|---|---|
| `ModuleNotFoundError: dataops_common` | plugins not on PYTHONPATH | `export PYTHONPATH=/opt/airflow/plugins:$PYTHONPATH` |
| `bandit` B105 false positives | test files scanned | `exclude_dirs = ["demo/tests", "labs/tests"]` in pyproject.toml |
| `DagBag` import error | Airflow not installed locally | DAG tests are skipped locally — they run in CI where Airflow is available |
| CD job not triggering | `workflow_run` requires CI workflow name to match exactly | check `workflows: ["DataOps CI"]` in cd.yml matches the `name:` in ci.yml |
| CD job runs even when CI failed | missing `if` condition on deploy job | add `if: ${{ github.event.workflow_run.conclusion == 'success' }}` |
