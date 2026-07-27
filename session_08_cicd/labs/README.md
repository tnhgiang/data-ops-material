# Session 08 Labs — CI/CD for Data Pipeline

## Lab 1 — Setup CI workflow (20 min)

Replicate what the instructor demonstrated: set up the same CI workflow on your own repository. Follow the demo runbook step by step.

---

## Lab 2 — Fix the broken pipeline (30 min)

The orders pipeline in `jobs/` was submitted by a colleague for code review. You found three files with issues. Your task is to write tests that reproduce each issue, then fix the code so the tests pass.

Work through each file in `jobs/` using the stubs in `tests/`. For each bug:

1. Complete the stub so the test **fails** against the current code
2. Fix the file so the test passes
3. Confirm the test **passes**

Run coverage after all fixes:

```bash
pytest tests/ --cov=jobs --cov-report=term-missing --cov-fail-under=70
```

**Acceptance checklist**

- [ ] All `xfail` tests now pass (not just xfail-pass — actually pass)
- [ ] All `NotImplementedError` stubs replaced with real assertions
- [ ] Coverage ≥ 70%
- [ ] No `print()` in any fixed file

---

## Lab 3 — CD: auto-deploy DAG (15 min)

Your team wants the DAG to deploy automatically whenever a change merges to `main` — no manual `git pull` on the server required.

Follow the demo runbook to install a self-hosted runner on your laptop, then create `.github/workflows/cd.yml` so that:

- CD only starts after CI passes
- The runner runs `git pull` locally, updating the `dags/` folder Airflow already mounts
- The workflow confirms the DAG is active via the Airflow API

Push a new DAG version, merge the PR, and confirm the DAG appears in the Airflow UI within 60 seconds.

**Acceptance checklist**

- [ ] In the Actions tab, the CD run shows CI completed (green) before CD started
- [ ] No manual `git pull` was needed to deploy
- [ ] DAG timestamp in the Airflow UI matches the merge time
