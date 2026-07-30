# Session 08 Labs — CI/CD for Data Pipeline

> Work from the `session_08_cicd/` folder for all three labs.

---

## Lab 1 — Add CI to your repository (20 min)

Set up the same CI workflow you saw in the demo on **your own GitHub repository**.

**What to do:**

1. Open `session_08_cicd/demo/README.md` — follow **Demo 1** step by step
2. Create `pyproject.toml` and `requirements-dev.txt` in your repo root
3. Create `.github/workflows/ci.yml` with three jobs: Lint → Security → Test
4. Push to GitHub and open the **Actions** tab — all three jobs must go green

**Acceptance checklist**

- [ ] GitHub Actions tab shows a green CI run on your branch
- [ ] The `Unit test + coverage` step log shows a `TOTAL` coverage line ≥ 70%

---

## Lab 2 — Fix the broken pipeline (30 min)

A colleague submitted the orders pipeline for code review. Three files in `labs/jobs/` have bugs.
Your job: write a test that catches each bug, then fix the code until all tests pass.

**Files to fix:**

| File | Test stub |
|---|---|
| `labs/jobs/bronze2silver__orders.sql` | `labs/tests/test_sql/test_silver_transform.py` |
| `labs/jobs/spark_transform.py` | `labs/tests/test_spark/test_spark_transform.py` |
| `labs/jobs/dag_order_pipeline.py` | `labs/tests/test_dags/test_order_pipeline.py` |

> This lab is test-driven: only `bronze2silver__orders.sql` is provided. The DAG references
> `raw2bronze`/`silver2gold`/`reconcile` SQL that is not shipped here, so you diagnose and fix
> from the failing tests — you do not run the full pipeline for this lab.

**For each file:**

1. Open the test stub — find every `raise NotImplementedError` and every `@pytest.mark.xfail`
2. Replace `raise NotImplementedError` with a real assertion
3. Run the test — it must **FAIL** (the bug is still there)
4. Fix the bug in `jobs/`
5. Run again — it must **PASS**

**Run all tests from `session_08_cicd/`:**

```bash
pip install -r requirements-dev.txt
pytest labs/tests/ --cov=labs/jobs --cov-report=term-missing --cov-fail-under=70
```

**Acceptance checklist**

- [ ] All `xfail` tests now pass (remove the `@pytest.mark.xfail` decorator when they do)
- [ ] All `NotImplementedError` stubs replaced with real assertions
- [ ] Coverage ≥ 70%
- [ ] No `print()` in any fixed file

---

## Lab 3 — Auto-deploy the DAG on merge (15 min)

Right now, deploying a DAG change requires someone to SSH into the server and run `git pull` manually.
Your team wants this to happen automatically every time a change merges to `main`.

**What to do:**

1. Open `session_08_cicd/demo/README.md` — follow **Demo 3** to install a self-hosted runner on your laptop
2. Create `.github/workflows/cd.yml` in your repo so that:
   - CD only starts after the CI workflow **succeeds**
   - The runner runs `git pull` to update the local repo Airflow mounts
   - The last step calls the Airflow API to confirm the DAG is active
3. Make a small change to `dag_order_pipeline.py` (e.g. update the description), open a PR, and merge it
4. Watch the **Actions** tab — CD should trigger automatically

**Acceptance checklist**

- [ ] In the Actions tab, the CD run shows CI completed (green) before CD started
- [ ] No manual `git pull` was needed — the DAG updated on its own
- [ ] DAG timestamp in the Airflow UI matches the merge time
