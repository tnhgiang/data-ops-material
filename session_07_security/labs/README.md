# Session 07 — Labs

Three hands-on labs on the demo stack (`session_07_security/docker-compose.yml` must be running and `session_07_customer_pii_pipeline` must have completed at least once so `silver`/`gold` contain data).

| Lab | Topic | File(s) | Findings |
|---|---|---|---|
| Lab 1 | Secrets Management | `dags/customer_export_dag.py` | 3 |
| Lab 2 | PII — Data Minimization & Masking | `jobs/build_marketing_customers.py`, `trino_rules_patch.json` | 3 |
| Lab 3 | Least Privilege, Log Hygiene & Audit | `jobs/sync_customers.py`, `sql/init_reporting.sql` | 6 |

## Rules

1. Code in each lab **runs correctly** — the issues are **security**, not functionality.
2. For each finding, submit all four parts: **(a)** exact file + line, **(b)** specific risk explanation (not just "insecure"), **(c)** a working fix, **(d)** evidence of retesting after the fix.
3. Do not modify the demo infrastructure (docker-compose, Vault seed, base Trino config) unless the lab explicitly requires it.

## Running lab code

The `labs/` directory is already mounted into both Airflow and Spark — **no file copying needed**:
- Airflow: `labs/dags/` → `/opt/airflow/dags/labs/dags/` — DAGs appear automatically in the UI.
- Spark: `/labs` → run directly; `PYTHONPATH=/jobs` allows lab jobs to `import security_utils`.

Edit lab files directly on the host — changes reflect inside containers immediately.

```bash
# Run a Spark job
JARS=/opt/jars/delta-spark_2.12-3.2.0.jar,/opt/jars/delta-storage-3.2.0.jar,/opt/jars/hadoop-aws-3.3.4.jar,/opt/jars/aws-java-sdk-bundle-1.12.262.jar,/opt/jars/postgresql-42.6.0.jar

# Lab 2
docker exec s07-spark-master /opt/spark/bin/spark-submit --jars $JARS /labs/jobs/build_marketing_customers.py --ds <YYYY-MM-DD>

# Lab 3
docker exec s07-spark-master /opt/spark/bin/spark-submit --jars $JARS /labs/jobs/sync_customers.py --ds <YYYY-MM-DD>

# Lab 3 SQL
docker exec -i session_07_security-postgres-source-1 \
  psql -U postgres -d dataops_source < labs/sql/init_reporting.sql

# Query Trino
docker compose exec demo-cli python /scripts/query_trino.py --user <user> --password '<password>' --sql "..."
```

## Lab 1 — Secrets Management

**File:** `dags/customer_export_dag.py`

The BI team needs a daily CSV export of customer counts by segment and region. A team member wrote this DAG — it works correctly and delivers the file.

This week the security team scanned the repository and **flagged this DAG with 3 findings**. Your task: identify all three, explain the risk of each, and fix the DAG so it still runs successfully.

**Available resources:**
- Vault is running at `http://vault:8200` — inspect: `docker exec -e VAULT_ADDR=http://127.0.0.1:8200 -e VAULT_TOKEN=root-demo-token s07-vault vault kv list secret/dataops`
- Airflow is configured with the Vault secrets backend (see `docker-compose.yml`, `AIRFLOW__SECRETS__*`)
- Airflow containers have `VAULT_ADDR` / `VAULT_TOKEN` env vars and `hvac`, `apache-airflow-providers-postgres` pre-installed
- Reference: how the main DAG (`demo/dags/customer_pii_pipeline.py`) connects to each system

**Acceptance criteria:**
1. `grep -niE "password|secret|access.?key" labs/dags/customer_export_dag.py` returns no credential **values** (variable names and Vault paths are fine).
2. DAG runs green and the CSV still appears in MinIO.
3. The Postgres connection does not use an account with more privileges than this job needs.
4. Explain: if this file had already been committed to Git, is fixing the code enough? What else needs to happen?

---

## Lab 2 — PII: Data Minimization & Masking

**Files:** `jobs/build_marketing_customers.py`, `trino_rules_patch.json`

The Marketing team needs data to analyze customer cohorts by segment, region, registration date, and financial tier, along with the ability to verify contact reachability for SMS/email campaigns.

The DPO (Data Protection Officer) reviewed both files and **rejected them with 3 findings**, noting: *"Violates data minimization and access control principles. Revise."*

**Confirmed business requirements (validated by the DPO):**
- Cohort analysis: `segment`, `region`, registration timestamp
- Financial distribution: **tier** only (not raw balance or score)
- SMS/email campaigns: need to know whether a customer is **contactable** — partially masked phone/email is sufficient, full values are not required
- No requirement exists for full customer identity

**Setup:**
```bash
# add the marketing user to Trino (provided — not a finding):
echo 'marketing:$2y$10$G.Xe8rLs5ALq7IBf5dgsZurmOnIozrb5RTbT5rxKjcD1p0J46mDG6' >> conf/trino/password.db
# password: MarketingDemo_2026!

# merge your fixed rule into conf/trino/rules.json, then:
docker compose restart trino
```

**Acceptance criteria:**
1. `gold.marketing_customers` contains no direct PII column that does not serve one of the confirmed business requirements above.
2. User `marketing` can query `gold.marketing_customers` with phone/email partially masked; **all other tables** → Access Denied.
3. Users `admin` and `analyst` are unaffected.
4. Demonstrate with actual query output for all three users.

---

## Lab 3 — Least Privilege, Log Hygiene & Audit

**Files:** `jobs/sync_customers.py`, `sql/init_reporting.sql`

An intern was assigned to write a customer sync job from the source system into the lakehouse staging area, plus a SQL script to create a service account for an upcoming reporting system. The code runs correctly and produces accurate numbers.

An internal PCI-DSS/PDPA audit **flagged this pair of files with 6 findings**. Your task: identify all six, explain the risk of each, fix them, and demonstrate the fixes.

**Available resources:**
- `demo/jobs/security_utils.py` — read it carefully for what it provides
- `sql/init_source.sql` from the demo — compare how `etl_reader` is created there
- Demo audit log at `logs/audit/pii_access.log`

**Acceptance criteria:**
1. After running the fixed job, **no PII values appear anywhere in the job output or logs**. Prove with grep on the logs.
2. Every read/write of PII data leaves an **audit record** answering: which job, which table, which columns, how many rows, when. Audit records must not contain PII values.
3. The reporting service account has only the minimum privileges needed to read the customer table. Prove: attempting `INSERT`/`DROP` with that account must be denied.
4. The job still functions correctly: `bronze.customers_sync` has the same row count as before the fix.
