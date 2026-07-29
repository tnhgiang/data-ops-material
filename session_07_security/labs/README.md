# Session 07 — Labs

Three hands-on labs on the demo stack (`session_07_security/docker-compose.yml` must be running and `session_07_customer_pii_pipeline` must have completed at least once so `silver`/`gold` contain data).

| Lab | Topic | File(s) | Findings |
|---|---|---|---|
| Lab 1 | Secrets Management | `dags/customer_export_dag.py` | 3 |
| Lab 2 | PII — Data Minimization & Masking | `jobs/build_marketing_customers.py`, `trino_rules_patch.json` | 3 |
| Lab 3 | Least Privilege & Log Hygiene | `jobs/sync_customers.py`, `sql/init_reporting.sql` | 3 |

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
# Lab 2
docker exec s07-spark-master /opt/spark/bin/spark-submit /labs/jobs/build_marketing_customers.py --ds <YYYY-MM-DD>

# Lab 3
docker exec s07-spark-master /opt/spark/bin/spark-submit /labs/jobs/sync_customers.py --ds <YYYY-MM-DD>

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

**How to fix:**

*F1 + F2 — pull all credentials from Vault directly via `hvac` (same pattern as demo Spark jobs):*
```python
import os
import hvac

vault = hvac.Client(url=os.environ["VAULT_ADDR"], token=os.environ["VAULT_TOKEN"])

# F1: DB credentials from Vault
pg_secret = vault.secrets.kv.v2.read_secret_version(
    path="dataops/postgres_source_lab", raise_on_deleted_version=True
)["data"]["data"]

conn = psycopg2.connect(
    host=pg_secret["host"], port=int(pg_secret["port"]),
    dbname=pg_secret["database"], user=pg_secret["username"], password=pg_secret["password"],
)

# F2: MinIO credentials from Vault
minio_secret = vault.secrets.kv.v2.read_secret_version(
    path="dataops/minio", raise_on_deleted_version=True
)["data"]["data"]

client = Minio(minio_secret["endpoint"].replace("http://", ""),
               access_key=minio_secret["access_key"],
               secret_key=minio_secret["secret_key"], secure=False)
```
Delete all hardcoded constants (`DB_*`, `MINIO_*`) at the top of the file.

*F3 — replace superuser account with least-privilege role:*
```sql
-- run inside postgres-source
CREATE USER lab_reader WITH PASSWORD 'LabReader_2026!';
GRANT CONNECT ON DATABASE dataops_source TO lab_reader;
GRANT USAGE ON SCHEMA raw TO lab_reader;
GRANT SELECT ON raw.customers TO lab_reader;
```
Then update the Vault secret — no code change needed:
```bash
docker exec -e VAULT_ADDR=http://127.0.0.1:8200 -e VAULT_TOKEN=root-demo-token s07-vault \
  vault kv patch secret/dataops/postgres_source_lab \
  username="lab_reader" password="LabReader_2026!"
```

---

## Lab 2 — PII: Data Minimization & Masking

**Files:** `jobs/build_marketing_customers.py`, `trino_rules_patch.json`

> **Note on `trino_rules_patch.json`:** The patch file intentionally grants `marketing` broad access (`"schema": ".*", "table": ".*"`) — this is the broken starting point for F3. Your task is to replace those table entries with a rule that restricts `marketing` to exactly `gold.marketing_customers`. Do **not** add the patch file as-is; read it, understand what's wrong, then write the correct entry.

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

**How to fix:**

*F1 — drop excess PII columns, replace raw financials with tier:*
```python
from pyspark.sql import functions as F

df_out = (
    df
    .withColumn("balance_tier",
        F.when(F.col("account_balance") < 50_000_000, "low")
         .when(F.col("account_balance") < 200_000_000, "medium")
         .otherwise("high"))
    .withColumn("updated_at", F.lit(ds).cast("date"))
    .select("customer_id", "segment", "region", "registered_at",
            "phone", "email", "balance_tier", "updated_at")
)
```

*F2 — mask phone/email at query layer via Trino (not in Spark):*

Do NOT mask in the Spark job — if you write masked values to disk, `admin` also loses access to the real values. Masking belongs in `trino_rules_patch.json`:
```json
"columns": [
  { "name": "phone", "mask": "regexp_replace(phone, '(\\d{3})\\d{4}(\\d{3})', '$1****$2')" },
  { "name": "email", "mask": "regexp_replace(email, '(^[^@]{2})[^@]+(@.+)', '$1***$2')" }
]
```

*F3 — restrict `marketing` to only `gold.marketing_customers` in Trino:*

Edit `labs/trino_rules_patch.json` so `marketing` has `SELECT` on exactly one table, and is denied everything else. Merge into `conf/trino/rules.json` then restart Trino:
```bash
docker compose restart trino
```

---

## Lab 3 — Least Privilege & Log Hygiene

**Files:** `jobs/sync_customers.py`, `sql/init_reporting.sql`

An intern was assigned to write a customer sync job from the source system into the lakehouse staging area, plus a SQL script to create a service account for an upcoming reporting system. The code runs correctly and produces accurate numbers.

An internal audit **flagged this pair of files with 3 findings**. Each finding is marked with a `# TODO` comment in the code. Your task: identify each one, explain the risk, and fix it.

**Available resources:**
- `demo/jobs/security_utils.py` — read it for what helpers are available
- `demo/sql/init_source.sql` — see how `etl_reader` is created as a reference for least privilege

**Acceptance criteria:**
1. After running the fixed job, **no PII values appear in logs or stdout**. Prove: `docker logs s07-spark-master 2>&1 | grep -i "phone\|cccd\|full_name"` returns nothing.
2. The reporting service account has only `SELECT` on `raw.customers`. Prove: `INSERT` with that account must be denied.
3. The job still produces the correct row count in `bronze.customers_sync`.
