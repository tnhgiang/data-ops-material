# Session 07 Demo — Secure Bronze/Silver/Gold PII Pipeline

This is a **working, secure** implementation of the pipeline described in the lab
spec (`../README.md`) — instead of the 5 injected vulnerabilities, every one of
them is fixed here, and you can point at exactly where. Stack: Airflow (Celery) +
Spark + MinIO + Hive Metastore + Trino + Vault + Postgres, reusing the same
pattern as `session_05_pyspark_lakehouse` / `session_06_performance_optimization`.

| Original vuln | How this demo fixes it |
|---|---|
| #1 Hardcoded DB/MinIO credentials | Vault-backed Airflow Connection (`dags/customer_pii_pipeline.py`) + `hvac` fetch in every Spark job (`jobs/security_utils.py`) |
| #2 Unmasked PII copied to DWH | Gold drops `full_name`/`address`/`date_of_birth` and buckets `account_balance`/`credit_score` (`jobs/transform_gold.py`); Trino masks the rest per role |
| #3 Over-privileged service account | `etl_reader` is `SELECT`-only on one table (`sql/init_source.sql`) |
| #4 DEBUG logs / `.show()` leaking PII | Every job sets `WARN`, uses `.count()`, never dumps rows |
| #5 No audit trail | `log_pii_access()` writes a JSON line per read/write, before and after (`jobs/security_utils.py`) |
| Bonus: masked view for analysts | Trino file-based access control (`conf/trino/rules.json`) instead of a hand-rolled SQL view |

## 1. Start the stack

```bash
cd session_07_security
mkdir -p logs/audit
docker compose up -d
```

Wait for everything to go healthy (`docker compose ps`) — Vault, `postgres-source`,
`hive-metastore` and `trino` all have healthchecks. First boot takes a few minutes
(image pulls + pip installs inside the Spark/Airflow containers).

Then create the empty Delta tables once:

```bash
JARS=/opt/jars/delta-spark_2.12-3.2.0.jar,/opt/jars/delta-storage-3.2.0.jar,/opt/jars/hadoop-aws-3.3.4.jar,/opt/jars/aws-java-sdk-bundle-1.12.262.jar,/opt/jars/postgresql-42.6.0.jar
docker exec s07-spark-master /opt/spark/bin/spark-submit --jars $JARS /jobs/init_schema.py
```

Airflow UI: http://localhost:8888 (airflow/airflow) · Vault UI: http://localhost:8200 (token `root-demo-token`) · Trino: http://localhost:8092 · MinIO console: http://localhost:9001 (minioadmin/minioadmin).

## 2. Prove there are no hardcoded secrets

```bash
grep -rniE "password|secret|access.?key" demo/dags/ demo/jobs/
```

Every hit is either a Vault path string (`"dataops/minio"`), a dict key name, or a
comment — never a literal credential. Compare with the injected
`DB_PASSWORD = "SuperSecret123!"` from the lab spec.

## 3. Look at what's actually in Vault

```bash
docker exec -e VAULT_ADDR=http://127.0.0.1:8200 -e VAULT_TOKEN=root-demo-token s07-vault vault kv get secret/dataops/minio
docker exec -e VAULT_ADDR=http://127.0.0.1:8200 -e VAULT_TOKEN=root-demo-token s07-vault vault kv get secret/dataops/postgres_source
docker exec -e VAULT_ADDR=http://127.0.0.1:8200 -e VAULT_TOKEN=root-demo-token s07-vault vault kv get secret/dataops/trino
docker exec -e VAULT_ADDR=http://127.0.0.1:8200 -e VAULT_TOKEN=root-demo-token s07-vault vault kv get secret/airflow/connections/postgres_source_conn
```

Point out: the DAG (`dags/customer_pii_pipeline.py`) never references
`postgres_source_conn`'s host/login/password directly — Airflow's
`VaultBackend` resolves `PostgresHook(postgres_conn_id="postgres_source_conn")`
against this exact path at task-run time.

## 4. Run the pipeline

In the Airflow UI, unpause and trigger `session_07_customer_pii_pipeline`. Watch
`validate_source_connection -> ingest_bronze -> transform_silver -> transform_gold
-> reconcile -> show_audit_summary` go green. `show_audit_summary`'s task log
prints the tail of the audit trail (see step 6).

## 5. Query as admin vs analyst — the masking payoff

Trino requires a password now (`file` password authenticator, see
`conf/trino/password.db` / `conf/trino/password-authenticator.properties`).
Pass `--password` explicitly. Demo bootstrap credentials (dev/test only):

> ⚠️ **Dev/test only.** These passwords are for classroom use. Never use hardcoded
> credentials in a real deployment — rotate and inject via a secrets manager.

| user | password | access |
|---|---|---|
| `admin` | `AdminDemo_2026!` | all columns, all regions |
| `analyst` | `AnalystDemo_2026!` | masked PII, all regions |
| `analyst_uk` | `AnalystUK_2026!` | masked PII, UK region only |
| `analyst_us` | `AnalystUS_2026!` | masked PII, US region only |

```bash
docker compose exec demo-cli python /scripts/query_trino.py \
  --user admin --password AdminDemo_2026! \
  --sql "SELECT customer_id, phone, email, cccd FROM delta.gold.customer_analytics LIMIT 5"

docker compose exec demo-cli python /scripts/query_trino.py \
  --user analyst --password AnalystDemo_2026! \
  --sql "SELECT customer_id, phone, email, cccd FROM delta.gold.customer_analytics LIMIT 5"
```

Connecting with any other tool (CLI, JDBC, DBeaver)? Use **HTTPS on port
8443** and set both the username *and* password explicitly — e.g. Trino CLI:
`trino --server https://localhost:8443 --insecure --user admin --password`
(it will prompt), or JDBC URL
`jdbc:trino://localhost:8443/delta/gold?SSL=true&SSLVerification=NONE`.
`SSLVerification=NONE` / `--insecure` is needed because the demo generates a
self-signed certificate at container startup; port 8092 stays plain-HTTP for
the internal healthcheck only and rejects authenticated queries.

`admin` sees real phone/email/cccd. `analyst` sees `phone`/`email` masked
(`090****678`, `jo***@gmail.com`) and `cccd` as `NULL` — same query, same table,
different masking rule, enforced by Trino (`conf/trino/rules.json`), not by the
application.

Then show defense in depth on the raw layer:

```bash
docker compose exec demo-cli python /scripts/query_trino.py \
  --user analyst --password AnalystDemo_2026! \
  --sql "SELECT * FROM delta.bronze.customers LIMIT 5"
# -> blocked: Trino ACL denies SELECT on bronze (rules.json) + MinIO trino-svc has no bronze bucket access (defense in depth)
```

## 6. Tail the audit trail

```bash
tail -20 logs/audit/pii_access.log
```

Each line is a JSON record: job, table, columns touched, row count, `ds`,
timestamp — answers "who read what PII, when, how much" without ever writing a
real phone/email/cccd value into a log file.

## 7. Least privilege at the DB layer

```bash
docker exec -it $(docker compose ps -q postgres-source) \
  psql -U etl_reader -d dataops_source -c "INSERT INTO raw.customers (customer_id) VALUES ('x')"
# -> ERROR: permission denied for table customers
```

`etl_reader` (used by the Vault-backed Airflow connection and by the Spark JDBC
read) can only `SELECT` — it cannot write, `TRUNCATE`, or `DROP`, unlike the
injected `SUPERUSER` from vuln #3.

## Demo simplifications (call these out explicitly)

- **Vault dev mode**: single root token, in-memory storage, no TLS. A real
  deployment uses a properly unsealed/HA Vault cluster and short-lived
  AppRole/Kubernetes-auth tokens instead of a static root token.
- **Trino identity**: password-authenticated (`file` authenticator,
  `conf/trino/password.db`, bcrypt-hashed) over HTTPS, but the TLS certificate
  is self-signed and regenerated on every container restart — clients must
  skip verification (`SSLVerification=NONE`). A real deployment uses a
  CA-issued certificate (or LDAPS/OAuth2) with verification left on.
- **Bootstrap passwords** (`docker-compose.yml`, `sql/init_source.sql`,
  `vault-init/seed_secrets.sh`) are demo-only literals, same tier as this repo's
  existing `MINIO_ROOT_PASSWORD: minioadmin`. Application code never contains
  them — only infra bootstrap files do.

## Shutdown

```bash
docker compose down -v   # -v also drops the Postgres/MinIO/metastore volumes
```
