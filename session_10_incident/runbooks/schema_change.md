# Runbook: Upstream Schema Change — Column Renamed in Source CSV

## Symptom

- `reconcile__revenue__fact_daily_revenue` task fails with check returned `False` (revenue = 0)
- All upstream Spark tasks (`raw2bronze__marketplace_orders`, `bronze2silver__orders`, `silver2gold__fact_daily_revenue`) are **green**
- Row counts in silver and gold are normal

## Trace in Marquez

1. Open `http://localhost:3000`
2. Select namespace **s3a://gold** → dataset **fact_daily_revenue**
3. Click the latest run → confirm **Output Fields**: `total_revenue`, `order_count`, `partner`, `order_date` are all present but values are suspect
4. Click the **upstream edge** to **s3a://silver / orders**
   - Confirm fields: `order_id`, `partner`, `amount`, `order_date`, `status`, `customer_email_masked`
   - `amount` is present in schema — the column exists but its values are **null** (`bronze2silver__orders` selected `amount` from bronze, but the source CSV no longer had it)
5. Click further upstream to **s3a://bronze / marketplace_orders**
   - Inspect the **Schema** tab: you will see **both `amount` and `revenue` columns**
   - Delta Lake merged schemas across runs; the `revenue` column appeared on the incident date
   - **Root cause confirmed**: upstream renamed `amount` → `revenue`. Bronze write succeeded (schema merged), silver read `amount` (null), gold summed nulls to 0.

## Fix

```bash
# 1. Restore the correct column name in the source file
python scripts/inject_incident.py --restore

# 2. Rerun all tasks for the incident date
docker exec session_10_incident-airflow-worker-1 \
  airflow tasks clear session_10_order_pipeline -s 2026-06-15 -e 2026-06-16 -y
```

## Verify

- All tasks turn green in Airflow UI
- Marquez bronze schema: `revenue` column no longer appears in new run
- `reconcile__revenue__fact_daily_revenue` passes (Trino query returns `True`)
- `total_revenue > 0` for the incident date

## Escalate If

- Source file on partner side still has the wrong column name → contact partner's data team
- Pipeline still fails after restore → check `bronze2silver__orders` logs for other schema issues
- Recurring pattern on the same partner → add a schema contract check in `raw2bronze__marketplace_orders`
