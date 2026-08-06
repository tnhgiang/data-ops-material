# Runbook: Upstream Schema Change — Column Renamed in Source CSV

## Symptom

- `reconciliation` task fails with:
  `total_revenue is 0.0 for <date> — expected > 0`
- All upstream Spark tasks (`bronze_load`, `silver_transform`, `gold_aggregate`) are **green**
- Row counts in silver and gold are normal

## Why This Is Hard to Spot Without Lineage

All tasks succeed and row counts look correct — the only signal is `total_revenue = 0` in gold.
Without a lineage graph you would need to manually inspect each job's output to find
where `amount` turned null. Marquez shows you the full dataset chain so you can pinpoint
the break in one trace.

## Trace in Marquez

1. Open `http://localhost:3000`
2. Select namespace **s3a://gold** → dataset **fact_daily_revenue**
3. Click the latest run → confirm **Output Fields**: `total_revenue`, `order_count`, `partner`, `order_date` are all present but values are suspect (check reconciliation log for `total_revenue=0.00`)
4. Click the **upstream edge** to **s3a://silver / orders**
   - Confirm fields: `order_id`, `partner`, `amount`, `order_date`, `status`, `customer_email_masked`
   - `amount` is present in schema — the column exists but its values are **null** (silver_transform selected `amount` from bronze, but the source CSV no longer had it)
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

- All 4 tasks turn green in Airflow UI
- Marquez bronze schema: `revenue` column no longer appears in new version
- `reconciliation` log: `Reconcile passed for 2026-06-15`
- `total_revenue > 0` for the incident date

## Escalate If

- Source file on partner side still has the wrong column name → contact partner's data team
- Pipeline still fails after restore → check `silver_transform` logs for other schema issues
- Recurring pattern on the same partner → add a schema contract check in `bronze_load`
