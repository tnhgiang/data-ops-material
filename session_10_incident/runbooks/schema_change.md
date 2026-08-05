# Runbook: Upstream Schema Change — Column Renamed in Source CSV

## Symptom

- `reconciliation` task fails with:
  `total_revenue is 0.0 for <date> — expected > 0`
- All upstream Spark tasks (`bronze_load`, `silver_transform`, `gold_aggregate`) are **green**
- Row counts in silver and gold are normal

## Why This Is Hard to Spot Without Lineage

All tasks succeed and row counts look correct — the only signal is revenue = 0 in gold.
Without a lineage graph you would need to manually inspect each job's output to find
where `amount` turned null. Marquez shows you the full dataset chain and which job
consumed which columns, so you can pinpoint the break in one trace.

## Trace in Marquez

1. Open `http://localhost:3000`
2. Search for dataset: `gold.fact_daily_revenue`
3. Click the run for the incident date → inspect **Output Facets**: `total_revenue = 0`
4. Click the **upstream edge** → `silver.orders`
5. Inspect the `silver_transform` job run → **Column Lineage** tab:
   - `orders.amount` ← `marketplace_orders.amount` ← source CSV `amount` column
   - If `amount` is missing from the CSV, this chain is broken (null propagation)
6. Click further upstream → `bronze.marketplace_orders` → inspect the **bronze_load** job run
   - Schema of source CSV shows column `revenue`, not `amount`
   - **Root cause confirmed**: upstream renamed `amount` → `revenue`

## Fix

```bash
# 1. Restore the correct column name in the source file
python scripts/inject_incident.py --restore

# 2. Clear and rerun all tasks for the incident run
docker exec session_10_incident-airflow-worker-1 \
  airflow tasks clear session_10_order_pipeline -s 2026-06-15 -e 2026-06-16 -y
```

## Verify

- All 4 tasks turn green in Airflow UI
- Marquez: lineage chain complete from CSV → bronze → silver → gold
- `reconciliation` log: `Reconcile passed for 2026-06-15`
- `total_revenue > 0` for the incident date

## Escalate If

- Source file on partner side still has the wrong column name → contact partner's data team
- Pipeline still fails after restore → check `silver_transform` logs for other schema issues
- Recurring pattern on the same partner → add a schema contract check in `bronze_load`
