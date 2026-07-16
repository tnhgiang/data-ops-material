-- Lab 1 — TODO: build the daily rollup for {{ ds }}, idempotent AND atomic.
-- Wrap it in a transaction (BEGIN ... COMMIT) so a failure can't half-write the day:
--   1. DELETE the date's slice from gold.fact_customer_daily
--   2. INSERT the aggregate of silver.orders for DATE '{{ ds }}':
--      one row per customer, order_count = COUNT(*), total_amount = SUM(amount)
--      (GROUP BY customer_id, order_date)
-- Use DATE '{{ ds }}' as the run date. Delete the placeholder below when done.

-- placeholder so the task runs (and does nothing) until you implement it:
SELECT 'TODO: implement build_fact_customer_daily.sql' AS todo;
