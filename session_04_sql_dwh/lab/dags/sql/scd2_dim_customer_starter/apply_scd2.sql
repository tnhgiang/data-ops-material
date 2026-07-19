-- Lab 2 — TODO: merge the day's snapshot into gold.dim_customer_scd2, in ONE
-- transaction, so rerunning the same date changes nothing (idempotent). Read from
-- silver.customer_snapshots (the validated snapshot), NOT bronze — the dimension is
-- built silver -> gold, same as the demo's dim_customer. Two steps:
--
--   Step A — CLOSE: for a customer whose CURRENT row has a tier different from
--            today's snapshot (silver.customer_snapshots for DATE '{{ ds }}'),
--            set effective_to = DATE '{{ ds }}' and is_current = false.
--
--   Step B — OPEN: INSERT a new current row (effective_from = DATE '{{ ds }}',
--            effective_to = NULL, is_current = true) for every snapshot customer
--            that has no current row with that same tier yet — new customers and
--            the ones you just closed. Omit customer_key (the sequence fills it).
--
-- Hint: "needs a new version" == there is NOT a current row with the same
--       (customer_id, tier). Delete the placeholder below when done.

-- placeholder so the task runs (and does nothing) until you implement it:
SELECT 'TODO: implement apply_scd2.sql' AS todo;
