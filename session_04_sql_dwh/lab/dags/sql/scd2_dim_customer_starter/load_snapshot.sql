-- Lab 2 source (given) — the day's customer snapshot, deterministic from {{ ds }}.
-- Customer 201 moves gold -> platinum on 2024-01-03; customer 202 stays silver.
-- Idempotent: rerunning a date replaces that date's rows.
BEGIN;

DELETE FROM bronze.customer_snapshots WHERE snapshot_date = DATE '{{ ds }}';

INSERT INTO bronze.customer_snapshots VALUES
    (201, CASE WHEN DATE '{{ ds }}' >= DATE '2024-01-03' THEN 'platinum' ELSE 'gold' END, DATE '{{ ds }}'),
    (202, 'silver', DATE '{{ ds }}');

COMMIT;
