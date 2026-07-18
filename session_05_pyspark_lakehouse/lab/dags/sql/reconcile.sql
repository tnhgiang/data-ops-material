-- Reconcile metrics for the Delta fact on one run date, in a single scan:
-- row count, SUM(amount), and the count of negative amounts (business rule).
-- The reconcile task compares these against the raw source read from MinIO.
SELECT
    COUNT(*)                            AS fact_count,
    COALESCE(SUM(amount), 0)            AS fact_sum,
    COUNT(*) FILTER (WHERE amount < 0)  AS negatives
FROM delta.lakehouse.fact_sales
WHERE order_date = DATE '{ds}'
