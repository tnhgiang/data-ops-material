SELECT
  (SELECT COUNT(*) FROM silver.orders   WHERE order_date = DATE '{{ ds }}')
  = 
  (SELECT COUNT(*) FROM gold.fact_sales WHERE order_date = DATE '{{ ds }}') AS count_match,
  
  ROUND((SELECT COALESCE(SUM(amount), 0) FROM silver.orders   WHERE order_date = DATE '{{ ds }}'), 2)
  = 
  ROUND((SELECT COALESCE(SUM(amount), 0) FROM gold.fact_sales WHERE order_date = DATE '{{ ds }}'), 2) AS sum_match,
  (
    SELECT COUNT(*) 
    FROM gold.fact_sales f
      WHERE f.order_date = DATE '{{ ds }}'
        AND (
          f.amount < 0
          OR f.customer_id IS NULL
          -- NOT EXISTS, not NOT IN: a single NULL in the dim key makes
          -- `NOT IN` never true, silently passing every orphan row.
          OR NOT EXISTS (
            SELECT 1 
            FROM gold.dim_customer d
            WHERE d.customer_id = f.customer_id
          )
        )
  ) = 0  AS rules_ok;
