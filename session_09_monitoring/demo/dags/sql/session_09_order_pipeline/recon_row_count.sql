SELECT (
    SELECT COUNT(*) FROM silver.orders
) = (
    SELECT COUNT(*) FROM gold_fact_orders WHERE order_date = '{{ ds }}'
)
