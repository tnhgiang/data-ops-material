SELECT (
    SELECT COUNT(*) FROM source_orders WHERE order_date = '{{ ds }}'
) = (
    SELECT COUNT(*) FROM gold_fact_orders WHERE order_date = '{{ ds }}'
)
