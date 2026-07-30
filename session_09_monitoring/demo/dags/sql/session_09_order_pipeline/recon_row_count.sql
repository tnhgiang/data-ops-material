SELECT (
    SELECT COUNT(*) FROM bronze.orders
) = (
    SELECT COUNT(*) FROM gold.fact_orders
) AS row_count_match;
