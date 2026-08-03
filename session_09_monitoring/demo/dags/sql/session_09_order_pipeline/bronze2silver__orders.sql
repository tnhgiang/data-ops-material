CREATE SCHEMA IF NOT EXISTS silver;

CREATE OR REPLACE TABLE silver.orders AS
SELECT
    order_id,
    customer_id,
    amount,
    status,
    order_date
FROM bronze.orders
WHERE order_id IS NOT NULL
  AND amount > 0;
