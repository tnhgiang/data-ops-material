BEGIN;

DELETE FROM bronze.orders_raw WHERE order_date = DATE '{{ ds }}';

INSERT INTO bronze.orders_raw
SELECT
    CAST(strftime(DATE '{{ ds }}', '%Y%m%d') AS BIGINT) * 100 + i AS order_id,
    101 + (i % 3)                                                 AS customer_id,
    (['Acme Corp', 'Globex', 'Initech'])[(i % 3) + 1]            AS customer_name,
    ROUND(20.0 + i * 7.5, 2)                                      AS amount,
    DATE '{{ ds }}'                                               AS order_date
FROM range(50) AS t(i)
UNION ALL
SELECT CAST(strftime(DATE '{{ ds }}', '%Y%m%d') AS BIGINT) * 100 + 90, NULL, NULL,     42.0,  DATE '{{ ds }}'
UNION ALL
SELECT CAST(strftime(DATE '{{ ds }}', '%Y%m%d') AS BIGINT) * 100 + 91, 102,  'Globex', -15.0, DATE '{{ ds }}';

COMMIT;
