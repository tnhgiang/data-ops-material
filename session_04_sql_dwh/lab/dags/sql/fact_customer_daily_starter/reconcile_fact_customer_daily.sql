-- Lab 1 — TODO: return ONE row of TRUE/FALSE columns. SQLCheckOperator fails the
-- task if any value is falsy, so every check must be TRUE. Compare the rollup back
-- to silver.orders for DATE '{{ ds }}':
--   check 1: SUM(silver.amount)  ==  SUM(fact.total_amount)
--   check 2: COUNT(silver rows)  ==  SUM(fact.order_count)
-- Return them as boolean columns, e.g. SELECT (<check1>) AS sum_match, (<check2>) AS count_match;
-- (Watch out for float rounding on the SUM comparison.)

-- placeholder: always fails until you write the real checks:
SELECT false AS not_implemented;
