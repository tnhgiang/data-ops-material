-- Bootstrap for the reporting service account.

-- TODO (Finding 1 & 2): SUPERUSER and ALL PRIVILEGES violate least privilege.
-- This account only needs to SELECT from raw.customers.
-- Replace with a read-only account following principle of least privilege.
-- Reference: demo/sql/init_source.sql — see how etl_reader is created.

CREATE USER reporting_svc WITH PASSWORD 'Reporting_2026!' SUPERUSER;

GRANT ALL PRIVILEGES ON DATABASE dataops_source TO reporting_svc;
GRANT ALL PRIVILEGES ON SCHEMA raw TO reporting_svc;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA raw TO reporting_svc;
