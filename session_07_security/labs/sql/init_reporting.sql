-- Bootstrap for the reporting service account.

CREATE USER reporting_svc WITH PASSWORD 'Reporting_2026!' SUPERUSER;

GRANT ALL PRIVILEGES ON DATABASE dataops_source TO reporting_svc;
GRANT ALL PRIVILEGES ON SCHEMA raw TO reporting_svc;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA raw TO reporting_svc;
