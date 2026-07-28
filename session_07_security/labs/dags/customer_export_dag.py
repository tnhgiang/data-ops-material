import csv
import io
import logging
from datetime import datetime

from airflow.decorators import dag, task

# TODO (Finding 1 & 2): replace hardcoded credentials with Vault lookups.
# Un-comment the block below and fill in the correct Vault paths.
# from airflow.hooks.base import BaseHook
# from airflow.models import Variable
#
# def _get_vault_conn(conn_id):
#     conn = BaseHook.get_connection(conn_id)
#     return conn
#
# DB connection — Vault path: "airflow/connections/postgres_source_lab"
# MINIO connection — Vault path: "airflow/connections/minio_lab"

DB_HOST = "postgres-source"
DB_PORT = 5432
DB_NAME = "dataops_source"
DB_USER = "postgres"
DB_PASSWORD = "postgres"  # Finding 1: hardcoded credential

MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "minioadmin"  # Finding 2: hardcoded credential
MINIO_SECRET_KEY = "minioadmin"


@dag(
    dag_id="session_07_lab1_customer_export",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["lab"],
)
def lab1_customer_export():
    @task
    def export_segment_summary(ds=None):
        logger = logging.getLogger(__name__)
        import psycopg2
        from minio import Minio

        # TODO (Finding 3): "postgres" is a superuser account.
        # Create and use a read-only account instead (see demo's init_source.sql for reference).
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
        )
        cur = conn.cursor()
        cur.execute("SELECT segment, region, COUNT(*) FROM raw.customers GROUP BY segment, region")
        rows = cur.fetchall()
        cur.close()
        conn.close()

        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(["segment", "region", "total"])
        writer.writerows(rows)
        payload = buf.getvalue().encode()

        client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False,
        )
        client.put_object(
            "warehouse",
            f"exports/segment_summary_{ds}.csv",
            io.BytesIO(payload),
            len(payload),
        )
        logger.info("Exported %d rows to exports/segment_summary_%s.csv", len(rows), ds)

    export_segment_summary()


lab1_customer_export()
