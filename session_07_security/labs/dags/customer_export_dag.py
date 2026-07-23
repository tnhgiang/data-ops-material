import csv
import io
from datetime import datetime

from airflow.decorators import dag, task

DB_HOST = "postgres-source"
DB_PORT = 5432
DB_NAME = "dataops_source"
DB_USER = "postgres"
DB_PASSWORD = "postgres"

MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
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
        import psycopg2
        from minio import Minio

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
        print(f"Exported {len(rows)} rows to exports/segment_summary_{ds}.csv")

    export_segment_summary()


lab1_customer_export()
