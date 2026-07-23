import json
import logging
import os
from datetime import datetime, timezone

import hvac

AUDIT_LOG_PATH = "/var/log/pii_audit/pii_access.log"

audit_logger = logging.getLogger("audit.pii_access")


def get_logger(name: str) -> logging.Logger:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    return logging.getLogger(name)


def get_vault_client() -> hvac.Client:
    addr = os.environ.get("VAULT_ADDR", "http://vault:8200")
    token = os.environ["VAULT_TOKEN"]
    client = hvac.Client(url=addr, token=token)
    if not client.is_authenticated():
        raise RuntimeError(f"Failed to authenticate to Vault at {addr}")
    return client


def get_vault_secret(path: str) -> dict:
    client = get_vault_client()
    response = client.secrets.kv.v2.read_secret_version(path=path, raise_on_deleted_version=True)
    return response["data"]["data"]


def log_pii_access(job_name: str, table: str, columns: list, record_count: int, ds: str) -> None:
    record = {
        "event": "PII_ACCESS",
        "job": job_name,
        "table": table,
        "pii_columns": columns,
        "record_count": record_count,
        "execution_date": ds,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    line = json.dumps(record)
    audit_logger.info(line)
    try:
        os.makedirs(os.path.dirname(AUDIT_LOG_PATH), exist_ok=True)
        with open(AUDIT_LOG_PATH, "a") as f:
            f.write(line + "\n")
    except OSError as e:
        audit_logger.warning(f"Could not write audit log file: {e}")
