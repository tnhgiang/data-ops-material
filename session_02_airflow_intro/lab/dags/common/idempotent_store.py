"""
Partition-based file store — a rerun overwrites its logical date's partition
instead of appending, so backfills stay safe.
"""

from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any


DATA_ROOT = Path(os.environ.get("DATAOPS_DATA_DIR", "/opt/airflow/data"))


def partition_dir(layer: str, table: str, logical_date: datetime) -> Path:
    """Return the Hive-style partition path for a logical date."""
    date_key = logical_date.strftime("%Y-%m-%d")
    return DATA_ROOT / layer / table / f"dt={date_key}"


def partition_file(layer: str, table: str, logical_date: datetime, filename: str) -> Path:
    return partition_dir(layer, table, logical_date) / filename


def write_json_atomic(path: Path, payload: Any) -> None:
    """Write JSON atomically: temp file in the same directory, then rename."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        dir=path.parent,
        delete=False,
        suffix=".tmp",
    ) as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        temp_path = Path(handle.name)
    temp_path.replace(path)


def read_json(path: Path) -> Any:
    with path.open(encoding="utf-8") as handle:
        return json.load(handle)


def write_records(layer: str, table: str, logical_date: datetime, records: list[dict]) -> Path:
    target = partition_file(layer, table, logical_date, "data.json")
    write_json_atomic(target, records)
    return target


def read_records(layer: str, table: str, logical_date: datetime) -> list[dict]:
    target = partition_file(layer, table, logical_date, "data.json")
    if not target.exists():
        return []
    payload = read_json(target)
    return payload if isinstance(payload, list) else []


def reconcile_counts(source_count: int, target_count: int, label: str) -> None:
    if source_count != target_count:
        raise ValueError(
            f"Reconciliation failed for {label}: source={source_count}, target={target_count}"
        )
