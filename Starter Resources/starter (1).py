"""
Day 3 - Incremental ETL Pipeline
Squad Python Specialization - Data Engineering

Run it twice in a row. The second run must change nothing.
"""
import csv
import json
import logging
from datetime import datetime
from pathlib import Path

WAREHOUSE_PATH = Path("data") / "transactions.csv"
BATCH_DIR = Path("incoming")
CHECKPOINT_PATH = Path("state") / "checkpoint.json"
LOG_PATH = Path("logs") / "etl.log"

KEY = "transaction_id"
COMPARED_FIELDS = ("customer_id", "merchant", "amount_kes", "status", "channel", "updated_at")


def setup_logging(path: Path = LOG_PATH) -> logging.Logger:
    """Configure a logger that writes to both the console and a log file."""
    raise NotImplementedError


def load_existing(path: Path = WAREHOUSE_PATH) -> dict[str, dict]:
    """Read the loaded transactions into a dict keyed by transaction_id."""
    raise NotImplementedError


def load_batch(path: Path) -> tuple[str, list[dict]]:
    """Read one JSON batch file and return (batch_id, transactions)."""
    raise NotImplementedError


def validate_record(record: dict) -> list[str]:
    """Return the reasons a batch record cannot be loaded (empty list means valid)."""
    raise NotImplementedError


def classify(existing: dict[str, dict], batch: list[dict]) -> dict[str, list[dict]]:
    """Split the batch into the buckets 'new', 'updated', 'duplicate' and 'invalid'.

    Use set operations on the keys. A record whose fields are identical to what is
    already loaded is a duplicate; a record that differs is an update only if it is newer.
    """
    raise NotImplementedError


def upsert(existing: dict[str, dict], new: list[dict], updated: list[dict]) -> dict[str, dict]:
    """Return the merged state: inserts for new keys, replacements for updated keys."""
    raise NotImplementedError


def read_checkpoint(path: Path = CHECKPOINT_PATH) -> dict:
    """Return the saved checkpoint, or a fresh one if none exists yet."""
    raise NotImplementedError


def write_checkpoint(checkpoint: dict, path: Path = CHECKPOINT_PATH) -> None:
    """Persist the checkpoint so an interrupted run never leaves a half-written file."""
    raise NotImplementedError


def save_warehouse(state: dict[str, dict], path: Path = WAREHOUSE_PATH) -> None:
    """Write the merged state back to CSV in a stable order."""
    raise NotImplementedError


def run() -> dict:
    """Process every batch not yet recorded in the checkpoint and return run statistics."""
    # TODO
    pass


if __name__ == "__main__":
    run()
