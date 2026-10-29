"""
Day 3: Incremental ETL Pipeline
Squad Python Specialization: Data Engineering

Loads every new JSON batch in incoming/ on top of data/transactions.csv:
new transactions are inserted, newer versions replace older ones, and
repeats are ignored. Running it again changes nothing.

Usage:  python incremental_etl.py
"""
# importing libraries
import csv                                   # read and write the warehouse CSV
import json                                  # batch files and the checkpoint
import logging                               # console + logs/etl.log
import os                                    # fsync and atomic file replace
import sys                                   # stdout handler and exit codes
import time                                  # run duration
from datetime import datetime                # parse and compare updated_at
from decimal import Decimal, InvalidOperation  # exact money amounts (no float rounding)
from pathlib import Path

# paths are relative to this file, so the script works from any folder
BASE_DIR = Path(__file__).resolve().parent
WAREHOUSE_PATH = BASE_DIR / "data" / "transactions.csv"
BATCH_DIR = BASE_DIR / "incoming"
BATCH_PATTERN = "batch_*.json"
CHECKPOINT_PATH = BASE_DIR / "state" / "checkpoint.json"
LOG_PATH = BASE_DIR / "logs" / "etl.log"

KEY = "transaction_id"
COMPARED_FIELDS = ("customer_id", "merchant", "amount_kes", "status", "channel", "updated_at")
FIELDNAMES = (KEY, *COMPARED_FIELDS)            # warehouse column order
BUCKETS = ("new", "updated", "duplicate", "invalid")
VALID_STATUSES = {"PENDING", "COMPLETED", "FAILED"}
CENT = Decimal("0.01")

log = logging.getLogger("incremental_etl")


# Signals a problem that must stop the whole run, such as a missing or damaged warehouse file.

class EtlError(Exception):
    """A problem that must stop the run, e.g. the warehouse file is missing or damaged."""


# ==============================================================================
# TASK 7 (setup): Log execution results
# One logger, two outputs: the console and logs/etl.log. run() writes the counts
# per bucket and one WARNING line per invalid record.
# ==============================================================================

# Handles logging to both console and a log file, with a consistent format.

def setup_logging(path: Path = LOG_PATH) -> logging.Logger:
    """Configure a logger that writes to both the console and a log file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    # "INFO  msg" / "WARNING msg": level padded to 5 characters, as in expected_output.md
    formatter = logging.Formatter("%(asctime)s %(levelname)-5s %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

    # remove old handlers so calling this twice does not print every line twice
    for handler in log.handlers:
        handler.close()
    log.handlers.clear()
    log.setLevel(logging.INFO)
    log.propagate = False

    # the file handler appends, so etl.log keeps one block per run
    for handler in (logging.StreamHandler(sys.stdout), logging.FileHandler(path, encoding="utf-8")):
        handler.setFormatter(formatter)
        log.addHandler(handler)
    return log


# ==============================================================================
# SHARED HELPERS
# Small functions used by several tasks: amounts, timestamps, comparing records,
# and writing files safely.
# ==============================================================================

# Converts an amount from the CSV or a batch into an exact Decimal, so money is never rounded.

def to_amount(value) -> Decimal:
    """Turn a CSV string or a JSON number into an exact Decimal."""
    try:
        amount = Decimal(str(value).strip())
    except InvalidOperation:
        raise ValueError(f"amount_kes {value!r} is not a number") from None
    if not amount.is_finite():
        raise ValueError(f"amount_kes {value!r} is not a number")
    return amount


# Turns an updated_at string into a datetime so versions can be compared.

def parse_timestamp(value) -> datetime:
    """Parse an ISO timestamp such as 2026-10-21T07:02:00 (raises ValueError if invalid)."""
    return datetime.fromisoformat(str(value).strip())


# Cleans a record so CSV rows and JSON records can be compared like for like.

def normalise(record: dict) -> dict:
    """Return a clean copy: stripped text, amount as a 2-decimal Decimal, one timestamp format.

    CSV rows and JSON records go through the same function, so they compare like for like
    (the CSV's "820.00" and the JSON's 820.00 both become Decimal("820.00")).
    """
    clean = {field: str(record[field]).strip() for field in FIELDNAMES}
    clean["amount_kes"] = to_amount(record["amount_kes"]).quantize(CENT)
    clean["updated_at"] = parse_timestamp(record["updated_at"]).isoformat(timespec="seconds")
    return clean


# Checks whether two records hold the same values in every compared field.

def same_fields(a: dict, b: dict) -> bool:
    """True when every compared field holds the same value."""
    return all(a[field] == b[field] for field in COMPARED_FIELDS)


# Checks whether one record was updated more recently than another.

def is_newer(a: dict, b: dict) -> bool:
    """True when record a was updated strictly after record b."""
    return parse_timestamp(a["updated_at"]) > parse_timestamp(b["updated_at"])


# Writes a file safely through a temporary copy, so a crash never leaves it half-written.

def atomic_write(path: Path, write_content) -> None:
    """Write to a temporary file next to `path`, then swap it in with a single rename.

    If the run crashes while writing, only the .tmp file is damaged; the real file still
    holds the last complete version. os.replace() is atomic, so a reader sees either the
    old file or the new one, never half of each.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    try:
        with open(tmp, "w", newline="", encoding="utf-8") as f:
            write_content(f)
            f.flush()
            os.fsync(f.fileno())        # make sure the bytes are on disk before the swap
        os.replace(tmp, path)
    except BaseException:
        tmp.unlink(missing_ok=True)     # leave no half-written temp file behind
        raise


# ==============================================================================
# TASK 1: Read the existing transactions
# Load data/transactions.csv into a dict keyed by transaction_id, with
# amount_kes converted to an exact Decimal.
# ==============================================================================

# Reads the warehouse CSV into a dictionary keyed by transaction_id.

def load_existing(path: Path = WAREHOUSE_PATH) -> dict[str, dict]:
    """Read the loaded transactions into a dict keyed by transaction_id."""
    if not path.exists():
        raise EtlError(f"Warehouse file not found: {path}. Copy the starting transactions.csv into data/ first.")

    existing = {}
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        missing = set(FIELDNAMES) - set(reader.fieldnames or [])
        if missing:
            raise EtlError(f"{path.name} is missing columns: {', '.join(sorted(missing))}")

        for row in reader:
            # the warehouse is the trusted copy: a bad row stops the run instead of being guessed at
            try:
                record = normalise(row)
            except (ValueError, TypeError) as exc:
                raise EtlError(f"{path.name} line {reader.line_num}: {exc}") from None
            if record[KEY] in existing:
                raise EtlError(f"{path.name} line {reader.line_num}: transaction_id {record[KEY]} appears twice")
            existing[record[KEY]] = record
    return existing


# ==============================================================================
# TASK 2: Load the new JSON batch
# Find the batch files in incoming/ and read each one. A missing or malformed
# file raises an error that run() logs and skips.
# ==============================================================================

# Finds the batch files waiting in incoming/, oldest first.

def discover_batches(folder: Path = BATCH_DIR) -> list[Path]:
    """Return the batch files in name order (the dates in the names sort correctly as text)."""
    if not folder.is_dir():
        log.warning(f"Batch folder not found: {folder}")
        return []
    return sorted(p for p in folder.glob(BATCH_PATTERN) if p.is_file())


# Reads one JSON batch file and returns its batch_id and transactions.

def load_batch(path: Path) -> tuple[str, list[dict]]:
    """Read one JSON batch file and return (batch_id, transactions).

    Raises OSError for a missing/unreadable file and ValueError for malformed JSON
    or a file without a batch_id and a transactions list; run() logs and skips both.
    """
    with open(path, encoding="utf-8") as f:
        payload = json.load(f, parse_float=Decimal)   # exact amounts, no float rounding

    if not isinstance(payload, dict):
        raise ValueError("top level is not a JSON object")
    batch_id = payload.get("batch_id")
    if not isinstance(batch_id, str) or not batch_id.strip():
        raise ValueError("missing or empty batch_id")
    transactions = payload.get("transactions")
    if not isinstance(transactions, list):
        raise ValueError("'transactions' is missing or not a list")
    return batch_id.strip(), transactions


# Lists every reason a batch record cannot be loaded.

def validate_record(record: dict) -> list[str]:
    """Return the reasons a batch record cannot be loaded (empty list means valid)."""
    if not isinstance(record, dict):
        return ["record is not a JSON object"]

    problems = []
    # every field must be present and not blank
    for field in FIELDNAMES:
        value = record.get(field)
        if value is None or (isinstance(value, str) and not value.strip()):
            problems.append(f"{field} is missing")

    # amount must be a JSON number (not text), positive, with at most 2 decimal places
    amount = record.get("amount_kes")
    if amount is not None:
        if isinstance(amount, bool) or not isinstance(amount, (int, float, Decimal)):
            problems.append(f"amount_kes {amount!r} is text, not a number")
        elif not Decimal(str(amount)).is_finite():
            problems.append(f"amount_kes {amount!r} is not a number")
        elif amount <= 0:
            problems.append(f"amount_kes {amount} is not positive")
        elif Decimal(str(amount)) != Decimal(str(amount)).quantize(CENT):
            problems.append(f"amount_kes {amount} has more than 2 decimal places")

    # status drives settlement, so only the known values are accepted
    status = record.get("status")
    if isinstance(status, str) and status.strip() and status.strip() not in VALID_STATUSES:
        problems.append(f"status {status!r} is not one of {', '.join(sorted(VALID_STATUSES))}")

    # updated_at decides which version wins, so it must be a real timestamp
    updated_at = record.get("updated_at")
    if updated_at not in (None, ""):
        try:
            parse_timestamp(updated_at)
        except ValueError:
            problems.append(f"updated_at {updated_at!r} is not an ISO timestamp")

    return problems


# ==============================================================================
# TASKS 3 AND 4: Compare records and classify them
# Set operations on the IDs find new and overlapping keys; only overlapping keys
# are compared field by field. Every record ends up in exactly one bucket:
# new, updated, duplicate or invalid.
# ==============================================================================

# Sorts the batch into new, updated, duplicate and invalid records.

def classify(existing: dict[str, dict], batch: list[dict]) -> dict[str, list[dict]]:
    """Split the batch into the buckets 'new', 'updated', 'duplicate' and 'invalid'.

    Every batch record lands in exactly one bucket:
      invalid   - fails validate_record()
      new       - transaction_id not in the warehouse
      updated   - fields differ from the stored row AND updated_at is newer
      duplicate - identical to the stored row, a repeat inside the batch, or a stale
                  (older) version of a row the warehouse has already moved past
    """
    buckets = {name: [] for name in BUCKETS}

    # 1. validate, and keep one version per ID when the batch repeats itself
    latest: dict[str, dict] = {}
    for index, record in enumerate(batch, start=1):
        problems = validate_record(record)
        if problems:
            txn_id = record.get(KEY) if isinstance(record, dict) else None
            buckets["invalid"].append({"index": index, KEY: txn_id, "reasons": problems, "record": record})
            continue

        record = normalise(record)
        kept = latest.get(record[KEY])
        if kept is None:
            latest[record[KEY]] = record
        elif not same_fields(record, kept) and is_newer(record, kept):
            buckets["duplicate"].append(kept)       # an older copy in the same batch is superseded
            latest[record[KEY]] = record
        else:
            buckets["duplicate"].append(record)     # same (or older) repeat inside the batch

    # 2. set operations on the keys
    batch_ids = set(latest)
    existing_ids = set(existing)
    new_ids = batch_ids - existing_ids              # never seen: insert
    overlap_ids = batch_ids & existing_ids          # already loaded: compare field by field

    buckets["new"] = [latest[txn_id] for txn_id in sorted(new_ids)]

    for txn_id in sorted(overlap_ids):
        incoming, stored = latest[txn_id], existing[txn_id]
        if same_fields(incoming, stored):
            buckets["duplicate"].append(incoming)
        elif is_newer(incoming, stored):
            buckets["updated"].append(incoming)
        else:
            # differs but is not newer: an out-of-order retry; the stored row stays
            log.info(f"Stale record {txn_id} ignored: incoming updated_at {incoming['updated_at']} "
                     f"is not newer than stored {stored['updated_at']}")
            buckets["duplicate"].append(incoming)

    return buckets


# ==============================================================================
# TASK 5: Apply an upsert-like transformation
# Insert new rows, replace updated rows, leave the rest untouched, and save the
# CSV through a temporary file so a crash cannot leave it half-written.
# ==============================================================================

# Merges new and updated records into the warehouse, leaving every other row untouched.

def upsert(existing: dict[str, dict], new: list[dict], updated: list[dict]) -> dict[str, dict]:
    """Return the merged state: inserts for new keys, replacements for updated keys.

    The input dict is not changed; every row not in new/updated is carried over untouched.
    """
    changes = {record[KEY]: record for record in [*new, *updated]}
    return {**existing, **changes}


# Writes the warehouse back to CSV, sorted by transaction_id.

def save_warehouse(state: dict[str, dict], path: Path = WAREHOUSE_PATH) -> None:
    """Write the merged state back to CSV in a stable order (sorted by transaction_id)."""
    def write_rows(f):
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES, lineterminator="\n")
        writer.writeheader()
        for txn_id in sorted(state):
            row = state[txn_id]
            writer.writerow({**row, "amount_kes": f"{row['amount_kes']:.2f}"})

    atomic_write(path, write_rows)


# ==============================================================================
# TASK 6: Maintain a processing checkpoint
# Record which batches are done. It is only written after the data is saved.
# ==============================================================================

# Creates an empty checkpoint for a first run.

def fresh_checkpoint() -> dict:
    return {"last_run_at": None, "processed_batches": [], "high_watermark": None, "row_count": 0}


# Loads the checkpoint, or starts fresh if it is missing or damaged.

def read_checkpoint(path: Path = CHECKPOINT_PATH) -> dict:
    """Return the saved checkpoint, or a fresh one if none exists yet.

    A damaged checkpoint is not fatal: starting fresh only means batches are re-read,
    and re-reading is safe because already-loaded records classify as duplicates.
    """
    if not path.exists():
        log.info("No checkpoint found - starting fresh")
        return fresh_checkpoint()
    try:
        with open(path, encoding="utf-8") as f:
            checkpoint = json.load(f)
        if not isinstance(checkpoint, dict) or not isinstance(checkpoint.get("processed_batches"), list):
            raise ValueError("unexpected checkpoint shape")
    except (OSError, ValueError) as exc:
        log.warning(f"Checkpoint {path.name} could not be read ({exc}) - starting fresh")
        return fresh_checkpoint()
    return {**fresh_checkpoint(), **checkpoint}


# Saves the checkpoint safely after the data has been written.

def write_checkpoint(checkpoint: dict, path: Path = CHECKPOINT_PATH) -> None:
    """Persist the checkpoint so an interrupted run never leaves a half-written file."""
    atomic_write(path, lambda f: f.write(json.dumps(checkpoint, indent=2) + "\n"))


# ==============================================================================
# TASKS 7 AND 8: Run the pipeline, log the results, stay idempotent
# Process every batch not yet in the checkpoint. Re-running is safe twice over:
# the checkpoint skips finished batches, and the classification rules turn
# anything already loaded into a duplicate.
# ==============================================================================

# Runs the whole pipeline: load, classify, upsert, save and checkpoint each new batch.

def run() -> dict:
    """Process every batch not yet recorded in the checkpoint and return run statistics."""
    started = time.perf_counter()
    log.info("Run started")

    existing = load_existing()
    rows_before = len(existing)
    log.info(f"Existing rows loaded: {rows_before}")

    checkpoint = read_checkpoint()
    processed = set(checkpoint["processed_batches"])
    totals = dict.fromkeys(BUCKETS, 0)
    batches_loaded = batches_skipped = 0

    for path in discover_batches():
        # a missing or malformed file is logged and skipped; it is retried on the next run
        try:
            batch_id, batch = load_batch(path)
        except (OSError, ValueError) as exc:
            log.error(f"Skipping {path.name}: {exc}")
            continue

        if batch_id in processed:
            log.info(f"Batch {batch_id} already processed - skipping")
            batches_skipped += 1
            continue

        log.info(f"Batch {batch_id}: {len(batch)} records read")
        buckets = classify(existing, batch)
        counts = {name: len(records) for name, records in buckets.items()}
        log.info(f"New: {counts['new']} | Updated: {counts['updated']} | "
                 f"Duplicates: {counts['duplicate']} | Invalid: {counts['invalid']}")
        for item in buckets["invalid"]:
            label = item[KEY] if item[KEY] else f"#{item['index']}"
            log.warning(f"Invalid record {label}: {'; '.join(item['reasons'])}")

        # 1. save the data (only if something changed) ...
        if counts["new"] or counts["updated"]:
            existing = upsert(existing, buckets["new"], buckets["updated"])
            save_warehouse(existing)

        # 2. ... and only then mark the batch as done
        checkpoint["processed_batches"].append(batch_id)
        processed.add(batch_id)
        checkpoint.update(
            last_run_at=datetime.now().isoformat(timespec="seconds"),
            high_watermark=max((row["updated_at"] for row in existing.values()), default=None),
            row_count=len(existing),
        )
        write_checkpoint(checkpoint)

        totals = {name: totals[name] + counts[name] for name in BUCKETS}
        batches_loaded += 1

    # a run with nothing new still reports its (zero) counts
    if batches_loaded == 0:
        log.info("New: 0 | Updated: 0 | Duplicates: 0 | Invalid: 0")

    log.info(f"Rows after load: {len(existing)}")
    log.info(f"Run finished in {time.perf_counter() - started:.2f}s")

    return {
        "batches_loaded": batches_loaded,
        "batches_skipped": batches_skipped,
        **totals,
        "rows_before": rows_before,
        "rows_after": len(existing),
    }


# Entry point: sets up logging, runs the pipeline and returns an exit code.

def main() -> int:
    setup_logging()
    try:
        run()
    except (EtlError, OSError) as exc:
        # nothing half-written: the warehouse and checkpoint are only ever replaced whole
        log.error(f"Run stopped: {exc}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
