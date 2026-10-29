# Lipa Haraka: Incremental ETL Pipeline

This project demonstrates an incremental, idempotent loader built for Lipa Haraka, a Nairobi payments
aggregator that keeps a running table of every M-Pesa and card transaction it processes. Each morning
the payment gateway sends a JSON batch of transactions that are new or have changed since yesterday:
pending payments that have since completed or failed, corrected amounts, and (because the gateway
retries) records that are already loaded.

Last month the old loader was re-run after a timeout and every transaction in that batch was counted
twice, inflating merchant settlements by over KES 400,000. This loader can be run any number of times
and always arrives at the same correct table. It keeps a checkpoint of the batches it has finished
and logs every decision it makes.

## Core Pipeline Features

* **Keyed Warehouse:** Reads `data/transactions.csv` into a dict keyed by `transaction_id`, with
  `amount_kes` as an exact `Decimal` (no float rounding on money).
* **Safe Batch Loading:** Reads every `incoming/batch_*.json`. A missing or malformed batch file is
  logged and skipped, and is retried on the next run.
* **Set-Based Comparison:** `batch_ids - existing_ids` gives the new keys and `batch_ids & existing_ids`
  gives the keys to compare field by field. Repeats inside the batch itself are collapsed first.
* **Four-Bucket Classification:** Every incoming record lands in exactly one of `new`, `updated`,
  `duplicate` or `invalid`. An older version never overwrites a newer one.
* **Upsert with Crash-Safe Writes:** New rows are inserted, updated rows are replaced, and everything
  else is untouched. Files are written to a `.tmp` file first and then swapped in with one atomic
  `os.replace()`, so a crash can never leave a half-written CSV or checkpoint.
* **Checkpoint After Commit:** `state/checkpoint.json` records the finished batches. It is written only
  after the data file has been saved.
* **Dual Logging:** Every run writes the same lines to the console and to `logs/etl.log`: counts per
  bucket and one WARNING line per invalid record.

## Requirements

* Python 3.10 or higher.
* Standard library only (`csv`, `json`, `logging`, `decimal`, `pathlib`, ...). `requirements.txt` lists
  no packages, so there is nothing to install.

## Project Structure

```text
.
├── incremental_etl.py
├── data/transactions.csv          # the warehouse (starts as the 20 already-loaded rows)
├── incoming/batch_2026-10-21.json # the morning batch
├── state/checkpoint.json          # batches already processed
├── logs/etl.log                   # one block per run
├── requirements.txt
└── README.md
```

## Usage

From the project folder:

```bash
python incremental_etl.py
```

The script finds its folders relative to its own location, so it can be run from anywhere. It exits
with code 0 on success and 1 if the run had to stop (for example, the warehouse file is missing).

To start over from the original state, copy the starting `transactions.csv` back into `data/` and
delete `state/checkpoint.json` and `logs/etl.log`.

## Task-by-Task Walkthrough

Each task in the brief, what it asks for, and how `incremental_etl.py` does it. The script is divided
into sections with matching `TASK` headers.

### Task 1: Read the existing transactions

**Asked:** read `transactions.csv` into a dict keyed by `transaction_id`, converting `amount_kes` to a
number, with the provided CSV copied into `data/` as the starting state.

**Done in `load_existing()`:** each CSV row is cleaned by `normalise()` (spaces stripped, `amount_kes`
turned into an exact `Decimal`, `updated_at` put in one format) and stored under its
`transaction_id`. `Decimal` is used instead of `float` because it is money: `0.1 + 0.2` is not exactly
`0.3` as a float. The warehouse is the trusted copy, so a missing file, a missing column, a bad amount or
a repeated ID stops the run with a clear error (naming the line where there is one) instead of being
guessed at.

### Task 2: Load the new JSON batch

**Asked:** read a batch file from `incoming/` and return its `batch_id` and list of transactions. A
malformed or missing file must be logged and skipped, not crash the run.

**Done in `discover_batches()` and `load_batch()`:** `discover_batches()` finds every
`incoming/batch_*.json`, oldest first. `load_batch()` parses the file with `parse_float=Decimal`, so
amounts stay exact, and checks that it has a `batch_id` and a `transactions` list. Any problem raises an
error that `run()` catches: it logs `ERROR Skipping <file>: <reason>` and moves on to the next file.
A skipped file is not recorded in the checkpoint, so it is tried again on the next run.

### Task 3: Compare records using sets and dictionaries

**Asked:** use set operations on transaction IDs to find which batch keys are new and which already
exist, then compare field values only for the overlapping keys. Repeated IDs inside the batch also
need handling.

**Done in `classify()`:**

```python
new_ids     = batch_ids - existing_ids   # never seen: insert
overlap_ids = batch_ids & existing_ids   # already loaded: compare field by field
```

Only the overlapping IDs are compared, using `same_fields()` across the six `COMPARED_FIELDS`. Before
that, repeats inside the batch are collapsed into a dict holding one version per ID, so the same ID
can never be inserted twice in one run.

### Task 4: Identify new, updated and duplicate records

**Asked:** sort the batch into `new`, `updated`, `duplicate` and `invalid`, and decide where a record
that differs from the stored one but is *older* belongs.

**Done in `validate_record()` and `classify()`:** `validate_record()` lists every reason a record cannot
be loaded; any reason sends it to `invalid`. The full rules, and why the older (stale) record is a
duplicate, are in [Classification Rules](#classification-rules) below.

### Task 5: Apply an upsert-like transformation

**Asked:** insert new rows, replace updated rows, leave everything else untouched, and write the file
so a crash in the middle of writing cannot leave it half-finished.

**Done in `upsert()`, `save_warehouse()` and `atomic_write()`:** `upsert()` builds a new dict,
`{**existing, **changes}`, so every row not in the batch is carried over as it is. `save_warehouse()`
writes the rows sorted by `transaction_id` through `atomic_write()`, which writes to
`transactions.csv.tmp`, forces it to disk, and then swaps it in with `os.replace()`. That swap is a
single step: anyone reading the file sees either the old version or the new one. If the write fails
half way, the temp file is deleted and the real file is untouched.

### Task 6: Maintain a processing checkpoint

**Asked:** `read_checkpoint()` and `write_checkpoint()` using the JSON shape in the expected output. The
checkpoint records which batches are done and must only be updated after the data file has been saved.

**Done in `read_checkpoint()` and `write_checkpoint()`:** the checkpoint holds `last_run_at`,
`processed_batches`, `high_watermark` (the latest `updated_at` in the warehouse) and `row_count`. In
`run()`, the order for each batch is always: save the data, *then* add the batch to the checkpoint.
The checkpoint is also written with `atomic_write()`. A missing or damaged checkpoint is not fatal: the
loader starts fresh, which is safe for the reason explained in Task 8.

### Task 7: Log execution results

**Asked:** `setup_logging()` with Python's `logging` module, writing to both the console and
`logs/etl.log`. Each run logs its counts per bucket and one WARNING line per invalid record, in the
expected format.

**Done in `setup_logging()` and `run()`:** one logger with two handlers, one for the screen and one for
the file. The file handler appends, so `etl.log` keeps one block per run. The format
`%(asctime)s %(levelname)-5s %(message)s` pads the level to 5 characters, giving `INFO  Run started`
and `WARNING Invalid record ...` exactly as in `expected_output.md`.

### Task 8: Ensure idempotent re-runs

**Asked:** run the pipeline twice and show that the second run changes no rows and reports zero new
and updated records. Also show what happens if the checkpoint is deleted and the loader runs again: the
data must still be correct.

**Done by the design of the whole pipeline:** there are two separate protections.

1. **The checkpoint** makes the second run skip the batch entirely (`already processed - skipping`).
2. **The classification rules** make even a re-read batch harmless: everything already loaded comes back
   as a duplicate, so nothing is inserted twice and nothing is overwritten by an older copy.

So correctness never depends on the checkpoint. The terminal output of all three runs is in
[Proof of Idempotent Re-runs](#proof-of-idempotent-re-runs).

## Classification Rules

Each batch record is checked in this order:

| Bucket | Rule |
|---|---|
| **invalid** | Fails validation: a blank or missing field, `amount_kes` that is text, not positive or has more than 2 decimals, a `status` other than PENDING / COMPLETED / FAILED, or an `updated_at` that is not an ISO timestamp. Logged as a WARNING and not loaded. |
| **new** | Valid, and its `transaction_id` is not in the warehouse. Inserted. |
| **updated** | Valid, already in the warehouse, at least one field differs, **and** its `updated_at` is strictly newer than the stored one. Replaces the stored row. |
| **duplicate** | Valid, but loading it would change nothing: identical to the stored row, a repeat of an ID earlier in the same batch, or a **stale** version (differs but is not newer). |

**Why a stale record is a duplicate, not an update or invalid.** `MP-50012` arrives as COMPLETED with
`updated_at` 13:00, but the stored row says FAILED at 14:45. The incoming copy is an out-of-order retry
of a version the warehouse has already moved past, so applying it would roll the payment back in time.
It is not *invalid*, because every field is well-formed and nothing about it needs fixing at the
gateway. Like a duplicate, it carries no information the warehouse does not already have, so the stored
row stays as it is. The loader also logs an INFO line for it so it can still be traced.

**Repeats inside one batch.** One version is kept per ID: an identical or older repeat goes to
`duplicate`, and a newer repeat replaces the earlier copy, which then counts as the duplicate. Only the
version that survives is compared with the warehouse.

## Results of the First Run

| Bucket | Records |
|---|---|
| New (6) | MP-50021, MP-50022, MP-50024, MP-50025, MP-50027, MP-50028 |
| Updated (3) | MP-50004 PENDING to COMPLETED; MP-50008 PENDING to FAILED; MP-50015 PENDING to COMPLETED with the amount corrected from 960.00 to 1,060.00 |
| Duplicate (5) | MP-50002, MP-50013, MP-50019 (identical); MP-50021 (repeated in the batch); MP-50012 (stale) |
| Invalid (3) | MP-50023 (`"3,450.00"` is text); record #10 (no `transaction_id`); MP-50026 (-350.00) |

The warehouse went from 20 to 26 rows. Diffing the result against the starting file shows exactly the
3 replaced rows and the 6 inserted rows, and nothing else.

## Proof of Idempotent Re-runs

**Run 1:**

```text
2026-10-29 11:16:59 INFO  Run started
2026-10-29 11:16:59 INFO  Existing rows loaded: 20
2026-10-29 11:16:59 INFO  No checkpoint found - starting fresh
2026-10-29 11:16:59 INFO  Batch BATCH-2026-10-21-01: 17 records read
2026-10-29 11:16:59 INFO  Stale record MP-50012 ignored: incoming updated_at 2026-10-20T13:00:00 is not newer than stored 2026-10-20T14:45:00
2026-10-29 11:16:59 INFO  New: 6 | Updated: 3 | Duplicates: 5 | Invalid: 3
2026-10-29 11:16:59 WARNING Invalid record MP-50023: amount_kes '3,450.00' is text, not a number
2026-10-29 11:16:59 WARNING Invalid record #10: transaction_id is missing
2026-10-29 11:16:59 WARNING Invalid record MP-50026: amount_kes -350.00 is not positive
2026-10-29 11:16:59 INFO  Rows after load: 26
2026-10-29 11:16:59 INFO  Run finished in 0.01s
```

**Run 2 (nothing new):** the batch is in the checkpoint, so it is skipped and no rows change.

```text
2026-10-29 11:16:59 INFO  Run started
2026-10-29 11:16:59 INFO  Existing rows loaded: 26
2026-10-29 11:16:59 INFO  Batch BATCH-2026-10-21-01 already processed - skipping
2026-10-29 11:16:59 INFO  New: 0 | Updated: 0 | Duplicates: 0 | Invalid: 0
2026-10-29 11:16:59 INFO  Rows after load: 26
2026-10-29 11:16:59 INFO  Run finished in 0.01s
```

The SHA-1 checksum of `data/transactions.csv` was `ca3f5167…c96acb8e` after both runs, so the file is
byte-for-byte identical.

**Run 3, after deleting `state/checkpoint.json`:** the loader no longer knows the batch was processed,
so it reads all 17 records again. Every record that was loaded last time is now identical to its stored
row, so the data is still correct:

```text
2026-10-29 11:17:18 INFO  Run started
2026-10-29 11:17:18 INFO  Existing rows loaded: 26
2026-10-29 11:17:18 INFO  No checkpoint found - starting fresh
2026-10-29 11:17:18 INFO  Batch BATCH-2026-10-21-01: 17 records read
2026-10-29 11:17:18 INFO  Stale record MP-50012 ignored: incoming updated_at 2026-10-20T13:00:00 is not newer than stored 2026-10-20T14:45:00
2026-10-29 11:17:18 INFO  New: 0 | Updated: 0 | Duplicates: 14 | Invalid: 3
2026-10-29 11:17:18 WARNING Invalid record MP-50023: amount_kes '3,450.00' is text, not a number
2026-10-29 11:17:18 WARNING Invalid record #10: transaction_id is missing
2026-10-29 11:17:18 WARNING Invalid record MP-50026: amount_kes -350.00 is not positive
2026-10-29 11:17:18 INFO  Rows after load: 26
2026-10-29 11:17:18 INFO  Run finished in 0.01s
```

0 new and 0 updated, 26 rows, and the same checksum. The checkpoint is rebuilt at the end of the run.
This is the key design point: **correctness does not depend on the checkpoint.** The checkpoint only
saves work and records history; the classification rules alone guarantee that a batch loaded twice
gives the same table. That is also why a crash *between* saving the data and writing the checkpoint is
harmless: the next run re-reads the batch and finds nothing to change.

(Run 3 was done on a copy of the project, so the submitted `state/` and `logs/` show exactly two runs.)

## Assumptions

* **Amounts must be JSON numbers.** `"3,450.00"` is unambiguous to a person, but it breaks the gateway's
  contract. For money, the loader rejects it and logs a warning so someone can confirm the value,
  rather than guessing. Amounts must be positive with at most 2 decimal places; a negative amount
  such as a refund is not something this table models, so -350.00 is rejected.
* **`updated_at` decides which version wins**, and an update must be *strictly* newer. A record that
  differs but has the same or an older timestamp never overwrites the stored row.
* **The warehouse is trusted.** A damaged warehouse file (a missing column, a bad amount, a repeated ID)
  stops the run with an error instead of being silently repaired.
* **The checkpoint is not trusted blindly.** A missing or unreadable checkpoint means "start fresh",
  which is safe for the reason above.
* **Batches are identified by `batch_id`** inside the file, not by file name, so a renamed copy of a
  processed batch is still skipped.
* **The warehouse is only rewritten when something changed.** A run with only duplicates and invalid
  records leaves the file untouched.
* **Invalid records are not stored.** They are only reported in the log, with the reason, for
  follow-up with the gateway.

## Testing Error Handling

These were run on a copy of the project:

| Test | Result |
|---|---|
| Delete `state/checkpoint.json` | Batch re-read, 0 new / 0 updated / 14 duplicates, data unchanged (shown above) |
| Corrupt checkpoint (`{not json`) | `WARNING Checkpoint checkpoint.json could not be read (...) - starting fresh`, data unchanged |
| Malformed batch (cut-off JSON) | `ERROR Skipping batch_2026-10-22.json: Expecting value: line 2 column 1`, run continues, exit 0 |
| Batch with no `batch_id` | `ERROR Skipping batch_2026-10-23.json: missing or empty batch_id` |
| Same ID twice in one batch, second copy newer | Newer copy applied (1 updated, 1 duplicate) |
| Unknown status and bad timestamp | `WARNING Invalid record MP-50030: status 'REVERSED' is not one of COMPLETED, FAILED, PENDING; updated_at 'yesterday' is not an ISO timestamp` |
| `data/transactions.csv` missing | `ERROR Run stopped: Warehouse file not found ...`, exit code 1 |
| Simulated disk failure half way through writing the CSV | Write fails, original file's checksum unchanged, no `.tmp` file left behind |
