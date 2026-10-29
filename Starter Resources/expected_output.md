## state/checkpoint.json (shape)

{
  "last_run_at": "<ISO timestamp>",
  "processed_batches": ["<batch_id>", "..."],
  "high_watermark": "<latest updated_at loaded>",
  "row_count": <int>
}

## logs/etl.log (one block per run)

<timestamp> INFO  Run started
<timestamp> INFO  Existing rows loaded: <n>
<timestamp> INFO  Batch <batch_id>: <n> records read
<timestamp> INFO  New: <n> | Updated: <n> | Duplicates: <n> | Invalid: <n>
<timestamp> WARNING Invalid record <id or index>: <reason>
<timestamp> INFO  Rows after load: <n>
<timestamp> INFO  Run finished in <seconds>s

## A second run with no new batches

<timestamp> INFO  Batch <batch_id> already processed - skipping
<timestamp> INFO  New: 0 | Updated: 0 | Duplicates: 0 | Invalid: 0

## data/transactions.csv
Same columns as the input file, one row per transaction_id, sorted by transaction_id.
