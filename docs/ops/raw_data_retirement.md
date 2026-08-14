# raw_data retirement: complaints_raw + violations_raw

Decision record and runbook, written 2026-08-14 after the storage deep-dive.

## The problem, measured

| Table | Heap | raw_data share of each row | Read by anything? |
|---|---|---|---|
| complaints_raw | 8,108 MB | 1,336 of 1,533 B (87%) | no |
| violations_raw | 3,498 MB | 1,224 of 1,555 B (79%) | no |
| permits_raw | 1,601 MB | in use | YES: `raw_data->>'job_type'` powers Flip Watch |

The payloads sit inline in the heap, not in TOAST, because the tuples are
under the ~2KB compile-time TOAST threshold. `toast_tuple_target = 128` was
tested on a 100k-row probe table and changed nothing: the parameter only
controls how far TOAST shrinks a tuple once the threshold triggers it, and
these rows never trigger it. There is no knob that moves sub-threshold
payloads out of line. The options are drop, or move to a side table.

Fourteen request-time query sites across seven API modules plus the nightly
scoring run scan these two tables. Every scan reads ~8x the bytes the promoted
columns need, which is why the buffer cache hit ratio sits near 59% no matter
what shared_buffers is set to: the working set is inflated past what the box
can cache. A year-long complaints aggregate reads 832k blocks (6.5GB) from
disk every time.

## Why drop is safe here

- A 5,000-row key inventory shows raw_data holds almost nothing that is not
  already a promoted column. The exceptions (resolution_description,
  council_district, coordinates) power no feature.
- Both datasets are permanently re-fetchable from Socrata by stable key:
  311 by `unique_key`, HPD violations by `violation_id`. This is a cache of
  an upstream row, not evidence. ACRIS deed payloads (ownership_raw, 104MB)
  are kept: those ARE provenance and the table is small.
- Belt and braces anyway: the archive phase writes the payloads to zstd
  ndjson (measured 10.6:1, ~640MB for complaints) locally and to R2 before
  the column is touched.

## Expected result

Database 16GB -> ~7GB. The two hottest tables fit in cache. Nightly scoring
and every complaints/violations query stop paying the 8x read tax.

## Sequence

1. Any time before the window:
   `sudo scripts/retire_raw_data.sh archive`
2. At the window (a few minutes of lock on the two tables; run after the
   nightly pipeline, e.g. 03:30-06:00 UTC, or any quiet hour):
   a. Deploy the prepared code commit (models + scrapers stop declaring and
      writing raw_data for these two tables; tests updated).
   b. `systemctl reload pulsecities`
   c. `sudo scripts/retire_raw_data.sh drop`
      The script refuses to run if the archives are missing or the models
      still declare the column.
3. Nothing else changes: ingest keeps working (the INSERT simply stops
   including the column), scoring queries are unchanged.

During the VACUUM FULL each table is locked in turn; API queries touching it
queue. nginx serves cached SSR pages meanwhile. Expect 5-15 minutes total.

## Restore (if a future feature wants the payloads)

```sql
CREATE TABLE complaints_payload (key text PRIMARY KEY, raw jsonb);
```
```bash
zstd -dcq complaints_raw_raw_data_YYYYMMDD.ndjson.zst |
  sudo -u postgres psql -d pulsecities \
    -c "COPY complaints_payload (raw) FROM STDIN" # then split key out of raw
```
Or simply re-fetch the rows from Socrata by key; both datasets are still
published.
