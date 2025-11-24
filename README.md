# DSDE Project Bulk Upload

This project provides batch-only ingestion of Scopus JSON exports into a PostgreSQL database. The current runner processes batches **sequentially** (no concurrency) to reduce lock contention and simplify failure handling while still leveraging bulk SQL operations internally.

## Key Components

- `processing/handler.py` – Implements `ScopusDBLoader` (synchronous) and retains `AsyncScopusDBLoader` (not used by the default runner) with a single public batch API `insert_papers_batch` / `insert_papers_batch_async`.
- `processing/upload_data.py` – Command-line runner that discovers JSON files under `processing/data`, groups them into batches, and uploads them sequentially.

Single-paper insertion paths were removed to simplify logic, reduce round trips, and avoid branching. Accumulate JSON dicts into batches (recommended size 25–200) before calling the loader.

## Environment Variables

- `CONN_STRING` – PostgreSQL connection string.
- `BATCH_SIZE` – Number of files per batch (default 50).
- `PREUPsert_METADATA` – Any truthy value triggers a global pre-scan/upsert of sources & affiliations to reduce per-batch contention (optional).

## Running

```bash
python processing/upload_data.py
```

Or with `uv` (if using a virtual environment manager):

```bash
uv run processing/upload_data.py
```

Progress and per-batch status are displayed via `tqdm` progress bars. A checkpoint file `.processing_upload_checkpoint` tracks processed file paths; re-running skips already ingested files.

On a batch failure the process aborts immediately; the failed batch is rolled back (previous successful batches remain committed).

## Notes

- The loader performs upserts on sources and affiliations in bulk prior to per-paper inserts to minimize SELECT/INSERT chatter.
- Errors in individual batches are logged; failed batch files remain absent from the checkpoint so they can be retried.
- Funding, keywords, authors, subject areas, and references are inserted with conflict-safe patterns to support re-runs.

## Future Enhancements

- Reintroduce optional concurrency with adaptive backoff for lock contention-sensitive tables.
- Structured logging (JSON) for batch outcomes.
- Dry-run mode that validates JSON structure without DB writes.
- Optional metrics export (Prometheus) for batch timing and row counts.
