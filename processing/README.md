# Scopus Data Processing Pipeline

This directory contains scripts for processing Scopus JSON data into CSV files and loading them into a PostgreSQL database.

## Overview

The pipeline consists of three main steps:

1. **JSON to CSV Export** - Convert Scopus JSON files to normalized CSV tables
2. **CSV to Database Load** - Load CSV files into PostgreSQL database
3. **Direct JSON to Database** - Alternative: Load JSON directly to database (legacy)

## Files

### Core Modules

- **`csv_exporter.py`** - Exports Scopus JSON to CSV files
- **`csv_to_db_loader.py`** - Loads CSV files into PostgreSQL database
- **`handler.py`** - Legacy: Direct JSON to database loader

### Scripts

- **`upload_data.py`** - CLI for exporting JSON to CSV
- **`load_csv_to_db.py`** - CLI for loading CSV files to database
- **`upload_to_database.py`** - CLI for direct JSON to database upload (legacy)

## Workflow

### Step 1: Export JSON to CSV

Convert your Scopus JSON files to normalized CSV tables:

```bash
uv run processing/upload_data.py \
  --data-dir processing/data \
  --batch-size 100 \
  --output-dir ./csv_output
```

**Options:**

- `--data-dir`: Directory containing JSON files (default: `./processing/data`)
- `--batch-size`: Number of papers per batch (default: 100)
- `--output-dir`: Where to save CSV files (default: `./csv_output`)

**Output:** Creates 13 CSV files representing normalized database tables:

- `sources.csv` - Journal/source information
- `affiliations.csv` - Institution information
- `authors.csv` - Author information
- `subject_areas.csv` - Subject classifications
- `keywords.csv` - Author and indexed keywords
- `papers.csv` - Paper metadata
- `paper_authors.csv` - Paper-author relationships
- `paper_author_affiliations.csv` - Author-affiliation relationships
- `paper_keywords.csv` - Paper-keyword relationships
- `paper_subject_areas.csv` - Paper-subject relationships
- `reference_papers.csv` - Paper references
- `funding_agencies.csv` - Funding agency information
- `paper_funding.csv` - Paper-funding relationships

### Step 2: Load CSV to Database

Load the generated CSV files into your PostgreSQL database:

```bash
uv run processing/load_csv_to_db.py \
  --csv-dir ./csv_output
```

**Options:**

- `--csv-dir`: Directory containing CSV files (default: `./csv_output`)
- `--async`: Use async loading (optional)

**Requirements:**

- PostgreSQL connection string in `.env` file as `CONN_STRING`
- Database schema already created (tables must exist)

### Alternative: Direct JSON to Database

For direct loading without CSV intermediary:

```bash
uv run processing/upload_to_database.py \
  --data-dir processing/data \
  --batch-size 75 \
  --concurrency 8 \
  --preupsert
```

**Options:**

- `--data-dir`: Directory containing JSON files
- `--batch-size`: Number of papers per batch
- `--concurrency`: Number of concurrent workers
- `--preupsert`: Pre-upsert metadata before processing papers

## Why Use CSV Export?

The CSV export approach offers several advantages:

1. **Decoupling** - Separate data extraction from database loading
2. **Portability** - CSV files can be loaded into any database or analyzed with other tools
3. **Debugging** - Inspect extracted data before loading
4. **Flexibility** - Easy to reload, subset, or transform data
5. **Version Control** - Track data changes over time
6. **Recovery** - Resume from CSV if database load fails

## Performance Tips

### For CSV Export:

- Use larger batch sizes (100-200) for better memory efficiency
- Process runs single-threaded but is I/O optimized

### For Database Loading:

- Ensure database has sufficient resources
- Run during low-traffic periods if possible
- Consider loading dimension tables separately first
- Use `--async` flag for concurrent loading (experimental)

### For Direct Upload:

- Adjust `--concurrency` based on database connection limits
- Use `--preupsert` to bulk-load metadata first
- Monitor database connection pool usage

## Error Handling

### CSV Export:

- Continues on individual file errors
- Reports errors but processes remaining files
- Checks for malformed JSON

### Database Loading:

- Transactional (all-or-nothing per run)
- Rolls back on error
- Uses `ON CONFLICT` for idempotent loads
- Safe to re-run on same data

## Database Schema

The loader expects tables matching this schema:

```sql
-- Dimension tables
sources (source_id, scopus_source_id UNIQUE, ...)
affiliations (affiliation_id, scopus_affiliation_id UNIQUE, ...)
authors (author_id, auid UNIQUE, ...)
subject_areas (subject_area_id, subject_code UNIQUE, ...)
keywords (keyword_id, keyword UNIQUE, ...)
funding_agencies (agency_id, scopus_agency_id UNIQUE, ...)

-- Fact table
papers (paper_id, scopus_id UNIQUE, ...)

-- Relationship tables
paper_authors (paper_author_id, paper_id, author_id, UNIQUE(paper_id, author_id))
paper_author_affiliations (paper_author_id, affiliation_id, UNIQUE(...))
paper_keywords (paper_id, keyword_id, UNIQUE(paper_id, keyword_id))
paper_subject_areas (paper_id, subject_area_id, UNIQUE(...))
reference_papers (paper_id, reference_sequence, UNIQUE(...))
paper_funding (paper_id, agency_id, grant_id)
```

## Environment Setup

Create a `.env` file in the project root:

```env
CONN_STRING=postgresql://user:password@host:port/database
```

## Examples

### Full Pipeline:

```bash
# 1. Export JSON to CSV
uv run processing/upload_data.py --batch-size 100

# 2. Load CSV to database
uv run processing/load_csv_to_db.py
```

### Process Subset:

```bash
# Export only 2018 data
uv run processing/upload_data.py \
  --data-dir processing/data/2018 \
  --output-dir ./csv_2018

# Load to database
uv run processing/load_csv_to_db.py --csv-dir ./csv_2018
```

### Incremental Updates:

```bash
# Export new data
uv run processing/upload_data.py \
  --data-dir processing/data/2024 \
  --output-dir ./csv_2024

# Load incrementally (uses ON CONFLICT)
uv run processing/load_csv_to_db.py --csv-dir ./csv_2024
```

## Troubleshooting

### "CSV header mismatch" error:

- CSV file format may be corrupted
- Regenerate CSV files from JSON

### "CONN_STRING not found" error:

- Check `.env` file exists in project root
- Verify `CONN_STRING` variable is set

### "Table does not exist" error:

- Run database migration/schema creation first
- Check database connection and permissions

### Out of memory during CSV export:

- Reduce `--batch-size` parameter
- Process subdirectories separately

### Database connection timeout:

- Reduce `--concurrency` for direct upload
- Check database connection limits
- Increase database `max_connections` setting

## License

See project LICENSE file.
