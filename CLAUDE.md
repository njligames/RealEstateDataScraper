# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

A single-file Python pipeline that pulls public property assessment data for the Town of Brookhaven (Suffolk County, NY) from NY State's open data portal, normalizes and deduplicates records, geocodes addresses, stores everything in PostgreSQL, and exposes a search REST API. A companion lead-capture table supports a real estate lead-generation use case.

## Environment Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
createdb brookhaven
export DATABASE_URL="postgresql://postgres:postgres@localhost:5432/brookhaven"
```

PostgreSQL 14+ must be running locally (or via Docker — see README.md).

## Pipeline Commands

```bash
# One-time setup
python pipeline.py --init

# Run stages individually
python pipeline.py --pull                      # download (cached by default)
python pipeline.py --pull --force              # force re-download
python pipeline.py --pull --max-age 24         # re-download if older than 24h
python pipeline.py --normalize
python pipeline.py --geocode
python pipeline.py --geocode --geocode-limit 2000
python pipeline.py --serve
python pipeline.py --serve --port 3000

# Run everything
python pipeline.py --full
python pipeline.py --full --force

# Diagnostics
python pipeline.py --status                    # show download cache state
python pipeline.py --verify                    # verify configured source URLs
python pipeline.py --discover                  # search NY Open Data for datasets
python pipeline.py --clean-cache
python pipeline.py --clean-cache-source nys_assessment

# If --pull succeeds but properties table stays empty
python debug_sources.py

# If DATA_SOURCES URLs stop working
python find_sources.py        # find new working dataset URLs
python setup_sources.py       # inspect field names and filter values for a dataset
```

## Architecture

All business logic lives in `pipeline.py` (~3300 lines). The three helper scripts (`find_sources.py`, `setup_sources.py`, `debug_sources.py`) are standalone diagnostic tools — they do not share code with the pipeline.

**Data flow:**

```
DATA_SOURCES config
       ↓
DataPuller → raw_records table (JSONB, deduplicated by checksum)
       ↓
PropertyNormalizer → properties table (structured columns)
       ↓
PropertyGeocoder → updates lat/lon on properties rows
       ↓
create_api() → Flask server on :8080
```

**Key classes in pipeline.py:**

- `DownloadManifest` — persists download metadata to `data/download_manifest.json`. Tracks ETags, checksums, and timestamps so HTTP requests use conditional GETs and redundant downloads are skipped.
- `Database` — thin psycopg2 wrapper. All SQL is inline here. `init_tables()` is idempotent. `upsert_property()` uses `ON CONFLICT (slug) DO UPDATE` with `COALESCE` so partial data from new pulls never overwrites existing populated fields.
- `DataSourceDiscovery` — searches Socrata and ArcGIS portals for datasets matching keyword queries. Used by `--discover` and as a fallback inside `DataPuller`.
- `DataPuller` — fetches from Socrata JSON API (paginated), ArcGIS REST, direct CSV/Excel URLs, or local files. Source type is determined by `DATA_SOURCES[name]["type"]`.
- `PropertyNormalizer` — maps raw JSONB columns to the standard schema via `FIELD_MAPPINGS` (a dict of canonical field → list of possible source column names). Assembles split address fields (the NYS dataset `7vem-aaz7` stores house number, street name, and suffix separately). Normalizes street abbreviations. Generates a URL slug for deduplication.
- `PropertyGeocoder` — tries US Census geocoder first, falls back to Nominatim. Both are rate-limited; 500 records takes ~5–10 min.
- `create_api()` — returns a Flask app. Supports full-text + filter search (`/api/properties/search`), single-property lookup with nearby comparables, area listings, lead capture (`POST /api/leads`), and pipeline stats.

**Database schema (4 tables):**

- `raw_records` — append-only store of everything downloaded, keyed on `(source, checksum)`. `raw_data` is JSONB.
- `properties` — normalized/deduplicated records. Primary key for deduplication is the address slug.
- `leads` — form submissions from the API.
- `areas` — Brookhaven hamlet names, slugs, and ZIP codes. Seeded by `--init` from the `BROOKHAVEN_AREAS` dict.

## Adding a New Data Source

1. Add an entry to `DATA_SOURCES` in `pipeline.py`. Supported types: `socrata_api`, `arcgis_rest`, `csv_download`, `excel_download`, `local_csv`.
2. Run `python setup_sources.py` to find the correct filter field names and verify the dataset has parcel-level records.
3. Add any new column names to `FIELD_MAPPINGS` inside `PropertyNormalizer` if they don't already match an existing alias.
4. Re-run `--normalize` (no need to re-pull if the file is already in `data/raw/`).

## FOIL Data Sources (pre-configured, awaiting files)

Two additional `local_csv` sources are already wired into `DATA_SOURCES` and ready to activate once FOIL files arrive:

**Suffolk County deed records** (`brookhaven_deeds`): Copy the file to `data/raw/brookhaven_deeds.csv`. Fields mapped: sale price/date, grantor, grantee, deed type. FOIL target: Suffolk County Clerk.

**Town of Brookhaven assessor detail** (`brookhaven_assessor`): Copy the file to `data/raw/brookhaven_assessor.csv`. Fields mapped: year built, sqft, beds/baths (full + half), stories, lot size, heating type, garage type, condition, land value. FOIL target: Brookhaven Assessor's Office, (631) 451-6302.

Once either file is in place, just run:
```bash
python pipeline.py --pull --normalize
```
The pipeline skips missing files with a warning, so having only one file is fine. New schema columns (`land_value`, `half_baths`, `stories`, `heating_type`, `garage_type`, `condition`, `grantor`, `grantee`, `deed_type`) are added automatically when `--init` is run against an existing database via `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`.

## MLS / IDX Source (RESO Web API)

A `mls_reso` source is wired into `DATA_SOURCES` with type `reso_api`. It is skipped silently unless `MLS_API_URL` is set. Once you have credentials from OneKey MLS (or an IDX vendor), set these env vars:

```bash
# OAuth 2.0 (preferred)
export MLS_API_URL="https://api.onekeymls.com/reso/odata"
export MLS_TOKEN_URL="https://api.onekeymls.com/oauth2/token"
export MLS_CLIENT_ID="your-client-id"
export MLS_CLIENT_SECRET="your-client-secret"

# — or — static bearer token
export MLS_ACCESS_TOKEN="your-token"
```

Then run `python pipeline.py --pull --normalize`. The puller uses OData `$filter` + `$skip`/`$top` pagination. Subsequent runs are incremental — only records with a `ModificationTimestamp` newer than the last pull are fetched, then merged into the local JSON cache by `ListingKey`.

New DB columns written by this source: `mls_number`, `listing_status`, `list_price`, `close_price`, `close_date`, `days_on_market`, `listing_date`, `listing_remarks`, `list_agent_name`, `list_office_name`.

## Known Issue

There are two `DataPuller` class definitions in `pipeline.py` (lines ~706 and ~1412). Python uses the second one. The first definition is dead code and should be removed.

## Data Directory

`data/raw/` — downloaded source files (gitignored)  
`data/clean/` — intermediate clean exports (gitignored)  
`data/download_manifest.json` — cache metadata (gitignored)
