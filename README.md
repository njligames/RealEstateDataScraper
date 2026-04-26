

```markdown
# Brookhaven Property Data Pipeline

A Python data pipeline that pulls public property data from official New York State sources, normalizes addresses, deduplicates records, geocodes properties, and serves a searchable REST API for the Town of Brookhaven, Suffolk County, New York.

## Files

| File | Purpose |
|------|---------|
| `pipeline.py` | Main pipeline — pulls, normalizes, geocodes, stores, and serves property data |
| `find_sources.py` | Searches NY Open Data and ArcGIS Online for working dataset URLs |
| `setup_sources.py` | Inspects datasets to find correct filter field names and values |
| `debug_sources.py` | Diagnoses empty data issues by testing each source and checking field types |
| `requirements.txt` | Python dependencies |
| `data/` | Directory for downloaded files and cache manifest (created automatically) |

## Prerequisites

**Python 3.9 or newer** and **PostgreSQL 14 or newer** are required.

Install PostgreSQL:

```
# macOS
brew install postgresql@15
brew services start postgresql@15

# Ubuntu / Debian
sudo apt update
sudo apt install postgresql postgresql-contrib libpq-dev
sudo systemctl start postgresql

# Docker
docker run -d --name brookhaven-db \
  -e POSTGRES_DB=brookhaven \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=postgres \
  -p 5432:5432 postgres:15
```

## Installation

```
mkdir brookhaven-pipeline
cd brookhaven-pipeline
```

Place all files in this directory, then:

```
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
createdb brookhaven
```

## Configuration

Set the database connection string:

```
export DATABASE_URL="postgresql://postgres:postgres@localhost:5432/brookhaven"
```

Add it to your shell profile to make it permanent:

```
echo 'export DATABASE_URL="postgresql://postgres:postgres@localhost:5432/brookhaven"' >> ~/.zshrc
source ~/.zshrc
```

## Quick Start

Run these commands in order. Each step is explained in detail below.

```
python find_sources.py
python setup_sources.py
python pipeline.py --verify
python pipeline.py --init
python pipeline.py --pull
python pipeline.py --normalize
python pipeline.py --geocode
python pipeline.py --serve
```

Or run everything at once:

```
python pipeline.py --full
```

## Step-by-Step Usage

### Step 1: Find Working Data Sources

Government data portals change URLs frequently. Start by finding what is currently available.

```
python find_sources.py
```

This script searches `data.ny.gov` with multiple property-related queries, searches ArcGIS Online for Suffolk County parcel layers, verifies each endpoint is live, tests whether Suffolk County data is present, checks known Suffolk County GIS hostnames, and prints a ready-to-paste `DATA_SOURCES` config block.

If the output shows better URLs than what is in `pipeline.py`, copy the generated `DATA_SOURCES` block and paste it into `pipeline.py`, replacing the existing one.

### Step 2: Inspect and Configure Sources

Once you know which datasets exist, inspect them to find the correct filter fields:

```
python setup_sources.py
```

This script pulls sample rows from each dataset and shows all field names and values, searches for county and municipality filter fields, tests that filtering to Suffolk County and Brookhaven actually returns data, counts total Brookhaven records, checks which fields map to the pipeline schema (address, assessed value, owner, etc.), and generates a corrected `DATA_SOURCES` config with the right filter field names and values.

Copy the generated output into `pipeline.py` if it differs from what is already there.

### Step 3: Verify Configured Sources

Confirm that all sources in `pipeline.py` are reachable:

```
python pipeline.py --verify
```

Each source will show a pass, fail, or warning status. If any sources fail, use `find_sources.py` or the discover command to find replacements:

```
python pipeline.py --discover
python pipeline.py --discover --discover-query "property sales suffolk"
python pipeline.py --discover --discover-query "assessment roll"
```

### Step 4: Initialize the Database

Create tables, indexes, and seed Brookhaven area data:

```
python pipeline.py --init
```

This creates the `raw_records` table for ingested source data stored as JSONB, the `properties` table for cleaned and normalized property records, the `leads` table for captured form submissions, the `areas` table for Brookhaven hamlets with their ZIP codes, and indexes for full-text search, geographic queries, and common filters.

This command is safe to run multiple times. It uses `CREATE TABLE IF NOT EXISTS`.

### Step 5: Pull Data

Download data from configured sources:

```
python pipeline.py --pull
```

The pull step checks whether cached files are still fresh before downloading. To force a re-download:

```
python pipeline.py --pull --force
```

To re-download only if files are older than a certain number of hours:

```
python pipeline.py --pull --max-age 24
```

### Step 6: Normalize and Deduplicate

Transform raw records into clean property data:

```
python pipeline.py --normalize
```

This step reads all raw records from the database, maps varied column names to a standard schema, assembles addresses from split fields (such as `parcel_address_number` plus `parcel_address_street` plus `parcel_address_suff`), normalizes street abbreviations (Street becomes ST, Avenue becomes AVE), generates URL slugs for each property, deduplicates by slug, and merges data from multiple sources using COALESCE to fill gaps.

### Step 7: Geocode Properties

Add latitude and longitude coordinates to properties that are missing them:

```
python pipeline.py --geocode
```

To geocode more properties in one run:

```
python pipeline.py --geocode --geocode-limit 2000
```

Geocoding uses the US Census Bureau geocoder first (free, no API key needed) with Nominatim as a fallback. Both are rate-limited. Geocoding 500 properties takes roughly 5 to 10 minutes.

### Step 8: Start the API Server

```
python pipeline.py --serve
```

To use a different port:

```
python pipeline.py --serve --port 3000
```

The API will be available at `http://localhost:8080/api/`.

## Debugging Empty Data

If the pipeline runs but no properties appear, use the debug script:

```
python debug_sources.py
```

This script tests the currently configured dataset and shows all fields and sample values, tests every known NYS dataset ID to find which ones have property-level address data versus aggregate summary data, searches the catalog for datasets with individual parcel records, shows which datasets are live and which are dead, and generates a suggested `DATA_SOURCES` update if it finds better sources.

Common causes of empty data include the configured dataset being aggregate municipal data instead of individual parcels, filter field names not matching the actual column names in the dataset, case sensitivity in filter values (Suffolk versus SUFFOLK), and API timeouts causing the pull to fail silently.

You can also check the database directly:

```
psql $DATABASE_URL -c "SELECT COUNT(*) FROM raw_records"
psql $DATABASE_URL -c "SELECT COUNT(*) FROM properties"
psql $DATABASE_URL -c "SELECT source, COUNT(*) FROM raw_records GROUP BY source"
psql $DATABASE_URL -c "SELECT city, COUNT(*) FROM properties GROUP BY city ORDER BY count DESC LIMIT 10"
```

## Pipeline Command Reference

```
python pipeline.py --init                    Create tables and seed areas
python pipeline.py --pull                    Pull data, skip if cached
python pipeline.py --pull --force            Force re-download all sources
python pipeline.py --pull --max-age 24       Re-download if older than 24 hours
python pipeline.py --normalize               Normalize raw records into properties
python pipeline.py --geocode                 Geocode up to 500 properties
python pipeline.py --geocode --geocode-limit 2000   Geocode up to 2000 properties
python pipeline.py --serve                   Start the API on port 8080
python pipeline.py --serve --port 3000       Start the API on port 3000
python pipeline.py --full                    Run init, pull, normalize, geocode, serve
python pipeline.py --full --force            Full pipeline with forced re-download
python pipeline.py --status                  Show download cache status
python pipeline.py --verify                  Check all source URLs are working
python pipeline.py --discover                Search NY Open Data for datasets
python pipeline.py --discover --discover-query "..."   Search with custom terms
python pipeline.py --clean-cache             Delete all cached downloads
python pipeline.py --clean-cache-source NAME Delete cache for one source
```

Commands can be combined:

```
python pipeline.py --pull --normalize
python pipeline.py --init --pull --force
python pipeline.py --geocode --serve
```

## Download Caching

The pipeline tracks downloads in `data/download_manifest.json` to avoid unnecessary re-downloads. Before downloading, it checks if a cached file exists and is still fresh. For HTTP sources, it sends conditional requests using ETag and If-Modified-Since headers. If the server returns 304 Not Modified, the cached file is reused. File checksums detect corruption or external modification. Ingestion is tracked separately so a file is not re-imported unless it changed.

To check what is cached:

```
python pipeline.py --status
```

To clear the cache:

```
python pipeline.py --clean-cache
python pipeline.py --clean-cache-source nys_assessment
```

## API Endpoints

All endpoints are under `http://localhost:8080/api/` when the server is running.

Health check:

```
curl http://localhost:8080/api/health
```

Search properties:

```
curl "http://localhost:8080/api/properties/search"
curl "http://localhost:8080/api/properties/search?q=main+street"
curl "http://localhost:8080/api/properties/search?city=Patchogue"
curl "http://localhost:8080/api/properties/search?zip=11772"
curl "http://localhost:8080/api/properties/search?beds=3&baths=2"
curl "http://localhost:8080/api/properties/search?min_price=300000&max_price=500000"
curl "http://localhost:8080/api/properties/search?sw_lat=40.7&sw_lng=-73.1&ne_lat=40.95&ne_lng=-72.8"
curl "http://localhost:8080/api/properties/search?city=Patchogue&beds=3&sort=price&order=desc&page=1&limit=10"
```

Search parameters: `q` (text search), `city`, `zip`, `beds` (minimum), `baths` (minimum), `min_price`, `max_price`, `min_sqft`, `max_sqft`, `sw_lat`, `sw_lng`, `ne_lat`, `ne_lng` (map bounding box), `sort` (price, beds, sqft, date, address), `order` (asc, desc), `page`, `limit` (max 100).

Get a single property with nearby comparables:

```
curl http://localhost:8080/api/properties/123-main-st-patchogue
```

List all Brookhaven areas with stats:

```
curl http://localhost:8080/api/areas
```

Get area detail with recent sales:

```
curl http://localhost:8080/api/areas/patchogue
```

Create a lead:

```
curl -X POST http://localhost:8080/api/leads \
  -H "Content-Type: application/json" \
  -d '{"name":"Jane Smith","email":"jane@example.com","phone":"631-555-0100","address":"123 Main St, Patchogue, NY 11772","message":"Thinking about selling","lead_type":"seller_valuation"}'
```

Lead types: `seller_valuation`, `cash_offer`, `consultation`, `general`.

Pipeline stats:

```
curl http://localhost:8080/api/stats
```

## Cron Jobs

To keep data fresh, schedule the pipeline to run automatically.

### Basic Cron Setup

Open your crontab:

```
crontab -e
```

Add one of these lines. Replace `/path/to/brookhaven-pipeline` with your actual directory.

Run every night at 2 AM:

```
0 2 * * * cd /path/to/brookhaven-pipeline && /path/to/brookhaven-pipeline/.venv/bin/python pipeline.py --pull --normalize --geocode --max-age 24 >> /path/to/brookhaven-pipeline/logs/cron.log 2>&1
```

Run Monday and Thursday at 3 AM:

```
0 3 * * 1,4 cd /path/to/brookhaven-pipeline && /path/to/brookhaven-pipeline/.venv/bin/python pipeline.py --pull --normalize --geocode --max-age 72 >> /path/to/brookhaven-pipeline/logs/cron.log 2>&1
```

Run every Sunday at 1 AM with forced re-download:

```
0 1 * * 0 cd /path/to/brookhaven-pipeline && /path/to/brookhaven-pipeline/.venv/bin/python pipeline.py --pull --normalize --geocode --geocode-limit 2000 --force >> /path/to/brookhaven-pipeline/logs/cron.log 2>&1
```

Make sure the logs directory exists:

```
mkdir -p /path/to/brookhaven-pipeline/logs
```

### Cron Wrapper Script

For better logging, locking, and error handling, create a wrapper script. Save this as `scripts/cron_pipeline.sh`:

```
#!/usr/bin/env bash
set -euo pipefail

# Edit these
PROJECT_DIR="/path/to/brookhaven-pipeline"
VENV_PYTHON="${PROJECT_DIR}/.venv/bin/python"
LOG_DIR="${PROJECT_DIR}/logs"
LOCK_FILE="/tmp/brookhaven_pipeline.lock"
MAX_AGE_HOURS=24
GEOCODE_LIMIT=500
LOG_RETENTION_DAYS=30

FORCE_FLAG="${1:-}"

mkdir -p "${LOG_DIR}"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="${LOG_DIR}/pipeline_${TIMESTAMP}.log"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "${LOG_FILE}"
}

# Prevent overlapping runs
if [ -f "${LOCK_FILE}" ]; then
    LOCK_PID=$(cat "${LOCK_FILE}" 2>/dev/null || echo "")
    if [ -n "${LOCK_PID}" ] && kill -0 "${LOCK_PID}" 2>/dev/null; then
        log "Pipeline already running (PID ${LOCK_PID}). Exiting."
        exit 0
    else
        log "Removing stale lock file."
        rm -f "${LOCK_FILE}"
    fi
fi

echo $$ > "${LOCK_FILE}"
trap 'rm -f "${LOCK_FILE}"' EXIT

log "Starting pipeline"
cd "${PROJECT_DIR}"

CMD="${VENV_PYTHON} pipeline.py --pull --normalize --geocode --geocode-limit ${GEOCODE_LIMIT} --max-age ${MAX_AGE_HOURS}"
if [ "${FORCE_FLAG}" = "--force" ]; then
    CMD="${CMD} --force"
fi

log "Running: ${CMD}"

if ${CMD} >> "${LOG_FILE}" 2>&1; then
    log "Pipeline completed successfully"
else
    log "Pipeline failed with exit code $?"
fi

# Delete logs older than retention period
find "${LOG_DIR}" -name "pipeline_*.log" -mtime +${LOG_RETENTION_DAYS} -delete 2>/dev/null

log "Done"
```

Make it executable and add to crontab:

```
chmod +x scripts/cron_pipeline.sh
crontab -e
```

```
0 2 * * * /path/to/brookhaven-pipeline/scripts/cron_pipeline.sh
0 1 * * 0 /path/to/brookhaven-pipeline/scripts/cron_pipeline.sh --force
```

### Verify Cron

```
crontab -l
```

Check logs after it runs:

```
ls -t logs/pipeline_*.log | head -1 | xargs tail -20
```

### macOS launchd

macOS can also use launchd instead of cron. Create the file `~/Library/LaunchAgents/com.brookhaven.pipeline.plist`:

```
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.brookhaven.pipeline</string>
    <key>ProgramArguments</key>
    <array>
        <string>/path/to/brookhaven-pipeline/scripts/cron_pipeline.sh</string>
    </array>
    <key>StartCalendarInterval</key>
    <dict>
        <key>Hour</key>
        <integer>2</integer>
        <key>Minute</key>
        <integer>0</integer>
    </dict>
    <key>StandardOutPath</key>
    <string>/path/to/brookhaven-pipeline/logs/launchd_stdout.log</string>
    <key>StandardErrorPath</key>
    <string>/path/to/brookhaven-pipeline/logs/launchd_stderr.log</string>
    <key>EnvironmentVariables</key>
    <dict>
        <key>DATABASE_URL</key>
        <string>postgresql://postgres:postgres@localhost:5432/brookhaven</string>
        <key>PATH</key>
        <string>/usr/local/bin:/usr/bin:/bin</string>
    </dict>
</dict>
</plist>
```

Replace `/path/to/brookhaven-pipeline` with your actual path, then load it:

```
launchctl load ~/Library/LaunchAgents/com.brookhaven.pipeline.plist
```

To unload:

```
launchctl unload ~/Library/LaunchAgents/com.brookhaven.pipeline.plist
```

### Linux systemd Timer

Create `/etc/systemd/system/brookhaven-pipeline.service`:

```
[Unit]
Description=Brookhaven Property Data Pipeline
After=postgresql.service

[Service]
Type=oneshot
User=youruser
WorkingDirectory=/path/to/brookhaven-pipeline
Environment=DATABASE_URL=postgresql://postgres:postgres@localhost:5432/brookhaven
ExecStart=/path/to/brookhaven-pipeline/.venv/bin/python pipeline.py --pull --normalize --geocode --max-age 24
```

Create `/etc/systemd/system/brookhaven-pipeline.timer`:

```
[Unit]
Description=Run Brookhaven pipeline daily

[Timer]
OnCalendar=*-*-* 02:00:00
Persistent=true

[Install]
WantedBy=timers.target
```

Enable it:

```
sudo systemctl daemon-reload
sudo systemctl enable brookhaven-pipeline.timer
sudo systemctl start brookhaven-pipeline.timer
systemctl list-timers | grep brookhaven
```

## Troubleshooting

**Cannot connect to database.** Check that PostgreSQL is running with `pg_isready` and that `DATABASE_URL` is set correctly with `echo $DATABASE_URL`. Test the connection with `psql $DATABASE_URL -c "SELECT 1"`.

**All sources failed or timed out.** The `data.ny.gov` API can be slow. Increase timeouts in `pipeline.py` by searching for `timeout=` and setting verification checks to 60 and data downloads to 300. Then retry.

**No records after pulling.** Run `python debug_sources.py` to check whether the configured dataset has property-level data or is aggregate summary data. Run `python setup_sources.py` to find the correct filter field names.

**Properties table is empty but raw_records has data.** The normalizer could not extract addresses. Check that the field names in `PropertyNormalizer.FIELD_MAPPINGS` match the actual column names in the source data. Run `python setup_sources.py` to see the mapping.

**Pipeline already running error.** A previous run crashed and left a lock file. Remove it with `rm /tmp/brookhaven_pipeline.lock`.

**Geocoding is slow.** It is intentionally rate-limited. Run it with a larger limit during off-hours: `python pipeline.py --geocode --geocode-limit 2000`.
```