# Brookhaven Property Data Pipeline

Pull public property data from New York State sources, normalize addresses, deduplicate records, geocode properties, and serve a searchable REST API for the Town of Brookhaven, Suffolk County, New York.

## Files

    pipeline.py          Main pipeline script
    find_sources.py      Search for working dataset URLs
    setup_sources.py     Inspect datasets and find correct filters
    debug_sources.py     Diagnose empty data issues
    requirements.txt     Python dependencies
    data/                Downloaded files and cache (created automatically)

## Prerequisites

Python 3.9+ and PostgreSQL 14+ are required.

Install PostgreSQL on macOS:

    brew install postgresql@15
    brew services start postgresql@15

Install PostgreSQL on Ubuntu:

    sudo apt update
    sudo apt install postgresql postgresql-contrib libpq-dev
    sudo systemctl start postgresql

Install PostgreSQL with Docker:

    docker run -d --name brookhaven-db \
      -e POSTGRES_DB=brookhaven \
      -e POSTGRES_USER=postgres \
      -e POSTGRES_PASSWORD=postgres \
      -p 5432:5432 postgres:15

## Installation

    python3 -m venv .venv
    source .venv/bin/activate
    pip install -r requirements.txt
    createdb brookhaven

Set the database connection:

    export DATABASE_URL="postgresql://postgres:postgres@localhost:5432/brookhaven"

To make it permanent add it to your shell profile:

    echo 'export DATABASE_URL="postgresql://postgres:postgres@localhost:5432/brookhaven"' >> ~/.zshrc
    source ~/.zshrc

## Quick Start

    python find_sources.py
    python setup_sources.py
    python pipeline.py --verify
    python pipeline.py --init
    python pipeline.py --pull
    python pipeline.py --normalize
    python pipeline.py --geocode
    python pipeline.py --serve

Or run everything at once:

    python pipeline.py --full

## Step by Step

### 1. Find Working Data Sources

Government portals change URLs frequently. Run the discovery tool first:

    python find_sources.py

This searches data.ny.gov and ArcGIS Online for property datasets, verifies
each endpoint, and prints a DATA_SOURCES config block you can paste into
pipeline.py.

### 2. Inspect and Configure Sources

Check exactly what fields and filter values each dataset uses:

    python setup_sources.py

This pulls sample rows, finds the correct county and municipality filter
field names, tests that filtering to Brookhaven returns data, counts total
records, and shows which fields map to the pipeline schema. Copy the
generated DATA_SOURCES into pipeline.py if it differs from what is there.

### 3. Verify Configured Sources

Confirm everything in pipeline.py is reachable:

    python pipeline.py --verify

If sources fail, search for replacements:

    python pipeline.py --discover
    python pipeline.py --discover --discover-query "property sales suffolk"

### 4. Initialize the Database

    python pipeline.py --init

Creates the raw_records, properties, leads, and areas tables along with
indexes. Seeds Brookhaven hamlet data. Safe to run multiple times.

### 5. Pull Data

    python pipeline.py --pull

Skips downloads if cached files are fresh. To force re-download:

    python pipeline.py --pull --force

To re-download only if files are older than a number of hours:

    python pipeline.py --pull --max-age 24

### 6. Normalize and Deduplicate

    python pipeline.py --normalize

Reads raw records, maps column names to a standard schema, assembles
addresses from split fields, normalizes street abbreviations, generates
slugs, and deduplicates.

### 7. Geocode Properties

    python pipeline.py --geocode

Uses the US Census geocoder with Nominatim as fallback. Both are
rate-limited. 500 properties takes about 5 to 10 minutes.

To geocode more at once:

    python pipeline.py --geocode --geocode-limit 2000

### 8. Start the API

    python pipeline.py --serve

Or on a different port:

    python pipeline.py --serve --port 3000

## Debugging Empty Data

If the pipeline runs but no properties appear:

    python debug_sources.py

This tests the configured dataset and shows whether it contains property
addresses or only aggregate summary data. It tests every known NYS dataset
ID and reports which ones have parcel-level records. It generates a
suggested DATA_SOURCES update if it finds better sources.

You can also check the database directly:

    psql $DATABASE_URL -c "SELECT COUNT(*) FROM raw_records"
    psql $DATABASE_URL -c "SELECT COUNT(*) FROM properties"
    psql $DATABASE_URL -c "SELECT source, COUNT(*) FROM raw_records GROUP BY source"
    psql $DATABASE_URL -c "SELECT city, COUNT(*) FROM properties GROUP BY city ORDER BY count DESC"

Common causes of empty data:

- Configured dataset is aggregate data instead of individual parcels
- Filter field names do not match the actual column names
- Case mismatch in filter values (Suffolk vs SUFFOLK)
- API timeouts causing the pull to fail

## All Pipeline Commands

    python pipeline.py --init
    python pipeline.py --pull
    python pipeline.py --pull --force
    python pipeline.py --pull --max-age 24
    python pipeline.py --normalize
    python pipeline.py --geocode
    python pipeline.py --geocode --geocode-limit 2000
    python pipeline.py --serve
    python pipeline.py --serve --port 3000
    python pipeline.py --full
    python pipeline.py --full --force
    python pipeline.py --status
    python pipeline.py --verify
    python pipeline.py --discover
    python pipeline.py --discover --discover-query "..."
    python pipeline.py --clean-cache
    python pipeline.py --clean-cache-source NAME

Commands can be combined:

    python pipeline.py --pull --normalize
    python pipeline.py --init --pull --force
    python pipeline.py --geocode --serve

## Download Caching

Downloads are tracked in data/download_manifest.json. Cached files are
reused if they are fresh. HTTP conditional requests (ETag, If-Modified-Since)
avoid re-downloading unchanged data. File checksums detect corruption.

    python pipeline.py --status
    python pipeline.py --clean-cache
    python pipeline.py --clean-cache-source nys_assessment

## API Endpoints

All under http://localhost:8080/api/ when the server is running.

Health check:

    curl http://localhost:8080/api/health

Search properties:

    curl "http://localhost:8080/api/properties/search"
    curl "http://localhost:8080/api/properties/search?q=main+street"
    curl "http://localhost:8080/api/properties/search?city=Patchogue"
    curl "http://localhost:8080/api/properties/search?zip=11772"
    curl "http://localhost:8080/api/properties/search?beds=3&baths=2"
    curl "http://localhost:8080/api/properties/search?min_price=300000&max_price=500000"
    curl "http://localhost:8080/api/properties/search?sw_lat=40.7&sw_lng=-73.1&ne_lat=40.95&ne_lng=-72.8"
    curl "http://localhost:8080/api/properties/search?city=Patchogue&beds=3&sort=price&order=desc&page=1&limit=10"

Search parameters: q (text), city, zip, beds (min), baths (min), min_price,
max_price, min_sqft, max_sqft, sw_lat, sw_lng, ne_lat, ne_lng (map bounds),
sort (price/beds/sqft/date/address), order (asc/desc), page, limit (max 100).

Single property with nearby comparables:

    curl http://localhost:8080/api/properties/123-main-st-patchogue

List areas:

    curl http://localhost:8080/api/areas

Area detail:

    curl http://localhost:8080/api/areas/patchogue

Create a lead:

    curl -X POST http://localhost:8080/api/leads \
      -H "Content-Type: application/json" \
      -d '{
        "name": "Jane Smith",
        "email": "jane@example.com",
        "phone": "631-555-0100",
        "address": "123 Main St, Patchogue, NY 11772",
        "message": "Thinking about selling",
        "lead_type": "seller_valuation"
      }'

Lead types: seller_valuation, cash_offer, consultation, general.

Pipeline stats:

    curl http://localhost:8080/api/stats

## Cron Jobs

### Simple Crontab Entry

Open crontab:

    crontab -e

Run every night at 2 AM (replace the path):

    0 2 * * * cd /path/to/project && /path/to/project/.venv/bin/python pipeline.py --pull --normalize --geocode --max-age 24 >> /path/to/project/logs/cron.log 2>&1

Run Monday and Thursday at 3 AM:

    0 3 * * 1,4 cd /path/to/project && /path/to/project/.venv/bin/python pipeline.py --pull --normalize --geocode --max-age 72 >> /path/to/project/logs/cron.log 2>&1

Force full refresh every Sunday at 1 AM:

    0 1 * * 0 cd /path/to/project && /path/to/project/.venv/bin/python pipeline.py --pull --normalize --geocode --geocode-limit 2000 --force >> /path/to/project/logs/cron.log 2>&1

Create the logs directory first:

    mkdir -p /path/to/project/logs

Verify your crontab:

    crontab -l

Check logs after a run:

    ls -t logs/pipeline_*.log | head -1 | xargs tail -20

### Wrapper Script

For locking, log rotation, and error handling, save this as
scripts/cron_pipeline.sh:

    #!/usr/bin/env bash
    set -euo pipefail

    PROJECT_DIR="/path/to/project"
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

    CMD="${VENV_PYTHON} pipeline.py --pull --normalize --geocode"
    CMD="${CMD} --geocode-limit ${GEOCODE_LIMIT} --max-age ${MAX_AGE_HOURS}"
    if [ "${FORCE_FLAG}" = "--force" ]; then
        CMD="${CMD} --force"
    fi

    log "Running: ${CMD}"

    if ${CMD} >> "${LOG_FILE}" 2>&1; then
        log "Completed successfully"
    else
        log "Failed with exit code $?"
    fi

    find "${LOG_DIR}" -name "pipeline_*.log" -mtime +${LOG_RETENTION_DAYS} -delete 2>/dev/null
    log "Done"

Make it executable and add to crontab:

    chmod +x scripts/cron_pipeline.sh

    crontab -e

    0 2 * * * /path/to/project/scripts/cron_pipeline.sh
    0 1 * * 0 /path/to/project/scripts/cron_pipeline.sh --force

### macOS launchd

Create ~/Library/LaunchAgents/com.brookhaven.pipeline.plist:

    <?xml version="1.0" encoding="UTF-8"?>
    <!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
      "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
    <plist version="1.0">
    <dict>
        <key>Label</key>
        <string>com.brookhaven.pipeline</string>
        <key>ProgramArguments</key>
        <array>
            <string>/path/to/project/scripts/cron_pipeline.sh</string>
        </array>
        <key>StartCalendarInterval</key>
        <dict>
            <key>Hour</key>
            <integer>2</integer>
            <key>Minute</key>
            <integer>0</integer>
        </dict>
        <key>StandardOutPath</key>
        <string>/path/to/project/logs/launchd_stdout.log</string>
        <key>StandardErrorPath</key>
        <string>/path/to/project/logs/launchd_stderr.log</string>
        <key>EnvironmentVariables</key>
        <dict>
            <key>DATABASE_URL</key>
            <string>postgresql://postgres:postgres@localhost:5432/brookhaven</string>
            <key>PATH</key>
            <string>/usr/local/bin:/usr/bin:/bin</string>
        </dict>
    </dict>
    </plist>

Replace /path/to/project with your actual path. Load it:

    launchctl load ~/Library/LaunchAgents/com.brookhaven.pipeline.plist

Unload:

    launchctl unload ~/Library/LaunchAgents/com.brookhaven.pipeline.plist

### Linux systemd

Create /etc/systemd/system/brookhaven-pipeline.service:

    [Unit]
    Description=Brookhaven Property Data Pipeline
    After=postgresql.service

    [Service]
    Type=oneshot
    User=youruser
    WorkingDirectory=/path/to/project
    Environment=DATABASE_URL=postgresql://postgres:postgres@localhost:5432/brookhaven
    ExecStart=/path/to/project/.venv/bin/python pipeline.py --pull --normalize --geocode --max-age 24

Create /etc/systemd/system/brookhaven-pipeline.timer:

    [Unit]
    Description=Run Brookhaven pipeline daily

    [Timer]
    OnCalendar=*-*-* 02:00:00
    Persistent=true

    [Install]
    WantedBy=timers.target

Enable:

    sudo systemctl daemon-reload
    sudo systemctl enable brookhaven-pipeline.timer
    sudo systemctl start brookhaven-pipeline.timer
    systemctl list-timers | grep brookhaven

## Troubleshooting

Cannot connect to database: Check PostgreSQL is running with pg_isready.
Verify DATABASE_URL with echo $DATABASE_URL. Test with
psql $DATABASE_URL -c "SELECT 1".

Sources timeout: data.ny.gov can be slow. Search pipeline.py for timeout=
and increase values to 60 for verification and 300 for downloads. Retry.

No records after pull: Run python debug_sources.py to check if the dataset
has property addresses or is aggregate only. Run python setup_sources.py to
find correct filter names.

Properties empty but raw_records has data: The normalizer cannot find
addresses. Run python setup_sources.py to check field name mappings.

Pipeline already running: A previous run crashed. Remove the lock file with
rm /tmp/brookhaven_pipeline.lock.

Geocoding is slow: It is rate-limited on purpose. Run with a larger limit
during off hours: python pipeline.py --geocode --geocode-limit 2000.