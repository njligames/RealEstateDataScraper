

```markdown
# Brookhaven Property Data Pipeline

A Python data pipeline that pulls public property data from official New York State and Suffolk County sources, normalizes addresses, deduplicates records, geocodes properties, and serves a searchable REST API — focused on the Town of Brookhaven, Suffolk County, New York.

## Table of Contents

- [Overview](#overview)
- [What It Does](#what-it-does)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
  - [1. Find Working Data Sources](#1-find-working-data-sources)
  - [2. Verify Configured Sources](#2-verify-configured-sources)
  - [3. Initialize the Database](#3-initialize-the-database)
  - [4. Pull Data](#4-pull-data)
  - [5. Normalize and Deduplicate](#5-normalize-and-deduplicate)
  - [6. Geocode Properties](#6-geocode-properties)
  - [7. Start the API Server](#7-start-the-api-server)
  - [8. Run the Full Pipeline](#8-run-the-full-pipeline)
- [Pipeline Commands Reference](#pipeline-commands-reference)
- [Download Caching](#download-caching)
- [API Endpoints](#api-endpoints)
- [Cron Jobs & Automation](#cron-jobs--automation)
  - [Basic Cron Setup](#basic-cron-setup)
  - [Cron Wrapper Script](#cron-wrapper-script)
  - [Common Cron Schedules](#common-cron-schedules)
  - [macOS launchd Setup](#macos-launchd-setup)
  - [Systemd Timer (Linux)](#systemd-timer-linux)
  - [Monitoring Cron Jobs](#monitoring-cron-jobs)
- [Testing](#testing)
- [Troubleshooting](#troubleshooting)
- [Legal & Data Notes](#legal--data-notes)

---

## Overview

This project provides three files:

| File | Purpose |
|------|---------|
| `pipeline.py` | Main data pipeline — pulls, normalizes, geocodes, stores, and serves property data |
| `find_sources.py` | Discovery tool — searches NY Open Data and ArcGIS Online for working dataset URLs |
| `requirements.txt` | Python dependencies |

The pipeline covers all Brookhaven hamlets including Patchogue, Medford, Shirley, Mastic, Centereach, Selden, Farmingville, Coram, Port Jefferson, Stony Brook, Setauket, Bellport, and more.

---

## What It Does

```
┌──────────────────────────┐
│  Official Public Sources  │
│  • data.ny.gov (Socrata) │
│  • ArcGIS Online         │
│  • Local CSV files       │
└────────────┬─────────────┘
             │ pull
             ▼
┌──────────────────────────┐
│  Raw Records (JSONB)     │
│  Stored with checksums   │
│  for deduplication       │
└────────────┬─────────────┘
             │ normalize
             ▼
┌──────────────────────────┐
│  Cleaned Properties      │
│  • Normalized addresses  │
│  • Deduped by slug       │
│  • Standardized fields   │
└────────────┬─────────────┘
             │ geocode
             ▼
┌──────────────────────────┐
│  Geocoded Properties     │
│  • US Census geocoder    │
│  • Nominatim fallback    │
│  • Lat/lon coordinates   │
└────────────┬─────────────┘
             │ serve
             ▼
┌──────────────────────────┐
│  REST API (Flask)        │
│  • Property search       │
│  • Area stats            │
│  • Lead capture          │
│  • Map bounds queries    │
└──────────────────────────┘
```

---

## Prerequisites

- **Python 3.9+**
- **PostgreSQL 14+**

### Install PostgreSQL

**macOS:**
```bash
brew install postgresql@15
brew services start postgresql@15
```

**Ubuntu/Debian:**
```bash
sudo apt update
sudo apt install postgresql postgresql-contrib libpq-dev
sudo systemctl start postgresql
```

**Docker:**
```bash
docker run -d \
  --name brookhaven-db \
  -e POSTGRES_DB=brookhaven \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=postgres \
  -p 5432:5432 \
  postgres:15
```

---

## Installation

```bash
# Clone or copy files into a directory
mkdir brookhaven-pipeline
cd brookhaven-pipeline
# Place pipeline.py, find_sources.py, and requirements.txt here

# Create virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Create the database
createdb brookhaven
```

---

## Configuration

Set the `DATABASE_URL` environment variable:

```bash
export DATABASE_URL="postgresql://postgres:postgres@localhost:5432/brookhaven"
```

Or for a custom setup:

```bash
export DATABASE_URL="postgresql://USER:PASSWORD@HOST:PORT/DBNAME"
```

You can add this to your shell profile (`~/.bashrc`, `~/.zshrc`) to make it permanent:

```bash
echo 'export DATABASE_URL="postgresql://postgres:postgres@localhost:5432/brookhaven"' >> ~/.zshrc
source ~/.zshrc
```

### Optional Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | `postgresql://postgres:postgres@localhost:5432/brookhaven` | PostgreSQL connection string |
| `GEOCODER_API_KEY` | *(empty)* | Optional Google Maps API key for geocoding |

---

## Usage

### 1. Find Working Data Sources

Government data portals frequently change URLs and dataset IDs. Run the discovery tool first to find what's currently available:

```bash
python find_sources.py
```

This will:
- Search `data.ny.gov` for property-related datasets
- Search ArcGIS Online for Suffolk County parcel layers
- Verify each endpoint is live
- Test whether Suffolk County data is available
- Print a ready-to-paste `DATA_SOURCES` config

If the discovery tool finds better URLs than what's configured in `pipeline.py`, update the `DATA_SOURCES` dictionary at the top of `pipeline.py` with the new URLs.

### 2. Verify Configured Sources

Check that all currently configured data sources are working:

```bash
python pipeline.py --verify
```

Example output:
```
══════════════════════════════════════════════════
  Verifying Data Source Endpoints
══════════════════════════════════════════════════

  ┌─ nys_assessment (socrata_api)
  │  Primary: https://data.ny.gov/resource/iq85-sdzs.json
  │  Status:  ✅ OK
  │  Fields:  county, municipality, swis_code, ...
  └────────────────────────────────────

  ┌─ suffolk_parcels_hub (arcgis_api)
  │  Status:  ❌ FAILED
  │  Trying fallbacks...
  └────────────────────────────────────
```

If sources are broken, use `--discover` to search for replacements:

```bash
python pipeline.py --discover
python pipeline.py --discover --discover-query "property sales suffolk"
python pipeline.py --discover --discover-query "assessment roll"
```

### 3. Initialize the Database

Create tables, indexes, and seed Brookhaven area data:

```bash
python pipeline.py --init
```

This creates:
- `raw_records` — raw ingested data stored as JSONB
- `properties` — cleaned, normalized property records
- `leads` — captured lead form submissions
- `areas` — Brookhaven hamlets with ZIP codes
- Full-text search, geographic, and filter indexes

You only need to run `--init` once (it uses `CREATE TABLE IF NOT EXISTS` so it's safe to re-run).

### 4. Pull Data

Download data from all configured sources:

```bash
# Normal pull (skips downloads if cached files are fresh)
python pipeline.py --pull

# Force re-download everything
python pipeline.py --pull --force

# Re-download only if files are older than 24 hours
python pipeline.py --pull --max-age 24
```

### 5. Normalize and Deduplicate

Transform raw records into clean, structured property data:

```bash
python pipeline.py --normalize
```

This step:
- Reads all raw records from the database
- Maps varied column names to a standard schema (handles differences between sources)
- Normalizes addresses (e.g., `Street` → `ST`, `Avenue` → `AVE`)
- Generates URL slugs for each property
- Deduplicates by slug using `INSERT ... ON CONFLICT DO UPDATE`
- Merges data from multiple sources (fills in gaps with `COALESCE`)

### 6. Geocode Properties

Add latitude/longitude coordinates to properties that are missing them:

```bash
# Geocode up to 500 properties (default)
python pipeline.py --geocode

# Geocode up to 2000 properties
python pipeline.py --geocode --geocode-limit 2000
```

Geocoding uses two services:
1. **US Census Bureau Geocoder** (free, no API key needed) — tried first
2. **Nominatim / OpenStreetMap** (free, rate-limited) — fallback

Both are rate-limited to avoid being blocked. Geocoding 500 properties takes approximately 5–10 minutes.

### 7. Start the API Server

```bash
# Start on default port 8080
python pipeline.py --serve

# Start on a custom port
python pipeline.py --serve --port 3000
```

The API will be available at `http://localhost:8080/api/`.

### 8. Run the Full Pipeline

Run all steps in sequence (init → pull → normalize → geocode → serve):

```bash
python pipeline.py --full

# With forced re-download
python pipeline.py --full --force
```

---

## Pipeline Commands Reference

| Command | Description |
|---------|-------------|
| `--init` | Initialize database tables and seed areas |
| `--pull` | Pull data from all sources (respects cache) |
| `--pull --force` | Force re-download all sources |
| `--pull --max-age 24` | Re-download if older than N hours |
| `--normalize` | Normalize raw records into properties |
| `--geocode` | Geocode properties missing coordinates |
| `--geocode --geocode-limit N` | Geocode up to N properties |
| `--serve` | Start the REST API server |
| `--serve --port 3000` | Start API on a specific port |
| `--full` | Run init + pull + normalize + geocode + serve |
| `--full --force` | Full pipeline with forced downloads |
| `--status` | Show download cache status |
| `--verify` | Verify all data source URLs are working |
| `--discover` | Search NY Open Data for datasets |
| `--discover --discover-query "..."` | Search with custom terms |
| `--clean-cache` | Remove all cached downloads |
| `--clean-cache-source NAME` | Remove cache for one source |

Combine flags as needed:

```bash
# Pull and normalize only (no geocode, no serve)
python pipeline.py --pull --normalize

# Initialize and pull with fresh downloads
python pipeline.py --init --pull --force

# Geocode then serve
python pipeline.py --geocode --serve
```

---

## Download Caching

The pipeline tracks downloads in `data/download_manifest.json` to avoid unnecessary re-downloads.

### How It Works

1. Before downloading, the pipeline checks if a cached file exists
2. If cached and fresh (within `--max-age`), the download is skipped
3. For HTTP sources, conditional requests (`If-None-Match`, `If-Modified-Since`) are used
4. If the server returns `304 Not Modified`, the cached file is reused
5. File checksums detect corruption or external changes
6. Ingestion is tracked separately — a file won't be re-ingested unless it changed

### Check Cache Status

```bash
python pipeline.py --status
```

Output:
```
══════════════════════════════════════════════════════════════
  Brookhaven Pipeline — Download Cache Status
══════════════════════════════════════════════════════════════

  ┌─ nys_assessment
  │  File:          /path/to/data/raw/nys_assessment.json
  │  File exists:   ✓    Size match: ✓
  │  Size:          52,428,800 bytes
  │  Downloaded:    2024-01-15T10:30:00+00:00
  │  Age:           2.5 hours  🟢
  │  Ingested:      ✓
  └────────────────────────────────────

  Summary:
    Sources:        3
    Files cached:   2/3
    Ingested:       2/3
    Total size:     83,886,080 bytes (80.0 MB)
```

### Clear Cache

```bash
# Remove all cached files
python pipeline.py --clean-cache

# Remove cache for one source
python pipeline.py --clean-cache-source nys_assessment
```

---

## API Endpoints

Once the server is running (`python pipeline.py --serve`), these endpoints are available:

### Health Check

```bash
curl http://localhost:8080/api/health
```

### Search Properties

```bash
# All properties (paginated)
curl "http://localhost:8080/api/properties/search"

# Search by address text
curl "http://localhost:8080/api/properties/search?q=main+street"

# Filter by city
curl "http://localhost:8080/api/properties/search?city=Patchogue"

# Filter by ZIP code
curl "http://localhost:8080/api/properties/search?zip=11772"

# Filter by beds and baths
curl "http://localhost:8080/api/properties/search?beds=3&baths=2"

# Filter by price range
curl "http://localhost:8080/api/properties/search?min_price=300000&max_price=500000"

# Filter by square footage
curl "http://localhost:8080/api/properties/search?min_sqft=1500&max_sqft=3000"

# Map bounding box
curl "http://localhost:8080/api/properties/search?sw_lat=40.7&sw_lng=-73.1&ne_lat=40.95&ne_lng=-72.8"

# Combined filters with sorting and pagination
curl "http://localhost:8080/api/properties/search?city=Patchogue&beds=3&sort=price&order=desc&page=1&limit=10"
```

### Get Single Property

```bash
curl http://localhost:8080/api/properties/123-main-st-patchogue
```

Returns property details plus up to 10 nearby properties.

### List Areas

```bash
curl http://localhost:8080/api/areas
```

### Get Area Detail

```bash
curl http://localhost:8080/api/areas/patchogue
```

Returns area stats (average price, property count) and recent sales.

### Create Lead

```bash
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
```

### Pipeline Stats

```bash
curl http://localhost:8080/api/stats
```

---

## Cron Jobs & Automation

To keep property data fresh, schedule the pipeline to run automatically.

### Basic Cron Setup

Open your crontab:

```bash
crontab -e
```

Add one of the schedules below. Each entry must be a **single line**.

#### Run every night at 2 AM

```cron
0 2 * * * cd /path/to/brookhaven-pipeline && /path/to/brookhaven-pipeline/.venv/bin/python pipeline.py --pull --normalize --geocode --max-age 24 >> /path/to/brookhaven-pipeline/logs/cron.log 2>&1
```

#### Run every Monday and Thursday at 3 AM

```cron
0 3 * * 1,4 cd /path/to/brookhaven-pipeline && /path/to/brookhaven-pipeline/.venv/bin/python pipeline.py --pull --normalize --geocode --max-age 72 >> /path/to/brookhaven-pipeline/logs/cron.log 2>&1
```

#### Run every Sunday at 1 AM (weekly)

```cron
0 1 * * 0 cd /path/to/brookhaven-pipeline && /path/to/brookhaven-pipeline/.venv/bin/python pipeline.py --pull --normalize --geocode --geocode-limit 2000 --max-age 168 >> /path/to/brookhaven-pipeline/logs/cron.log 2>&1
```

**Important:** Replace `/path/to/brookhaven-pipeline` with your actual project directory path.

### Cron Wrapper Script

For more robust scheduling, create a wrapper script that handles logging, locking, and error notification.

Save this as `scripts/cron_pipeline.sh`:

```bash
#!/usr/bin/env bash
#
# Cron wrapper for Brookhaven data pipeline.
# Handles logging, lock files, log rotation, and error alerts.
#
# Usage:
#   ./scripts/cron_pipeline.sh
#   ./scripts/cron_pipeline.sh --force
#

set -euo pipefail

# ── EDIT THESE SETTINGS ──────────────────────────────
PROJECT_DIR="/path/to/brookhaven-pipeline"
VENV_PYTHON="${PROJECT_DIR}/.venv/bin/python"
LOG_DIR="${PROJECT_DIR}/logs"
LOCK_FILE="/tmp/brookhaven_pipeline.lock"
MAX_AGE_HOURS=24
GEOCODE_LIMIT=500
LOG_RETENTION_DAYS=30
NOTIFY_EMAIL=""  # Set to receive error alerts (requires mail command)
# ─────────────────────────────────────────────────────

FORCE_FLAG="${1:-}"

# ── Setup ──
mkdir -p "${LOG_DIR}"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="${LOG_DIR}/pipeline_${TIMESTAMP}.log"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "${LOG_FILE}"
}

# ── Lock file (prevent overlapping runs) ──
if [ -f "${LOCK_FILE}" ]; then
    LOCK_PID=$(cat "${LOCK_FILE}" 2>/dev/null || echo "")
    if [ -n "${LOCK_PID}" ] && kill -0 "${LOCK_PID}" 2>/dev/null; then
        log "ERROR: Pipeline already running (PID ${LOCK_PID}). Exiting."
        exit 0
    else
        log "WARNING: Removing stale lock file."
        rm -f "${LOCK_FILE}"
    fi
fi

# Create lock
echo $$ > "${LOCK_FILE}"
trap 'rm -f "${LOCK_FILE}"' EXIT

# ── Run pipeline ──
log "Starting Brookhaven pipeline"
log "Project dir: ${PROJECT_DIR}"
log "Log file: ${LOG_FILE}"

cd "${PROJECT_DIR}"

# Build command
CMD="${VENV_PYTHON} pipeline.py --pull --normalize --geocode --geocode-limit ${GEOCODE_LIMIT} --max-age ${MAX_AGE_HOURS}"

if [ "${FORCE_FLAG}" = "--force" ]; then
    CMD="${CMD} --force"
    log "Force mode enabled"
fi

log "Running: ${CMD}"

if ${CMD} >> "${LOG_FILE}" 2>&1; then
    log "Pipeline completed successfully"
else
    EXIT_CODE=$?
    log "ERROR: Pipeline failed with exit code ${EXIT_CODE}"

    # Send email alert if configured
    if [ -n "${NOTIFY_EMAIL}" ] && command -v mail &>/dev/null; then
        tail -50 "${LOG_FILE}" | mail -s "Brookhaven Pipeline FAILED (exit ${EXIT_CODE})" "${NOTIFY_EMAIL}"
        log "Error notification sent to ${NOTIFY_EMAIL}"
    fi

    exit ${EXIT_CODE}
fi

# ── Rotate old logs ──
if [ -d "${LOG_DIR}" ]; then
    DELETED=$(find "${LOG_DIR}" -name "pipeline_*.log" -mtime +${LOG_RETENTION_DAYS} -delete -print | wc -l)
    if [ "${DELETED}" -gt 0 ]; then
        log "Cleaned up ${DELETED} log files older than ${LOG_RETENTION_DAYS} days"
    fi
fi

log "Done"
```

Make it executable:

```bash
chmod +x scripts/cron_pipeline.sh
```

**Edit the `PROJECT_DIR` variable** at the top to match your actual path.

Then add to crontab:

```bash
crontab -e
```

```cron
# Run pipeline nightly at 2 AM using the wrapper script
0 2 * * * /path/to/brookhaven-pipeline/scripts/cron_pipeline.sh

# Force a full refresh every Sunday at 1 AM
0 1 * * 0 /path/to/brookhaven-pipeline/scripts/cron_pipeline.sh --force
```

### Common Cron Schedules

```cron
# ┌───────── minute (0-59)
# │ ┌─────── hour (0-23)
# │ │ ┌───── day of month (1-31)
# │ │ │ ┌─── month (1-12)
# │ │ │ │ ┌─ day of week (0-7, 0 and 7 = Sunday)
# │ │ │ │ │
# * * * * * command

# Every night at 2 AM
  0 2 * * * /path/to/scripts/cron_pipeline.sh

# Every 12 hours (2 AM and 2 PM)
  0 2,14 * * * /path/to/scripts/cron_pipeline.sh

# Every Monday and Thursday at 3 AM
  0 3 * * 1,4 /path/to/scripts/cron_pipeline.sh

# Every Sunday at 1 AM (weekly full refresh)
  0 1 * * 0 /path/to/scripts/cron_pipeline.sh --force

# First day of every month at midnight
  0 0 1 * * /path/to/scripts/cron_pipeline.sh --force

# Every 6 hours
  0 */6 * * * /path/to/scripts/cron_pipeline.sh
```

### Verify Your Crontab

```bash
# List current cron jobs
crontab -l

# Check cron is running (Linux)
systemctl status cron

# Check cron is running (macOS)
sudo launchctl list | grep cron
```

### macOS launchd Setup

macOS uses `launchd` instead of cron (though cron still works). For a more macOS-native approach:

Create `~/Library/LaunchAgents/com.brookhaven.pipeline.plist`:

```xml
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

**Replace `/path/to/brookhaven-pipeline`** with your actual path, then load it:

```bash
launchctl load ~/Library/LaunchAgents/com.brookhaven.pipeline.plist
```

Manage:

```bash
# Check if it's loaded
launchctl list | grep brookhaven

# Unload
launchctl unload ~/Library/LaunchAgents/com.brookhaven.pipeline.plist

# Run immediately (for testing)
launchctl start com.brookhaven.pipeline
```

### Systemd Timer (Linux)

For Linux servers, systemd timers are more robust than cron.

Create `/etc/systemd/system/brookhaven-pipeline.service`:

```ini
[Unit]
Description=Brookhaven Property Data Pipeline
After=postgresql.service

[Service]
Type=oneshot
User=youruser
WorkingDirectory=/path/to/brookhaven-pipeline
Environment=DATABASE_URL=postgresql://postgres:postgres@localhost:5432/brookhaven
ExecStart=/path/to/brookhaven-pipeline/.venv/bin/python pipeline.py --pull --normalize --geocode --max-age 24
StandardOutput=append:/path/to/brookhaven-pipeline/logs/systemd.log
StandardError=append:/path/to/brookhaven-pipeline/logs/systemd_error.log
```

Create `/etc/systemd/system/brookhaven-pipeline.timer`:

```ini
[Unit]
Description=Run Brookhaven pipeline daily

[Timer]
OnCalendar=*-*-* 02:00:00
Persistent=true
RandomizedDelaySec=900

[Install]
WantedBy=timers.target
```

Enable:

```bash
sudo systemctl daemon-reload
sudo systemctl enable brookhaven-pipeline.timer
sudo systemctl start brookhaven-pipeline.timer

# Check status
systemctl status brookhaven-pipeline.timer
systemctl list-timers | grep brookhaven

# Run manually
sudo systemctl start brookhaven-pipeline.service

# View logs
journalctl -u brookhaven-pipeline.service -f
```

### Monitoring Cron Jobs

#### Check Recent Logs

```bash
# View latest log
ls -t logs/pipeline_*.log | head -1 | xargs cat

# Tail the latest log
ls -t logs/pipeline_*.log | head -1 | xargs tail -f

# Search for errors in recent logs
grep -i "error\|fail" logs/pipeline_*.log | tail -20
```

#### Quick Health Check Script

Save as `scripts/check_pipeline.sh`:

```bash
#!/usr/bin/env bash
# Quick check that the pipeline is working

PROJECT_DIR="/path/to/brookhaven-pipeline"
LOG_DIR="${PROJECT_DIR}/logs"

echo "=== Brookhaven Pipeline Health Check ==="
echo ""

# Last run
LATEST_LOG=$(ls -t "${LOG_DIR}"/pipeline_*.log 2>/dev/null | head -1)
if [ -n "${LATEST_LOG}" ]; then
    MOD_TIME=$(stat -f "%Sm" -t "%Y-%m-%d %H:%M" "${LATEST_LOG}" 2>/dev/null || stat -c "%y" "${LATEST_LOG}" 2>/dev/null | cut -d. -f1)
    echo "Last run: ${MOD_TIME}"
    echo "Log file: ${LATEST_LOG}"

    if grep -q "completed successfully" "${LATEST_LOG}" 2>/dev/null; then
        echo "Status:   ✅ Success"
    elif grep -qi "error\|fail" "${LATEST_LOG}" 2>/dev/null; then
        echo "Status:   ❌ Errors detected"
        echo ""
        echo "Recent errors:"
        grep -i "error\|fail" "${LATEST_LOG}" | tail -5
    else
        echo "Status:   ⚠️  Unknown"
    fi
else
    echo "Status: No log files found. Pipeline may not have run yet."
fi

echo ""

# Cache status
echo "=== Cache Status ==="
cd "${PROJECT_DIR}"
source .venv/bin/activate 2>/dev/null
python pipeline.py --status 2>/dev/null || echo "Could not check cache status"
```

---

## Testing

If you have the test scripts from the project, run them against the API:

```bash
# Make test scripts executable
chmod +x tests/*.sh

# Quick smoke test
./tests/test_smoke.sh http://localhost:8080

# Full test suite
./tests/test_api.sh http://localhost:8080

# Data quality checks
./tests/test_data_quality.sh http://localhost:8080

# Load test
./tests/test_load.sh http://localhost:8080 10 100
```

Or test manually with `curl`:

```bash
# Health check
curl -s http://localhost:8080/api/health | python3 -m json.tool

# Search
curl -s "http://localhost:8080/api/properties/search?city=Patchogue&limit=3" | python3 -m json.tool

# Stats
curl -s http://localhost:8080/api/stats | python3 -m json.tool
```

---

## Troubleshooting

### "Cannot connect to database"

```bash
# Check PostgreSQL is running
pg_isready

# Check your DATABASE_URL
echo $DATABASE_URL

# Test connection
psql $DATABASE_URL -c "SELECT 1"
```

### "No records after pulling"

```bash
# Check which sources are working
python pipeline.py --verify

# Search for new sources
python find_sources.py

# Check if raw records were stored
psql $DATABASE_URL -c "SELECT source, COUNT(*) FROM raw_records GROUP BY source"
```

### "All sources failed"

Government data URLs change frequently. Run the discovery tool:

```bash
python find_sources.py
```

Update the `DATA_SOURCES` dictionary in `pipeline.py` with the new working URLs from the output.

### "Geocoding is slow"

Geocoding is intentionally rate-limited to avoid being blocked. To speed things up:

- Use a Google Maps API key (set `GEOCODER_API_KEY`)
- Run geocoding separately with a higher limit: `python pipeline.py --geocode --geocode-limit 2000`
- Schedule geocoding during off-hours via cron

### "Pipeline already running" (lock file)

If the pipeline crashed and left a stale lock file:

```bash
rm /tmp/brookhaven_pipeline.lock
```

### Checking What's in the Database

```bash
# Property count
psql $DATABASE_URL -c "SELECT COUNT(*) FROM properties"

# Properties per city
psql $DATABASE_URL -c "SELECT city, COUNT(*) FROM properties GROUP BY city ORDER BY COUNT(*) DESC"

# Recent leads
psql $DATABASE_URL -c "SELECT name, email, lead_type, created_at FROM leads ORDER BY created_at DESC LIMIT 10"

# Geocoding coverage
psql $DATABASE_URL -c "SELECT COUNT(*) FILTER (WHERE latitude IS NOT NULL) as geocoded, COUNT(*) as total FROM properties"
```

---

## Legal & Data Notes

- **All data comes from official public records** — NY State Open Data, county assessment rolls, and public GIS layers
- **No scraping of private listing sites** (Zillow, Redfin, Realtor.com, etc.)
- **No bypassing of access controls** — only publicly available API endpoints and downloads are used
- The pipeline respects rate limits and identifies itself with a descriptive User-Agent string
- If you use this to generate real estate leads, ensure compliance with:
  - Your state's real estate advertising regulations
  - Fair housing laws
  - Privacy regulations (provide a privacy policy, get consent for communications)
  - CAN-SPAM Act (for email follow-ups)
```