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
