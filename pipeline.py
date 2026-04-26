"""
Brookhaven Property Data Pipeline
==================================
Pulls public property data from official Suffolk County / NY sources,
normalizes, deduplicates, geocodes, and stores in PostgreSQL.

Features:
    - Skips downloads if files already exist (with configurable max age)
    - ETag / Last-Modified caching for HTTP downloads
    - Tracks download metadata in a manifest file
    - Supports --force flag to re-download everything

Dependencies:
    pip install requests beautifulsoup4 psycopg2-binary python-slugify
    pip install pandas openpyxl geopy flask flask-cors

Environment Variables:
    DATABASE_URL        - PostgreSQL connection string
    GEOCODER_API_KEY    - Google Maps or Census geocoder key (optional)

Usage:
    python pipeline.py --init                Initialize database tables
    python pipeline.py --pull                Pull source data (skip if cached)
    python pipeline.py --pull --force        Force re-download all sources
    python pipeline.py --pull --max-age 48   Re-download if older than 48 hours
    python pipeline.py --normalize           Normalize and deduplicate
    python pipeline.py --geocode             Geocode missing coordinates
    python pipeline.py --serve               Start search API
    python pipeline.py --full                Run full pipeline then serve
    python pipeline.py --full --force        Full pipeline with forced re-download
"""

import os
import re
import csv
import json
import time
import hashlib
import logging
import argparse
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Any, Tuple
from dataclasses import dataclass, field, asdict
from pathlib import Path

import requests
import pandas as pd
import psycopg2
import psycopg2.extras
from psycopg2 import sql
from bs4 import BeautifulSoup
from slugify import slugify
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from flask import Flask, request, jsonify
from flask_cors import CORS

# ──────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/brookhaven"
)

GEOCODER_API_KEY = os.environ.get("GEOCODER_API_KEY", "")

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
RAW_DIR = os.path.join(DATA_DIR, "raw")
CLEAN_DIR = os.path.join(DATA_DIR, "clean")
MANIFEST_PATH = os.path.join(DATA_DIR, "download_manifest.json")

# Default max age for cached downloads (hours)
DEFAULT_MAX_AGE_HOURS = 168  # 7 days

# Suffolk County and NY Open Data endpoints
DATA_SOURCES = {
    "nys_7vem_aaz7": {
        "type": "socrata_api",
        "base_url": "https://data.ny.gov/resource/7vem-aaz7.json",
        "fallback_urls": [],
        "description": "Property Assessment Data from Local Assessment Rolls",
        "filename": "nys_7vem_aaz7.json",
        "filters": {
            "county_name": "SUFFOLK",
        },
        "brookhaven_filter": True,
        "page_size": 50000,
    },

    "nys_ykg4_r7ad": {
        "type": "socrata_api",
        "base_url": "https://data.ny.gov/resource/ykg4-r7ad.json",
        "fallback_urls": [],
        "description": "Summary of Real Property Tax Exemptions by Code by Municipality: Beginning Roll ",
        "filename": "nys_ykg4_r7ad.json",
        "filters": {
            "county_name": "SUFFOLK",
        },
        "brookhaven_filter": True,
        "page_size": 50000,
    },

    "nys_bsmp_6um6": {
        "type": "socrata_api",
        "base_url": "https://data.ny.gov/resource/bsmp-6um6.json",
        "fallback_urls": [],
        "description": "Residential Assessment Ratios:  Beginning Rate Year 1982",
        "filename": "nys_bsmp_6um6.json",
        "filters": {
            "county_name": "SUFFOLK",
        },
        "brookhaven_filter": True,
        "page_size": 50000,
    },

    "nys_e6pv_77bh": {
        "type": "socrata_api",
        "base_url": "https://data.ny.gov/resource/e6pv-77bh.json",
        "fallback_urls": [],
        "description": "Equalization Rates:  Beginning Rate Year 1954",
        "filename": "nys_e6pv_77bh.json",
        "filters": {
            "county_name": "SUFFOLK",
        },
        "brookhaven_filter": True,
        "page_size": 50000,
    },

    "arcgis_ce716a866cc94a0797c9": {
        "type": "arcgis_api",
        "base_url": "",
        "layer_url": "https://services7.arcgis.com/u68hBtg9YrRh9HYX/arcgis/rest/services/opengov_feature_service/FeatureServer/0/query",
        "fallback_layer_urls": [],
        "description": "opengov_feature_service",
        "filename": "arcgis_ce716a866cc94a0797c9.json",
        "where_clause": "1=1",
        "fallback_where_clauses": ["1=1"],
        "page_size": 2000,
    },

    "arcgis_8af5cef967f8474a9f26": {
        "type": "arcgis_api",
        "base_url": "",
        "layer_url": "https://services6.arcgis.com/EbVsqZ18sv1kVJ3k/arcgis/rest/services/NYS_Tax_Parcels_Public/FeatureServer/0/query",
        "fallback_layer_urls": [],
        "description": "NYS Tax Parcels Public",
        "filename": "arcgis_8af5cef967f8474a9f26.json",
        "where_clause": "1=1",
        "fallback_where_clauses": ["1=1"],
        "page_size": 2000,
    },

    "local_csv": {
        "type": "local_csv",
        "path": os.path.join(RAW_DIR, "brookhaven_parcels.csv"),
        "description": "Local export of Brookhaven data",
        "filename": "brookhaven_parcels.csv",
    },
}


# Brookhaven hamlets and their ZIP codes
BROOKHAVEN_AREAS = {
    "Patchogue": ["11772"],
    "Medford": ["11763"],
    "Shirley": ["11967"],
    "Mastic": ["11950"],
    "Mastic Beach": ["11951"],
    "Centereach": ["11720"],
    "Selden": ["11784"],
    "Farmingville": ["11738"],
    "Coram": ["11727"],
    "Bellport": ["11713"],
    "Port Jefferson": ["11777"],
    "Port Jefferson Station": ["11776"],
    "Mount Sinai": ["11766"],
    "Miller Place": ["11764"],
    "Rocky Point": ["11778"],
    "Ridge": ["11961"],
    "Middle Island": ["11953"],
    "Yaphank": ["11980"],
    "East Patchogue": ["11772"],
    "North Patchogue": ["11772"],
    "Holtsville": ["11742"],
    "Lake Grove": ["11755"],
    "Stony Brook": ["11790"],
    "Setauket": ["11733"],
    "East Setauket": ["11733"],
}

BROOKHAVEN_ZIPS = set()
for zips in BROOKHAVEN_AREAS.values():
    BROOKHAVEN_ZIPS.update(zips)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("pipeline")


# ──────────────────────────────────────────────
# Download Manifest (tracks what was downloaded and when)
# ──────────────────────────────────────────────

class DownloadManifest:
    """
    Tracks download metadata so we can skip re-downloads.

    Stores per-source:
        - filepath
        - download timestamp (UTC ISO)
        - file size in bytes
        - file MD5 checksum
        - HTTP ETag (if provided by server)
        - HTTP Last-Modified (if provided by server)
        - HTTP status code from last download
        - whether the file was ingested into the database
    """

    def __init__(self, manifest_path: str = MANIFEST_PATH):
        self.path = manifest_path
        self.entries: Dict[str, Dict] = {}
        self._load()

    def _load(self):
        if os.path.exists(self.path):
            try:
                with open(self.path, "r") as f:
                    self.entries = json.load(f)
                logger.debug("Loaded manifest with %d entries", len(self.entries))
            except (json.JSONDecodeError, IOError) as e:
                logger.warning("Could not load manifest: %s. Starting fresh.", e)
                self.entries = {}
        else:
            self.entries = {}

    def save(self):
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        with open(self.path, "w") as f:
            json.dump(self.entries, f, indent=2, default=str)
        logger.debug("Saved manifest with %d entries", len(self.entries))

    def get(self, source_name: str) -> Optional[Dict]:
        return self.entries.get(source_name)

    def set(self, source_name: str, entry: Dict):
        self.entries[source_name] = entry
        self.save()

    def mark_ingested(self, source_name: str):
        if source_name in self.entries:
            self.entries[source_name]["ingested"] = True
            self.entries[source_name]["ingested_at"] = datetime.now(timezone.utc).isoformat()
            self.save()

    def is_fresh(self, source_name: str, max_age_hours: float) -> bool:
        """Check if a cached download is still fresh (not expired)."""
        entry = self.entries.get(source_name)
        if not entry:
            return False

        filepath = entry.get("filepath", "")
        if not filepath or not os.path.exists(filepath):
            return False

        # Check file size matches
        actual_size = os.path.getsize(filepath)
        recorded_size = entry.get("file_size", 0)
        if actual_size != recorded_size:
            logger.info(
                "File size mismatch for %s: recorded=%d actual=%d",
                source_name, recorded_size, actual_size
            )
            return False

        # Check age
        downloaded_at = entry.get("downloaded_at")
        if not downloaded_at:
            return False

        try:
            download_time = datetime.fromisoformat(downloaded_at)
            if download_time.tzinfo is None:
                download_time = download_time.replace(tzinfo=timezone.utc)
            age = datetime.now(timezone.utc) - download_time
            max_age = timedelta(hours=max_age_hours)

            if age <= max_age:
                return True
            else:
                logger.info(
                    "Cache expired for %s: age=%s, max_age=%s",
                    source_name, age, max_age
                )
                return False
        except (ValueError, TypeError) as e:
            logger.warning("Could not parse download time for %s: %s", source_name, e)
            return False

    def needs_ingestion(self, source_name: str) -> bool:
        """Check if a downloaded file has not yet been ingested."""
        entry = self.entries.get(source_name)
        if not entry:
            return True
        return not entry.get("ingested", False)

    def file_checksum(self, filepath: str) -> str:
        """Compute MD5 of a file."""
        md5 = hashlib.md5()
        with open(filepath, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                md5.update(chunk)
        return md5.hexdigest()


# ──────────────────────────────────────────────
# Data Models
# ──────────────────────────────────────────────

@dataclass
class RawRecord:
    source: str
    raw_data: dict
    fetched_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    checksum: str = ""

    def compute_checksum(self):
        raw_str = json.dumps(self.raw_data, sort_keys=True)
        self.checksum = hashlib.md5(raw_str.encode()).hexdigest()
        return self.checksum


@dataclass
class CleanProperty:
    address: str
    slug: str
    city: str = ""
    state: str = "NY"
    zip_code: str = ""
    parcel_id: str = ""
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    lot_size: Optional[float] = None
    square_feet: Optional[int] = None
    beds: Optional[int] = None
    baths: Optional[float] = None
    year_built: Optional[int] = None
    assessed_value: Optional[int] = None
    last_sale_price: Optional[int] = None
    last_sale_date: Optional[str] = None
    property_class: Optional[str] = None
    owner_name: Optional[str] = None


# ──────────────────────────────────────────────
# Database Layer
# ──────────────────────────────────────────────

class Database:
    def __init__(self, dsn: str):
        self.dsn = dsn
        self.conn = None

    def connect(self):
        self.conn = psycopg2.connect(self.dsn)
        self.conn.autocommit = False
        logger.info("Connected to database")

    def close(self):
        if self.conn:
            self.conn.close()

    def execute(self, query: str, params=None):
        with self.conn.cursor() as cur:
            cur.execute(query, params)
        self.conn.commit()

    def fetch_all(self, query: str, params=None) -> List[Dict]:
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, params)
            return [dict(row) for row in cur.fetchall()]

    def fetch_one(self, query: str, params=None) -> Optional[Dict]:
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, params)
            row = cur.fetchone()
            return dict(row) if row else None

    def init_tables(self):
        """Create all tables needed for the pipeline."""
        ddl = """
        CREATE TABLE IF NOT EXISTS raw_records (
            id          SERIAL PRIMARY KEY,
            source      TEXT NOT NULL,
            raw_data    JSONB NOT NULL,
            checksum    TEXT NOT NULL,
            fetched_at  TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(source, checksum)
        );

        CREATE TABLE IF NOT EXISTS properties (
            id              SERIAL PRIMARY KEY,
            address         TEXT NOT NULL,
            slug            TEXT UNIQUE NOT NULL,
            city            TEXT,
            state           TEXT DEFAULT 'NY',
            zip             TEXT,
            parcel_id       TEXT,
            latitude        DOUBLE PRECISION,
            longitude       DOUBLE PRECISION,
            lot_size        DOUBLE PRECISION,
            square_feet     INTEGER,
            beds            INTEGER,
            baths           REAL,
            year_built      INTEGER,
            assessed_value  INTEGER,
            last_sale_price INTEGER,
            last_sale_date  DATE,
            property_class  TEXT,
            owner_name      TEXT,
            created_at      TIMESTAMPTZ DEFAULT NOW(),
            updated_at      TIMESTAMPTZ DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS leads (
            id          SERIAL PRIMARY KEY,
            name        TEXT NOT NULL,
            email       TEXT NOT NULL,
            phone       TEXT,
            address     TEXT,
            message     TEXT,
            lead_type   TEXT DEFAULT 'general',
            created_at  TIMESTAMPTZ DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS areas (
            id          SERIAL PRIMARY KEY,
            name        TEXT UNIQUE NOT NULL,
            slug        TEXT UNIQUE NOT NULL,
            description TEXT,
            zip_codes   TEXT[]
        );

        CREATE INDEX IF NOT EXISTS idx_properties_address
            ON properties USING gin(to_tsvector('english', address));
        CREATE INDEX IF NOT EXISTS idx_properties_city    ON properties(city);
        CREATE INDEX IF NOT EXISTS idx_properties_zip     ON properties(zip);
        CREATE INDEX IF NOT EXISTS idx_properties_slug    ON properties(slug);
        CREATE INDEX IF NOT EXISTS idx_properties_parcel  ON properties(parcel_id);
        CREATE INDEX IF NOT EXISTS idx_properties_latlon
            ON properties(latitude, longitude);
        CREATE INDEX IF NOT EXISTS idx_properties_beds    ON properties(beds);
        CREATE INDEX IF NOT EXISTS idx_properties_baths   ON properties(baths);
        CREATE INDEX IF NOT EXISTS idx_properties_price   ON properties(last_sale_price);
        CREATE INDEX IF NOT EXISTS idx_raw_source         ON raw_records(source);
        """
        self.execute(ddl)
        logger.info("Database tables initialized")

        for name, zips in BROOKHAVEN_AREAS.items():
            self.execute("""
                INSERT INTO areas (name, slug, zip_codes)
                VALUES (%s, %s, %s)
                ON CONFLICT (slug) DO UPDATE SET
                    zip_codes = EXCLUDED.zip_codes
            """, (name, slugify(name), zips))
        self.conn.commit()
        logger.info("Seeded %d areas", len(BROOKHAVEN_AREAS))

    def store_raw_record(self, record: RawRecord) -> bool:
        """Store a raw record. Returns True if new, False if duplicate."""
        try:
            self.execute("""
                INSERT INTO raw_records (source, raw_data, checksum, fetched_at)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (source, checksum) DO NOTHING
            """, (
                record.source,
                json.dumps(record.raw_data),
                record.checksum,
                record.fetched_at,
            ))
            return True
        except Exception as e:
            self.conn.rollback()
            logger.warning("Failed to store raw record: %s", e)
            return False

    def upsert_property(self, prop: CleanProperty):
        """Insert or update a cleaned property."""
        self.execute("""
            INSERT INTO properties (
                address, slug, city, state, zip, parcel_id,
                latitude, longitude, lot_size, square_feet,
                beds, baths, year_built, assessed_value,
                last_sale_price, last_sale_date, property_class, owner_name,
                updated_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                NOW()
            )
            ON CONFLICT (slug) DO UPDATE SET
                city = COALESCE(EXCLUDED.city, properties.city),
                state = COALESCE(EXCLUDED.state, properties.state),
                zip = COALESCE(EXCLUDED.zip, properties.zip),
                parcel_id = COALESCE(EXCLUDED.parcel_id, properties.parcel_id),
                latitude = COALESCE(EXCLUDED.latitude, properties.latitude),
                longitude = COALESCE(EXCLUDED.longitude, properties.longitude),
                lot_size = COALESCE(EXCLUDED.lot_size, properties.lot_size),
                square_feet = COALESCE(EXCLUDED.square_feet, properties.square_feet),
                beds = COALESCE(EXCLUDED.beds, properties.beds),
                baths = COALESCE(EXCLUDED.baths, properties.baths),
                year_built = COALESCE(EXCLUDED.year_built, properties.year_built),
                assessed_value = COALESCE(EXCLUDED.assessed_value, properties.assessed_value),
                last_sale_price = COALESCE(EXCLUDED.last_sale_price, properties.last_sale_price),
                last_sale_date = COALESCE(EXCLUDED.last_sale_date, properties.last_sale_date),
                property_class = COALESCE(EXCLUDED.property_class, properties.property_class),
                owner_name = COALESCE(EXCLUDED.owner_name, properties.owner_name),
                updated_at = NOW()
        """, (
            prop.address, prop.slug, prop.city, prop.state, prop.zip_code,
            prop.parcel_id or None,
            prop.latitude, prop.longitude, prop.lot_size, prop.square_feet,
            prop.beds, prop.baths, prop.year_built, prop.assessed_value,
            prop.last_sale_price, prop.last_sale_date or None,
            prop.property_class, prop.owner_name,
        ))

    def get_ungeocodeds(self, limit=500) -> List[Dict]:
        return self.fetch_all("""
            SELECT id, address, city, state, zip
            FROM properties
            WHERE latitude IS NULL OR longitude IS NULL
            LIMIT %s
        """, (limit,))

    def update_coordinates(self, property_id: int, lat: float, lon: float):
        self.execute("""
            UPDATE properties SET latitude = %s, longitude = %s, updated_at = NOW()
            WHERE id = %s
        """, (lat, lon, property_id))


# ──────────────────────────────────────────────
# Address Normalization
# ──────────────────────────────────────────────

class AddressNormalizer:
    """Normalize and standardize addresses for deduplication."""

    STREET_ABBREVS = {
        "street": "ST", "st": "ST", "st.": "ST",
        "avenue": "AVE", "ave": "AVE", "ave.": "AVE",
        "road": "RD", "rd": "RD", "rd.": "RD",
        "drive": "DR", "dr": "DR", "dr.": "DR",
        "lane": "LN", "ln": "LN", "ln.": "LN",
        "court": "CT", "ct": "CT", "ct.": "CT",
        "place": "PL", "pl": "PL", "pl.": "PL",
        "boulevard": "BLVD", "blvd": "BLVD", "blvd.": "BLVD",
        "circle": "CIR", "cir": "CIR",
        "terrace": "TER", "ter": "TER",
        "trail": "TRL", "trl": "TRL",
        "way": "WAY",
        "highway": "HWY", "hwy": "HWY",
        "parkway": "PKWY", "pkwy": "PKWY",
        "north": "N", "south": "S", "east": "E", "west": "W",
        "n.": "N", "s.": "S", "e.": "E", "w.": "W",
        "northeast": "NE", "northwest": "NW",
        "southeast": "SE", "southwest": "SW",
        "apartment": "APT", "apt": "APT", "apt.": "APT",
        "unit": "UNIT", "suite": "STE", "ste": "STE",
        "#": "APT",
    }

    @classmethod
    def normalize(cls, address: str) -> str:
        if not address:
            return ""
        addr = address.upper().strip()
        addr = re.sub(r'\s+', ' ', addr)
        addr = re.sub(r'[^\w\s\-#]', '', addr)
        words = addr.split()
        normalized_words = []
        for word in words:
            lower = word.lower().rstrip('.')
            if lower in cls.STREET_ABBREVS:
                normalized_words.append(cls.STREET_ABBREVS[lower])
            else:
                normalized_words.append(word)
        return " ".join(normalized_words)

    @classmethod
    def make_slug(cls, address: str, city: str = "") -> str:
        combined = f"{address} {city}".strip()
        return slugify(combined)


# ──────────────────────────────────────────────
# Data Source Discovery Helper
# ──────────────────────────────────────────────

class DataSourceDiscovery:
    """
    Helper to find and verify working dataset URLs
    on the NY Open Data portal (Socrata) and Suffolk County GIS.
    """

    # Known dataset search terms
    NY_OPEN_DATA_SEARCH = "https://data.ny.gov/api/catalog/v1"
    SUFFOLK_GIS_BASE = "https://gisservices.suffolkcountyny.gov/arcgis/rest/services"

    @staticmethod
    def search_ny_datasets(query: str = "real property suffolk") -> List[Dict]:
        """Search NY Open Data portal for datasets."""
        try:
            params = {
                "q": query,
                "domains": "data.ny.gov",
                "search_context": "data.ny.gov",
                "limit": 20,
            }
            resp = requests.get(
                DataSourceDiscovery.NY_OPEN_DATA_SEARCH,
                params=params,
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()

            results = []
            for item in data.get("results", []):
                resource = item.get("resource", {})
                results.append({
                    "name": resource.get("name", ""),
                    "id": resource.get("id", ""),
                    "description": resource.get("description", "")[:200],
                    "type": resource.get("type", ""),
                    "updated_at": resource.get("updatedAt", ""),
                    "url": f"https://data.ny.gov/resource/{resource.get('id', '')}.json",
                    "page_url": item.get("link", ""),
                })
            return results

        except Exception as e:
            logger.error("Failed to search NY Open Data: %s", e)
            return []

    @staticmethod
    def verify_socrata_endpoint(url: str) -> Dict:
        """Check if a Socrata API endpoint is alive and return metadata."""
        try:
            # Try fetching 1 row
            test_url = f"{url}?$limit=1"
            resp = requests.get(test_url, timeout=10)

            if resp.status_code == 200:
                data = resp.json()
                return {
                    "status": "ok",
                    "url": url,
                    "sample_fields": list(data[0].keys()) if data else [],
                    "http_status": 200,
                }
            else:
                return {
                    "status": "error",
                    "url": url,
                    "http_status": resp.status_code,
                    "message": resp.text[:200],
                }
        except Exception as e:
            return {
                "status": "error",
                "url": url,
                "message": str(e),
            }

    @staticmethod
    def verify_arcgis_endpoint(layer_url: str) -> Dict:
        """Check if an ArcGIS REST endpoint is alive."""
        try:
            params = {
                "where": "1=1",
                "outFields": "*",
                "resultRecordCount": 1,
                "f": "json",
            }
            resp = requests.get(layer_url, params=params, timeout=15)
            data = resp.json()

            if "features" in data:
                fields = []
                if data["features"]:
                    fields = list(data["features"][0].get("attributes", {}).keys())
                return {
                    "status": "ok",
                    "url": layer_url,
                    "sample_fields": fields,
                    "feature_count": len(data["features"]),
                }
            elif "error" in data:
                return {
                    "status": "error",
                    "url": layer_url,
                    "message": data["error"].get("message", str(data["error"])),
                }
            else:
                return {
                    "status": "unknown",
                    "url": layer_url,
                    "response_keys": list(data.keys()),
                }
        except Exception as e:
            return {
                "status": "error",
                "url": layer_url,
                "message": str(e),
            }


# ──────────────────────────────────────────────
# Updated DataPuller with Socrata + ArcGIS support
# ──────────────────────────────────────────────

class DataPuller:
    """Pull data from Socrata APIs, ArcGIS REST, CSV downloads, and local files."""

    def __init__(self, db: Database, force: bool = False, max_age_hours: float = DEFAULT_MAX_AGE_HOURS):
        self.db = db
        self.force = force
        self.max_age_hours = max_age_hours
        self.manifest = DownloadManifest()
        self.discovery = DataSourceDiscovery()

        os.makedirs(RAW_DIR, exist_ok=True)
        os.makedirs(CLEAN_DIR, exist_ok=True)

    def pull_all(self):
        logger.info("Starting data pull from all sources")
        logger.info(
            "Mode: %s | Max age: %d hours",
            "FORCE (re-download all)" if self.force else "CACHED (skip fresh files)",
            self.max_age_hours,
        )

        for source_name, source_config in DATA_SOURCES.items():
            try:
                source_type = source_config["type"]

                if source_type == "csv_download":
                    self._pull_csv_download(source_name, source_config)
                elif source_type == "local_csv":
                    self._pull_local_csv(source_name, source_config)
                elif source_type == "socrata_api":
                    self._pull_socrata(source_name, source_config)
                elif source_type == "arcgis_api":
                    self._pull_arcgis(source_name, source_config)
                else:
                    logger.warning("[%s] Unknown source type: %s", source_name, source_type)

            except Exception as e:
                logger.error("[%s] Failed to pull: %s", source_name, e)

        self._print_manifest_summary()

    # ── Socrata API (NY Open Data) ──

    def _pull_socrata(self, source_name: str, config: dict):
        """
        Pull data from a Socrata API with pagination and filtering.
        Socrata uses SoQL for queries.
        """
        if not self._should_download(source_name):
            filepath = os.path.join(RAW_DIR, config["filename"])
            if self._should_ingest(source_name, filepath):
                self._ingest_json_file(source_name, filepath)
                self.manifest.mark_ingested(source_name)
            return

        base_url = config["base_url"]
        fallback_url = config.get("fallback_url")
        filters = config.get("filters", {})
        page_size = config.get("page_size", 50000)
        filename = config["filename"]
        filepath = os.path.join(RAW_DIR, filename)

        # Verify endpoint is alive
        logger.info("[%s] Verifying Socrata endpoint: %s", source_name, base_url)
        check = self.discovery.verify_socrata_endpoint(base_url)

        if check["status"] != "ok":
            logger.warning("[%s] Primary endpoint failed: %s", source_name, check.get("message", ""))

            if fallback_url:
                logger.info("[%s] Trying fallback: %s", source_name, fallback_url)
                check = self.discovery.verify_socrata_endpoint(fallback_url)
                if check["status"] == "ok":
                    base_url = fallback_url
                    logger.info("[%s] Fallback endpoint works", source_name)
                else:
                    logger.error("[%s] Fallback also failed: %s", source_name, check.get("message", ""))
                    self._try_discover_dataset(source_name)
                    return
            else:
                logger.error("[%s] No fallback URL configured", source_name)
                self._try_discover_dataset(source_name)
                return

        logger.info("[%s] Endpoint OK. Available fields: %s",
                     source_name, ", ".join(check.get("sample_fields", [])[:10]))

        # Build SoQL filter
        where_parts = []
        for field, value in filters.items():
            # Check if this field exists in the dataset
            if check.get("sample_fields") and field not in check["sample_fields"]:
                # Try uppercase
                upper_field = field.upper()
                if upper_field in [f.upper() for f in check.get("sample_fields", [])]:
                    # Find the actual casing
                    for f in check["sample_fields"]:
                        if f.upper() == upper_field:
                            field = f
                            break
                else:
                    logger.warning(
                        "[%s] Filter field '%s' not found in dataset. Skipping filter.",
                        source_name, field,
                    )
                    continue
            where_parts.append(f"{field} = '{value}'")

        where_clause = " AND ".join(where_parts) if where_parts else None

        # Paginate through results
        all_records = []
        offset = 0
        page = 0

        while True:
            params = {
                "$limit": page_size,
                "$offset": offset,
                "$order": ":id",
            }
            if where_clause:
                params["$where"] = where_clause

            logger.info(
                "[%s] Fetching page %d (offset %d, limit %d)",
                source_name, page + 1, offset, page_size,
            )

            try:
                resp = requests.get(base_url, params=params, timeout=120)

                if resp.status_code == 404:
                    logger.error("[%s] 404 Not Found. Dataset may have been removed.", source_name)
                    self._try_discover_dataset(source_name)
                    return

                resp.raise_for_status()
                data = resp.json()

                if not data:
                    logger.info("[%s] No more records at offset %d", source_name, offset)
                    break

                all_records.extend(data)
                logger.info(
                    "[%s] Got %d records (total so far: %d)",
                    source_name, len(data), len(all_records),
                )

                if len(data) < page_size:
                    break

                offset += page_size
                page += 1

                # Rate limit
                time.sleep(0.5)

            except requests.exceptions.HTTPError as e:
                logger.error("[%s] HTTP error on page %d: %s", source_name, page + 1, e)
                break
            except Exception as e:
                logger.error("[%s] Error on page %d: %s", source_name, page + 1, e)
                break

        if not all_records:
            logger.warning("[%s] No records retrieved", source_name)
            return

        # Save to file
        with open(filepath, "w") as f:
            json.dump(all_records, f)

        file_size = os.path.getsize(filepath)
        file_checksum = self.manifest.file_checksum(filepath)

        logger.info(
            "[%s] Saved %d records to %s (%s bytes)",
            source_name, len(all_records), filepath, f"{file_size:,}",
        )

        # Store in database
        count = 0
        for item in all_records:
            raw = RawRecord(source=source_name, raw_data=item)
            raw.compute_checksum()
            self.db.store_raw_record(raw)
            count += 1
            if count % 5000 == 0:
                self.db.conn.commit()

        self.db.conn.commit()

        # Update manifest
        manifest_entry = {
            "source_name": source_name,
            "filepath": filepath,
            "filename": filename,
            "url": base_url,
            "downloaded_at": datetime.now(timezone.utc).isoformat(),
            "last_check_at": datetime.now(timezone.utc).isoformat(),
            "file_size": file_size,
            "file_checksum": file_checksum,
            "http_status": 200,
            "etag": None,
            "last_modified": None,
            "content_type": "application/json",
            "ingested": True,
            "ingested_at": datetime.now(timezone.utc).isoformat(),
            "record_count": count,
            "filters_used": filters,
        }
        self.manifest.set(source_name, manifest_entry)

        logger.info("[%s] Complete: %d records stored", source_name, count)

    # ── ArcGIS REST API (Suffolk County GIS) ──

    def _pull_arcgis(self, source_name: str, config: dict):
        """
        Pull data from an ArcGIS REST API with pagination.
        ArcGIS uses resultOffset + resultRecordCount for paging.
        """
        if not self._should_download(source_name):
            filepath = os.path.join(RAW_DIR, config["filename"])
            if self._should_ingest(source_name, filepath):
                self._ingest_json_file(source_name, filepath)
                self.manifest.mark_ingested(source_name)
            return

        layer_url = config["layer_url"]
        where_clause = config.get("where_clause", "1=1")
        page_size = config.get("page_size", 2000)
        filename = config["filename"]
        filepath = os.path.join(RAW_DIR, filename)

        # Verify endpoint
        logger.info("[%s] Verifying ArcGIS endpoint: %s", source_name, layer_url)
        check = self.discovery.verify_arcgis_endpoint(layer_url)

        if check["status"] != "ok":
            logger.error(
                "[%s] ArcGIS endpoint check failed: %s",
                source_name, check.get("message", "unknown error"),
            )
            return

        logger.info("[%s] ArcGIS endpoint OK. Fields: %s",
                     source_name, ", ".join(check.get("sample_fields", [])[:10]))

        # First get total count
        count_params = {
            "where": where_clause,
            "returnCountOnly": "true",
            "f": "json",
        }
        try:
            count_resp = requests.get(layer_url, params=count_params, timeout=15)
            count_data = count_resp.json()
            total_count = count_data.get("count", 0)
            logger.info("[%s] Total features matching filter: %d", source_name, total_count)
        except Exception as e:
            logger.warning("[%s] Could not get count: %s", source_name, e)
            total_count = None

        # Paginate
        all_features = []
        offset = 0
        page = 0

        while True:
            params = {
                "where": where_clause,
                "outFields": "*",
                "resultOffset": offset,
                "resultRecordCount": page_size,
                "f": "json",
                "orderByFields": "OBJECTID ASC",
            }

            logger.info(
                "[%s] Fetching page %d (offset %d, limit %d%s)",
                source_name, page + 1, offset, page_size,
                f", total: {total_count}" if total_count else "",
            )

            try:
                resp = requests.get(layer_url, params=params, timeout=60)
                data = resp.json()

                if "error" in data:
                    logger.error("[%s] ArcGIS error: %s", source_name, data["error"])
                    break

                features = data.get("features", [])
                if not features:
                    break

                # Extract attributes (flatten from ArcGIS feature format)
                for feature in features:
                    attrs = feature.get("attributes", {})
                    # Include geometry if present
                    geom = feature.get("geometry", {})
                    if geom:
                        attrs["_longitude"] = geom.get("x")
                        attrs["_latitude"] = geom.get("y")
                    all_features.append(attrs)

                logger.info(
                    "[%s] Got %d features (total so far: %d)",
                    source_name, len(features), len(all_features),
                )

                # Check if there are more
                exceeded = data.get("exceededTransferLimit", False)
                if len(features) < page_size and not exceeded:
                    break

                offset += page_size
                page += 1
                time.sleep(0.3)

            except Exception as e:
                logger.error("[%s] Error on page %d: %s", source_name, page + 1, e)
                break

        if not all_features:
            logger.warning("[%s] No features retrieved", source_name)
            return

        # Save to file
        with open(filepath, "w") as f:
            json.dump(all_features, f)

        file_size = os.path.getsize(filepath)
        file_checksum = self.manifest.file_checksum(filepath)

        logger.info(
            "[%s] Saved %d features to %s (%s bytes)",
            source_name, len(all_features), filepath, f"{file_size:,}",
        )

        # Store in database
        count = 0
        for item in all_features:
            raw = RawRecord(source=source_name, raw_data=item)
            raw.compute_checksum()
            self.db.store_raw_record(raw)
            count += 1
            if count % 5000 == 0:
                self.db.conn.commit()

        self.db.conn.commit()

        manifest_entry = {
            "source_name": source_name,
            "filepath": filepath,
            "filename": filename,
            "url": layer_url,
            "downloaded_at": datetime.now(timezone.utc).isoformat(),
            "last_check_at": datetime.now(timezone.utc).isoformat(),
            "file_size": file_size,
            "file_checksum": file_checksum,
            "http_status": 200,
            "etag": None,
            "last_modified": None,
            "content_type": "application/json",
            "ingested": True,
            "ingested_at": datetime.now(timezone.utc).isoformat(),
            "record_count": count,
        }
        self.manifest.set(source_name, manifest_entry)

        logger.info("[%s] Complete: %d records stored", source_name, count)

    # ── CSV Download (with HTTP caching) ──

    def _pull_csv_download(self, source_name: str, config: dict):
        """Download a CSV from a public URL, with HTTP caching."""
        url = config["url"]
        filename = config["filename"]
        filepath = os.path.join(RAW_DIR, filename)

        if not self._should_download(source_name):
            if self._should_ingest(source_name, filepath):
                self._ingest_csv(source_name, filepath)
                self.manifest.mark_ingested(source_name)
            return

        logger.info("[%s] Downloading from %s", source_name, url)

        headers = {
            "User-Agent": "BrookhavenPropertyPipeline/1.0 (public data research)"
        }

        cached_entry = self.manifest.get(source_name)
        if cached_entry and not self.force:
            if cached_entry.get("etag"):
                headers["If-None-Match"] = cached_entry["etag"]
            if cached_entry.get("last_modified"):
                headers["If-Modified-Since"] = cached_entry["last_modified"]

        try:
            response = requests.get(url, headers=headers, stream=True, timeout=300)
        except requests.exceptions.RequestException as e:
            logger.error("[%s] Download failed: %s", source_name, e)
            return

        if response.status_code == 304:
            logger.info("[%s] 304 Not Modified — using cached file", source_name)
            if cached_entry:
                cached_entry["last_check_at"] = datetime.now(timezone.utc).isoformat()
                cached_entry["http_status"] = 304
                self.manifest.set(source_name, cached_entry)
            if self._should_ingest(source_name, filepath):
                self._ingest_csv(source_name, filepath)
                self.manifest.mark_ingested(source_name)
            return

        if response.status_code == 404:
            logger.error("[%s] 404 Not Found at %s", source_name, url)
            return

        response.raise_for_status()

        with open(filepath, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        file_size = os.path.getsize(filepath)
        file_checksum = self.manifest.file_checksum(filepath)

        manifest_entry = {
            "source_name": source_name,
            "filepath": filepath,
            "filename": filename,
            "url": url,
            "downloaded_at": datetime.now(timezone.utc).isoformat(),
            "last_check_at": datetime.now(timezone.utc).isoformat(),
            "file_size": file_size,
            "file_checksum": file_checksum,
            "http_status": response.status_code,
            "etag": response.headers.get("ETag"),
            "last_modified": response.headers.get("Last-Modified"),
            "content_type": response.headers.get("Content-Type"),
            "ingested": False,
            "ingested_at": None,
        }
        self.manifest.set(source_name, manifest_entry)

        logger.info("[%s] Downloaded %s (%s bytes)", source_name, filepath, f"{file_size:,}")

        self._ingest_csv(source_name, filepath)
        self.manifest.mark_ingested(source_name)

    # ── Local CSV ──

    def _pull_local_csv(self, source_name: str, config: dict):
        """Read a locally available CSV file with change detection."""
        filepath = config["path"]
        if not os.path.exists(filepath):
            logger.warning("[%s] Local file not found: %s", source_name, filepath)
            return

        current_checksum = self.manifest.file_checksum(filepath)
        current_size = os.path.getsize(filepath)
        cached_entry = self.manifest.get(source_name)

        if cached_entry:
            if (cached_entry.get("file_checksum") == current_checksum
                    and cached_entry.get("ingested", False)
                    and not self.force):
                logger.info("[%s] SKIP — local file unchanged", source_name)
                return

        logger.info("[%s] Reading local file %s", source_name, filepath)

        manifest_entry = {
            "source_name": source_name,
            "filepath": filepath,
            "filename": config.get("filename", os.path.basename(filepath)),
            "url": None,
            "downloaded_at": datetime.now(timezone.utc).isoformat(),
            "last_check_at": datetime.now(timezone.utc).isoformat(),
            "file_size": current_size,
            "file_checksum": current_checksum,
            "http_status": None,
            "etag": None,
            "last_modified": None,
            "content_type": None,
            "ingested": False,
            "ingested_at": None,
        }
        self.manifest.set(source_name, manifest_entry)

        self._ingest_csv(source_name, filepath)
        self.manifest.mark_ingested(source_name)

    # ── Helpers ──

    def _should_download(self, source_name: str) -> bool:
        """Decide whether we need to download this source."""
        if self.force:
            logger.info("[%s] Force flag — will re-download", source_name)
            return True

        if self.manifest.is_fresh(source_name, self.max_age_hours):
            entry = self.manifest.get(source_name)
            age_str = self._format_age(entry.get("downloaded_at", ""))
            logger.info(
                "[%s] SKIP download — cached (age: %s, max: %dh)",
                source_name, age_str, self.max_age_hours,
            )
            return False

        return True

    def _should_ingest(self, source_name: str, filepath: str) -> bool:
        """Decide whether we need to ingest this source."""
        entry = self.manifest.get(source_name)
        if not entry:
            return True

        if os.path.exists(filepath):
            current_checksum = self.manifest.file_checksum(filepath)
            recorded_checksum = entry.get("file_checksum", "")
            if current_checksum != recorded_checksum:
                logger.info("[%s] File changed — will re-ingest", source_name)
                return True

        if self.manifest.needs_ingestion(source_name):
            logger.info("[%s] Not yet ingested", source_name)
            return True

        logger.info("[%s] SKIP ingestion — already done", source_name)
        return False

    def _ingest_json_file(self, source_name: str, filepath: str):
        """Ingest a cached JSON file into raw_records."""
        if not os.path.exists(filepath):
            logger.warning("[%s] JSON file not found: %s", source_name, filepath)
            return

        logger.info("[%s] Ingesting JSON file %s", source_name, filepath)

        with open(filepath, "r") as f:
            records = json.load(f)

        count = 0
        for item in records:
            if not self._is_brookhaven(item):
                continue
            raw = RawRecord(source=source_name, raw_data=item)
            raw.compute_checksum()
            self.db.store_raw_record(raw)
            count += 1
            if count % 5000 == 0:
                self.db.conn.commit()

        self.db.conn.commit()
        logger.info("[%s] Ingested %d records from JSON", source_name, count)

    def _ingest_csv(self, source_name: str, filepath: str):
        """Read CSV, filter to Brookhaven, store raw records."""
        logger.info("[%s] Ingesting CSV %s", source_name, filepath)

        count = 0
        skipped = 0

        try:
            df = pd.read_csv(filepath, dtype=str, low_memory=False)
            df.columns = [c.strip().lower().replace(' ', '_') for c in df.columns]
            total_rows = len(df)
            logger.info("[%s] CSV has %d rows", source_name, total_rows)

            for _, row in df.iterrows():
                record_dict = {k: v for k, v in row.to_dict().items() if pd.notna(v)}
                if not self._is_brookhaven(record_dict):
                    skipped += 1
                    continue

                raw = RawRecord(source=source_name, raw_data=record_dict)
                raw.compute_checksum()
                self.db.store_raw_record(raw)
                count += 1
                if count % 5000 == 0:
                    self.db.conn.commit()
                    logger.info("[%s] Ingested %d so far (%d skipped)", source_name, count, skipped)

        except Exception as e:
            logger.error("[%s] Error reading CSV: %s", source_name, e)
            self.db.conn.rollback()
            return

        self.db.conn.commit()
        logger.info("[%s] Done: %d stored, %d skipped", source_name, count, skipped)

    def _is_brookhaven(self, record: dict) -> bool:
        """Check if a record belongs to Brookhaven."""
        # Check various field name patterns (case-insensitive)
        record_lower = {k.lower(): v for k, v in record.items()}

        town_fields = ['municipality', 'town', 'city', 'muni_name',
                        'municipal', 'township', 'town_name',
                        'municipality_name', 'muni']
        for f in town_fields:
            val = str(record_lower.get(f, "")).upper().strip()
            if "BROOKHAVEN" in val:
                return True

        zip_fields = ['zip', 'zip_code', 'zipcode', 'postal_code', 'zip5']
        for f in zip_fields:
            val = str(record_lower.get(f, "")).strip()[:5]
            if val in BROOKHAVEN_ZIPS:
                return True

        county_fields = ['county', 'county_name']
        for f in county_fields:
            val = str(record_lower.get(f, "")).upper().strip()
            if "SUFFOLK" in val:
                return True

        return False

    def _try_discover_dataset(self, source_name: str):
        """When an endpoint fails, search for alternatives."""
        logger.info("[%s] Searching NY Open Data for alternative datasets...", source_name)

        search_terms = {
            "nys_assessment": "real property assessment roll",
            "nys_sales": "real property sales",
        }
        query = search_terms.get(source_name, "real property suffolk")
        results = self.discovery.search_ny_datasets(query)

        if results:
            logger.info("[%s] Found %d potential datasets:", source_name, len(results))
            for r in results[:5]:
                logger.info(
                    "  • %s (id: %s, type: %s)\n    URL: %s\n    Updated: %s",
                    r["name"], r["id"], r["type"], r["url"], r["updated_at"],
                )
        else:
            logger.warning("[%s] No alternative datasets found", source_name)

        logger.info(
            "[%s] To use a discovered dataset, update DATA_SOURCES in the script "
            "with the correct base_url.",
            source_name,
        )

    def _format_age(self, iso_timestamp: str) -> str:
        try:
            dt = datetime.fromisoformat(iso_timestamp)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            age = datetime.now(timezone.utc) - dt
            hours = age.total_seconds() / 3600
            if hours < 1:
                return f"{int(age.total_seconds() / 60)}m"
            elif hours < 24:
                return f"{hours:.1f}h"
            else:
                return f"{hours / 24:.1f}d"
        except (ValueError, TypeError):
            return "unknown"

    def _print_manifest_summary(self):
        logger.info("─── Download Manifest Summary ───")
        for source_name, entry in self.manifest.entries.items():
            filepath = entry.get("filepath", "?")
            exists = "✓" if os.path.exists(filepath) else "✗"
            size = entry.get("file_size", 0)
            age = self._format_age(entry.get("downloaded_at", ""))
            ingested = "✓" if entry.get("ingested") else "✗"
            records = entry.get("record_count", "?")
            http_status = entry.get("http_status", "—")

            logger.info(
                "  %s | file:%s size:%s age:%s ingested:%s records:%s http:%s",
                source_name, exists, f"{size:,}", age, ingested, records, http_status,
            )
        logger.info("────────────────────────────────")

# ──────────────────────────────────────────────
# Data Pullers (with caching)
# ──────────────────────────────────────────────

class DataPuller:
    """Pull data from Socrata APIs, ArcGIS REST, portals, and local files."""

    def __init__(self, db: Database, force: bool = False, max_age_hours: float = DEFAULT_MAX_AGE_HOURS):
        self.db = db
        self.force = force
        self.max_age_hours = max_age_hours
        self.manifest = DownloadManifest()
        self.discovery = DataSourceDiscovery()

        os.makedirs(RAW_DIR, exist_ok=True)
        os.makedirs(CLEAN_DIR, exist_ok=True)

    def pull_all(self):
        logger.info("Starting data pull from all sources")
        logger.info(
            "Mode: %s | Max age: %d hours",
            "FORCE" if self.force else "CACHED",
            self.max_age_hours,
        )

        for source_name, source_config in DATA_SOURCES.items():
            try:
                source_type = source_config["type"]

                if source_type == "csv_download":
                    self._pull_csv_download(source_name, source_config)
                elif source_type == "local_csv":
                    self._pull_local_csv(source_name, source_config)
                elif source_type == "socrata_api":
                    self._pull_socrata(source_name, source_config)
                elif source_type == "arcgis_api":
                    self._pull_arcgis(source_name, source_config)
                elif source_type == "portal_check":
                    self._check_portal(source_name, source_config)
                else:
                    logger.warning("[%s] Unknown type: %s", source_name, source_type)

            except Exception as e:
                logger.error("[%s] Failed: %s", source_name, e)

        self._print_manifest_summary()

    # ── Socrata with fallback URLs ──

    def _pull_socrata(self, source_name: str, config: dict):
        """Pull from Socrata API, trying fallback URLs if primary fails."""
        if not self._should_download(source_name):
            filepath = os.path.join(RAW_DIR, config["filename"])
            if self._should_ingest(source_name, filepath):
                self._ingest_json_file(source_name, filepath)
                self.manifest.mark_ingested(source_name)
            return

        # Build list of URLs to try
        urls_to_try = [config["base_url"]]
        urls_to_try.extend(config.get("fallback_urls", []))

        filters = config.get("filters", {})
        page_size = config.get("page_size", 50000)
        filename = config["filename"]
        filepath = os.path.join(RAW_DIR, filename)

        working_url = None
        working_fields = []

        for url in urls_to_try:
            logger.info("[%s] Trying Socrata endpoint: %s", source_name, url)
            check = self.discovery.verify_socrata_endpoint(url)

            if check["status"] == "ok":
                working_url = url
                working_fields = check.get("sample_fields", [])
                logger.info("[%s] ✅ Endpoint works: %s", source_name, url)
                logger.info("[%s] Fields: %s", source_name, ", ".join(working_fields[:10]))
                break
            else:
                logger.warning(
                    "[%s] ❌ Endpoint failed: %s — %s",
                    source_name, url, check.get("message", ""),
                )

        if not working_url:
            logger.error("[%s] All Socrata endpoints failed. Searching for alternatives...", source_name)
            self._try_discover_dataset(source_name)
            return

        # Build SoQL filter using available fields
        where_parts = []
        for field, value in filters.items():
            matched_field = self._match_field_name(field, working_fields)
            if matched_field:
                where_parts.append(f"{matched_field} = '{value}'")
            else:
                logger.warning("[%s] Filter field '%s' not found. Available: %s",
                             source_name, field, ", ".join(working_fields[:15]))

        where_clause = " AND ".join(where_parts) if where_parts else None

        # Paginate
        all_records = []
        offset = 0
        page_num = 0

        while True:
            params = {
                "$limit": page_size,
                "$offset": offset,
                "$order": ":id",
            }
            if where_clause:
                params["$where"] = where_clause

            logger.info("[%s] Page %d (offset %d)", source_name, page_num + 1, offset)

            try:
                resp = requests.get(working_url, params=params, timeout=120)

                if resp.status_code == 404:
                    logger.error("[%s] 404 during pagination", source_name)
                    break

                resp.raise_for_status()
                data = resp.json()

                if not data:
                    break

                # Filter to Brookhaven if needed
                if config.get("brookhaven_filter"):
                    data = [r for r in data if self._is_brookhaven(r)]

                all_records.extend(data)
                logger.info("[%s] Got %d records (total: %d)", source_name, len(data), len(all_records))

                if len(data) < page_size:
                    break

                offset += page_size
                page_num += 1
                time.sleep(0.5)

            except requests.exceptions.HTTPError as e:
                logger.error("[%s] HTTP error: %s", source_name, e)
                break
            except Exception as e:
                logger.error("[%s] Error: %s", source_name, e)
                break

        if not all_records:
            logger.warning("[%s] No records retrieved", source_name)
            return

        # Save to file
        with open(filepath, "w") as f:
            json.dump(all_records, f)

        file_size = os.path.getsize(filepath)
        file_checksum = self.manifest.file_checksum(filepath)

        # Store in DB
        count = 0
        for item in all_records:
            raw = RawRecord(source=source_name, raw_data=item)
            raw.compute_checksum()
            self.db.store_raw_record(raw)
            count += 1
            if count % 5000 == 0:
                self.db.conn.commit()

        self.db.conn.commit()

        self.manifest.set(source_name, {
            "source_name": source_name,
            "filepath": filepath,
            "filename": filename,
            "url": working_url,
            "downloaded_at": datetime.now(timezone.utc).isoformat(),
            "last_check_at": datetime.now(timezone.utc).isoformat(),
            "file_size": file_size,
            "file_checksum": file_checksum,
            "http_status": 200,
            "etag": None,
            "last_modified": None,
            "content_type": "application/json",
            "ingested": True,
            "ingested_at": datetime.now(timezone.utc).isoformat(),
            "record_count": count,
            "working_url": working_url,
            "filters_used": filters,
        })

        logger.info("[%s] Complete: %d records from %s", source_name, count, working_url)

    # ── ArcGIS with fallback URLs and WHERE clauses ──

    def _pull_arcgis(self, source_name: str, config: dict):
        """Pull from ArcGIS REST API, trying fallbacks."""
        if not self._should_download(source_name):
            filepath = os.path.join(RAW_DIR, config["filename"])
            if self._should_ingest(source_name, filepath):
                self._ingest_json_file(source_name, filepath)
                self.manifest.mark_ingested(source_name)
            return

        # Try layer URLs
        layer_urls = [config["layer_url"]]
        layer_urls.extend(config.get("fallback_layer_urls", []))

        where_clauses = [config.get("where_clause", "1=1")]
        where_clauses.extend(config.get("fallback_where_clauses", []))

        page_size = config.get("page_size", 2000)
        filename = config["filename"]
        filepath = os.path.join(RAW_DIR, filename)

        working_url = None
        working_where = None
        working_fields = []

        # Try each URL + WHERE combination
        for url in layer_urls:
            for where in where_clauses:
                logger.info("[%s] Trying ArcGIS: %s WHERE %s", source_name, url, where)

                try:
                    params = {
                        "where": where,
                        "outFields": "*",
                        "resultRecordCount": 1,
                        "f": "json",
                    }
                    resp = requests.get(url, params=params, timeout=15)
                    data = resp.json()

                    if "error" in data:
                        logger.warning("[%s] Error: %s", source_name, data["error"].get("message", ""))
                        continue

                    features = data.get("features", [])
                    if features:
                        attrs = features[0].get("attributes", {})
                        working_url = url
                        working_where = where
                        working_fields = list(attrs.keys())
                        logger.info("[%s] ✅ Works! Fields: %s",
                                   source_name, ", ".join(working_fields[:10]))
                        break
                    else:
                        logger.warning("[%s] No features returned", source_name)

                except Exception as e:
                    logger.warning("[%s] Failed: %s", source_name, e)

            if working_url:
                break

        if not working_url:
            logger.error("[%s] All ArcGIS endpoints failed", source_name)
            return

        # Get total count
        total_count = None
        try:
            count_resp = requests.get(working_url, params={
                "where": working_where,
                "returnCountOnly": "true",
                "f": "json",
            }, timeout=15)
            total_count = count_resp.json().get("count")
            logger.info("[%s] Total features: %s", source_name, total_count)
        except Exception:
            pass

        # Paginate
        all_features = []
        offset = 0
        page_num = 0

        while True:
            params = {
                "where": working_where,
                "outFields": "*",
                "resultOffset": offset,
                "resultRecordCount": page_size,
                "f": "json",
                "orderByFields": "OBJECTID ASC",
                "returnGeometry": "true",
            }

            logger.info("[%s] Page %d (offset %d%s)",
                       source_name, page_num + 1, offset,
                       f"/{total_count}" if total_count else "")

            try:
                resp = requests.get(working_url, params=params, timeout=60)
                data = resp.json()

                if "error" in data:
                    logger.error("[%s] ArcGIS error: %s", source_name, data["error"])
                    break

                features = data.get("features", [])
                if not features:
                    break

                for feature in features:
                    attrs = feature.get("attributes", {})
                    geom = feature.get("geometry", {})
                    if geom:
                        attrs["_longitude"] = geom.get("x")
                        attrs["_latitude"] = geom.get("y")
                    all_features.append(attrs)

                logger.info("[%s] Got %d (total: %d)", source_name, len(features), len(all_features))

                exceeded = data.get("exceededTransferLimit", False)
                if len(features) < page_size and not exceeded:
                    break

                offset += page_size
                page_num += 1
                time.sleep(0.3)

            except Exception as e:
                logger.error("[%s] Page error: %s", source_name, e)
                break

        if not all_features:
            logger.warning("[%s] No features retrieved", source_name)
            return

        # Filter to Brookhaven if we used a broad WHERE
        if working_where == "1=1":
            before = len(all_features)
            all_features = [f for f in all_features if self._is_brookhaven(f)]
            logger.info("[%s] Filtered %d -> %d Brookhaven features",
                       source_name, before, len(all_features))

        # Save + ingest
        with open(filepath, "w") as f:
            json.dump(all_features, f)

        file_size = os.path.getsize(filepath)
        file_checksum = self.manifest.file_checksum(filepath)

        count = 0
        for item in all_features:
            raw = RawRecord(source=source_name, raw_data=item)
            raw.compute_checksum()
            self.db.store_raw_record(raw)
            count += 1
            if count % 5000 == 0:
                self.db.conn.commit()

        self.db.conn.commit()

        self.manifest.set(source_name, {
            "source_name": source_name,
            "filepath": filepath,
            "filename": filename,
            "url": working_url,
            "downloaded_at": datetime.now(timezone.utc).isoformat(),
            "last_check_at": datetime.now(timezone.utc).isoformat(),
            "file_size": file_size,
            "file_checksum": file_checksum,
            "http_status": 200,
            "etag": None,
            "last_modified": None,
            "content_type": "application/json",
            "ingested": True,
            "ingested_at": datetime.now(timezone.utc).isoformat(),
            "record_count": count,
            "working_url": working_url,
            "working_where": working_where,
        })

        logger.info("[%s] Complete: %d records", source_name, count)

    # ── Portal Check (discover downloadable files) ──

    def _check_portal(self, source_name: str, config: dict):
        """
        Check government portals for downloadable data files.
        This doesn't scrape — it just finds links to official downloads.
        """
        check_urls = config.get("check_urls", [])

        logger.info("[%s] Checking portal pages for downloadable data...", source_name)

        for url in check_urls:
            try:
                resp = requests.get(url, timeout=15, headers={
                    "User-Agent": "BrookhavenPropertyPipeline/1.0"
                })

                if resp.status_code != 200:
                    logger.warning("[%s] %s returned %d", source_name, url, resp.status_code)
                    continue

                soup = BeautifulSoup(resp.text, "html.parser")

                # Look for download links (CSV, XLSX, ZIP)
                download_links = []
                for a in soup.find_all("a", href=True):
                    href = a["href"].lower()
                    text = a.get_text().lower()

                    if any(ext in href for ext in [".csv", ".xlsx", ".xls", ".zip", ".txt"]):
                        download_links.append({
                            "url": a["href"] if a["href"].startswith("http") else f"{url.rstrip('/')}/{a['href'].lstrip('/')}",
                            "text": a.get_text().strip()[:100],
                            "type": "direct_file",
                        })
                    elif any(kw in text for kw in ["download", "export", "data", "assessment", "sales"]):
                        download_links.append({
                            "url": a["href"] if a["href"].startswith("http") else f"{url.rstrip('/')}/{a['href'].lstrip('/')}",
                            "text": a.get_text().strip()[:100],
                            "type": "possible_data_link",
                        })

                if download_links:
                    logger.info("[%s] Found %d potential download links on %s:",
                               source_name, len(download_links), url)
                    for link in download_links[:10]:
                        logger.info("  • [%s] %s\n    %s",
                                   link["type"], link["text"], link["url"])
                else:
                    logger.info("[%s] No download links found on %s", source_name, url)

            except Exception as e:
                logger.warning("[%s] Could not check %s: %s", source_name, url, e)

    # ── CSV Download ──

    def _pull_csv_download(self, source_name: str, config: dict):
        """Download CSV with HTTP caching."""
        url = config["url"]
        filename = config["filename"]
        filepath = os.path.join(RAW_DIR, filename)

        if not self._should_download(source_name):
            if self._should_ingest(source_name, filepath):
                self._ingest_csv(source_name, filepath)
                self.manifest.mark_ingested(source_name)
            return

        logger.info("[%s] Downloading from %s", source_name, url)

        headers = {"User-Agent": "BrookhavenPropertyPipeline/1.0"}
        cached_entry = self.manifest.get(source_name)
        if cached_entry and not self.force:
            if cached_entry.get("etag"):
                headers["If-None-Match"] = cached_entry["etag"]
            if cached_entry.get("last_modified"):
                headers["If-Modified-Since"] = cached_entry["last_modified"]

        try:
            response = requests.get(url, headers=headers, stream=True, timeout=300)
        except requests.exceptions.RequestException as e:
            logger.error("[%s] Download failed: %s", source_name, e)
            return

        if response.status_code == 304:
            logger.info("[%s] 304 Not Modified", source_name)
            if cached_entry:
                cached_entry["last_check_at"] = datetime.now(timezone.utc).isoformat()
                self.manifest.set(source_name, cached_entry)
            if self._should_ingest(source_name, filepath):
                self._ingest_csv(source_name, filepath)
                self.manifest.mark_ingested(source_name)
            return

        if response.status_code == 404:
            logger.error("[%s] 404 Not Found", source_name)
            return

        response.raise_for_status()

        with open(filepath, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        file_size = os.path.getsize(filepath)
        file_checksum = self.manifest.file_checksum(filepath)

        self.manifest.set(source_name, {
            "source_name": source_name,
            "filepath": filepath,
            "filename": filename,
            "url": url,
            "downloaded_at": datetime.now(timezone.utc).isoformat(),
            "last_check_at": datetime.now(timezone.utc).isoformat(),
            "file_size": file_size,
            "file_checksum": file_checksum,
            "http_status": response.status_code,
            "etag": response.headers.get("ETag"),
            "last_modified": response.headers.get("Last-Modified"),
            "content_type": response.headers.get("Content-Type"),
            "ingested": False,
            "ingested_at": None,
        })

        logger.info("[%s] Downloaded %s (%s bytes)", source_name, filepath, f"{file_size:,}")

        self._ingest_csv(source_name, filepath)
        self.manifest.mark_ingested(source_name)

    # ── Local CSV ──

    def _pull_local_csv(self, source_name: str, config: dict):
        """Read local CSV with change detection."""
        filepath = config["path"]
        if not os.path.exists(filepath):
            logger.warning("[%s] File not found: %s", source_name, filepath)
            return

        current_checksum = self.manifest.file_checksum(filepath)
        cached_entry = self.manifest.get(source_name)

        if cached_entry:
            if (cached_entry.get("file_checksum") == current_checksum
                    and cached_entry.get("ingested", False)
                    and not self.force):
                logger.info("[%s] SKIP — unchanged", source_name)
                return

        logger.info("[%s] Reading %s", source_name, filepath)

        self.manifest.set(source_name, {
            "source_name": source_name,
            "filepath": filepath,
            "filename": config.get("filename", os.path.basename(filepath)),
            "url": None,
            "downloaded_at": datetime.now(timezone.utc).isoformat(),
            "last_check_at": datetime.now(timezone.utc).isoformat(),
            "file_size": os.path.getsize(filepath),
            "file_checksum": current_checksum,
            "http_status": None,
            "ingested": False,
        })

        self._ingest_csv(source_name, filepath)
        self.manifest.mark_ingested(source_name)

    # ── Shared Helpers ──

    def _match_field_name(self, target: str, available_fields: List[str]) -> Optional[str]:
        """Case-insensitive field name matching."""
        target_lower = target.lower()
        for f in available_fields:
            if f.lower() == target_lower:
                return f
        # Try partial match
        for f in available_fields:
            if target_lower in f.lower() or f.lower() in target_lower:
                return f
        return None

    def _should_download(self, source_name: str) -> bool:
        if self.force:
            logger.info("[%s] Force — will download", source_name)
            return True
        if self.manifest.is_fresh(source_name, self.max_age_hours):
            entry = self.manifest.get(source_name)
            age = self._format_age(entry.get("downloaded_at", ""))
            logger.info("[%s] SKIP — cached (age: %s)", source_name, age)
            return False
        return True

    def _should_ingest(self, source_name: str, filepath: str) -> bool:
        entry = self.manifest.get(source_name)
        if not entry:
            return True
        if os.path.exists(filepath):
            current = self.manifest.file_checksum(filepath)
            if current != entry.get("file_checksum", ""):
                return True
        if self.manifest.needs_ingestion(source_name):
            return True
        return False

    def _ingest_json_file(self, source_name: str, filepath: str):
        if not os.path.exists(filepath):
            return
        logger.info("[%s] Ingesting JSON %s", source_name, filepath)
        with open(filepath, "r") as f:
            records = json.load(f)
        count = 0
        for item in records:
            if not self._is_brookhaven(item):
                continue
            raw = RawRecord(source=source_name, raw_data=item)
            raw.compute_checksum()
            self.db.store_raw_record(raw)
            count += 1
            if count % 5000 == 0:
                self.db.conn.commit()
        self.db.conn.commit()
        logger.info("[%s] Ingested %d records", source_name, count)

    def _ingest_csv(self, source_name: str, filepath: str):
        logger.info("[%s] Ingesting CSV %s", source_name, filepath)
        count = 0
        skipped = 0
        try:
            df = pd.read_csv(filepath, dtype=str, low_memory=False)
            df.columns = [c.strip().lower().replace(' ', '_') for c in df.columns]
            for _, row in df.iterrows():
                record_dict = {k: v for k, v in row.to_dict().items() if pd.notna(v)}
                if not self._is_brookhaven(record_dict):
                    skipped += 1
                    continue
                raw = RawRecord(source=source_name, raw_data=record_dict)
                raw.compute_checksum()
                self.db.store_raw_record(raw)
                count += 1
                if count % 5000 == 0:
                    self.db.conn.commit()
        except Exception as e:
            logger.error("[%s] CSV error: %s", source_name, e)
            self.db.conn.rollback()
            return
        self.db.conn.commit()
        logger.info("[%s] Done: %d stored, %d skipped", source_name, count, skipped)

    def _is_brookhaven(self, record: dict) -> bool:
        record_lower = {k.lower(): str(v).strip() for k, v in record.items() if v}
        town_fields = ['municipality', 'town', 'city', 'muni_name',
                        'municipal', 'township', 'town_name',
                        'municipality_name', 'muni']
        for f in town_fields:
            if "BROOKHAVEN" in record_lower.get(f, "").upper():
                return True
        zip_fields = ['zip', 'zip_code', 'zipcode', 'postal_code', 'zip5']
        for f in zip_fields:
            if record_lower.get(f, "")[:5] in BROOKHAVEN_ZIPS:
                return True
        county_fields = ['county', 'county_name']
        for f in county_fields:
            if "SUFFOLK" in record_lower.get(f, "").upper():
                return True
        return False

    def _try_discover_dataset(self, source_name: str):
        logger.info("[%s] Searching for alternative datasets...", source_name)
        queries = {
            "nys_assessment": "real property assessment roll",
            "nys_sales": "real property sales",
        }
        query = queries.get(source_name, "real property suffolk")
        results = self.discovery.search_ny_datasets(query)
        if results:
            logger.info("[%s] Found %d alternatives:", source_name, len(results))
            for r in results[:5]:
                logger.info("  • %s (id: %s)\n    %s", r["name"], r["id"], r["url"])
        else:
            logger.warning("[%s] No alternatives found", source_name)

    def _format_age(self, ts: str) -> str:
        try:
            dt = datetime.fromisoformat(ts)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            hours = (datetime.now(timezone.utc) - dt).total_seconds() / 3600
            if hours < 1: return f"{int(hours * 60)}m"
            elif hours < 24: return f"{hours:.1f}h"
            else: return f"{hours / 24:.1f}d"
        except: return "unknown"

    def _print_manifest_summary(self):
        logger.info("─── Manifest Summary ───")
        for name, entry in self.manifest.entries.items():
            fp = entry.get("filepath", "?")
            exists = "✓" if os.path.exists(fp) else "✗"
            size = entry.get("file_size", 0)
            age = self._format_age(entry.get("downloaded_at", ""))
            ingested = "✓" if entry.get("ingested") else "✗"
            records = entry.get("record_count", "?")
            logger.info("  %s | file:%s size:%s age:%s ingested:%s records:%s",
                       name, exists, f"{size:,}", age, ingested, records)
        logger.info("────────────────────────")


# ──────────────────────────────────────────────
# Normalizer & Deduplicator
# ──────────────────────────────────────────────

class PropertyNormalizer:
    """Transform raw records into clean, deduplicated properties."""

    FIELD_MAPPINGS = {
        "address": [
            "address", "property_address", "street_address", "location",
            "prop_addr", "situs_address", "full_address", "street",
            "print_key_address",
        ],
        "city": [
            "city", "municipality", "town", "muni_name", "village",
            "hamlet", "locality",
        ],
        "zip_code": [
            "zip", "zip_code", "zipcode", "postal_code", "zip5",
        ],
        "parcel_id": [
            "parcel_id", "parcel", "tax_map", "print_key", "sbl",
            "swis_sbl", "parcel_number", "pin", "apn",
        ],
        "square_feet": [
            "square_feet", "sqft", "sq_ft", "living_area",
            "total_living_area", "gross_living_area", "building_area",
        ],
        "lot_size": [
            "lot_size", "lot_area", "land_area", "acres", "lot_acres",
            "land_sqft",
        ],
        "beds": [
            "beds", "bedrooms", "bed", "num_bedrooms", "total_bedrooms",
        ],
        "baths": [
            "baths", "bathrooms", "bath", "full_baths", "total_baths",
            "num_baths",
        ],
        "year_built": [
            "year_built", "yr_built", "year_constructed", "built_year",
            "effective_year",
        ],
        "assessed_value": [
            "assessed_value", "total_av", "full_market_value",
            "assessed_total", "total_assessed_value", "assessment",
            "full_mkt_val",
        ],
        "last_sale_price": [
            "sale_price", "last_sale_price", "sold_price", "price",
            "sale_amount",
        ],
        "last_sale_date": [
            "sale_date", "last_sale_date", "sold_date", "date_of_sale",
            "deed_date",
        ],
        "property_class": [
            "property_class", "prop_class", "class_code", "use_code",
            "land_use", "property_type", "building_class",
        ],
        "owner_name": [
            "owner", "owner_name", "owner_1", "primary_owner",
            "owner_name_1",
        ],
        "latitude": [
            "latitude", "lat", "y",
        ],
        "longitude": [
            "longitude", "lng", "lon", "long", "x",
        ],
    }

    def __init__(self, db: Database):
        self.db = db
        self.normalizer = AddressNormalizer()

    def normalize_all(self):
        """Pull raw records and normalize into properties table."""
        logger.info("Starting normalization pass")

        offset = 0
        batch_size = 1000
        total_processed = 0
        total_stored = 0

        while True:
            rows = self.db.fetch_all("""
                SELECT id, source, raw_data
                FROM raw_records
                ORDER BY id
                LIMIT %s OFFSET %s
            """, (batch_size, offset))

            if not rows:
                break

            for row in rows:
                raw_data = row["raw_data"]
                if isinstance(raw_data, str):
                    raw_data = json.loads(raw_data)

                prop = self._extract_property(raw_data, row["source"])
                if prop and prop.address:
                    self.db.upsert_property(prop)
                    total_stored += 1

                total_processed += 1

            self.db.conn.commit()
            offset += batch_size
            logger.info("Processed %d raw records, stored %d properties",
                        total_processed, total_stored)

        logger.info("Normalization complete: %d processed, %d stored",
                     total_processed, total_stored)

    def _extract_property(self, raw: dict, source: str) -> Optional[CleanProperty]:
        """Extract and normalize a property from a raw record."""
        try:
            address_raw = self._find_field(raw, "address") or ""
            city_raw = self._find_field(raw, "city") or ""
            zip_raw = self._find_field(raw, "zip_code") or ""

            if not address_raw:
                return None

            address = AddressNormalizer.normalize(address_raw)
            city = city_raw.strip().title() if city_raw else "Brookhaven"
            zip_code = str(zip_raw).strip()[:5] if zip_raw else ""

            slug = AddressNormalizer.make_slug(address, city)
            if not slug:
                return None

            prop = CleanProperty(
                address=address,
                slug=slug,
                city=city,
                state="NY",
                zip_code=zip_code,
                parcel_id=self._find_field(raw, "parcel_id") or "",
                latitude=self._safe_float(self._find_field(raw, "latitude")),
                longitude=self._safe_float(self._find_field(raw, "longitude")),
                lot_size=self._safe_float(self._find_field(raw, "lot_size")),
                square_feet=self._safe_int(self._find_field(raw, "square_feet")),
                beds=self._safe_int(self._find_field(raw, "beds")),
                baths=self._safe_float(self._find_field(raw, "baths")),
                year_built=self._safe_int(self._find_field(raw, "year_built")),
                assessed_value=self._safe_int(self._find_field(raw, "assessed_value")),
                last_sale_price=self._safe_int(self._find_field(raw, "last_sale_price")),
                last_sale_date=self._safe_date(self._find_field(raw, "last_sale_date")),
                property_class=self._find_field(raw, "property_class"),
                owner_name=self._find_field(raw, "owner_name"),
            )

            return prop

        except Exception as e:
            logger.debug("Failed to extract property: %s", e)
            return None

    def _find_field(self, raw: dict, our_field: str) -> Optional[str]:
        """Try multiple possible column names to find a field value."""
        possible_names = self.FIELD_MAPPINGS.get(our_field, [])
        for name in possible_names:
            val = raw.get(name)
            if val and str(val).strip():
                return str(val).strip()
        return None

    @staticmethod
    def _safe_int(val) -> Optional[int]:
        if val is None:
            return None
        try:
            cleaned = re.sub(r'[^\d.-]', '', str(val))
            return int(float(cleaned)) if cleaned else None
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _safe_float(val) -> Optional[float]:
        if val is None:
            return None
        try:
            cleaned = re.sub(r'[^\d.-]', '', str(val))
            return float(cleaned) if cleaned else None
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _safe_date(val) -> Optional[str]:
        if val is None:
            return None
        try:
            for fmt in ["%Y-%m-%d", "%m/%d/%Y", "%m-%d-%Y", "%Y%m%d",
                         "%d-%b-%Y", "%B %d, %Y", "%m/%d/%y"]:
                try:
                    dt = datetime.strptime(str(val).strip(), fmt)
                    return dt.strftime("%Y-%m-%d")
                except ValueError:
                    continue
            return None
        except Exception:
            return None


# ──────────────────────────────────────────────
# Geocoder
# ──────────────────────────────────────────────

class PropertyGeocoder:
    """Geocode properties with missing coordinates."""

    def __init__(self, db: Database):
        self.db = db
        self.census_url = "https://geocoding.geo.census.gov/geocoder/locations/onelineaddress"
        self.nominatim = Nominatim(user_agent="brookhaven_property_pipeline")
        self.geocode_nominatim = RateLimiter(
            self.nominatim.geocode,
            min_delay_seconds=1.1,
            max_retries=2,
        )

    def geocode_all(self, limit: int = 500):
        """Geocode all properties missing coordinates."""
        ungeocodeds = self.db.get_ungeocodeds(limit=limit)
        logger.info("Found %d properties to geocode", len(ungeocodeds))

        success = 0
        failed = 0

        for prop in ungeocodeds:
            full_address = (
                f"{prop['address']}, "
                f"{prop['city'] or 'Brookhaven'}, "
                f"{prop['state'] or 'NY'} "
                f"{prop['zip'] or ''}"
            )

            coords = self._geocode_census(full_address)
            if not coords:
                coords = self._geocode_nominatim(full_address)

            if coords:
                lat, lon = coords
                self.db.update_coordinates(prop["id"], lat, lon)
                success += 1
                logger.debug("Geocoded: %s -> (%f, %f)", full_address, lat, lon)
            else:
                failed += 1
                logger.debug("Failed to geocode: %s", full_address)

            time.sleep(0.5)

            if (success + failed) % 50 == 0:
                self.db.conn.commit()
                logger.info("Geocoded %d / %d so far (%d failed)",
                            success, success + failed, failed)

        self.db.conn.commit()
        logger.info("Geocoding complete: %d success, %d failed", success, failed)

    def _geocode_census(self, address: str) -> Optional[Tuple[float, float]]:
        """Use US Census Bureau geocoder (free, no key)."""
        try:
            params = {
                "address": address,
                "benchmark": "Public_AR_Current",
                "format": "json",
            }
            resp = requests.get(self.census_url, params=params, timeout=10)
            data = resp.json()
            matches = data.get("result", {}).get("addressMatches", [])
            if matches:
                coords = matches[0]["coordinates"]
                return (coords["y"], coords["x"])
        except Exception as e:
            logger.debug("Census geocode failed: %s", e)
        return None

    def _geocode_nominatim(self, address: str) -> Optional[Tuple[float, float]]:
        """Fallback to Nominatim."""
        try:
            location = self.geocode_nominatim(address)
            if location:
                return (location.latitude, location.longitude)
        except Exception as e:
            logger.debug("Nominatim geocode failed: %s", e)
        return None


# ──────────────────────────────────────────────
# Search API (Flask)
# ──────────────────────────────────────────────

def create_api(db: Database) -> Flask:
    """Create a Flask API for property search and lead capture."""

    app = Flask(__name__)
    CORS(app)

    @app.route("/api/health", methods=["GET"])
    def health():
        return jsonify({"status": "ok", "timestamp": datetime.utcnow().isoformat()})

    @app.route("/api/properties/search", methods=["GET"])
    def search_properties():
        q = request.args.get("q", "").strip()
        city = request.args.get("city", "").strip()
        zip_code = request.args.get("zip", "").strip()
        beds = safe_int(request.args.get("beds"))
        baths = safe_float(request.args.get("baths"))
        min_price = safe_int(request.args.get("min_price"))
        max_price = safe_int(request.args.get("max_price"))
        min_sqft = safe_int(request.args.get("min_sqft"))
        max_sqft = safe_int(request.args.get("max_sqft"))
        sw_lat = safe_float(request.args.get("sw_lat"))
        sw_lng = safe_float(request.args.get("sw_lng"))
        ne_lat = safe_float(request.args.get("ne_lat"))
        ne_lng = safe_float(request.args.get("ne_lng"))
        sort = request.args.get("sort", "address")
        order = request.args.get("order", "asc")
        page = max(1, safe_int(request.args.get("page")) or 1)
        limit = min(100, max(1, safe_int(request.args.get("limit")) or 20))

        conditions = []
        params = []

        if q:
            conditions.append("to_tsvector('english', address) @@ plainto_tsquery('english', %s)")
            params.append(q)
        if city:
            conditions.append("LOWER(city) = LOWER(%s)")
            params.append(city)
        if zip_code:
            conditions.append("zip = %s")
            params.append(zip_code)
        if beds is not None:
            conditions.append("beds >= %s")
            params.append(beds)
        if baths is not None:
            conditions.append("baths >= %s")
            params.append(baths)
        if min_price is not None:
            conditions.append("last_sale_price >= %s")
            params.append(min_price)
        if max_price is not None:
            conditions.append("last_sale_price <= %s")
            params.append(max_price)
        if min_sqft is not None:
            conditions.append("square_feet >= %s")
            params.append(min_sqft)
        if max_sqft is not None:
            conditions.append("square_feet <= %s")
            params.append(max_sqft)
        if all(v is not None for v in [sw_lat, sw_lng, ne_lat, ne_lng]):
            conditions.append("""
                latitude IS NOT NULL AND longitude IS NOT NULL
                AND latitude BETWEEN %s AND %s
                AND longitude BETWEEN %s AND %s
            """)
            params.extend([sw_lat, ne_lat, sw_lng, ne_lng])

        where_clause = " AND ".join(conditions) if conditions else "TRUE"

        sort_map = {
            "price": "last_sale_price",
            "beds": "beds",
            "baths": "baths",
            "sqft": "square_feet",
            "date": "last_sale_date",
            "address": "address",
            "assessed": "assessed_value",
        }
        sort_col = sort_map.get(sort, "address")
        sort_dir = "DESC" if order.lower() == "desc" else "ASC"

        offset = (page - 1) * limit

        count_result = db.fetch_one(
            f"SELECT COUNT(*) as total FROM properties WHERE {where_clause}",
            params,
        )
        total = count_result["total"] if count_result else 0

        results = db.fetch_all(f"""
            SELECT id, address, slug, city, state, zip, parcel_id,
                   latitude, longitude, lot_size, square_feet,
                   beds, baths, year_built, assessed_value,
                   last_sale_price, last_sale_date, property_class
            FROM properties
            WHERE {where_clause}
            ORDER BY {sort_col} {sort_dir} NULLS LAST
            LIMIT %s OFFSET %s
        """, params + [limit, offset])

        for r in results:
            if r.get("last_sale_date"):
                r["last_sale_date"] = str(r["last_sale_date"])

        return jsonify({
            "total": total,
            "page": page,
            "limit": limit,
            "total_pages": (total + limit - 1) // limit,
            "results": results,
        })

    @app.route("/api/properties/<slug>", methods=["GET"])
    def get_property(slug: str):
        prop = db.fetch_one("SELECT * FROM properties WHERE slug = %s", (slug,))
        if not prop:
            return jsonify({"error": "Property not found"}), 404

        for key in ["last_sale_date", "created_at", "updated_at"]:
            if prop.get(key):
                prop[key] = str(prop[key])

        nearby = []
        if prop.get("latitude") and prop.get("longitude"):
            nearby = db.fetch_all("""
                SELECT id, address, slug, city, zip, beds, baths,
                       square_feet, last_sale_price, last_sale_date,
                       latitude, longitude
                FROM properties
                WHERE id != %s
                  AND latitude BETWEEN %s - 0.01 AND %s + 0.01
                  AND longitude BETWEEN %s - 0.01 AND %s + 0.01
                ORDER BY ABS(latitude - %s) + ABS(longitude - %s) ASC
                LIMIT 10
            """, (
                prop["id"],
                prop["latitude"], prop["latitude"],
                prop["longitude"], prop["longitude"],
                prop["latitude"], prop["longitude"],
            ))
            for n in nearby:
                if n.get("last_sale_date"):
                    n["last_sale_date"] = str(n["last_sale_date"])

        return jsonify({"property": prop, "nearby": nearby})

    @app.route("/api/areas", methods=["GET"])
    def list_areas():
        areas = db.fetch_all("""
            SELECT a.id, a.name, a.slug, a.description, a.zip_codes,
                   COUNT(p.id) as property_count,
                   AVG(p.last_sale_price) as avg_sale_price,
                   AVG(p.assessed_value) as avg_assessed_value
            FROM areas a
            LEFT JOIN properties p ON p.zip = ANY(a.zip_codes)
            GROUP BY a.id
            ORDER BY a.name
        """)
        for a in areas:
            if a.get("avg_sale_price"):
                a["avg_sale_price"] = round(float(a["avg_sale_price"]))
            if a.get("avg_assessed_value"):
                a["avg_assessed_value"] = round(float(a["avg_assessed_value"]))
        return jsonify({"areas": areas})

    @app.route("/api/areas/<slug>", methods=["GET"])
    def get_area(slug: str):
        area = db.fetch_one("SELECT * FROM areas WHERE slug = %s", (slug,))
        if not area:
            return jsonify({"error": "Area not found"}), 404

        zip_codes = area.get("zip_codes", [])

        stats = db.fetch_one("""
            SELECT
                COUNT(*) as total_properties,
                AVG(last_sale_price) as avg_sale_price,
                AVG(assessed_value) as avg_assessed_value,
                AVG(square_feet) as avg_sqft,
                MIN(last_sale_price) as min_price,
                MAX(last_sale_price) as max_price
            FROM properties
            WHERE zip = ANY(%s)
        """, (zip_codes,))

        recent_sales = db.fetch_all("""
            SELECT id, address, slug, city, zip, beds, baths,
                   square_feet, last_sale_price, last_sale_date
            FROM properties
            WHERE zip = ANY(%s)
              AND last_sale_price IS NOT NULL
            ORDER BY last_sale_date DESC NULLS LAST
            LIMIT 20
        """, (zip_codes,))

        for s in recent_sales:
            if s.get("last_sale_date"):
                s["last_sale_date"] = str(s["last_sale_date"])

        for key in ["avg_sale_price", "avg_assessed_value", "avg_sqft"]:
            if stats and stats.get(key):
                stats[key] = round(float(stats[key]))

        return jsonify({
            "area": area,
            "stats": stats,
            "recent_sales": recent_sales,
        })

    @app.route("/api/leads", methods=["POST"])
    def create_lead():
        body = request.get_json()
        if not body:
            return jsonify({"error": "Request body required"}), 400

        name = body.get("name", "").strip()
        email = body.get("email", "").strip()

        if not name or not email:
            return jsonify({"error": "Name and email required"}), 400
        if not re.match(r'^[^@]+@[^@]+\.[^@]+$', email):
            return jsonify({"error": "Invalid email"}), 400

        lead = db.fetch_one("""
            INSERT INTO leads (name, email, phone, address, message, lead_type)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING id, name, email, lead_type, created_at
        """, (
            name,
            email,
            body.get("phone", "").strip() or None,
            body.get("address", "").strip() or None,
            body.get("message", "").strip() or None,
            body.get("lead_type", "general"),
        ))
        db.conn.commit()

        if lead and lead.get("created_at"):
            lead["created_at"] = str(lead["created_at"])

        logger.info("New lead: %s (%s) - %s", name, email, body.get("lead_type"))
        return jsonify({"success": True, "lead": lead}), 201

    @app.route("/api/stats", methods=["GET"])
    def pipeline_stats():
        raw_count = db.fetch_one("SELECT COUNT(*) as count FROM raw_records")
        prop_count = db.fetch_one("SELECT COUNT(*) as count FROM properties")
        lead_count = db.fetch_one("SELECT COUNT(*) as count FROM leads")
        geocoded = db.fetch_one("""
            SELECT COUNT(*) as count FROM properties
            WHERE latitude IS NOT NULL AND longitude IS NOT NULL
        """)

        # Include manifest info
        manifest = DownloadManifest()
        downloads = {}
        for source_name, entry in manifest.entries.items():
            downloads[source_name] = {
                "downloaded_at": entry.get("downloaded_at"),
                "file_size": entry.get("file_size"),
                "ingested": entry.get("ingested", False),
                "http_status": entry.get("http_status"),
            }

        return jsonify({
            "raw_records": raw_count["count"] if raw_count else 0,
            "properties": prop_count["count"] if prop_count else 0,
            "leads": lead_count["count"] if lead_count else 0,
            "geocoded": geocoded["count"] if geocoded else 0,
            "downloads": downloads,
        })

    return app


# ──────────────────────────────────────────────
# Utility Functions
# ──────────────────────────────────────────────

def safe_int(val) -> Optional[int]:
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None

def safe_float(val) -> Optional[float]:
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


# ──────────────────────────────────────────────
# Main CLI
# ──────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Brookhaven Property Data Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python pipeline.py --init                    Initialize database
    python pipeline.py --pull                    Pull data (skip cached)
    python pipeline.py --pull --force            Force re-download all
    python pipeline.py --pull --max-age 48       Re-download if older than 48h
    python pipeline.py --normalize               Normalize raw records
    python pipeline.py --geocode                 Geocode missing coords
    python pipeline.py --serve                   Start API server
    python pipeline.py --full                    Run everything
    python pipeline.py --full --force            Run everything, force downloads
    python pipeline.py --status                  Show download cache status
    python pipeline.py --clean-cache             Remove all cached downloads
    python pipeline.py --verify                  Verify all data source URLs
    python pipeline.py --discover                Search NY Open Data for datasets
        """
    )

    parser.add_argument("--init", action="store_true",
                        help="Initialize database tables")
    parser.add_argument("--pull", action="store_true",
                        help="Pull source data (skips if cached)")
    parser.add_argument("--normalize", action="store_true",
                        help="Normalize and deduplicate")
    parser.add_argument("--geocode", action="store_true",
                        help="Geocode missing coordinates")
    parser.add_argument("--geocode-limit", type=int, default=500,
                        help="Max properties to geocode (default: 500)")
    parser.add_argument("--serve", action="store_true",
                        help="Start search API")
    parser.add_argument("--port", type=int, default=8080,
                        help="API port (default: 8080)")
    parser.add_argument("--full", action="store_true",
                        help="Run full pipeline then serve")
    parser.add_argument("--force", action="store_true",
                        help="Force re-download even if cached")
    parser.add_argument("--max-age", type=float, default=DEFAULT_MAX_AGE_HOURS,
                        help=f"Max cache age in hours (default: {DEFAULT_MAX_AGE_HOURS})")
    parser.add_argument("--status", action="store_true",
                        help="Show download cache status")
    parser.add_argument("--clean-cache", action="store_true",
                        help="Remove all cached downloads and manifest")
    parser.add_argument("--clean-cache-source", type=str, default=None,
                        help="Remove cached download for a specific source")
    parser.add_argument("--verify", action="store_true",
                        help="Verify all configured data source URLs are working")
    parser.add_argument("--discover", action="store_true",
                        help="Search NY Open Data for property datasets")
    parser.add_argument("--discover-query", type=str,
                        default="real property suffolk",
                        help="Search query for dataset discovery (default: 'real property suffolk')")

    args = parser.parse_args()

    # ── Commands that don't need a database connection ──

    if args.status:
        show_cache_status()
        return

    if args.clean_cache:
        clean_all_cache()
        return

    if args.clean_cache_source:
        clean_source_cache(args.clean_cache_source)
        return

    if args.verify:
        verify_sources()
        return

    if args.discover:
        discover_datasets(args.discover_query)
        return

    # ── Commands that need a database connection ──

    db = Database(DATABASE_URL)
    db.connect()

    try:
        if args.init or args.full:
            db.init_tables()

        if args.pull or args.full:
            puller = DataPuller(
                db=db,
                force=args.force,
                max_age_hours=args.max_age,
            )
            puller.pull_all()

        if args.normalize or args.full:
            normalizer = PropertyNormalizer(db)
            normalizer.normalize_all()

        if args.geocode or args.full:
            geocoder = PropertyGeocoder(db)
            geocoder.geocode_all(limit=args.geocode_limit)

        if args.serve or args.full:
            app = create_api(db)
            logger.info("Starting API server on port %d", args.port)
            app.run(host="0.0.0.0", port=args.port, debug=False)

        if not any([args.init, args.pull, args.normalize,
                     args.geocode, args.serve, args.full]):
            parser.print_help()

    finally:
        db.close()


# ──────────────────────────────────────────────
# Verify Sources Command
# ──────────────────────────────────────────────

def verify_sources():
    """Verify all configured data source URLs are working."""
    discovery = DataSourceDiscovery()

    print("\n══════════════════════════════════════════════════")
    print("  Verifying Data Source Endpoints")
    print("══════════════════════════════════════════════════\n")

    pass_count = 0
    fail_count = 0
    warn_count = 0

    for source_name, config in DATA_SOURCES.items():
        source_type = config["type"]
        print(f"  ┌─ {source_name} ({source_type})")

        if source_type == "socrata_api":
            # Try primary URL
            url = config["base_url"]
            print(f"  │  Primary: {url}")
            result = discovery.verify_socrata_endpoint(url)

            if result["status"] == "ok":
                fields = result.get("sample_fields", [])
                print(f"  │  Status:  ✅ OK")
                print(f"  │  Fields:  {', '.join(fields[:8])}{'...' if len(fields) > 8 else ''}")
                pass_count += 1
            else:
                print(f"  │  Status:  ❌ FAILED — {result.get('message', 'unknown')}")
                fail_count += 1

                # Try fallbacks
                for fb_url in config.get("fallback_urls", []):
                    print(f"  │  Fallback: {fb_url}")
                    fb_result = discovery.verify_socrata_endpoint(fb_url)
                    if fb_result["status"] == "ok":
                        fields = fb_result.get("sample_fields", [])
                        print(f"  │  Status:   ✅ OK")
                        print(f"  │  Fields:   {', '.join(fields[:8])}")
                        print(f"  │  ⚠️  UPDATE base_url to this fallback!")
                        pass_count += 1
                        fail_count -= 1  # Override the primary failure
                        break
                    else:
                        print(f"  │  Status:   ❌ FAILED — {fb_result.get('message', '')}")

        elif source_type == "arcgis_api":
            url = config["layer_url"]
            where = config.get("where_clause", "1=1")
            print(f"  │  Primary:  {url}")
            print(f"  │  WHERE:    {where}")

            result = discovery.verify_arcgis_endpoint(url)

            if result["status"] == "ok":
                fields = result.get("sample_fields", [])
                print(f"  │  Status:   ✅ OK")
                print(f"  │  Fields:   {', '.join(fields[:8])}{'...' if len(fields) > 8 else ''}")
                pass_count += 1

                # Also verify the WHERE clause returns data
                try:
                    params = {
                        "where": where,
                        "outFields": "*",
                        "resultRecordCount": 1,
                        "f": "json",
                    }
                    resp = requests.get(url, params=params, timeout=15)
                    data = resp.json()
                    features = data.get("features", [])
                    if features:
                        print(f"  │  Filter:   ✅ WHERE clause returns data")
                    else:
                        print(f"  │  Filter:   ⚠️  WHERE clause returned 0 features")
                        warn_count += 1

                        # Try fallback WHERE clauses
                        for fb_where in config.get("fallback_where_clauses", []):
                            params["where"] = fb_where
                            resp2 = requests.get(url, params=params, timeout=15)
                            data2 = resp2.json()
                            if data2.get("features"):
                                print(f"  │  Alt filter: ✅ '{fb_where}' works")
                                print(f"  │  ⚠️  UPDATE where_clause to: {fb_where}")
                                break

                except Exception as e:
                    print(f"  │  Filter:   ⚠️  Could not test WHERE: {e}")
                    warn_count += 1
            else:
                print(f"  │  Status:   ❌ FAILED — {result.get('message', 'unknown')}")
                fail_count += 1

                # Try fallback layer URLs
                for fb_url in config.get("fallback_layer_urls", []):
                    print(f"  │  Fallback: {fb_url}")
                    fb_result = discovery.verify_arcgis_endpoint(fb_url)
                    if fb_result["status"] == "ok":
                        print(f"  │  Status:   ✅ OK")
                        print(f"  │  ⚠️  UPDATE layer_url to this fallback!")
                        pass_count += 1
                        fail_count -= 1
                        break
                    else:
                        print(f"  │  Status:   ❌ FAILED — {fb_result.get('message', '')}")

        elif source_type == "csv_download":
            url = config["url"]
            print(f"  │  URL: {url}")
            try:
                resp = requests.head(url, timeout=10, allow_redirects=True)
                if resp.status_code == 200:
                    size = resp.headers.get("Content-Length", "unknown")
                    ctype = resp.headers.get("Content-Type", "unknown")
                    print(f"  │  Status:  ✅ OK (size: {size}, type: {ctype})")
                    pass_count += 1
                elif resp.status_code == 404:
                    print(f"  │  Status:  ❌ 404 Not Found")
                    fail_count += 1
                else:
                    print(f"  │  Status:  ⚠️  HTTP {resp.status_code}")
                    warn_count += 1
            except Exception as e:
                print(f"  │  Status:  ❌ {e}")
                fail_count += 1

        elif source_type == "local_csv":
            path = config["path"]
            print(f"  │  Path: {path}")
            if os.path.exists(path):
                size = os.path.getsize(path)
                print(f"  │  Status:  ✅ File exists ({size:,} bytes)")
                pass_count += 1
            else:
                print(f"  │  Status:  ⚠️  File not found (optional)")
                warn_count += 1

        elif source_type == "portal_check":
            urls = config.get("check_urls", [])
            for url in urls:
                print(f"  │  URL: {url}")
                try:
                    resp = requests.get(url, timeout=10, headers={
                        "User-Agent": "BrookhavenPropertyPipeline/1.0"
                    })
                    if resp.status_code == 200:
                        print(f"  │  Status:  ✅ Reachable")
                        pass_count += 1
                    else:
                        print(f"  │  Status:  ⚠️  HTTP {resp.status_code}")
                        warn_count += 1
                except Exception as e:
                    print(f"  │  Status:  ❌ {e}")
                    fail_count += 1

        print(f"  └────────────────────────────────────\n")

    # Summary
    print("══════════════════════════════════════════════════")
    print(f"  ✅ Passed:   {pass_count}")
    print(f"  ⚠️  Warnings: {warn_count}")
    print(f"  ❌ Failed:   {fail_count}")
    print("══════════════════════════════════════════════════")

    if fail_count > 0:
        print("\n  Some sources are broken. Try:")
        print("    python pipeline.py --discover")
        print("  to find replacement dataset IDs.\n")
    else:
        print("\n  All sources look good! Run:")
        print("    python pipeline.py --full\n")


# ──────────────────────────────────────────────
# Discover Datasets Command
# ──────────────────────────────────────────────

def discover_datasets(query: str):
    """Search NY Open Data for relevant datasets."""
    discovery = DataSourceDiscovery()

    print(f"\n  Searching NY Open Data for: '{query}'\n")

    results = discovery.search_ny_datasets(query)

    if not results:
        print("  No datasets found. Try different search terms:")
        print("    python pipeline.py --discover --discover-query 'property tax'")
        print("    python pipeline.py --discover --discover-query 'assessment roll'")
        print("    python pipeline.py --discover --discover-query 'real estate sales'")
        print()
        return

    print(f"  Found {len(results)} datasets:\n")

    for i, r in enumerate(results, 1):
        print(f"  {i}. {r['name']}")
        print(f"     ID:       {r['id']}")
        print(f"     Type:     {r['type']}")
        print(f"     Updated:  {r['updated_at']}")
        print(f"     API URL:  {r['url']}")
        if r.get('page_url'):
            print(f"     Page:     {r['page_url']}")
        print(f"     Desc:     {r['description'][:150]}")

        # Quick verify
        check = discovery.verify_socrata_endpoint(r['url'])
        if check["status"] == "ok":
            fields = check.get("sample_fields", [])
            print(f"     Status:   ✅ LIVE")
            print(f"     Fields:   {', '.join(fields[:8])}")
        else:
            print(f"     Status:   ❌ Not accessible")

        print()

    print("  To use a dataset, update DATA_SOURCES in pipeline.py:")
    print("    \"base_url\": \"https://data.ny.gov/resource/<ID>.json\"")
    print()
    print("  Useful additional searches:")
    print("    python pipeline.py --discover --discover-query 'real property sales'")
    print("    python pipeline.py --discover --discover-query 'assessment roll suffolk'")
    print("    python pipeline.py --discover --discover-query 'property transfer'")
    print("    python pipeline.py --discover --discover-query 'tax parcel'")
    print()


# ──────────────────────────────────────────────
# Cache Management Commands
# ──────────────────────────────────────────────

def show_cache_status():
    """Display current download cache status."""
    manifest = DownloadManifest()

    if not manifest.entries:
        print("\n  No downloads cached yet.\n")
        print("  Run: python pipeline.py --pull")
        return

    print("\n══════════════════════════════════════════════════════════════")
    print("  Brookhaven Pipeline — Download Cache Status")
    print("══════════════════════════════════════════════════════════════\n")

    for source_name, entry in manifest.entries.items():
        filepath = entry.get("filepath", "?")
        file_exists = os.path.exists(filepath) if filepath else False
        file_size = entry.get("file_size", 0)
        downloaded_at = entry.get("downloaded_at", "never")
        last_check = entry.get("last_check_at", "never")
        ingested = entry.get("ingested", False)
        ingested_at = entry.get("ingested_at", "never")
        http_status = entry.get("http_status", "—")
        etag = entry.get("etag", "none")
        last_modified = entry.get("last_modified", "none")
        checksum = (entry.get("file_checksum") or "none")[:16]

        # Calculate age
        age_str = "unknown"
        is_fresh = False
        try:
            dt = datetime.fromisoformat(downloaded_at)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            age = datetime.now(timezone.utc) - dt
            hours = age.total_seconds() / 3600
            if hours < 1:
                age_str = f"{int(age.total_seconds() / 60)} minutes"
            elif hours < 24:
                age_str = f"{hours:.1f} hours"
            else:
                age_str = f"{hours / 24:.1f} days"
            is_fresh = hours <= DEFAULT_MAX_AGE_HOURS
        except (ValueError, TypeError):
            pass

        fresh_icon = "🟢" if is_fresh else "🔴"
        exists_icon = "✓" if file_exists else "✗"
        ingested_icon = "✓" if ingested else "✗"

        # Check if file on disk matches recorded size
        size_match = "?"
        if file_exists:
            actual_size = os.path.getsize(filepath)
            size_match = "✓" if actual_size == file_size else f"✗ (disk: {actual_size:,})"

        print(f"  ┌─ {source_name}")
        print(f"  │  File:          {filepath}")
        print(f"  │  File exists:   {exists_icon}    Size match: {size_match}")
        print(f"  │  Size:          {file_size:,} bytes")
        print(f"  │  Checksum:      {checksum}")
        print(f"  │  Downloaded:    {downloaded_at}")
        print(f"  │  Age:           {age_str}  {fresh_icon}")
        print(f"  │  Last check:    {last_check}")
        print(f"  │  HTTP status:   {http_status}")
        print(f"  │  ETag:          {etag}")
        print(f"  │  Last-Modified: {last_modified}")
        print(f"  │  Ingested:      {ingested_icon}  ({ingested_at})")
        print(f"  └────────────────────────────────────\n")

    # Summary
    total = len(manifest.entries)
    cached = sum(
        1 for e in manifest.entries.values()
        if os.path.exists(e.get("filepath", ""))
    )
    ingested_count = sum(
        1 for e in manifest.entries.values()
        if e.get("ingested", False)
    )
    total_size = sum(
        e.get("file_size", 0) for e in manifest.entries.values()
    )

    print("  Summary:")
    print(f"    Sources:        {total}")
    print(f"    Files cached:   {cached}/{total}")
    print(f"    Ingested:       {ingested_count}/{total}")
    print(f"    Total size:     {total_size:,} bytes ({total_size / 1024 / 1024:.1f} MB)")
    print(f"    Manifest file:  {MANIFEST_PATH}")
    print(f"    Data directory: {DATA_DIR}")
    print()


def clean_all_cache():
    """Remove all cached downloads and the manifest."""
    manifest = DownloadManifest()

    if not manifest.entries:
        print("  No cached downloads to clean.")
        return

    print("\n  Cleaning all cached downloads...\n")

    removed_files = 0
    removed_bytes = 0

    for source_name, entry in manifest.entries.items():
        filepath = entry.get("filepath", "")
        if filepath and os.path.exists(filepath):
            file_size = os.path.getsize(filepath)
            os.remove(filepath)
            removed_files += 1
            removed_bytes += file_size
            print(f"    Removed: {filepath} ({file_size:,} bytes)")
        else:
            print(f"    Skipped: {filepath} (not found)")

    # Remove manifest
    if os.path.exists(MANIFEST_PATH):
        os.remove(MANIFEST_PATH)
        print(f"\n    Removed manifest: {MANIFEST_PATH}")

    print(f"\n  Done. Removed {removed_files} files ({removed_bytes:,} bytes)")
    print("  Next run of --pull will re-download everything.\n")


def clean_source_cache(source_name: str):
    """Remove cached download for a specific source."""
    manifest = DownloadManifest()
    entry = manifest.get(source_name)

    if not entry:
        print(f"\n  Source '{source_name}' not found in manifest.")
        print("  Available sources:")
        for name in manifest.entries:
            print(f"    - {name}")
        print()
        return

    filepath = entry.get("filepath", "")
    if filepath and os.path.exists(filepath):
        file_size = os.path.getsize(filepath)
        os.remove(filepath)
        print(f"\n  Removed: {filepath} ({file_size:,} bytes)")
    else:
        print(f"\n  File not found on disk: {filepath}")

    # Remove from manifest
    del manifest.entries[source_name]
    manifest.save()
    print(f"  Removed '{source_name}' from manifest.")
    print(f"  Next run of --pull will re-download this source.\n")


# ──────────────────────────────────────────────
# Entry Point
# ──────────────────────────────────────────────

if __name__ == "__main__":
    main()
