"""
analyze_farm.py — Zipcode Farm Analysis for Real Estate Brokers
===============================================================
Queries the Brookhaven property database and ranks every zipcode by how
attractive it is to "farm" as a listing agent.

Metrics calculated per zipcode:
  - Total homes
  - Homes sold in the last 12 months  (turnover count)
  - Turnover rate  = sold_last_12mo / total_homes
  - Average & median sale price
  - Average days on market  (from MLS data if available)
  - Unique listing agents active in the last 12 months  (from MLS)
  - Agent-to-listing ratio  = active_agents / homes_sold_last_12mo
  - Commission Opportunity Score  = (turnover_rate × avg_price × total_homes) / max(active_agents, 1)
  - Rank  (by Commission Opportunity Score, descending)

Usage:
    python analyze_farm.py                       # analyze all Brookhaven zips
    python analyze_farm.py --months 24           # extend lookback window
    python analyze_farm.py --out results.csv     # custom output path
    python analyze_farm.py --min-homes 100       # skip tiny zips

Output:
    Prints a ranked summary table to the terminal.
    Saves full results to farm_analysis.csv (or --out path).

Requirements:
    DATABASE_URL must be set in the environment (same as pipeline.py).
    Run  python pipeline.py --pull --normalize  first to populate the database.
"""

import os
import sys
import csv
import argparse
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional

try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    print("psycopg2 not found. Run:  pip install psycopg2-binary")
    sys.exit(1)

# ──────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/brookhaven"
)

DEFAULT_LOOKBACK_MONTHS = 12
DEFAULT_OUTPUT_PATH = "farm_analysis.csv"
DEFAULT_MIN_HOMES = 50   # skip zips with too few records to be meaningful

# Estimated buyer-side commission rate used in opportunity score (3% typical)
COMMISSION_RATE = 0.03


# ──────────────────────────────────────────────
# Database helpers
# ──────────────────────────────────────────────

def connect() -> psycopg2.extensions.connection:
    try:
        conn = psycopg2.connect(DATABASE_URL)
        conn.autocommit = True
        return conn
    except psycopg2.OperationalError as e:
        print(f"\nCould not connect to database: {e}")
        print("Check that DATABASE_URL is set and PostgreSQL is running.\n")
        sys.exit(1)


def fetch(conn, query: str, params=None) -> List[Dict]:
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, params)
            return [dict(row) for row in cur.fetchall()]
    except psycopg2.errors.UndefinedColumn as e:
        print(f"\nDatabase schema is out of date: {e}")
        print("Run  python pipeline.py --init  to add missing columns, then retry.\n")
        sys.exit(1)
    except psycopg2.Error as e:
        print(f"\nDatabase query failed: {e}")
        sys.exit(1)


# ──────────────────────────────────────────────
# Analysis queries
# ──────────────────────────────────────────────

def analyze_zipcodes(
    conn,
    lookback_months: int,
    min_homes: int,
) -> List[Dict]:
    """Return one dict per zipcode with all farm metrics."""

    cutoff = datetime.now(timezone.utc) - timedelta(days=lookback_months * 30.44)
    cutoff_date = cutoff.date()

    # ── Base property counts and values per zip ──
    base_rows = fetch(conn, """
        SELECT
            zip,
            COUNT(*)                                            AS total_homes,
            COUNT(*) FILTER (
                WHERE last_sale_date >= %s
                  AND last_sale_price IS NOT NULL
                  AND last_sale_price > 0
            )                                                   AS sold_in_window,
            AVG(last_sale_price) FILTER (
                WHERE last_sale_date >= %s
                  AND last_sale_price IS NOT NULL
                  AND last_sale_price > 0
            )                                                   AS avg_sale_price,
            PERCENTILE_CONT(0.5) WITHIN GROUP (
                ORDER BY last_sale_price
            ) FILTER (
                WHERE last_sale_date >= %s
                  AND last_sale_price IS NOT NULL
                  AND last_sale_price > 0
            )                                                   AS median_sale_price,
            AVG(days_on_market) FILTER (
                WHERE days_on_market IS NOT NULL
                  AND days_on_market > 0
                  AND listing_date >= %s
            )                                                   AS avg_dom,
            COUNT(DISTINCT list_agent_name) FILTER (
                WHERE list_agent_name IS NOT NULL
                  AND listing_date >= %s
            )                                                   AS active_agents,
            -- Assessed value as price proxy until deed/MLS data arrives
            AVG(assessed_value) FILTER (
                WHERE assessed_value IS NOT NULL
                  AND assessed_value > 0
            )                                                   AS avg_assessed_value,
            PERCENTILE_CONT(0.5) WITHIN GROUP (
                ORDER BY assessed_value
            ) FILTER (
                WHERE assessed_value IS NOT NULL
                  AND assessed_value > 0
            )                                                   AS median_assessed_value
        FROM properties
        WHERE zip IS NOT NULL
          AND zip != ''
        GROUP BY zip
        HAVING COUNT(*) >= %s
        ORDER BY zip
    """, (
        cutoff_date, cutoff_date, cutoff_date, cutoff_date, cutoff_date,
        min_homes,
    ))

    results = []
    for row in base_rows:
        zip_code       = row["zip"]
        total_homes    = int(row["total_homes"] or 0)
        sold_in_window = int(row["sold_in_window"] or 0)
        avg_dom        = float(row["avg_dom"]) if row["avg_dom"] else None
        active_agents  = int(row["active_agents"] or 0)

        # Use actual sale prices when available; fall back to assessed value
        has_sale_prices = bool(row["avg_sale_price"])
        avg_price    = float(row["avg_sale_price"] or row["avg_assessed_value"] or 0)
        median_price = float(row["median_sale_price"] or row["median_assessed_value"] or 0)
        price_is_estimate = not has_sale_prices and avg_price > 0

        # Annualise the turnover rate if the lookback isn't exactly 12 months.
        # When no sale dates exist, use a market-standard 7% as a placeholder
        # so the opportunity score is still meaningful for relative ranking.
        annualisation = 12.0 / lookback_months
        if sold_in_window > 0:
            turnover_rate = sold_in_window / total_homes * annualisation
        else:
            turnover_rate = 0.07  # industry benchmark placeholder

        agent_to_listing = (
            round(active_agents / sold_in_window, 2) if sold_in_window > 0 else None
        )

        # Commission Opportunity Score
        # = estimated total commissions available ÷ number of competing agents
        #   = (turnover_rate × avg_price × total_homes × commission_rate) ÷ max(agents, 1)
        gross_commission  = turnover_rate * avg_price * total_homes * COMMISSION_RATE
        opportunity_score = gross_commission / max(active_agents, 1)

        results.append({
            "zip":                zip_code,
            "total_homes":        total_homes,
            "sold_last_period":   sold_in_window,
            "turnover_rate_pct":  round(turnover_rate * 100, 2),
            "avg_sale_price":     int(avg_price) if avg_price else None,
            "median_sale_price":  int(median_price) if median_price else None,
            "avg_days_on_market": round(avg_dom, 1) if avg_dom is not None else None,
            "active_agents":      active_agents if active_agents > 0 else None,
            "agent_to_listing_ratio": agent_to_listing,
            "commission_opportunity_score": int(opportunity_score),
            "_price_is_estimate": price_is_estimate,
            "_turnover_is_estimate": sold_in_window == 0,
        })

    # Determine whether we have any sales data at all
    has_sales_data = any(r["sold_last_period"] > 0 for r in results)

    # Always sort by opportunity score — even estimated scores give a useful ranking
    results.sort(
        key=lambda r: (r["commission_opportunity_score"], r["turnover_rate_pct"]),
        reverse=True,
    )

    for i, r in enumerate(results, start=1):
        r["rank"] = i

    return results


# ──────────────────────────────────────────────
# Output
# ──────────────────────────────────────────────

COLUMNS = [
    "rank",
    "zip",
    "total_homes",
    "sold_last_period",
    "turnover_rate_pct",
    "avg_sale_price",
    "median_sale_price",
    "avg_days_on_market",
    "active_agents",
    "agent_to_listing_ratio",
    "commission_opportunity_score",
]

HEADERS = {
    "rank":                        "Rank",
    "zip":                         "Zip",
    "total_homes":                 "Total Homes",
    "sold_last_period":            "Sold (Period)",
    "turnover_rate_pct":           "Turnover %",
    "avg_sale_price":              "Avg Sale Price",
    "median_sale_price":           "Median Sale Price",
    "avg_days_on_market":          "Avg DOM",
    "active_agents":               "Active Agents",
    "agent_to_listing_ratio":      "Agent/Listing Ratio",
    "commission_opportunity_score":"Opportunity Score",
}


def fmt(value, key: str) -> str:
    if value is None:
        return "—"
    if key in ("avg_sale_price", "median_sale_price", "commission_opportunity_score"):
        return f"${value:,}"
    if key == "turnover_rate_pct":
        return f"{value}%"
    return str(value)


def print_table(rows: List[Dict], lookback_months: int):
    if not rows:
        print("\nNo zipcodes found. Run  python pipeline.py --pull --normalize  first.\n")
        return

    has_sales_data    = any(r["sold_last_period"] > 0 for r in rows)
    has_price_est     = any(r.get("_price_is_estimate") for r in rows)
    has_turnover_est  = any(r.get("_turnover_is_estimate") for r in rows)

    col_widths = {k: len(HEADERS[k]) for k in COLUMNS}
    for row in rows:
        for k in COLUMNS:
            col_widths[k] = max(col_widths[k], len(fmt(row.get(k), k)))

    header_line = "  ".join(HEADERS[k].ljust(col_widths[k]) for k in COLUMNS)
    sep_line    = "  ".join("-" * col_widths[k] for k in COLUMNS)

    print(f"\n{'=' * len(sep_line)}")
    print(f"  Brookhaven Zipcode Farm Analysis  "
          f"(lookback: {lookback_months} months, "
          f"commission rate: {int(COMMISSION_RATE*100)}%)")
    if has_price_est:
        print(f"  * Avg/Median Sale Price = assessed value estimate (no deed/MLS data yet)")
    if has_turnover_est:
        print(f"  * Turnover % = 7% industry benchmark (no sale dates yet)")
    if has_price_est or has_turnover_est:
        print(f"    Scores are for relative ranking only. Connect deed/MLS data for actuals.")
    print(f"{'=' * len(sep_line)}\n")
    print(header_line)
    print(sep_line)
    for row in rows:
        print("  ".join(fmt(row.get(k), k).ljust(col_widths[k]) for k in COLUMNS))
    print()


def write_csv(rows: List[Dict], output_path: str, lookback_months: int):
    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS, extrasaction="ignore")
        writer.writerow(HEADERS)  # human-readable header row
        writer.writerows(rows)
    print(f"Results saved to {output_path}  ({len(rows)} zipcodes, "
          f"{lookback_months}-month lookback)\n")


# ──────────────────────────────────────────────
# Zip listing
# ──────────────────────────────────────────────

ZIP_LISTING_COLUMNS = [
    "address", "city", "zip", "owner_name", "property_class",
    "assessed_value", "full_market_value",
    "beds", "baths", "half_baths", "square_feet", "year_built",
    "lot_size", "last_sale_price", "last_sale_date",
    "listing_status", "list_price", "days_on_market",
    "latitude", "longitude",
]

ZIP_LISTING_HEADERS = {
    "address":          "Address",
    "city":             "City",
    "zip":              "Zip",
    "owner_name":       "Owner",
    "property_class":   "Class",
    "assessed_value":   "Assessed Value",
    "full_market_value":"Market Value",
    "beds":             "Beds",
    "baths":            "Baths",
    "half_baths":       "Half Baths",
    "square_feet":      "Sqft",
    "year_built":       "Year Built",
    "lot_size":         "Lot Size",
    "last_sale_price":  "Last Sale Price",
    "last_sale_date":   "Last Sale Date",
    "listing_status":   "MLS Status",
    "list_price":       "List Price",
    "days_on_market":   "DOM",
    "latitude":         "Lat",
    "longitude":        "Lng",
}


def list_zip(conn, zip_code: str) -> List[Dict]:
    """Return all properties in a zipcode, ordered by address."""
    # full_market_value lives in raw_data; pull it via a lateral join if available
    rows = fetch(conn, """
        SELECT
            p.address,
            p.city,
            p.zip,
            p.owner_name,
            p.property_class,
            p.assessed_value,
            p.beds,
            p.baths,
            p.half_baths,
            p.square_feet,
            p.year_built,
            p.lot_size,
            p.last_sale_price,
            p.last_sale_date,
            p.listing_status,
            p.list_price,
            p.days_on_market,
            p.latitude,
            p.longitude,
            CAST(
                COALESCE(
                    (SELECT r.raw_data->>'full_market_value'
                     FROM raw_records r
                     WHERE r.raw_data->>'print_key_code' = p.parcel_id
                     LIMIT 1),
                    NULL
                ) AS NUMERIC
            ) AS full_market_value
        FROM properties p
        WHERE p.zip = %s
        ORDER BY p.address
    """, (zip_code,))
    return rows


def print_zip_summary(rows: List[Dict], zip_code: str):
    """Print a brief summary header before the CSV is saved."""
    if not rows:
        print(f"\nNo properties found for zip {zip_code}.\n")
        return

    assessed   = [r["assessed_value"] for r in rows if r.get("assessed_value")]
    sale_prices = [r["last_sale_price"] for r in rows if r.get("last_sale_price")]

    print(f"\nZip {zip_code}  —  {len(rows):,} properties")
    if assessed:
        print(f"  Avg assessed value:  ${int(sum(assessed)/len(assessed)):,}")
    if sale_prices:
        print(f"  Avg last sale price: ${int(sum(sale_prices)/len(sale_prices)):,}")
        print(f"  Sales on record:     {len(sale_prices):,}")
    print()


def write_zip_csv(rows: List[Dict], zip_code: str, output_path: str):
    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(
            f, fieldnames=ZIP_LISTING_COLUMNS, extrasaction="ignore"
        )
        writer.writerow(ZIP_LISTING_HEADERS)
        writer.writerows(rows)
    print(f"Saved {len(rows):,} properties → {output_path}\n")


# ──────────────────────────────────────────────
# Entrypoint
# ──────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Rank Brookhaven zipcodes by farm attractiveness, or list all properties in a zip.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python analyze_farm.py                         Rank all zips, save to farm_analysis.csv
    python analyze_farm.py --months 24             Use a 24-month lookback window
    python analyze_farm.py --out report.csv        Custom output filename
    python analyze_farm.py --min-homes 200         Skip zips with fewer than 200 properties
    python analyze_farm.py --zip 11772             List all properties in zip 11772
    python analyze_farm.py --zip 11772 --out patchogue.csv   Custom output path
        """,
    )
    parser.add_argument(
        "--months", type=int, default=DEFAULT_LOOKBACK_MONTHS,
        help=f"Lookback window in months for sales/agent activity (default: {DEFAULT_LOOKBACK_MONTHS})",
    )
    parser.add_argument(
        "--out", type=str, default=None,
        help="Output CSV path (default: farm_analysis.csv or <zip>.csv with --zip)",
    )
    parser.add_argument(
        "--min-homes", type=int, default=DEFAULT_MIN_HOMES,
        help=f"Minimum homes required to include a zipcode (default: {DEFAULT_MIN_HOMES})",
    )
    parser.add_argument(
        "--zip", type=str, default=None,
        help="List all properties in a specific zipcode and save to CSV",
    )

    args = parser.parse_args()
    conn = connect()

    # ── Single zip listing mode ──
    if args.zip:
        zip_code    = args.zip.strip()
        output_path = args.out or f"{zip_code}.csv"
        print(f"Querying properties for zip {zip_code}...")
        rows = list_zip(conn, zip_code)
        conn.close()
        print_zip_summary(rows, zip_code)
        if rows:
            write_zip_csv(rows, zip_code, output_path)
        return

    # ── Farm ranking mode ──
    output_path = args.out or DEFAULT_OUTPUT_PATH
    print(f"Querying database... (lookback: {args.months} months, "
          f"min homes: {args.min_homes})")

    rows = analyze_zipcodes(conn, args.months, args.min_homes)
    conn.close()

    print_table(rows, args.months)
    write_csv(rows, output_path, args.months)

    if rows:
        top = rows[0]
        est = " (estimated)" if top.get("_price_is_estimate") else ""
        avg_price_str = f"avg price ${top['avg_sale_price']:,}{est}, " if top["avg_sale_price"] else ""
        print(f"Top zip to farm: {top['zip']}  "
              f"(turnover {top['turnover_rate_pct']}%, "
              f"{avg_price_str}"
              f"opportunity score ${top['commission_opportunity_score']:,})")
        print()


if __name__ == "__main__":
    main()
