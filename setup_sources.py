#!/usr/bin/env python3
"""
setup_sources.py
================
Inspects the working datasets, finds the correct filter field names,
and generates a ready-to-paste DATA_SOURCES config for pipeline.py.

Usage: python setup_sources.py
"""

import requests
import json
import time


def inspect_dataset(name, url):
    """Get full field list and sample data from a Socrata dataset."""
    print(f"\n{'='*60}")
    print(f"  Inspecting: {name}")
    print(f"  URL: {url}")
    print(f"{'='*60}\n")

    # Get sample rows
    resp = requests.get(url, params={"$limit": 3}, timeout=15)
    if resp.status_code != 200:
        print(f"  ❌ HTTP {resp.status_code}")
        return None

    data = resp.json()
    if not data:
        print(f"  ❌ Empty")
        return None

    fields = list(data[0].keys())
    print(f"  Total fields: {len(fields)}\n")

    print(f"  All fields and sample values:")
    print(f"  {'─'*55}")
    for key in sorted(fields):
        val = data[0].get(key, "")
        print(f"  {key:45s} {str(val)[:60]}")
    print(f"  {'─'*55}")

    # Find the right county/municipality filter fields
    print(f"\n  Looking for filter fields...\n")

    filter_info = {}

    for field in fields:
        field_lower = field.lower()

        if "county" in field_lower:
            # Get distinct values
            try:
                vresp = requests.get(url, params={
                    "$select": f"distinct {field}",
                    "$limit": 30,
                    "$order": field,
                }, timeout=10)
                if vresp.status_code == 200:
                    vals = sorted(set(
                        str(row.get(field, "")).strip()
                        for row in vresp.json()
                        if row.get(field)
                    ))
                    suffolk_vals = [v for v in vals if "SUFFOLK" in v.upper()]
                    print(f"  County field: '{field}'")
                    print(f"    Sample values: {vals[:10]}")
                    if suffolk_vals:
                        print(f"    ✅ Suffolk value: {suffolk_vals[0]}")
                        filter_info["county_field"] = field
                        filter_info["county_value"] = suffolk_vals[0]
                    else:
                        print(f"    ⚠️  No Suffolk match in values")
            except Exception as e:
                print(f"    Error checking {field}: {e}")

        if "municip" in field_lower or "town" in field_lower or "muni" in field_lower:
            try:
                # First filter to Suffolk, then get municipalities
                params = {
                    "$select": f"distinct {field}",
                    "$limit": 50,
                    "$order": field,
                }
                if filter_info.get("county_field"):
                    params["$where"] = f"{filter_info['county_field']} = '{filter_info['county_value']}'"

                vresp = requests.get(url, params=params, timeout=10)
                if vresp.status_code == 200:
                    vals = sorted(set(
                        str(row.get(field, "")).strip()
                        for row in vresp.json()
                        if row.get(field)
                    ))
                    brookhaven_vals = [v for v in vals if "BROOKHAVEN" in v.upper()]
                    print(f"  Municipality field: '{field}'")
                    print(f"    Sample values: {vals[:15]}")
                    if brookhaven_vals:
                        print(f"    ✅ Brookhaven value: {brookhaven_vals[0]}")
                        filter_info["muni_field"] = field
                        filter_info["muni_value"] = brookhaven_vals[0]
                    else:
                        print(f"    ⚠️  No Brookhaven match")
            except Exception as e:
                print(f"    Error checking {field}: {e}")

    # Test the filter
    if filter_info.get("county_field"):
        where_parts = []
        where_parts.append(
            f"{filter_info['county_field']} = '{filter_info['county_value']}'"
        )
        if filter_info.get("muni_field"):
            where_parts.append(
                f"{filter_info['muni_field']} = '{filter_info['muni_value']}'"
            )

        where = " AND ".join(where_parts)
        print(f"\n  Testing filter: $where={where}")

        try:
            tresp = requests.get(url, params={
                "$where": where,
                "$limit": 3,
            }, timeout=15)
            if tresp.status_code == 200:
                filtered = tresp.json()
                print(f"  ✅ Filter works! Got {len(filtered)} sample rows")
                if filtered:
                    print(f"\n  Sample Brookhaven property:")
                    print(f"  {'─'*55}")
                    for key in sorted(filtered[0].keys()):
                        val = filtered[0].get(key, "")
                        print(f"  {key:45s} {str(val)[:60]}")

                # Get total count
                cresp = requests.get(url, params={
                    "$select": "count(*)",
                    "$where": where,
                }, timeout=15)
                if cresp.status_code == 200:
                    count_data = cresp.json()
                    if count_data:
                        total = count_data[0].get("count", "?")
                        print(f"\n  📊 Total Brookhaven records: {total}")
                        filter_info["total_count"] = total

            else:
                print(f"  ❌ Filter failed: HTTP {tresp.status_code}")
        except Exception as e:
            print(f"  ❌ Filter error: {e}")

    return {
        "url": url,
        "fields": fields,
        "sample": data[0],
        "filter_info": filter_info,
    }


def main():
    print("\n" + "=" * 60)
    print("  Brookhaven Data Source Setup")
    print("  Inspecting working datasets to find correct filters")
    print("=" * 60)

    results = {}

    # ── Dataset 1: Assessment Roll (7vem-aaz7) ──
    r1 = inspect_dataset(
        "Property Assessment Data from Local Assessment Rolls",
        "https://data.ny.gov/resource/7vem-aaz7.json",
    )
    if r1:
        results["assessment"] = r1

    time.sleep(1)

    # ── Dataset 2: Residential Assessment Ratios (8vgb-zm6e) ──
    r2 = inspect_dataset(
        "Residential Assessment Ratios",
        "https://data.ny.gov/resource/8vgb-zm6e.json",
    )
    if r2:
        results["ratios"] = r2

    # ── Generate DATA_SOURCES ──
    print(f"\n\n{'='*60}")
    print(f"  GENERATED DATA_SOURCES")
    print(f"  Replace the DATA_SOURCES dict in pipeline.py with this:")
    print(f"{'='*60}\n")

    print("DATA_SOURCES = {")

    if "assessment" in results:
        fi = results["assessment"]["filter_info"]
        county_field = fi.get("county_field", "county_name")
        county_value = fi.get("county_value", "SUFFOLK")
        muni_field = fi.get("muni_field", "municipality_name")
        muni_value = fi.get("muni_value", "BROOKHAVEN")
        total = fi.get("total_count", "?")

        print(f'    # Assessment Roll — {total} Brookhaven records')
        print(f'    "nys_assessment": {{')
        print(f'        "type": "socrata_api",')
        print(f'        "base_url": "https://data.ny.gov/resource/7vem-aaz7.json",')
        print(f'        "fallback_urls": [],')
        print(f'        "description": "NYS Property Assessment Data from Local Assessment Rolls",')
        print(f'        "filename": "nys_assessment.json",')
        print(f'        "filters": {{')
        print(f'            "{county_field}": "{county_value}",')
        if muni_field:
            print(f'            "{muni_field}": "{muni_value}",')
        print(f'        }},')
        print(f'        "brookhaven_filter": False,  # Server-side filter handles it')
        print(f'        "page_size": 50000,')
        print(f'    }},')
        print()

    if "ratios" in results:
        fi = results["ratios"]["filter_info"]
        # This dataset might be NYC-only; the output will show
        print(f'    # Residential Assessment Ratios')
        print(f'    "nys_ratios": {{')
        print(f'        "type": "socrata_api",')
        print(f'        "base_url": "https://data.ny.gov/resource/8vgb-zm6e.json",')
        print(f'        "fallback_urls": [],')
        print(f'        "description": "Residential Assessment Ratios",')
        print(f'        "filename": "nys_ratios.json",')
        print(f'        "filters": {{}},')
        print(f'        "brookhaven_filter": True,')
        print(f'        "page_size": 50000,')
        print(f'    }},')
        print()

    print('    "local_csv": {')
    print('        "type": "local_csv",')
    print('        "path": os.path.join(RAW_DIR, "brookhaven_parcels.csv"),')
    print('        "description": "Local export of Brookhaven data",')
    print('        "filename": "brookhaven_parcels.csv",')
    print('    },')
    print("}")

    # ── Also update FIELD_MAPPINGS hint ──
    if "assessment" in results:
        fields = results["assessment"]["fields"]
        print(f"\n\n{'='*60}")
        print(f"  FIELD MAPPING NOTES")
        print(f"  These are the actual field names in dataset 7vem-aaz7.")
        print(f"  Check that PropertyNormalizer.FIELD_MAPPINGS covers them.")
        print(f"{'='*60}\n")

        mapping_hints = {
            "address": [f for f in fields if any(
                kw in f.lower() for kw in ["address", "street", "location"]
            )],
            "city": [f for f in fields if any(
                kw in f.lower() for kw in ["municip", "town", "city", "muni"]
            )],
            "parcel_id": [f for f in fields if any(
                kw in f.lower() for kw in ["print_key", "parcel", "sbl", "swis"]
            )],
            "assessed_value": [f for f in fields if any(
                kw in f.lower() for kw in ["assess", "value", "market"]
            )],
            "property_class": [f for f in fields if any(
                kw in f.lower() for kw in ["class", "property_class", "use"]
            )],
            "owner": [f for f in fields if any(
                kw in f.lower() for kw in ["owner", "name"]
            )],
            "year_built": [f for f in fields if any(
                kw in f.lower() for kw in ["year", "built"]
            )],
            "square_feet": [f for f in fields if any(
                kw in f.lower() for kw in ["sqft", "sq_ft", "square", "area", "size"]
            )],
            "lot_size": [f for f in fields if any(
                kw in f.lower() for kw in ["lot", "acre", "land"]
            )],
        }

        for our_field, matched in mapping_hints.items():
            if matched:
                print(f"  {our_field:20s} → {matched}")
            else:
                print(f"  {our_field:20s} → ⚠️  No matching field found")

        unmapped = [f for f in fields if not any(
            f in matched for matched in mapping_hints.values()
        )]
        if unmapped:
            print(f"\n  Fields not mapped to anything:")
            for f in unmapped:
                print(f"    • {f}")


if __name__ == "__main__":
    main()