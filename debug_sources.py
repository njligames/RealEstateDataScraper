#!/usr/bin/env python3
"""
debug_sources.py
================
Tests each data source and shows exactly what data comes back.
Run this to understand why the pipeline might be empty.

Usage: python debug_sources.py
"""

import requests
import json
import sys


def test_socrata_dataset(name, url, filters=None):
    """Fetch a few rows from a Socrata dataset and show what's in it."""
    print(f"\n{'='*60}")
    print(f"  Testing: {name}")
    print(f"  URL: {url}")
    print(f"{'='*60}\n")

    # First: get raw sample with no filters
    try:
        resp = requests.get(url, params={"$limit": 3}, timeout=15)
        if resp.status_code != 200:
            print(f"  ❌ HTTP {resp.status_code}")
            print(f"  Response: {resp.text[:500]}")
            return False

        data = resp.json()
        if not data:
            print(f"  ❌ Empty response (no rows)")
            return False

        print(f"  ✅ Got {len(data)} sample rows")
        print(f"  Fields ({len(data[0])} total):")
        for key in sorted(data[0].keys()):
            val = data[0][key]
            print(f"    • {key}: {str(val)[:80]}")

    except Exception as e:
        print(f"  ❌ Error: {e}")
        return False

    # Check if this has property-level data (addresses)
    fields = set(data[0].keys())
    address_fields = fields & {
        "address", "property_address", "street_address", "location",
        "situs_address", "full_address", "print_key_address",
        "street_name", "street_number",
    }
    parcel_fields = fields & {
        "parcel_id", "parcel", "print_key", "sbl", "swis_sbl",
        "tax_map", "parcel_number",
    }

    print(f"\n  Address fields found: {address_fields or 'NONE'}")
    print(f"  Parcel ID fields found: {parcel_fields or 'NONE'}")

    if not address_fields and not parcel_fields:
        print(f"\n  ⚠️  WARNING: This dataset does NOT have property-level data.")
        print(f"  It appears to be aggregate/summary data.")
        print(f"  This won't work for building a property search site.")

    # Now try with Suffolk/Brookhaven filters
    if filters:
        print(f"\n  Testing with filters: {filters}")
        params = {"$limit": 5}

        where_parts = []
        for field, value in filters.items():
            # Try exact match
            where_parts.append(f"upper({field}) = '{value.upper()}'")

        if where_parts:
            params["$where"] = " AND ".join(where_parts)

        try:
            resp2 = requests.get(url, params=params, timeout=15)
            if resp2.status_code == 200:
                filtered = resp2.json()
                print(f"  Filtered results: {len(filtered)} rows")
                if filtered:
                    print(f"  Sample row:")
                    for key, val in sorted(filtered[0].items()):
                        print(f"    • {key}: {str(val)[:80]}")
                else:
                    print(f"  ⚠️  No rows match the filter.")
                    print(f"  Trying without filter to check available values...")

                    # Show what county/municipality values exist
                    for check_field in ["county", "county_name", "municipality", "municipality_name"]:
                        if check_field in fields:
                            resp3 = requests.get(url, params={
                                "$select": f"distinct {check_field}",
                                "$limit": 20,
                            }, timeout=10)
                            if resp3.status_code == 200:
                                vals = [row.get(check_field, "") for row in resp3.json()]
                                print(f"  Available {check_field} values: {vals[:15]}")
            else:
                print(f"  ❌ Filter query failed: HTTP {resp2.status_code}")
                print(f"  {resp2.text[:300]}")

        except Exception as e:
            print(f"  ❌ Filter error: {e}")

    return True


def search_for_property_datasets():
    """Search specifically for datasets with property-level address data."""
    print(f"\n{'='*60}")
    print(f"  Searching for datasets with PROPERTY-LEVEL data")
    print(f"  (addresses, parcels, individual properties)")
    print(f"{'='*60}\n")

    searches = [
        "real property parcel suffolk",
        "tax parcel suffolk county",
        "property address suffolk",
        "assessment roll detail suffolk",
        "parcel data long island",
        "real property detail",
    ]

    found = []

    for query in searches:
        print(f"  Searching: '{query}'")
        try:
            resp = requests.get("https://data.ny.gov/api/catalog/v1", params={
                "q": query,
                "domains": "data.ny.gov",
                "limit": 10,
                "only": "datasets",
            }, timeout=15)
            results = resp.json().get("results", [])

            for item in results:
                resource = item.get("resource", {})
                dataset_id = resource.get("id", "")
                name = resource.get("name", "")
                columns = resource.get("columns_name", [])
                col_lower = [c.lower() for c in columns]

                # Only care about datasets with address-level data
                has_address = any(
                    kw in " ".join(col_lower)
                    for kw in ["address", "street", "parcel", "sbl", "print_key", "location"]
                )

                has_suffolk = any(
                    kw in " ".join(col_lower)
                    for kw in ["county", "municipality", "swis"]
                )

                if has_address and has_suffolk:
                    if dataset_id not in [f["id"] for f in found]:
                        found.append({
                            "id": dataset_id,
                            "name": name,
                            "columns": columns,
                            "url": f"https://data.ny.gov/resource/{dataset_id}.json",
                        })

        except Exception as e:
            print(f"    Error: {e}")

    print(f"\n  Found {len(found)} datasets with property-level data:\n")

    for ds in found:
        url = ds["url"]
        print(f"  • {ds['name']}")
        print(f"    ID:  {ds['id']}")
        print(f"    URL: {url}")

        # Verify it works
        try:
            resp = requests.get(url, params={"$limit": 1}, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                if data:
                    fields = list(data[0].keys())
                    print(f"    ✅ LIVE — {len(fields)} fields")
                    print(f"    Fields: {', '.join(fields[:12])}")

                    # Check for Suffolk
                    for county_field in ["county", "county_name"]:
                        if county_field in fields:
                            resp2 = requests.get(url, params={
                                "$where": f"upper({county_field}) = 'SUFFOLK'",
                                "$limit": 1,
                            }, timeout=10)
                            if resp2.status_code == 200 and resp2.json():
                                print(f"    ✅ Has Suffolk data (field: {county_field})")
                                break
                else:
                    print(f"    ⚠️  Empty")
            else:
                print(f"    ❌ HTTP {resp.status_code}")
        except Exception as e:
            print(f"    ❌ {e}")

        print()

    return found


def test_known_property_datasets():
    """Test specific dataset IDs that are known to have property-level data."""
    print(f"\n{'='*60}")
    print(f"  Testing Known Property-Level Datasets")
    print(f"{'='*60}\n")

    # These are dataset IDs that have historically had property-level data
    candidates = [
        # Assessment roll details (individual parcels)
        ("iq85-sdzs", "Assessment Roll (current in DATA_SOURCES)"),
        ("7vem-aaz7", "Assessment Roll (alternate ID 1)"),
        ("bc4h-nfse", "Assessment Roll (alternate ID 2)"),

        # Sales data
        ("93cg-qhsh", "Property Sales (alternate 1)"),
        ("647i-zynz", "Property Sales (alternate 2)"),
        ("tkrr-baa2", "Property Sales (alternate 3)"),
        ("7yg5-mwsk", "Sales Assessment Merge"),

        # Tax/parcel data
        ("f56s-gdhp", "STAR Exemption"),
        ("awqr-357k", "STAR (alternate)"),

        # Other property datasets
        ("8vgb-zm6e", "Residential Assessment Ratios"),
        ("nbt6-wksb", "Municipal Profiles"),
        ("tx6p-abi5", "Property Tax Rates"),
    ]

    working = []

    for dataset_id, description in candidates:
        url = f"https://data.ny.gov/resource/{dataset_id}.json"
        sys.stdout.write(f"  {dataset_id} ({description})... ")
        sys.stdout.flush()

        try:
            resp = requests.get(url, params={"$limit": 1}, timeout=8)
            if resp.status_code == 200:
                data = resp.json()
                if data:
                    fields = list(data[0].keys())

                    # Classify the data
                    has_address = any(f in " ".join(fields).lower() for f in [
                        "address", "street", "location", "print_key",
                        "sbl", "parcel",
                    ])
                    has_individual = len(fields) > 10  # Aggregate data tends to have few fields

                    if has_address and has_individual:
                        print(f"✅ LIVE — HAS ADDRESSES — {len(fields)} fields")
                        working.append({
                            "id": dataset_id,
                            "description": description,
                            "url": url,
                            "fields": fields,
                            "has_address": True,
                        })
                    elif has_individual:
                        print(f"✅ LIVE — no addresses — {len(fields)} fields")
                        working.append({
                            "id": dataset_id,
                            "description": description,
                            "url": url,
                            "fields": fields,
                            "has_address": False,
                        })
                    else:
                        print(f"⚠️  LIVE but only {len(fields)} fields (aggregate?)")
                else:
                    print(f"⚠️  LIVE but empty")
            elif resp.status_code == 404:
                print(f"❌ Not found")
            else:
                print(f"❌ HTTP {resp.status_code}")
        except Exception as e:
            print(f"❌ {e}")

    # Show the best candidates
    print(f"\n\n{'='*60}")
    print(f"  BEST CANDIDATES FOR PIPELINE")
    print(f"{'='*60}\n")

    address_datasets = [w for w in working if w.get("has_address")]
    other_datasets = [w for w in working if not w.get("has_address")]

    if address_datasets:
        print("  Datasets WITH property addresses (best for pipeline):\n")
        for ds in address_datasets:
            print(f"  ✅ {ds['description']}")
            print(f"     ID:     {ds['id']}")
            print(f"     URL:    {ds['url']}")
            print(f"     Fields: {', '.join(ds['fields'][:12])}")
            print()
    else:
        print("  ❌ No datasets with property addresses found on data.ny.gov")
        print("     The assessment roll may be aggregate (municipal-level) data.")
        print()
        print("  Alternative data sources to try:")
        print("  • Suffolk County Real Property Tax Service Agency")
        print("    https://www.suffolkcountyny.gov/rpts")
        print("  • Download assessment rolls directly from county")
        print("  • FOIL request to Town of Brookhaven assessor")
        print("  • Purchase from a data vendor (ATTOM, CoreLogic, etc.)")

    if other_datasets:
        print("\n  Other working datasets (no addresses, but may have useful data):\n")
        for ds in other_datasets:
            print(f"  ⚠️  {ds['description']}")
            print(f"     ID:     {ds['id']}")
            print(f"     Fields: {', '.join(ds['fields'][:12])}")
            print()

    # Generate updated DATA_SOURCES
    if address_datasets:
        print(f"\n{'='*60}")
        print(f"  SUGGESTED DATA_SOURCES UPDATE")
        print(f"  Copy this into pipeline.py")
        print(f"{'='*60}\n")

        print('DATA_SOURCES = {')
        for i, ds in enumerate(address_datasets):
            name = f"nys_dataset_{i+1}"
            print(f'    "{name}": {{')
            print(f'        "type": "socrata_api",')
            print(f'        "base_url": "{ds["url"]}",')
            print(f'        "fallback_urls": [],')
            print(f'        "description": "{ds["description"]}",')
            print(f'        "filename": "{name}.json",')
            print(f'        "filters": {{}},  # Add county/municipality filter after checking field names')
            print(f'        "brookhaven_filter": True,')
            print(f'        "page_size": 50000,')
            print(f'    }},')
        print('    "local_csv": {')
        print('        "type": "local_csv",')
        print('        "path": os.path.join(RAW_DIR, "brookhaven_parcels.csv"),')
        print('        "description": "Local export of Brookhaven data",')
        print('        "filename": "brookhaven_parcels.csv",')
        print('    },')
        print('}')


def main():
    print("\n" + "=" * 60)
    print("  Brookhaven Pipeline — Data Source Debugger")
    print("=" * 60)

    # 1. Test the currently configured source
    print("\n\nPHASE 1: Testing currently configured dataset")
    test_socrata_dataset(
        "NYS Assessment Roll (iq85-sdzs)",
        "https://data.ny.gov/resource/iq85-sdzs.json",
        filters={"county": "SUFFOLK"},
    )

    # 2. Test all known dataset IDs
    print("\n\nPHASE 2: Testing all known dataset IDs")
    test_known_property_datasets()

    # 3. Search for property-level datasets
    print("\n\nPHASE 3: Searching for property-level datasets")
    search_for_property_datasets()


if __name__ == "__main__":
    main()