#!/usr/bin/env python3
"""
find_sources.py
===============
Searches NY Open Data and ArcGIS Online for working
Brookhaven / Suffolk County property datasets.

Run:  python find_sources.py

It will output a ready-to-paste DATA_SOURCES dict.
"""

import requests
import json
import time
import sys


def search_socrata(query, domain="data.ny.gov", limit=15):
    """Search Socrata catalog."""
    url = f"https://{domain}/api/catalog/v1"
    params = {
        "q": query,
        "domains": domain,
        "search_context": domain,
        "limit": limit,
        "only": "datasets",
    }
    try:
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        return resp.json().get("results", [])
    except Exception as e:
        print(f"  Search error: {e}")
        return []


def verify_socrata(api_url, test_filter=None):
    """Verify a Socrata endpoint and return field names."""
    try:
        params = {"$limit": 2}
        if test_filter:
            params["$where"] = test_filter
        resp = requests.get(api_url, params=params, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            if data:
                fields = list(data[0].keys())
                return {"ok": True, "fields": fields, "sample": data[0]}
            return {"ok": True, "fields": [], "sample": None}
        return {"ok": False, "status": resp.status_code, "body": resp.text[:200]}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def verify_arcgis(query_url, where="1=1"):
    """Verify an ArcGIS REST endpoint."""
    try:
        params = {
            "where": where,
            "outFields": "*",
            "resultRecordCount": 2,
            "f": "json",
            "returnGeometry": "true",
        }
        resp = requests.get(query_url, params=params, timeout=15)
        data = resp.json()
        if "features" in data and data["features"]:
            attrs = data["features"][0].get("attributes", {})
            geom = data["features"][0].get("geometry", {})
            return {
                "ok": True,
                "fields": list(attrs.keys()),
                "has_geometry": bool(geom),
                "sample": attrs,
            }
        if "error" in data:
            return {"ok": False, "error": data["error"].get("message", str(data["error"]))}
        return {"ok": False, "error": "No features returned"}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def search_arcgis_online(query):
    """Search ArcGIS Online / Hub for public datasets."""
    url = "https://www.arcgis.com/sharing/rest/search"
    params = {
        "q": query,
        "f": "json",
        "num": 15,
        "sortField": "modified",
        "sortOrder": "desc",
    }
    try:
        resp = requests.get(url, params=params, timeout=15)
        data = resp.json()
        return data.get("results", [])
    except Exception as e:
        print(f"  ArcGIS Online search error: {e}")
        return []


def main():
    print("\n" + "=" * 60)
    print("  Brookhaven / Suffolk County Data Source Finder")
    print("=" * 60)

    working_sources = []

    # ── 1. Search Socrata (data.ny.gov) ──

    socrata_searches = [
        "real property assessment suffolk",
        "real property sales suffolk",
        "property tax suffolk",
        "assessment roll suffolk",
        "property sales new york",
        "real property tax",
    ]

    seen_ids = set()
    all_socrata = []

    print("\n── Searching data.ny.gov ──\n")

    for query in socrata_searches:
        print(f"  Searching: '{query}'")
        results = search_socrata(query)
        for item in results:
            resource = item.get("resource", {})
            dataset_id = resource.get("id", "")
            if dataset_id in seen_ids:
                continue
            seen_ids.add(dataset_id)

            name = resource.get("name", "")
            columns = resource.get("columns_name", [])
            updated = resource.get("updatedAt", "")

            # Only include if it has property-related columns
            col_str = " ".join(columns).lower()
            if not any(kw in col_str for kw in [
                "county", "property", "parcel", "assess", "sale",
                "municipality", "swis", "tax", "roll"
            ]):
                continue

            all_socrata.append({
                "id": dataset_id,
                "name": name,
                "columns": columns,
                "updated": updated,
                "url": f"https://data.ny.gov/resource/{dataset_id}.json",
            })

        time.sleep(0.3)

    print(f"\n  Found {len(all_socrata)} potential Socrata datasets. Verifying...\n")

    for ds in all_socrata:
        api_url = ds["url"]
        print(f"  Checking: {ds['name'][:60]}")
        print(f"  URL:      {api_url}")

        result = verify_socrata(api_url)
        if result["ok"]:
            fields = result["fields"]
            print(f"  Status:   ✅ LIVE")
            print(f"  Fields:   {', '.join(fields[:10])}")

            # Check if it has Suffolk data
            has_suffolk = False
            suffolk_filter = None

            # Try different filter field names
            for county_field in ["county", "county_name", "cnty_name"]:
                if county_field in fields:
                    test = verify_socrata(api_url, f"{county_field} = 'SUFFOLK'")
                    if test["ok"] and test.get("sample"):
                        has_suffolk = True
                        suffolk_filter = f"{county_field} = 'SUFFOLK'"
                        print(f"  Suffolk:  ✅ Has Suffolk data (filter: {county_field})")
                        break
                    # Try uppercase field
                    test2 = verify_socrata(api_url, f"upper({county_field}) = 'SUFFOLK'")
                    if test2["ok"] and test2.get("sample"):
                        has_suffolk = True
                        suffolk_filter = f"upper({county_field}) = 'SUFFOLK'"
                        print(f"  Suffolk:  ✅ Has Suffolk data (filter: upper({county_field}))")
                        break

            if not has_suffolk:
                print(f"  Suffolk:  ⚠️  Could not confirm Suffolk data")

            working_sources.append({
                "type": "socrata",
                "name": ds["name"],
                "id": ds["id"],
                "url": api_url,
                "fields": fields,
                "has_suffolk": has_suffolk,
                "suffolk_filter_field": suffolk_filter,
                "updated": ds["updated"],
            })
        else:
            print(f"  Status:   ❌ {result.get('error', result.get('status', 'failed'))}")

        print()
        time.sleep(0.3)

    # ── 2. Search ArcGIS Online for Suffolk parcels ──

    arcgis_searches = [
        "suffolk county parcels",
        "suffolk county tax map",
        "suffolk county property",
        "brookhaven parcels",
        "long island parcels",
    ]

    seen_arcgis = set()
    all_arcgis = []

    print("\n── Searching ArcGIS Online ──\n")

    for query in arcgis_searches:
        print(f"  Searching: '{query}'")
        results = search_arcgis_online(query)
        for item in results:
            item_id = item.get("id", "")
            if item_id in seen_arcgis:
                continue
            seen_arcgis.add(item_id)

            title = item.get("title", "")
            item_type = item.get("type", "")
            item_url = item.get("url", "")
            owner = item.get("owner", "")

            if item_type in ["Feature Service", "Map Service"] and item_url:
                all_arcgis.append({
                    "id": item_id,
                    "title": title,
                    "type": item_type,
                    "url": item_url,
                    "owner": owner,
                })

        time.sleep(0.3)

    print(f"\n  Found {len(all_arcgis)} potential ArcGIS services. Verifying...\n")

    for svc in all_arcgis:
        base_url = svc["url"].rstrip("/")
        print(f"  Checking: {svc['title'][:60]}")
        print(f"  URL:      {base_url}")
        print(f"  Owner:    {svc['owner']}")

        # Try common layer patterns
        layer_urls = [
            f"{base_url}/0/query",
            f"{base_url}/query",
        ]

        # If it's a FeatureServer or MapServer, try /0/query
        if not base_url.endswith(("/FeatureServer", "/MapServer")):
            layer_urls.extend([
                f"{base_url}/FeatureServer/0/query",
                f"{base_url}/MapServer/0/query",
            ])

        found = False
        for layer_url in layer_urls:
            result = verify_arcgis(layer_url)
            if result["ok"]:
                fields = result["fields"]
                print(f"  Layer:    ✅ {layer_url}")
                print(f"  Fields:   {', '.join(fields[:10])}")
                print(f"  Geometry: {'✅' if result.get('has_geometry') else '❌'}")

                # Check for Brookhaven filter
                for where in [
                    "MUNI_NAME = 'BROOKHAVEN'",
                    "MUNI = 'BROOKHAVEN'",
                    "TOWN = 'BROOKHAVEN'",
                    "MUNICIPALITY = 'BROOKHAVEN'",
                ]:
                    brookhaven_test = verify_arcgis(layer_url, where)
                    if brookhaven_test["ok"]:
                        print(f"  Filter:   ✅ '{where}' works")
                        working_sources.append({
                            "type": "arcgis",
                            "name": svc["title"],
                            "id": svc["id"],
                            "layer_url": layer_url,
                            "fields": fields,
                            "where_clause": where,
                            "has_geometry": result.get("has_geometry", False),
                            "owner": svc["owner"],
                        })
                        found = True
                        break

                if not found:
                    print(f"  Filter:   ⚠️  No Brookhaven filter found, but layer works")
                    working_sources.append({
                        "type": "arcgis",
                        "name": svc["title"],
                        "id": svc["id"],
                        "layer_url": layer_url,
                        "fields": fields,
                        "where_clause": "1=1",
                        "has_geometry": result.get("has_geometry", False),
                        "owner": svc["owner"],
                        "note": "No Brookhaven-specific filter found — will need client-side filtering",
                    })
                    found = True
                break

        if not found:
            print(f"  Status:   ❌ No working layer found")

        print()
        time.sleep(0.3)

    # ── 3. Try known Suffolk County GIS hostnames ──

    print("\n── Trying known Suffolk County GIS hostnames ──\n")

    known_hosts = [
        "gisservices.suffolkcountyny.gov",
        "gis.suffolkcountyny.gov",
        "maps.suffolkcountyny.gov",
        "services.arcgis.com",
        "services1.arcgis.com",
        "services6.arcgis.com",
    ]

    for host in known_hosts:
        try:
            resp = requests.get(f"https://{host}", timeout=5)
            print(f"  {host}: ✅ Reachable (HTTP {resp.status_code})")
        except requests.exceptions.ConnectionError:
            print(f"  {host}: ❌ Cannot connect")
        except Exception as e:
            print(f"  {host}: ⚠️  {e}")

    # ── Summary ──

    print("\n" + "=" * 60)
    print("  RESULTS SUMMARY")
    print("=" * 60)

    socrata_working = [s for s in working_sources if s["type"] == "socrata"]
    arcgis_working = [s for s in working_sources if s["type"] == "arcgis"]

    print(f"\n  Working Socrata datasets:  {len(socrata_working)}")
    print(f"  Working ArcGIS layers:    {len(arcgis_working)}")

    if socrata_working:
        print("\n  ── Socrata (data.ny.gov) ──")
        for s in socrata_working:
            suffolk = "✅" if s.get("has_suffolk") else "⚠️"
            print(f"\n  {suffolk} {s['name'][:60]}")
            print(f"     ID:     {s['id']}")
            print(f"     URL:    {s['url']}")
            print(f"     Fields: {', '.join(s['fields'][:8])}")

    if arcgis_working:
        print("\n  ── ArcGIS ──")
        for s in arcgis_working:
            print(f"\n  ✅ {s['name'][:60]}")
            print(f"     Layer:  {s['layer_url']}")
            print(f"     WHERE:  {s['where_clause']}")
            print(f"     Fields: {', '.join(s['fields'][:8])}")
            if s.get("note"):
                print(f"     Note:   {s['note']}")

    # ── Generate DATA_SOURCES ──

    print("\n" + "=" * 60)
    print("  GENERATED DATA_SOURCES")
    print("  Copy-paste this into pipeline.py")
    print("=" * 60 + "\n")

    print("DATA_SOURCES = {")

    # Add best Socrata sources
    for i, s in enumerate(socrata_working[:4]):
        safe_name = s["id"].replace("-", "_")
        filter_field = "county"
        if s.get("suffolk_filter_field"):
            # Extract field name from filter string
            ff = s["suffolk_filter_field"]
            if "upper(" in ff:
                filter_field = ff.split("(")[1].split(")")[0]
            else:
                filter_field = ff.split(" = ")[0]

        print(f'    "nys_{safe_name}": {{')
        print(f'        "type": "socrata_api",')
        print(f'        "base_url": "{s["url"]}",')
        print(f'        "fallback_urls": [],')
        print(f'        "description": "{s["name"][:80]}",')
        print(f'        "filename": "nys_{safe_name}.json",')
        print(f'        "filters": {{')
        print(f'            "{filter_field}": "SUFFOLK",')
        print(f'        }},')
        print(f'        "brookhaven_filter": True,')
        print(f'        "page_size": 50000,')
        print(f'    }},')
        print()

    # Add best ArcGIS sources
    for s in arcgis_working[:2]:
        safe_name = s["id"][:20].replace("-", "_")
        print(f'    "arcgis_{safe_name}": {{')
        print(f'        "type": "arcgis_api",')
        print(f'        "base_url": "",')
        print(f'        "layer_url": "{s["layer_url"]}",')
        print(f'        "fallback_layer_urls": [],')
        print(f'        "description": "{s["name"][:80]}",')
        print(f'        "filename": "arcgis_{safe_name}.json",')
        print(f'        "where_clause": "{s["where_clause"]}",')
        print(f'        "fallback_where_clauses": ["1=1"],')
        print(f'        "page_size": 2000,')
        print(f'    }},')
        print()

    # Always include local CSV
    print('    "local_csv": {')
    print('        "type": "local_csv",')
    print('        "path": os.path.join(RAW_DIR, "brookhaven_parcels.csv"),')
    print('        "description": "Local export of Brookhaven data",')
    print('        "filename": "brookhaven_parcels.csv",')
    print('    },')

    print("}")
    print()


if __name__ == "__main__":
    main()