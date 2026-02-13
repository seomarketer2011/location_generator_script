#!/usr/bin/env python3
"""
local_areas.py

Resolve towns -> OSM boundary relation (UK-focused), then pull neighbourhoods/places inside
that boundary via Overpass, and optionally check Wikipedia pages.

USAGE
-----
1) Resolve towns:
   python local_areas.py resolve towns.csv

2) Generate neighbourhood lists:
   python local_areas.py generate town_id_map.csv

Optional:
   python local_areas.py generate town_id_map.csv --no-wiki

NOTES
-----
- Replace the contact email below with YOUR OWN.
- Script is "no-guess": if a town can’t be uniquely resolved, it goes into needs_review.csv.
- UK fallback: if Nominatim is ambiguous, it tries Overpass boundary=administrative (admin_level 6 or 8).
- Overpass endpoints are rotated automatically if one rate-limits (429) or is down.
"""

import csv
import json
import os
import re
import sys
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import requests

# --- CONFIG ---
NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
OVERPASS_URLS = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
    "https://overpass.nchc.org.tw/api/interpreter",
    "https://overpass.openstreetmap.ru/api/interpreter",
]
WIKI_API = "https://en.wikipedia.org/w/api.php"

# IMPORTANT: put your real contact email/domain here (helps avoid blocks)
HEADERS = {"User-Agent": "LocalAreaGenerator/1.3 (contact: you@yourdomain.com)"}

CACHE_DIR = "cache"
os.makedirs(CACHE_DIR, exist_ok=True)

# UK-only by default. Remove if you want global.
COUNTRY_CODES = "gb"

# Which OSM place tags count as "child locations"
PLACE_TAG_REGEX = r"^(suburb|neighbourhood|quarter|village|hamlet)$"

# Sorting preference: broader areas first
PLACE_RANK = {"suburb": 1, "neighbourhood": 2, "quarter": 3, "village": 4, "hamlet": 5}


# -----------------------
# DATA MODELS / HELPERS
# -----------------------
@dataclass
class TownInput:
    town: str
    county_or_region: str
    country: str


@dataclass
class Candidate:
    display_name: str
    osm_type: str
    osm_id: int
    class_: str
    type_: str
    lat: float
    lon: float


def slug(s: str) -> str:
    s = s.strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return s.strip("_")


def cache_get(path: str) -> Optional[object]:
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return None


def cache_set(path: str, data: object) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def write_csv(path: str, header: List[str], rows: List[List[str]]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)


def read_towns_csv(path: str) -> List[TownInput]:
    out: List[TownInput] = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            town = (r.get("town") or "").strip()
            if not town:
                continue
            out.append(
                TownInput(
                    town=town,
                    county_or_region=(r.get("county_or_region") or "").strip(),
                    country=(r.get("country") or "").strip() or "United Kingdom",
                )
            )
    return out


# -----------------------------------------
# OVERPASS: AUTO FALLBACK + BACKOFF LOGIC
# -----------------------------------------
def overpass_post(query: str, timeout: int = 120) -> dict:
    """
    Try multiple Overpass endpoints. If one fails (429/5xx/timeout), try the next.
    Uses exponential backoff and respects Retry-After when provided.

    Returns JSON dict or raises RuntimeError after exhausting retries.
    """
    last_err: Optional[Exception] = None
    backoff = 2  # seconds
    max_backoff = 60

    # Up to 6 rounds; each round tries all endpoints
    for _round in range(1, 7):
        any_progress = False

        for url in OVERPASS_URLS:
            try:
                r = requests.post(url, data=query.encode("utf-8"), headers=HEADERS, timeout=timeout)

                # Rate limit / overload
                if r.status_code == 429:
                    any_progress = True
                    # Try next endpoint immediately; only sleep if Retry-After is big
                    ra = r.headers.get("Retry-After")
                    if ra and ra.isdigit():
                        wait_s = int(ra)
                        # If the server explicitly asks for a wait, respect it (but cap it)
                        time.sleep(min(wait_s, max_backoff))
                    continue

                # Temporary server errors
                if r.status_code in (502, 503, 504):
                    any_progress = True
                    continue

                r.raise_for_status()

                # JSON parse safety
                try:
                    return r.json()
                except Exception as e:
                    last_err = e
                    any_progress = True
                    continue

            except Exception as e:
                last_err = e
                any_progress = True
                continue

        # If we got here, every endpoint failed this round
        time.sleep(backoff)
        backoff = min(backoff * 2, max_backoff)

        # If literally nothing happened (unlikely), bail
        if not any_progress:
            break

    raise RuntimeError(f"Overpass failed after retries. Last error: {last_err}")


# ----------------------------
# STEP A: Resolve town boundary
# ----------------------------
def nominatim_search(t: TownInput, limit: int = 10) -> List[Candidate]:
    parts = [t.town]
    if t.county_or_region:
        parts.append(t.county_or_region)
    if t.country:
        parts.append(t.country)
    q = ", ".join(parts)

    cache_path = os.path.join(CACHE_DIR, f"nominatim_{slug(q)}.json")
    cached = cache_get(cache_path)

    if cached is None:
        params = {
            "q": q,
            "format": "jsonv2",
            "addressdetails": 1,
            "limit": limit,
        }
        if COUNTRY_CODES:
            params["countrycodes"] = COUNTRY_CODES

        r = requests.get(NOMINATIM_URL, params=params, headers=HEADERS, timeout=30)
        r.raise_for_status()
        cached = r.json()
        cache_set(cache_path, cached)
        time.sleep(1.1)  # polite to Nominatim

    out: List[Candidate] = []
    for item in cached:
        out.append(
            Candidate(
                display_name=item.get("display_name", ""),
                osm_type=item.get("osm_type", ""),
                osm_id=int(item.get("osm_id", 0)),
                class_=item.get("class", ""),
                type_=item.get("type", ""),
                lat=float(item.get("lat", 0.0)),
                lon=float(item.get("lon", 0.0)),
            )
        )
    return out


def overpass_find_uk_admin_relation(town_name: str) -> List[Tuple[int, str]]:
    """
    UK-only resolver fallback:
    Find admin boundary relations at admin_level 6 or 8 matching town name patterns.
    Returns list of (relation_id, name).
    """
    name = town_name.strip()
    esc = re.escape(name)

    # Covers common LA patterns (unitary authorities often admin_level=6, metro boroughs often =8)
    pattern = rf"^({esc}|{esc}\s+Borough|Borough\s+of\s+{esc}|{esc}\s+Borough\s+Council|Metropolitan\s+Borough\s+of\s+{esc})$"

    cache_path = os.path.join(CACHE_DIR, f"overpass_resolve_admin_{slug(name)}.json")
    cached = cache_get(cache_path)
    if cached is None:
        query = f"""
        [out:json][timeout:60];
        // UK boundary as search area
        relation["boundary"="administrative"]["admin_level"="2"]["ISO3166-1"="GB"]->.uk;
        .uk map_to_area -> .gb;
        (
          relation(area.gb)
            ["boundary"="administrative"]
            ["admin_level"~"^(6|8)$"]
            ["name"~"{pattern}"];
        );
        out tags;
        """
        cached = overpass_post(query, timeout=120)
        cache_set(cache_path, cached)

    matches: List[Tuple[int, str]] = []
    for el in (cached or {}).get("elements", []):
        if el.get("type") != "relation":
            continue
        rid = int(el.get("id", 0))
        nm = (el.get("tags", {}) or {}).get("name", "") or ""
        if rid and nm:
            matches.append((rid, nm))
    return matches


def score_candidate(c: Candidate, t: TownInput) -> int:
    s = 0
    dn = c.display_name.lower()

    # Prefer relations (generator wants relation->area)
    if c.osm_type != "relation":
        s -= 5
    else:
        s += 2

    # Strong preference for administrative boundaries
    if c.class_ == "boundary":
        s += 6
    if c.type_ == "administrative":
        s += 6

    # Context matches
    if t.town and t.town.lower() in dn:
        s += 2
    if t.county_or_region and t.county_or_region.lower() in dn:
        s += 2
    if t.country and t.country.lower() in dn:
        s += 1

    if "borough" in dn:
        s += 1

    return s


def resolve_town_no_guess(t: TownInput) -> Tuple[Optional[Candidate], List[Candidate]]:
    candidates = nominatim_search(t, limit=10)

    # First: unique admin boundary relation from Nominatim list
    admin_rels = [
        c for c in candidates
        if c.osm_type == "relation" and c.class_ == "boundary" and c.type_ == "administrative"
    ]
    if len(admin_rels) == 1:
        return admin_rels[0], candidates

    # If multiple admin boundary relations, try pick unique best by score
    if len(admin_rels) > 1:
        scored = sorted(((score_candidate(c, t), c) for c in admin_rels), key=lambda x: x[0], reverse=True)
        if len(scored) >= 2 and scored[0][0] > scored[1][0]:
            return scored[0][1], candidates

    # Fallback: UK admin boundary via Overpass (admin_level 6 or 8)
    if (COUNTRY_CODES or "").lower() == "gb":
        matches = overpass_find_uk_admin_relation(t.town)
        if len(matches) == 1:
            rid, nm = matches[0]
            return Candidate(
                display_name=f"{nm}, United Kingdom",
                osm_type="relation",
                osm_id=rid,
                class_="boundary",
                type_="administrative",
                lat=0.0,
                lon=0.0,
            ), candidates

    # Final fallback: accept only if ONE candidate passes a threshold uniquely
    scored_all = sorted(((score_candidate(c, t), c) for c in candidates), key=lambda x: x[0], reverse=True)
    passing = [pair for pair in scored_all if pair[0] >= 10]
    if len(passing) == 1:
        return passing[0][1], candidates
    if len(passing) >= 2 and passing[0][0] > passing[1][0]:
        return passing[0][1], candidates

    return None, candidates


def cmd_resolve(towns_csv: str) -> None:
    towns = read_towns_csv(towns_csv)

    map_header = ["town", "county_or_region", "country", "osm_type", "osm_id", "display_name", "lat", "lon"]
    review_header = map_header + ["class", "type", "score"]

    map_rows: List[List[str]] = []
    review_rows: List[List[str]] = []

    for t in towns:
        resolved, candidates = resolve_town_no_guess(t)
        if resolved:
            map_rows.append(
                [
                    t.town,
                    t.county_or_region,
                    t.country,
                    resolved.osm_type,
                    str(resolved.osm_id),
                    resolved.display_name,
                    str(resolved.lat),
                    str(resolved.lon),
                ]
            )
            print(f"OK: {t.town} -> {resolved.display_name} ({resolved.osm_type}/{resolved.osm_id})")
        else:
            for c in candidates[:10]:
                review_rows.append(
                    [
                        t.town,
                        t.county_or_region,
                        t.country,
                        c.osm_type,
                        str(c.osm_id),
                        c.display_name,
                        str(c.lat),
                        str(c.lon),
                        c.class_,
                        c.type_,
                        str(score_candidate(c, t)),
                    ]
                )
            print(f"REVIEW: {t.town} (ambiguous or low confidence)")

    write_csv("town_id_map.csv", map_header, map_rows)
    write_csv("needs_review.csv", review_header, review_rows)

    print("\nCreated:")
    print(" - town_id_map.csv (auto-resolved towns)")
    print(" - needs_review.csv (ambiguous towns you must choose manually)")


# -----------------------------------
# STEP B: Pull places + Wikipedia info
# -----------------------------------
def overpass_places_in_relation(relation_id: int) -> dict:
    cache_path = os.path.join(CACHE_DIR, f"overpass_relation_{relation_id}.json")
    cached = cache_get(cache_path)
    if cached is not None:
        return cached

    area_id = 3600000000 + relation_id
    query = f"""
    [out:json][timeout:90];
    area({area_id})->.a;
    (
      node["place"~"{PLACE_TAG_REGEX}"](area.a);
      way["place"~"{PLACE_TAG_REGEX}"](area.a);
      relation["place"~"{PLACE_TAG_REGEX}"](area.a);
    );
    out tags center;
    """

    cached = overpass_post(query, timeout=120)
    cache_set(cache_path, cached)
    return cached


def wiki_opensearch(query: str) -> Tuple[str, str]:
    """
    Wikipedia sometimes returns 403 if you don't identify your client.
    We send a proper User-Agent and safely return empty fields if blocked.
    """
    params = {
        "action": "opensearch",
        "search": query,
        "limit": "1",
        "namespace": "0",
        "format": "json",
    }
    try:
        r = requests.get(WIKI_API, params=params, headers=HEADERS, timeout=30)
        if r.status_code == 403:
            return "", ""
        r.raise_for_status()
        payload = r.json()
        titles = payload[1]
        urls = payload[3]
        if titles and urls:
            return titles[0], urls[0]
        return "", ""
    except Exception:
        return "", ""


def extract_places(overpass_json: dict) -> List[Dict[str, str]]:
    places: List[Dict[str, str]] = []
    for el in overpass_json.get("elements", []):
        tags = el.get("tags", {})
        name = (tags.get("name") or "").strip()
        place_tag = (tags.get("place") or "").strip()
        if not name or not place_tag:
            continue
        places.append({"name": name, "place": place_tag})
    return places


def dedupe_and_sort(children: List[Dict[str, str]]) -> List[Dict[str, str]]:
    seen = set()
    out: List[Dict[str, str]] = []
    for c in children:
        key = c["name"].strip().lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(c)

    out.sort(key=lambda c: (PLACE_RANK.get(c["place"], 99), c["name"].lower()))
    return out


def cmd_generate(map_csv: str, no_wiki: bool) -> None:
    rows: List[Dict[str, str]] = []
    with open(map_csv, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append(r)

    long_rows: List[List[str]] = []
    pivot_rows: List[List[str]] = []

    pivot_header = ["Parent"] + [f"Child_{i}" for i in range(1, 26)]

    for r in rows:
        town = (r.get("town") or "").strip()
        osm_type = (r.get("osm_type") or "").strip()
        osm_id = int(r.get("osm_id") or 0)

        if not town or not osm_type or not osm_id:
            continue

        if osm_type != "relation":
            print(f"SKIP (not relation): {town} -> {osm_type}/{osm_id}. Use a relation boundary in town_id_map.csv.")
            continue

        overpass = overpass_places_in_relation(osm_id)
        places = dedupe_and_sort(extract_places(overpass))

        enriched = []
        for p in places:
            if no_wiki:
                title, url = "", ""
            else:
                title, url = wiki_opensearch(f"{p['name']} {town}")
                time.sleep(0.25)  # be polite to Wikipedia
            enriched.append({**p, "wiki_title": title, "wiki_url": url})

        for e in enriched:
            long_rows.append([town, e["name"], e["place"], e["wiki_title"], e["wiki_url"]])

        children = [e["name"] for e in enriched]
        row = [town] + children[:25]
        row += [""] * (1 + 25 - len(row))
        pivot_rows.append(row)

        print(f"{town}: found {len(children)} place-tagged areas (wrote long + pivot)")

    write_csv(
        "neighbourhoods_long.csv",
        ["Parent", "Child", "Place_Tag", "Wikipedia_Title", "Wikipedia_URL"],
        long_rows,
    )
    write_csv("neighbourhoods_pivot.csv", pivot_header, pivot_rows)

    print("\nCreated:")
    print(" - neighbourhoods_long.csv (all areas + wikipedia matches)")
    print(" - neighbourhoods_pivot.csv (Parent + Child_1..Child_25)")


def main() -> None:
    args = sys.argv[1:]
    if len(args) < 2:
        print("Usage:")
        print("  python local_areas.py resolve towns.csv")
        print("  python local_areas.py generate town_id_map.csv [--no-wiki]")
        sys.exit(1)

    no_wiki = False
    if "--no-wiki" in args:
        no_wiki = True
        args.remove("--no-wiki")

    cmd = args[0].strip().lower()
    path = args[1].strip()

    if cmd == "resolve":
        cmd_resolve(path)
    elif cmd == "generate":
        cmd_generate(path, no_wiki=no_wiki)
    else:
        raise SystemExit("Unknown command. Use 'resolve' or 'generate'.")


if __name__ == "__main__":
    main()
