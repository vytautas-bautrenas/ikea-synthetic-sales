"""IKEA GB store locator -> data/dim_stores.csv for ingest_stores.py.

https://www.ikea.com/gb/en/stores/ is the index: it groups every live GB store
under IKEA's own region headings ("Greater London", "Scotland & North East"...)
and links each store page. Each store page carries the address, postcode and
coordinates twice-encoded, and both halves are used here:

  * schema.org FurnitureStore JSON-LD -- name, street, city, postcode, lat/lon;
  * the hydration blob keyed by the store's own "storePageUrl" -- the numeric
    store id and buClassification (STORE = blue box, SMALL = small format).

The locator's own vocabulary is kept: region is its heading, country is its ISO
code, store_type is its classification. The only normalisation is the postcode,
because the site mixes "CR0 4UZ" and "CR04UZ" and the warehouse joins postcodes.
"""

import csv
import html
import json
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from scripts_download import get_bytes

DATA_DIR = Path(__file__).parent / "../data"
OUT_CSV = DATA_DIR / "dim_stores.csv"
INDEX_URL = "https://www.ikea.com/gb/en/stores/"
COLUMNS = [
    "store_id",
    "store_name",
    "city",
    "region",
    "country",
    "postcode",
    "address",
    "lat",
    "lon",
    "store_type",
    "blue_box",
]
# buClassification.code -> (store_type, blue_box). An unknown code is a new
# store format, not a parse error, so the run stops instead of guessing.
FORMATS = {"STORE": ("Blue box", "true"), "SMALL": ("Small format", "false")}
# /stores/ also links these index-style pages from the same sections.
NOT_A_STORE = {"plan-and-order-points", "ikea-store-events-pub92d571d0"}
LAT_RANGE, LON_RANGE = (49.8, 61.0), (-8.7, 2.0)
MIN_STORES = 20  # the locator listed 26 when this was written

HEADING = re.compile(r"<h2[^>]*>(.*?)</h2>(.*?)(?=<h2|</section>)", re.DOTALL)
STORE_LINK = re.compile(r'href="' + INDEX_URL + r'([a-z0-9-]+)/"')
JSON_LD = re.compile(r'<script type="application/ld\+json">(.*?)</script>', re.DOTALL)
# The id and classification sit a few fields after the page URL, so the search
# starts there: a reordered blob then fails loudly instead of matching a
# neighbouring store's metadata.
META = re.compile(r'"id":"(\d+)","buClassification":\{"code":"([A-Z_]+)"')
POSTCODE = re.compile(r"[A-Z]{1,2}\d[A-Z\d]?\d[A-Z]{2}")


def get(url):
    """GET a URL with an explicit User-Agent and retry transient errors."""
    return get_bytes(url, timeout=120).decode("utf-8")


def text(fragment):
    """Tags out, entities in, whitespace collapsed."""
    return re.sub(r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", "", fragment))).strip()


def parse_index(page):
    """[(region, slug)] in locator order: regions are the locator's own."""
    entries, seen = [], set()
    for heading, block in HEADING.findall(page):
        region = text(heading)
        for slug in STORE_LINK.findall(block):
            if slug in seen or slug in NOT_A_STORE or "-pub" in slug:
                continue
            seen.add(slug)
            entries.append((region, slug))
    return entries


def parse_postcode(value):
    """Postcode "bt39ej" -> "BT3 9EJ": the site mixes spaced and unspaced forms."""
    compact = re.sub(r"[^A-Za-z0-9]", "", value).upper()
    if not POSTCODE.fullmatch(compact):
        sys.exit(f"malformed postcode {value!r}")
    return f"{compact[:-3]} {compact[-3:]}"


def parse_store(region, slug):
    """One locator entry -> one dim_stores row (as CSV strings)."""
    url = f"{INDEX_URL}{slug}/"
    page = get(url)

    blocks = [json.loads(block) for block in JSON_LD.findall(page)]
    store = next((b for b in blocks if b.get("@type") == "FurnitureStore"), None)
    if store is None:
        sys.exit(f"{url}: no FurnitureStore JSON-LD -- page layout changed")
    address, geo = store["address"], store["geo"]

    marker = page.find(f'"storePageUrl":"{url}"')
    meta = META.search(page, marker if marker >= 0 else len(page))
    if meta is None:
        sys.exit(f"{url}: no store id / buClassification -- page layout changed")
    store_id, code = meta.group(1), meta.group(2)
    if code not in FORMATS:
        sys.exit(f"{url}: unknown buClassification {code!r} -- add it to FORMATS")

    lat, lon = round(float(geo["latitude"]), 6), round(float(geo["longitude"]), 6)
    inside_gb = (
        LAT_RANGE[0] <= lat <= LAT_RANGE[1] and LON_RANGE[0] <= lon <= LON_RANGE[1]
    )
    if not inside_gb:
        sys.exit(f"{url}: ({lat}, {lon}) is not in GB")
    if address["addressCountry"].upper() != "GB":
        sys.exit(f"{url}: addressCountry {address['addressCountry']!r} is not GB")

    return {
        "store_id": store_id,
        "store_name": text(store["name"]),
        "city": text(address["addressLocality"]),
        "region": region,
        "country": address["addressCountry"].upper(),
        "postcode": parse_postcode(address["postalCode"]),
        "address": text(address["streetAddress"]),
        "lat": f"{lat:.6f}",  # NUMERIC(9,6) in BigQuery: 6 places, not 16
        "lon": f"{lon:.6f}",
        "store_type": FORMATS[code][0],
        "blue_box": FORMATS[code][1],
    }


def main():
    start = time.perf_counter()
    entries = parse_index(get(INDEX_URL))
    if len(entries) < MIN_STORES:
        sys.exit(f"{INDEX_URL}: only {len(entries)} stores parsed -- layout changed?")
    print(f"source: IKEA GB store locator, {len(entries)} stores")

    with ThreadPoolExecutor(max_workers=8) as pool:
        rows = list(pool.map(lambda entry: parse_store(*entry), entries))

    ids = [row["store_id"] for row in rows]
    if len(set(ids)) != len(ids):
        sys.exit(f"duplicate store_id in {ids}")

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(OUT_CSV, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows(rows)

    print(
        f"done in {time.perf_counter() - start:.1f}s -> {OUT_CSV.resolve()} "
        f"({OUT_CSV.stat().st_size / 1e3:.1f} KB)"
    )


if __name__ == "__main__":
    main()