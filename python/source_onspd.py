import json
import time
import urllib.parse
import zipfile
from pathlib import Path

from scripts_download import download, get_bytes

DATA_ROOT = Path(__file__).parent / "../data"
DATA_DIR = DATA_ROOT / "ONSPD_MAY_2026"
ARCHIVE = DATA_ROOT / "ONSPD_MAY_2026.zip"

RELEASE = "ONSPD_MAY_2026"
TITLE = "ONS Postcode Directory (May 2026)"
OWNER = "ONSGeography_data"
SEARCH_URL = "https://www.arcgis.com/sharing/rest/search"
ITEM_META_URL = "https://www.arcgis.com/sharing/rest/content/items/{}?f=json"
ITEM_DATA_URL = "https://www.arcgis.com/sharing/rest/content/items/{}/data"
TIMEOUT = 900

# The only members extracted, i.e. every file the pipeline reads. A rename
# upstream should fail here, with the zip still on disk, not mid-ingest.
KEEP = (
    f"Data/{RELEASE}_UK.csv",  # ingest_onspd.py CSV_PATH
    "Documents/CTY County names and codes UK as at 05_25.csv",  # ingest_onspd.py
    "Documents/LAD Local Authority District names and codes UK as at 04_25.csv",
    "Documents/BUA Built Up Area names and codes EW as at 04_24.csv",
    "User Guide/ONSPD User Guide May 2026.pdf",  # source_postcode_areas.py
)
# Parsed out of that PDF by source_postcode_areas.py, so not in the zip but kept.
GENERATED = "Documents/postcode_areas.csv"


def find_item():
    """Resolve the portal item for this release -> (item id, size in bytes)."""
    query = urllib.parse.urlencode(
        {"f": "json", "num": 50, "q": f'title:"{TITLE}" owner:{OWNER}'}
    )
    results = json.loads(
        get_bytes(f"{SEARCH_URL}?{query}", timeout=TIMEOUT)
    ).get("results", [])

    matches = [
        item
        for item in results
        if item.get("type") == "CSV Collection"
        and item.get("name") == f"{RELEASE}.zip"
        and item.get("access") == "public"
    ]
    if not matches:
        raise SystemExit(
            f"portal returned no public {RELEASE}.zip named {TITLE!r} "
            f"({len(results)} item(s) searched)"
        )
    item = max(matches, key=lambda entry: entry.get("modified", 0))
    # search results carry size=-1, so the metadata endpoint is the only source.
    size = json.loads(
        get_bytes(ITEM_META_URL.format(item["id"]), timeout=TIMEOUT)
    ).get("size", -1)
    return item["id"], size


def extract(archive):
    """Extract the kept members, after checking they are all in the archive."""
    with zipfile.ZipFile(archive) as zipped:
        names = zipped.namelist()
        missing = sorted(set(KEEP) - set(names))
        if missing:
            raise SystemExit(f"{archive.name} does not contain: {missing}")
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        for name in KEEP:
            zipped.extract(name, DATA_DIR)  # reads to EOF, so CRCs are checked
    return [DATA_DIR / name for name in KEEP]


def prune(keep):
    """Delete everything else under DATA_DIR, then any directory left empty."""
    keep = {path.resolve() for path in keep}
    removed = freed = 0
    paths = sorted(DATA_DIR.rglob("*"), key=lambda path: len(path.parts), reverse=True)
    for path in paths:
        if path.is_dir() and not path.is_symlink():
            if not any(path.iterdir()):
                path.rmdir()
        elif path.resolve() not in keep:
            freed += path.stat().st_size
            path.unlink()
            removed += 1
    return removed, freed


def main():
    start = time.perf_counter()
    item_id, size = find_item()
    print(f"source: {TITLE}, OGL v3.0, via portal item {item_id} ({size:,} bytes)")

    download(
        ITEM_DATA_URL.format(item_id),
        ARCHIVE,
        expected_size=size,
        timeout=TIMEOUT,
    )
    kept = extract(ARCHIVE)

    absent = [str(path.relative_to(DATA_DIR)) for path in kept if not path.exists()]
    if absent:
        raise SystemExit(f"extracted but not on disk: {absent}")

    removed, freed = prune(kept + [DATA_DIR / GENERATED])
    ARCHIVE.unlink()  # kept files are verified, so the 235 MB zip is redundant

    print(
        f"wrote {len(kept)} files to {DATA_DIR.resolve()}: "
        f"{sum(path.stat().st_size for path in kept) / 1e9:.2f} GB kept"
    )
    if removed:
        print(f"pruned {removed} unread files: {freed / 1e9:.2f} GB freed")
    if not (DATA_DIR / GENERATED).exists():
        print(f"note: {GENERATED} absent -- run `make source-postcode-areas`")
    print(f"done in {time.perf_counter() - start:.1f}s")


if __name__ == "__main__":
    main()
