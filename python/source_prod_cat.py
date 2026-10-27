import json
import time
import zipfile
from pathlib import Path

from scripts_download import CHUNK, download, get_bytes

DATA_ROOT = Path(__file__).parent / "../data"
OUT_CSV = DATA_ROOT / "Ikea_Product_Catalog.csv" 
ARCHIVE = DATA_ROOT / "ikea_product_catalog.zip"

DATASET = "ranjithpanicker/ikea-global-product-catalog-and-price-2026"
TITLE = "IKEA Global Product Catalog (46 Countries) - 2026"
LIST_URL = f"https://www.kaggle.com/api/v1/datasets/list/{DATASET}"
DOWNLOAD_URL = f"https://www.kaggle.com/api/v1/datasets/download/{DATASET}"
TIMEOUT = 900


EXPECTED_HEADER = [
    "unique_id",
    "product_id",
    "product_name",
    "product_type",
    "product_measurements",
    "product_description",
    "main_category",
    "sub_category",
    "product_rating",
    "product_rating_count",
    "badge",
    "online_sellable",
    "url",
    "price",
    "currency",
    "discount",
    "sale_tag",
    "country",
]


def find_file():
    """Ask the dataset listing for its one CSV -> (file name, size in bytes)."""
    files = json.loads(get_bytes(LIST_URL, timeout=TIMEOUT)).get("datasetFiles", [])

    csvs = [entry for entry in files if entry.get("name", "").endswith(".csv")]
    if len(csvs) != 1:
        raise SystemExit(
            f"{DATASET} lists {len(csvs)} CSV file(s), expected exactly 1: "
            f"{[entry.get('name') for entry in files]}"
        )
    entry = csvs[0]
    return entry["name"], entry.get("totalBytes") or -1


def extract(archive, member, size, destination):
    """Write that one member to destination, checking its header -> row count."""
    partial = destination.with_name(destination.name + ".part")
    rows = 0
    with zipfile.ZipFile(archive) as zipped:
        if member not in zipped.namelist():
            raise SystemExit(
                f"{archive.name} does not contain {member!r}: {zipped.namelist()}"
            )
        with zipped.open(member) as source, open(partial, "wb") as handle:
            header = source.readline()  # utf-8-sig: the member starts with a BOM
            columns = header.decode("utf-8-sig").rstrip("\r\n").split(",")
            if columns[: len(EXPECTED_HEADER)] != EXPECTED_HEADER:
                raise SystemExit(
                    f"{member}: header != ingest_prod_cat.py COLUMNS\n"
                    f"  csv      : {columns}\n"
                    f"  expected : {EXPECTED_HEADER}"
                )
            handle.write(header)
            tail = b""
            while chunk := source.read(CHUNK):  # to EOF, so the CRC is checked
                handle.write(chunk)
                rows += chunk.count(b"\n")
                tail = chunk[-1:]
            if tail not in (b"\n", b""):  # final row without a trailing newline
                rows += 1

    written = partial.stat().st_size
    if size > 0 and written != size:
        raise SystemExit(
            f"short extract: {partial} is {written:,} bytes, listing says {size:,}"
        )
    partial.replace(destination)
    return rows


def main():
    start = time.perf_counter()
    member, size = find_file()
    print(f"source: {TITLE}, via {DATASET} ({size:,} bytes)")

    download(DOWNLOAD_URL, ARCHIVE, timeout=TIMEOUT)
    rows = extract(ARCHIVE, member, size, OUT_CSV)
    ARCHIVE.unlink()  # the CSV is verified, so the 121 MB zip is redundant
    print(f"wrote {OUT_CSV.name}: {rows:,} rows")

    print(f"done in {time.perf_counter() - start:.1f}s -> {OUT_CSV.resolve()}")


if __name__ == "__main__":
    main()
