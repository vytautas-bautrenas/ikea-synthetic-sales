import csv
import io
import time
import zipfile
from pathlib import Path

from scripts_download import download

DATA_DIR = Path(__file__).parent / "../data/street_names"
OUT_CSV = DATA_DIR / "road_names.csv"
DOWNLOAD_URL = (
    "https://api.os.uk/downloads/v1/products/OpenNames/downloads"
    "?area=GB&format=CSV&redirect"
)
NAME_AT, LOCAL_TYPE_AT, DISTRICT_AT = 2, 7, 16


def main():
    start = time.perf_counter()
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    archive = DATA_DIR / "opname_csv_gb.zip"
    download(DOWNLOAD_URL, archive)

    with zipfile.ZipFile(archive) as zipped:
        pairs = {
            (row[NAME_AT].strip(), row[DISTRICT_AT].strip())
            for member in zipped.namelist()
            if member.endswith(".csv")
            for row in csv.reader(
                io.TextIOWrapper(zipped.open(member), encoding="utf-8-sig")
            )
            if row[LOCAL_TYPE_AT].strip().lower() == "named road"
            and row[NAME_AT].strip()
            and row[DISTRICT_AT].strip()
        }
    assert pairs, "no Named Road rows: the layout or the filter has changed"

    with open(OUT_CSV, "w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["road_name", "postcode_district"])
        writer.writerows(pairs)
    archive.unlink()  # 100 MB, read once
    print(
        f"wrote {OUT_CSV.name}: {len(pairs):,} pairs | "
        f"{len({district for _, district in pairs}):,} districts | "
        f"{len({name for name, _ in pairs}):,} distinct road names"
    )
    print(f"done in {time.perf_counter() - start:.1f}s -> {DATA_DIR.resolve()}")


if __name__ == "__main__":
    main()
