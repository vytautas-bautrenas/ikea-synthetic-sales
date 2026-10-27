import csv
import re
import subprocess
import sys
import time
from pathlib import Path

PDF_PATH = (
    Path(__file__).parent
    / "../data/ONSPD_MAY_2026/User Guide/ONSPD User Guide May 2026.pdf"
)
OUT_CSV = Path(__file__).parent / "../data/ONSPD_MAY_2026/Documents/postcode_areas.csv"
COLUMNS = ["postcode_area", "area_code"]

# A data row is: area name, 1-2 letter code, then five integer columns. The name
# column never contains a run of two spaces, which is what separates the fields.
ROW = re.compile(
    r"^([A-Za-z][A-Za-z .&'()-]*?) {2,}([A-Z]{1,2}) {2,}"
    r"([0-9,]+) {2,}([0-9,]+) {2,}([0-9,]+) {2,}([0-9,]+) {2,}([0-9,]+)$"
)
# "Total" closes the table and its name column is empty, so ROW cannot match it;
# these are the subtotal rows that ROW would otherwise accept as a data row.
SUBTOTALS = {"United Kingdom", "Great Britain", "Channel Islands"}


def pdf_text(pdf_path):
    if not pdf_path.exists():
        sys.exit(f"{pdf_path} not found")
    result = subprocess.run(
        ["pdftotext", "-layout", str(pdf_path), "-"],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        sys.exit(f"pdftotext failed: {result.stderr.strip()}")
    return result.stdout


def parse_table(text):
    lines = [line.strip() for line in text.splitlines()]
    try:
        start = next(
            i
            for i, line in enumerate(lines)
            if line.startswith("Postcode Area") and "Post" in line
        )
    except StopIteration:
        sys.exit("Table 2 header not found -- is this the ONSPD User Guide?")

    rows, seen = [], set()
    for line in lines[start:]:
        if line.startswith("Total"):
            break
        match = ROW.match(line)
        if not match:
            continue
        name, code = match.group(1).strip(), match.group(2)
        if name in SUBTOTALS or code in seen:
            continue
        seen.add(code)
        rows.append((name, code))
    return rows


def main():
    start = time.perf_counter()
    rows = parse_table(pdf_text(PDF_PATH))
    if not rows:
        sys.exit("no postcode areas parsed")

    with open(OUT_CSV, "w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(COLUMNS)
        writer.writerows(rows)

    print(
        f"wrote {OUT_CSV}: {len(rows)} postcode areas | "
        f"{OUT_CSV.stat().st_size / 1e3:.1f} KB in {time.perf_counter() - start:.1f}s"
    )

if __name__ == "__main__":
    main()
