import csv
import os
import sys
import tempfile
import time
from functools import partial
from pathlib import Path

import google.api_core.retry
import polars as pl
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

CSV_PATH = Path(__file__).parent / "../data/ONSPD_MAY_2026/Data/ONSPD_MAY_2026_UK.csv"
LOOKUPS_DIR = Path(__file__).parent / "../data/ONSPD_MAY_2026/Documents"
PROJECT = os.environ.get("GCLOUD_PROJECT", "ikea-sales")
LOCATION = os.environ.get("LOCATION", "europe-west2")
DATASET = "ingest"

SCHEMA = [
    bigquery.SchemaField("pcds", "STRING"),
    bigquery.SchemaField("dointr", "INT64"),
    bigquery.SchemaField("doterm", "INT64"),
    bigquery.SchemaField("east1m", "INT64"),
    bigquery.SchemaField("north1m", "INT64"),
    bigquery.SchemaField("ctry25cd", "STRING"),
    bigquery.SchemaField("lat", "NUMERIC", precision=9, scale=6),
    bigquery.SchemaField("long", "NUMERIC", precision=9, scale=6),
    bigquery.SchemaField("cty25cd", "STRING"),
    bigquery.SchemaField("lad25cd", "STRING"),
    bigquery.SchemaField("bua24cd", "STRING"),
]

DESTINATIONS = [field.name for field in SCHEMA]

SCAN = {
    "pcds": pl.String,
    "dointr": pl.Int64,
    "doterm": pl.Int64,
    "east1m": pl.Int64,
    "north1m": pl.Int64,
    "ctry25cd": pl.String,
    "lat": pl.Decimal(9, 6),
    "long": pl.Decimal(9, 6),
    "cty25cd": pl.String,
    "lad25cd": pl.String,
    "bua24cd": pl.String,
}

LOOKUPS = {
    "onspd_cty_names": (
        "CTY County names and codes UK as at 05_25.csv",
        {"CTY25CD": "cty25cd", "CTY25NM": "cty25nm"},
    ),
    "onspd_lad_names": (
        "LAD Local Authority District names and codes UK as at 04_25.csv",
        {"LAD25CD": "lad25cd", "LAD25NM": "lad25nm"},
    ),
    "onspd_bua_names": (
        "BUA Built Up Area names and codes EW as at 04_24.csv",
        {"BUA24CD": "bua24cd", "BUA24NM": "bua24nm"},
    ),
    "onspd_postcode_areas": (
        "postcode_areas.csv",
        {"postcode_area": "postcode_area", "area_code": "area_code"},
    ),
}


def scan_onspd(csv_path):
    """Typed scan of the kept columns, parsed as text first to sidestep inference."""
    return pl.scan_csv(csv_path, infer_schema_length=0, schema_overrides=SCAN).select(
        DESTINATIONS
    )


def read_lookup(csv_path, rename):
    """Read the named columns and drop placeholder rows.
    "(pseudo) ..." rows are placeholders for codes with no real place name; the
    count is printed so a shortfall against the source file stays visible.
    """
    values = {destination: [] for destination in rename.values()}
    dropped = 0
    with open(csv_path, newline="", encoding="utf-8-sig") as handle:
        for record in csv.DictReader(handle):
            row = [(record.get(source) or "").strip() for source in rename]
            if not row[0]:
                continue
            if any(value.startswith("(pseudo)") for value in row):
                dropped += 1
                continue
            for destination, value in zip(values, row):
                values[destination].append(value or None)
    if dropped:
        print(f"{csv_path.name}: dropped {dropped:,} placeholder rows")
    return pl.DataFrame(
        {name: pl.Series(name, series) for name, series in values.items()}
    )


def load_csv(client, csv_path, table, columns, reader=None, schema=None):
    """Local file -> Parquet -> BigQuery, verified; exits on any mismatch."""
    start = time.perf_counter()
    if not csv_path.exists():
        sys.exit(f"{csv_path} not found")

    with open(csv_path, newline="", encoding="utf-8-sig") as handle:
        header = next(csv.reader(handle))

    if bool(set(columns) - set(header)):
        sys.exit(
            f"{csv_path.name}: header != expected\n  csv      : {header}\n  expected : "
            f"{columns}"
        )

    destination = f"{PROJECT}.{DATASET}.{table}"

    with tempfile.TemporaryDirectory() as tmp:
        parquet_path = Path(tmp) / "load.parquet"
        frame = reader(csv_path) if reader else pl.scan_csv(csv_path)
        if isinstance(frame, pl.LazyFrame):
            frame.sink_parquet(parquet_path)
        else:
            frame.write_parquet(parquet_path)
        print(
            f"read {csv_path.name}: {parquet_path.stat().st_size / 1e6:.1f} MB parquet "
            f"in {time.perf_counter() - start:.1f}s"
        )

        with open(parquet_path, "rb") as parquet_file:
            job = client.load_table_from_file(
                parquet_file,
                destination,
                job_config=bigquery.LoadJobConfig(
                    schema=schema,
                    source_format=bigquery.SourceFormat.PARQUET,
                    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                    create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
                ),
            )
            job.result(retry=google.api_core.retry.Retry(deadline=15))  # raises on failure (bad rows / import errors)

    loaded = job.output_rows or 0
    table_ref = client.get_table(destination)
    if job.errors or table_ref.num_rows != loaded:
        sys.exit(
            f"{destination}: row-count mismatch (BQ={table_ref.num_rows:,} "
            f"loaded={loaded:,} errors={len(job.errors or [])})"
        )
    print(
        f"wrote {destination}: {table_ref.num_rows:,} rows verified | "
        f"{table_ref.num_bytes:,} bytes"
    )
    print(f"done in {time.perf_counter() - start:.1f}s\n")


def main():
    client = bigquery.Client(project=PROJECT, location=LOCATION)
    try:
        client.get_dataset(f"{PROJECT}.{DATASET}")
    except NotFound:
        client.create_dataset(f"{PROJECT}.{DATASET}")

    load_csv(
        client=client,
        csv_path=CSV_PATH,
        table="ONSPD",
        columns=DESTINATIONS,
        reader=scan_onspd,
        schema=SCHEMA,
    )

    for lookup_table, (lookup_csv, rename) in LOOKUPS.items():
        load_csv(
            client=client,
            csv_path=LOOKUPS_DIR / lookup_csv,
            table=lookup_table,
            columns=list(rename),
            reader=partial(read_lookup, rename=rename),
        )


if __name__ == "__main__":
    main()
