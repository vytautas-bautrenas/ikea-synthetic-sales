import csv
import os
import sys
import tempfile
import time
from pathlib import Path

import google.api_core.retry
import polars as pl
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

CSV_PATH = Path(__file__).parent / "../data/dim_stores.csv"
PROJECT = os.environ.get("GCLOUD_PROJECT", "ikea-sales")
LOCATION = os.environ.get("LOCATION", "europe-west2")
DATASET = "serve"
TABLE = "dim_stores"
SCHEMA = [
    bigquery.SchemaField("store_id", "INT64"),
    bigquery.SchemaField("store_name", "STRING"),
    bigquery.SchemaField("city", "STRING"),
    bigquery.SchemaField("region", "STRING"),
    bigquery.SchemaField("country", "STRING"),
    bigquery.SchemaField("postcode", "STRING"),
    bigquery.SchemaField("address", "STRING"),
    bigquery.SchemaField("lat", "NUMERIC", precision=9, scale=6),
    bigquery.SchemaField("lon", "NUMERIC", precision=9, scale=6),
    bigquery.SchemaField("store_type", "STRING"),
    bigquery.SchemaField("blue_box", "BOOL"),
]
COLUMNS = [field.name for field in SCHEMA]


def main():
    start = time.perf_counter()
    if not CSV_PATH.exists():
        sys.exit(f"{CSV_PATH} not found")

    with open(CSV_PATH, newline="", encoding="utf-8-sig") as handle:
        header = next(csv.reader(handle))
    if header[: len(COLUMNS)] != COLUMNS:
        sys.exit(
            f"{CSV_PATH.name}: header != COLUMNS\n  csv      : {header}\n  expected : "
            f"{COLUMNS}"
        )

    destination = f"{PROJECT}.{DATASET}.{TABLE}"
    client = bigquery.Client(project=PROJECT, location=LOCATION)
    try:
        client.get_dataset(f"{PROJECT}.{DATASET}")
    except NotFound:
        client.create_dataset(f"{PROJECT}.{DATASET}")

    with tempfile.TemporaryDirectory() as tmp:
        parquet_path = Path(tmp) / "load.parquet"
        pl.scan_csv(
            CSV_PATH,
            schema_overrides={"lat": pl.Decimal(9, 6), "lon": pl.Decimal(9, 6)},
        ).sink_parquet(parquet_path)
        print(
            f"read {CSV_PATH.name}: {parquet_path.stat().st_size / 1e3:.0f} KB parquet "
            f"in {time.perf_counter() - start:.1f}s"
        )

        with open(parquet_path, "rb") as parquet_file:
            job = client.load_table_from_file(
                parquet_file,
                destination,
                job_config=bigquery.LoadJobConfig(
                    schema=SCHEMA,
                    source_format=bigquery.SourceFormat.PARQUET,
                    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                    create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
                ),
            )
            job.result(retry=google.api_core.retry.Retry(deadline=15))  # raises on failure (bad rows / import errors)

    loaded = job.output_rows or 0
    table = client.get_table(destination)
    if job.errors or table.num_rows != loaded:
        sys.exit(
            f"{destination}: row-count mismatch (BQ={table.num_rows:,} "
            f"loaded={loaded:,} errors={len(job.errors or [])})"
        )
    print(
        f"wrote {destination}: {table.num_rows:,} rows verified | {table.num_bytes:,} "
        f"bytes"
    )
    print(f"done in {time.perf_counter() - start:.1f}s")


if __name__ == "__main__":
    main()
