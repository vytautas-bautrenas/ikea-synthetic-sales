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

CSV_PATH = Path(__file__).parent / "../data/Ikea_Product_Catalog.csv"
PROJECT = os.environ.get("GCLOUD_PROJECT", "ikea-sales")
LOCATION = os.environ.get("LOCATION", "europe-west2")
DATASET = "ingest"
TABLE = "ikea_product_catalog"
SCHEMA = [
    bigquery.SchemaField("unique_id", "STRING"),
    bigquery.SchemaField("product_id", "INT64"),
    bigquery.SchemaField("product_name", "STRING"),
    bigquery.SchemaField("product_type", "STRING"),
    bigquery.SchemaField("product_measurements", "STRING"),
    bigquery.SchemaField("product_description", "STRING"),
    bigquery.SchemaField("main_category", "STRING"),
    bigquery.SchemaField("sub_category", "STRING"),
    bigquery.SchemaField("product_rating", "NUMERIC", precision=2, scale=1),
    bigquery.SchemaField("product_rating_count", "INT64"),
    bigquery.SchemaField("badge", "STRING"),
    bigquery.SchemaField("online_sellable", "BOOL"),
    bigquery.SchemaField("url", "STRING"),
    bigquery.SchemaField("price_100", "INT64"),
    bigquery.SchemaField("currency", "STRING"),
    bigquery.SchemaField("discount", "STRING"),
    bigquery.SchemaField("sale_tag", "STRING"),
    bigquery.SchemaField("country", "STRING"),
]
DESTINATIONS = [field.name for field in SCHEMA]
# the CSV ships "price" and the table stores "price_100": same order, one rename
COLUMNS = ["price" if name == "price_100" else name for name in DESTINATIONS]


def clean(csv_path):
    """Typed scan with the catalog's null/type conversions, in destination order."""
    return (
        pl.scan_csv(csv_path)
        .with_columns(
            pl.col("product_rating").cast(pl.Decimal(2, 1), strict=False),
            pl.col("product_rating_count")
            .cast(pl.Float64, strict=False)
            .cast(pl.Int64),
            (pl.col("price") * 100).round().cast(pl.Int64).alias("price_100"),
        )
        .select(DESTINATIONS)
    )


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
        clean(CSV_PATH).sink_parquet(parquet_path)
        print(
            f"read {CSV_PATH.name}: {parquet_path.stat().st_size / 1e6:.1f} MB parquet "
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
