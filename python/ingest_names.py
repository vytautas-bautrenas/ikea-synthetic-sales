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

DATA_DIR = Path(__file__).parent / "../data/school_census_names"
PROJECT = os.environ.get("GCLOUD_PROJECT", "ikea-sales")
LOCATION = os.environ.get("LOCATION", "europe-west2")
DATASET = "ingest"
# table -> (csv, {source column: destination column})
TABLES = {
    "uk_census_forenames": (
        "forenames.csv",
        {"Forename": "forename", "Count": "name_count"},
    ),
    "uk_census_surnames": (
        "surnames.csv",
        {"Surname": "surname", "Count": "name_count"},
    ),
}


def main():
    start = time.perf_counter()
    client = bigquery.Client(project=PROJECT, location=LOCATION)
    try:
        client.get_dataset(f"{PROJECT}.{DATASET}")
    except NotFound:
        client.create_dataset(f"{PROJECT}.{DATASET}")

    for table, (csv_name, rename) in TABLES.items():
        csv_path = DATA_DIR / csv_name
        if not csv_path.exists():
            sys.exit(f"{csv_path} not found -- run source_names.py first")

        with open(csv_path, newline="", encoding="utf-8-sig") as handle:
            header = next(csv.reader(handle))
        if header[: len(rename)] != list(rename):
            sys.exit(
                f"{csv_name}: header != expected\n  csv      : {header}\n  expected : "
                f"{list(rename)}"
            )

        destination = f"{PROJECT}.{DATASET}.{table}"
        with tempfile.TemporaryDirectory() as tmp:
            parquet_path = Path(tmp) / "load.parquet"
            pl.scan_csv(csv_path).rename(rename).sink_parquet(parquet_path)
            print(
                f"read {csv_name}: {parquet_path.stat().st_size / 1e3:.0f} KB parquet "
                f"in {time.perf_counter() - start:.1f}s"
            )

            with open(parquet_path, "rb") as parquet_file:
                job = client.load_table_from_file(
                    parquet_file,
                    destination,
                    job_config=bigquery.LoadJobConfig(
                        source_format=bigquery.SourceFormat.PARQUET,
                        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
                    ),
                )
                job.result(retry=google.api_core.retry.Retry(deadline=15))  # raises on failure (bad rows / import errors)

        loaded = job.output_rows or 0
        table_ref = client.get_table(destination)
        counted = next(client.query(f"SELECT COUNT(*) FROM `{destination}`").result())[
            0
        ]
        if job.errors or not (table_ref.num_rows == counted == loaded):
            sys.exit(
                f"{destination}: row-count mismatch (BQ={table_ref.num_rows:,} "
                f"COUNT(*)={counted:,} "
                f"loaded={loaded:,} errors={len(job.errors or [])})"
            )
        print(
            f"wrote {destination}: {table_ref.num_rows:,} rows verified | "
            f"{table_ref.num_bytes:,} bytes"
        )

    print(f"done in {time.perf_counter() - start:.1f}s")


if __name__ == "__main__":
    main()
