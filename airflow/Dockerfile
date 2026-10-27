FROM apache/airflow:slim-3.3.2-python3.14

USER root
# Install poppler-utils for source_postcode_areas.py (pdftotext)
RUN apt-get update && \
    apt-get install -y --no-install-recommends poppler-utils && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow
# Install pipeline & dbt dependencies using pre-installed uv directly into Airflow's Python 3.14 environment
RUN uv pip install --no-cache --python /home/airflow/.local/bin/python \
    "polars>=1.44.1" \
    "google-cloud-bigquery>=3.44.0" \
    "dbt-bigquery>=1.12.0" \
    "jinja2>=3.1.6" \
    "psycopg[binary]>=3.1" \
    "asyncpg>=0.30" \
    "apache-airflow-providers-fab"