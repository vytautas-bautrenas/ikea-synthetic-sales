"""FastAPI Application for UK Sales Geographic Gridded Heatmap.

Serves the interactive 10km x 10km Square Grid Heatmap overlaid on
smooth UK national and coastal boundaries, sourced from BigQuery table
`serve.agg_sales_grid_10km`.
"""

import json
import os
import threading
import urllib.request
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, Query, Request
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from google.cloud import bigquery

# Setup Paths & Environment
BASE_DIR = Path(__file__).resolve().parent
TEMPLATES_DIR = BASE_DIR / "templates"

PROJECT = os.environ.get("GCLOUD_PROJECT", "ikea-sales")
DATASET = 'serve'
LOCATION = os.environ.get("LOCATION", "europe-west2")

# ONS Open Geography Portal REST endpoint for UK national boundaries (GeoJSON)
# Using BUC (Ultra-Generalised 500m) for fast web vector loading without timeouts
ONS_UK_BOUNDARIES_URL = (
    "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/"
    "Countries_December_2024_Boundaries_UK_BUC/FeatureServer/0/query"
    "?where=1%3D1&outFields=*&f=geojson"
)

# In-memory cached dataset to prevent redundant BigQuery queries on map interactions
_CACHE_GRID: Optional[List[Dict[str, Any]]] = None
_CACHE_UK_BOUNDARIES: Optional[Dict[str, Any]] = None


@asynccontextmanager
async def lifespan(_app: FastAPI):
    """Pre-warm BigQuery grid and ONS boundary caches in the background on startup."""
    threading.Thread(target=get_uk_boundaries, daemon=True).start()
    threading.Thread(target=load_grid_data, daemon=True).start()
    yield


app = FastAPI(title="IKEA UK Sales Heatmap", version="1.0.0", lifespan=lifespan)
app.add_middleware(GZipMiddleware, minimum_size=1000)
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


def get_uk_boundaries() -> Dict[str, Any]:
    """Fetch UK boundaries GeoJSON dynamically from ONS Open Geography and cache in-memory."""
    global _CACHE_UK_BOUNDARIES
    if _CACHE_UK_BOUNDARIES is not None:
        return _CACHE_UK_BOUNDARIES

    try:
        req = urllib.request.Request(
            ONS_UK_BOUNDARIES_URL,
            headers={"User-Agent": "ikea-sales-heatmap/1.0"}
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            _CACHE_UK_BOUNDARIES = data
            return _CACHE_UK_BOUNDARIES
    except Exception as e:
        print(f"[ERROR] Failed to fetch ONS UK boundaries: {e}")
        return {"type": "FeatureCollection", "features": []}


@app.get("/api/boundaries/uk", response_class=JSONResponse)
def get_boundaries():
    """Dynamic API endpoint returning UK national boundaries GeoJSON from ONS."""
    return get_uk_boundaries()


def load_grid_data(force_refresh: bool = False) -> List[Dict[str, Any]]:
    """Fetch 10km grid sales aggregates from BigQuery."""
    global _CACHE_GRID
    if _CACHE_GRID is not None and not force_refresh:
        return _CACHE_GRID

    query = f"""
    SELECT
        grid_lat,
        grid_long,
        max_lat,
        max_long,
        center_lat,
        center_long,
        country,
        local_authority,
        total_orders,
        total_sales_gbp,
        avg_order_value_gbp
    FROM `{PROJECT}.{DATASET}.agg_sales_grid_10km`
    ORDER BY total_sales_gbp DESC
    """

    try:
        bq_client = bigquery.Client(project=PROJECT, location=LOCATION)
        query_job = bq_client.query(query)
        rows = list(query_job.result())

        data = []
        for r in rows:
            data.append({
                "grid_id": f"{r.grid_lat}_{r.grid_long}",
                "bounds": [
                    [float(r.grid_lat), float(r.grid_long)],
                    [float(r.max_lat), float(r.max_long)],
                ],
                "center": [float(r.center_lat), float(r.center_long)],
                "country": str(r.country or "United Kingdom"),
                "local_authority": str(r.local_authority or ""),
                "total_orders": int(r.total_orders or 0),
                "total_sales_gbp": float(round(r.total_sales_gbp or 0.0, 2)),
                "avg_order_value_gbp": float(round(r.avg_order_value_gbp or 0.0, 2)),
            })

        _CACHE_GRID = data
        return _CACHE_GRID

    except Exception as e:
        print(f"[ERROR] Failed to load grid data from BigQuery: {e}")
        return []


def format_abbreviated_currency(val: float) -> str:
    """Format number abbreviated to the nearest m, bn, k, tn, e.g. £123bn, £12tn, £13k."""
    if val is None or val == 0:
        return "£0"
    abs_val = abs(val)
    sign = "-" if val < 0 else ""
    if abs_val >= 1e12:
        return f"{sign}£{int(round(abs_val / 1e12))}tn"
    elif abs_val >= 1e9:
        return f"{sign}£{int(round(abs_val / 1e9))}bn"
    elif abs_val >= 1e6:
        return f"{sign}£{int(round(abs_val / 1e6))}m"
    elif abs_val >= 1e3:
        return f"{sign}£{int(round(abs_val / 1e3))}k"
    else:
        return f"{sign}£{int(round(abs_val))}"


def get_summary_stats(data: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Compute aggregate KPIs across all 10km grid cells."""
    if not data:
        return {
            "total_sales_formatted": "£0",
            "total_orders_formatted": "0",
            "total_cells": 0,
            "top_cell": "N/A",
        }

    total_sales = sum(d["total_sales_gbp"] for d in data)
    total_orders = sum(d["total_orders"] for d in data)
    total_cells = len(data)
    top_cell = f"{data[0]['local_authority']} ({format_abbreviated_currency(data[0]['total_sales_gbp'])})" if data else "N/A"

    return {
        "total_sales_formatted": f"£{int(round(total_sales)):,}",
        "total_orders_formatted": f"{total_orders:,}",
        "total_cells": total_cells,
        "top_cell": top_cell,
    }


@app.get("/health")
def health_check():
    """Health check endpoint."""
    return {"status": "ok"}


@app.get("/api/grid", response_class=JSONResponse)
def get_grid(country: Optional[str] = Query(None), refresh: bool = False):
    """API endpoint returning 10km grid squares with sales metrics and bounds."""
    data = load_grid_data(force_refresh=refresh)
    if country and country != "ALL":
        return [d for d in data if d["country"].lower() == country.lower()]
    return data


@app.get("/", response_class=HTMLResponse)
def heatmap_page(request: Request):
    """Render the Interactive 10km Gridded Heatmap Dashboard page.

    Refreshes BigQuery data cache whenever the webpage is loaded/refreshed.
    """
    data = load_grid_data(force_refresh=True)
    summary = get_summary_stats(data)

    return templates.TemplateResponse(
        request=request,
        name="map.html",
        context={
            "summary": summary,
        },
    )
