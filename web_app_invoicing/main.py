"""FastAPI Web Invoicing Application.

Direct BigQuery lookup for Order ID + Postcode verification.
Formats output matching the official IKEA VAT invoice specification.
"""

import io
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import barcode
from barcode.writer import SVGWriter
from fastapi import FastAPI, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from google.cloud import bigquery

# Setup Paths & Environment
TEMPLATES_DIR = Path(__file__).resolve().parent / "templates"

PROJECT = os.environ.get("GCLOUD_PROJECT", "ikea-sales")
DATASET = "serve"
LOCATION = os.environ.get("LOCATION", "europe-west2")

_BQ_CLIENT: Optional[bigquery.Client] = None


def get_bq_client() -> bigquery.Client:
    """Return a shared BigQuery client instance to reuse OAuth sessions across requests."""
    global _BQ_CLIENT
    if _BQ_CLIENT is None:
        _BQ_CLIENT = bigquery.Client(project=PROJECT, location=LOCATION)
    return _BQ_CLIENT


app = FastAPI(title="IKEA Sales Web Invoicing App", version="1.0.0")
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


def format_currency_val(cents: Optional[int]) -> str:
    """Format integer pence value to two decimal places (e.g. 50 -> '0.50')."""
    if cents is None:
        return "0.00"
    return f"{cents / 100.0:.2f}"


def format_date_dmy(dt_val: Any) -> str:
    """Format datetime as DD/MM/YYYY."""
    if not dt_val:
        return ""
    if isinstance(dt_val, datetime):
        return dt_val.strftime("%d/%m/%Y")
    dt_str = str(dt_val)
    try:
        parsed = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        return parsed.strftime("%d/%m/%Y")
    except Exception:
        # Fallback if already date-like YYYY-MM-DD
        parts = dt_str[:10].split("-")
        if len(parts) == 3:
            return f"{parts[2]}/{parts[1]}/{parts[0]}"
        return dt_str[:10]


def format_article_number(product_id: Any) -> str:
    """Format 8-digit product ID as IKEA article number 'XXX.XXX.XX'."""
    if not product_id:
        return "000.000.00"
    raw_digits = re.sub(r"\D", "", str(product_id)).zfill(8)
    if len(raw_digits) >= 8:
        return f"{raw_digits[:3]}.{raw_digits[3:6]}.{raw_digits[6:8]}"
    return raw_digits


def generate_barcode_svg(code_str: str) -> str:
    """Generate authentic, standard Code 128 SVG barcode decodable by standard readers."""
    try:
        code = barcode.get("code128", str(code_str), writer=SVGWriter())
        buf = io.BytesIO()
        code.write(
            buf,
            options={
                "write_text": False,
                "module_height": 7.0,
                "module_width": 0.22,
                "quiet_zone": 2.2,
            },
        )
        svg_str = buf.getvalue().decode("utf-8")
        svg_start = svg_str.find("<svg")
        if svg_start == -1:
            return ""
        clean_svg = svg_str[svg_start:]

        # Extract root SVG dimensions (e.g. width="43.560mm" height="9.000mm")
        m_w = re.search(r'\bwidth="([\d\.]+)mm"', clean_svg)
        m_h = re.search(r'\bheight="([\d\.]+)mm"', clean_svg)
        w_val = m_w.group(1) if m_w else "43.560"
        h_val = m_h.group(1) if m_h else "9.000"

        # Remove "mm" unit from all coordinates and dimensions so they match viewBox user units
        clean_svg = re.sub(r'(\b(?:x|y|width|height)="[\d\.]+)(mm)(")', r'\1\3', clean_svg)

        # Update root <svg> with viewBox and responsive display style, stripping width/height
        def fix_root_svg(match: re.Match) -> str:
            tag = match.group(0)
            tag = re.sub(r'\s*\bwidth="[^"]*"', '', tag)
            tag = re.sub(r'\s*\bheight="[^"]*"', '', tag)
            return f'{tag[:-1]} viewBox="0 0 {w_val} {h_val}" style="height: 28px; width: auto; display: inline-block;">'

        clean_svg = re.sub(r'<svg\b[^>]*>', fix_root_svg, clean_svg, count=1)
        return clean_svg
    except Exception as e:
        print(f"[ERROR] Failed to generate barcode: {e}")
        return ""


@app.get("/health")
def health_check():
    """Health check endpoint."""
    return {"status": "ok"}


@app.get("/", response_class=HTMLResponse)
def search_page(request: Request, error: Optional[str] = None):
    """Render the Invoice Search landing page."""
    return templates.TemplateResponse(
        request=request,
        name="search.html",
        context={
            "error": error,
            "now_year": datetime.now().year,
        },
    )


@app.get("/invoice", response_class=HTMLResponse)
def lookup_invoice(
    request: Request,
    order_id: str = Query(..., description="Order ID number"),
    postcode: str = Query(..., description="Customer billing/delivery postcode"),
):
    """Perform BigQuery point lookup and render VAT invoice matching sample."""
    raw_order_id = order_id.strip()
    raw_postcode = postcode.strip()

    digits_match = re.findall(r"\d+", raw_order_id)
    if not digits_match:
        return templates.TemplateResponse(
            request=request,
            name="search.html",
            context={
                "error": f"Invalid Order ID format: '{raw_order_id}'. Please enter numbers only.",
                "order_id": raw_order_id,
                "postcode": raw_postcode,
                "now_year": datetime.now().year,
            },
        )
    
    clean_order_id = int("".join(digits_match))
    clean_postcode = re.sub(r"\s+", "", raw_postcode).upper()

    query = fr"""
    SELECT
        o.order_id,
        o.delivery_type,
        o.order_datetime,
        o.delivery_datetime,
        o.payment_method,
        o.total_goods_100,
        o.picking_delivery_100,
        o.order_vat_rate_100,
        o.total_sales_100,
        o.total_vat_100,
        o.total_sales_excl_vat_100,
        p.vat_rate,

        -- Customer Info
        c.first_name,
        c.last_name,
        c.address_line_1,
        c.city_town,
        c.postcode,
        c.country,

        -- Line Item Info
        ol.line_number,
        ol.line_quantity,
        ol.line_price_100,
        ol.line_total_100,

        -- Product Info
        COALESCE(p.product_name, CONCAT('IKEA Item #', CAST(ol.product_id AS STRING))) AS product_name,
        ol.product_id

    FROM `{PROJECT}.{DATASET}.fact_orders` AS o
    JOIN `{PROJECT}.{DATASET}.dim_customers` AS c
      ON o.customer_id = c.customer_id
    LEFT JOIN `{PROJECT}.{DATASET}.fact_order_lines` AS ol
      ON o.order_id = ol.order_id
    LEFT JOIN `{PROJECT}.{DATASET}.dim_products` AS p
      ON ol.product_id = p.product_id

    WHERE o.order_id = @order_id
      AND c.customer_id = (
        SELECT customer_id
        FROM `{PROJECT}.{DATASET}.fact_orders`
        WHERE order_id = @order_id
      )
      AND UPPER(REGEXP_REPLACE(c.postcode, r'\s+', '')) = @postcode

    ORDER BY ol.line_number ASC
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("order_id", "INT64", clean_order_id),
            bigquery.ScalarQueryParameter("postcode", "STRING", clean_postcode),
        ]
    )

    try:
        bq_client = get_bq_client()
        query_job = bq_client.query(query, job_config=job_config)
        rows = list(query_job.result())

        if not rows:
            return templates.TemplateResponse(
                request=request,
                name="search.html",
                context={
                    "error": f"No order found matching Order ID '{raw_order_id}' and Postcode '{raw_postcode}'. Please verify your order details.",
                    "order_id": raw_order_id,
                    "postcode": raw_postcode,
                    "now_year": datetime.now().year,
                },
            )

        first = rows[0]
        
        # Format dates
        order_date_str = format_date_dmy(first.order_datetime)
        delivery_date_str = format_date_dmy(first.delivery_datetime)
        
        # 2-digit year for invoice number
        year_2digit = first.order_datetime.strftime("%y")

        invoice_number = f"GBINV{year_2digit}{clean_order_id:012d}"

        # Section title condition: "Delivery address:" for parcel/truck, "Billing address:" otherwise
        is_delivery = first.delivery_type in ("parcel", "truck")
        address_section_title = "Delivery address:" if is_delivery else "Billing address:"

        # Line items
        items: List[Dict[str, Any]] = []
        for r in rows:
            if r.line_number is not None:
                items.append({
                    "art_no": format_article_number(r.product_id),
                    "description": r.product_name,
                    "quantity": r.line_quantity,
                    "unit_price": format_currency_val(r.line_price_100),
                    "vat_rate": f"{r.vat_rate:.2%}",
                    "total_price": f"£ {format_currency_val(r.line_total_100)}",
                })

        # The Picking and delivery fee is added as another item in the product list table.
        # Art. No. "000.000.00", omit if £0.00
        
        if first.picking_delivery_100 > 0:
            fee_label = "Delivery fee - parcel" if first.delivery_type == "parcel" else "Delivery fee - truck"
            items.append({
                "art_no": "000.000.00",
                "description": fee_label,
                "quantity": 1,
                "unit_price": format_currency_val(first.picking_delivery_100),
                "vat_rate": f"{first.order_vat_rate_100 / 100:.2%}",
                "total_price": f"£ {format_currency_val(first.picking_delivery_100)}",
            })

        # Payment details
        payment_type_name = first.payment_method.replace("_", " ").title()

        # Barcode
        barcode_svg = generate_barcode_svg(invoice_number)

        return templates.TemplateResponse(
            request=request,
            name="invoice.html",
            context={
                "order_id": first.order_id,
                "invoice_number": invoice_number,
                "order_date": order_date_str,
                "delivery_date": delivery_date_str,
                "first_name": first.first_name,
                "last_name": first.last_name,
                "address_line_1": first.address_line_1,
                "city_town": first.city_town,
                "postcode": first.postcode,
                "country": first.country,
                "address_section_title": address_section_title,
                "items": items,
                "goods_total": format_currency_val(first.total_goods_100),
                "total_amount": format_currency_val(first.total_sales_100),
                "net_amount": format_currency_val(first.total_sales_excl_vat_100),
                "vat_amount": format_currency_val(first.total_vat_100),
                "vat_rate": f"{first.order_vat_rate_100 / 100:.2%}",
                "payment_type_name": payment_type_name,
                "barcode_svg": barcode_svg,
                "now_year": datetime.now().year,
            },
        )

    except Exception as e:
        print(f"[ERROR] Failed to fetch invoice from BigQuery: {e}")
        return templates.TemplateResponse(
            request=request,
            name="search.html",
            context={
                "error": "Something went wrong while fetching your invoice — please try again.",
                "order_id": raw_order_id,
                "postcode": raw_postcode,
                "now_year": datetime.now().year,
            },
        )
