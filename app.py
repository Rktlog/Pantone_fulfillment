import os
import io
import re
import sys
import json
import time
import threading
import requests
import pandas as pd
import streamlit as st
from collections import deque
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
from database import (
    save_order,
    save_shipment,
    log_error
)

load_dotenv()

# -------------------------------------------------------------------
# CONFIG
# -------------------------------------------------------------------
CIN7_ACCOUNT_ID = os.getenv("CIN7_ACCOUNT_ID")
CIN7_API_KEY = os.getenv("CIN7_API_KEY")
CIN7_BASE_URL = "https://inventory.dearsystems.com/ExternalApi/v2"

# "Ready to ship" = packed (fully or partially), and not yet fully shipped.
# A packed order awaiting dispatch shows shipping as NOT SHIPPED or SHIPPING.
READY_PACKING_STATUSES = {
    s.strip().upper()
    for s in os.getenv("CIN7_PACKING_STATUSES", "PACKED,PARTIALLY PACKED").split(",")
    if s.strip()
}
READY_SHIPPING_STATUSES = {
    s.strip().upper()
    for s in os.getenv("CIN7_SHIPPING_STATUSES", "NOT SHIPPED,SHIPPING,PARTIALLY SHIPPED").split(",")
    if s.strip()
}

# Only show orders whose number starts with this prefix. "" disables it.
ORDER_NUMBER_PREFIX = os.getenv("CIN7_ORDER_PREFIX", "SQ").strip().upper()

# DEAR holds 17k+ sales oldest-first, so paging from the front never reaches
# recent orders. Instead we ask for sales UPDATED recently, which surfaces the
# ones actually being worked on. Window defaults to the last 30 days.
CIN7_UPDATED_SINCE = os.getenv("CIN7_UPDATED_SINCE", "").strip()
CIN7_LOOKBACK_DAYS = int(os.getenv("CIN7_LOOKBACK_DAYS", "30"))

# Fetch tuning. Detail calls run in parallel; more workers = faster but heavier
# on DEAR's rate limit. Cap total detail fetches so a huge window stays bounded.
DETAIL_FETCH_WORKERS = int(os.getenv("CIN7_FETCH_WORKERS", "8"))
MAX_DETAIL_FETCHES = int(os.getenv("CIN7_MAX_DETAILS", "150"))

# Carrier name written into DEAR when we push tracking back.
DEFAULT_CARRIER = os.getenv("CIN7_CARRIER", "Australia Post")

# Sender/payer account for the AusPost Parcel Send CSV.
AUSPOST_SENDER_ACCOUNT = os.getenv("AUSPOST_SENDER_ACCOUNT", "")
AUSPOST_PAYER_ACCOUNT = os.getenv("AUSPOST_PAYER_ACCOUNT", AUSPOST_SENDER_ACCOUNT)

EXPORTS_DIR = "./saved_exports"
os.makedirs(EXPORTS_DIR, exist_ok=True)
HISTORY_FILE = "fulfillment_history.csv"
QUEUE_FILE = "csv_queue.json"

st.set_page_config(page_title="Cin7 Core CSV Fulfillment", layout="wide")
st.title("📦 Cin7 Core (DEAR) CSV Fulfillment")


def get_cin7_headers():
    return {
        "api-auth-accountid": CIN7_ACCOUNT_ID,
        "api-auth-applicationkey": CIN7_API_KEY,
        "Content-Type": "application/json"
    }


# -------------------------------------------------------------------
# RATE LIMITING - DEAR allows 60 calls per 60 seconds per account. The
# saleList paging scan and the parallel per-order detail fetches all draw
# from that same budget, so this limiter is shared across every call site
# below (both the paging loop and the ThreadPoolExecutor workers).
# -------------------------------------------------------------------
CIN7_MAX_CALLS_PER_WINDOW = int(os.getenv("CIN7_RATE_LIMIT", "50"))  # buffer under 60
CIN7_RATE_WINDOW_SECONDS = 60

_rate_lock = threading.Lock()
_call_times = deque()


def _cin7_rate_limit_wait():
    """
    Blocks (holding the lock) until there's room in the rolling 60-second
    call budget, then records this call. Serialising the wait itself is
    intentional and simple: only one thread is ever sleeping/counting at a
    time, so the total call rate across all threads stays under the limit.
    """
    with _rate_lock:
        now = time.monotonic()
        while _call_times and now - _call_times[0] > CIN7_RATE_WINDOW_SECONDS:
            _call_times.popleft()
        if len(_call_times) >= CIN7_MAX_CALLS_PER_WINDOW:
            sleep_for = CIN7_RATE_WINDOW_SECONDS - (now - _call_times[0]) + 0.2
            if sleep_for > 0:
                time.sleep(sleep_for)
            now = time.monotonic()
            while _call_times and now - _call_times[0] > CIN7_RATE_WINDOW_SECONDS:
                _call_times.popleft()
        _call_times.append(now)


def _cin7_request(method, path_or_url, params=None, json_body=None, timeout=30, max_retries=5):
    """
    Shared entry point for every Cin7/DEAR HTTP call. Applies the rate
    limiter before each attempt, and retries with backoff on 429 (respecting
    a Retry-After header if DEAR sends one) instead of surfacing the error
    straight to the user.
    """
    url = path_or_url if path_or_url.startswith("http") else f"{CIN7_BASE_URL}/{path_or_url}"
    res = None
    for attempt in range(max_retries):
        _cin7_rate_limit_wait()
        try:
            if method == "GET":
                res = requests.get(url, headers=get_cin7_headers(), params=params, timeout=timeout)
            else:
                res = requests.post(url, headers=get_cin7_headers(), json=json_body, timeout=timeout)
        except requests.exceptions.RequestException:
            if attempt == max_retries - 1:
                raise
            time.sleep(2 * (attempt + 1))
            continue

        if res.status_code == 429:
            retry_after = res.headers.get("Retry-After")
            try:
                wait = float(retry_after) if retry_after else (5 * (attempt + 1))
            except ValueError:
                wait = 5 * (attempt + 1)
            time.sleep(wait)
            continue

        return res

    return res  # last response after exhausting retries (still 429, most likely)


def load_queue_from_disk():
    if not os.path.exists(QUEUE_FILE):
        return []
    try:
        with open(QUEUE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        log_error("load_queue", str(e))
        return []


def save_queue_to_disk():
    try:
        with open(QUEUE_FILE, "w", encoding="utf-8") as f:
            json.dump(st.session_state.get("csv_queue", []), f)
    except Exception as e:
        log_error("save_queue", str(e))


if "orders_list" not in st.session_state:
    st.session_state["orders_list"] = []
if "csv_queue" not in st.session_state:
    st.session_state["csv_queue"] = load_queue_from_disk()

DIM_PRESETS = {
    "20 x 25 x 5 (Default)": (20.0, 25.0, 5.0),
    "30 x 20 x 15": (30.0, 20.0, 15.0),
    "40 x 30 x 20": (40.0, 30.0, 20.0),
    "60 x 40 x 30": (60.0, 40.0, 30.0)
}

DIM_TIERS = [
    (1.0, (20.0, 25.0, 5.0), "20 x 25 x 5 (Default)"),
    (2.0, (30.0, 20.0, 15.0), "30 x 20 x 15"),
]
DIM_TIER_OVER = ((40.0, 30.0, 20.0), "40 x 30 x 20")

# -------------------------------------------------------------------
# PANTONE PRODUCT DIMS & WEIGHT LOOKUP
# (from pantone_dims_and_weight.xlsx — Dimensions/Weight columns)
# -------------------------------------------------------------------
# Exact product-code matches. dims are (Length, Width, Height) in cm, weight in kg.
PANTONE_EXACT = {
    "GP1601BCOY26": (25.0, 15.0, 8.0, 0.5),
    "GP5101C": (25.0, 15.0, 8.0, 0.5),
    "FHIP110C": (25.0, 15.0, 8.0, 0.5),
    "FHIP120C": (25.0, 15.0, 8.0, 0.5),
    "FHIP210C": (30.0, 30.0, 20.0, 6.0),
    "FHIP220C": (25.0, 15.0, 8.0, 0.5),
    "FHIP230C": (30.0, 30.0, 20.0, 5.0),
    "FHIC400C": (43.0, 44.0, 26.0, 10.0),
    "FHIC410C": (25.0, 15.0, 8.0, 0.5),
    "FHIC200C": (25.0, 15.0, 8.0, 0.5),
    "FHIC210C": (25.0, 15.0, 8.0, 0.5),
    "FHIC300C": (30.0, 30.0, 20.0, 1.5),
    "FHIC310C": (25.0, 15.0, 8.0, 0.5),
    "FHIC110C": (25.0, 15.0, 8.0, 0.5),
    "FHIP310B": (25.0, 15.0, 8.0, 0.5),
    "FHIP530B": (30.0, 30.0, 20.0, 6.0),
    "FHIP610A": (25.0, 20.0, 8.0, 0.5),
    "STG203": (25.0, 15.0, 8.0, 0.5),
    "GB1507C": (30.0, 23.0, 5.0, 0.5),
    "GG1507C": (25.0, 15.0, 8.0, 0.5),
    "FHIP410N": (25.0, 15.0, 8.0, 0.5),
    "M40115B": (18.0, 15.0, 8.0, 0.5),
    "M40291B": (18.0, 15.0, 8.0, 0.5),
    "M50051": (18.0, 15.0, 8.0, 0.5),
    "M50052": (18.0, 15.0, 8.0, 0.5),
    "M93000": (18.0, 15.0, 8.0, 0.5),
    "M40328B": (18.0, 15.0, 8.0, 0.5),
    "M50130": (18.0, 15.0, 8.0, 0.5),
    "M50135": (18.0, 15.0, 8.0, 0.5),
    "M50150": (18.0, 15.0, 8.0, 0.5),
    "M50315B": (18.0, 15.0, 8.0, 0.5),
    "M50215B-YRKIT": (18.0, 15.0, 8.0, 0.5),
    "M50215B": (18.0, 15.0, 8.0, 0.5),
    "M60040": (18.0, 15.0, 8.0, 0.5),
    "GP1609B": (25.0, 15.0, 8.0, 0.5),
    "GP6102B": (25.0, 15.0, 8.0, 0.5),
    "GG6103B": (25.0, 15.0, 8.0, 0.5),
    "GG6104B": (25.0, 15.0, 8.0, 0.5),
    "GPG301B": (25.0, 15.0, 8.0, 0.5),
    "GP1601B": (25.0, 15.0, 8.0, 0.5),
    "GPG304B": (35.0, 25.0, 8.0, 4.5),
    "GPC305B": (48.0, 48.0, 26.0, 15.0),
    "GP1606B": (30.0, 30.0, 20.0, 5.0),
    "GP1608B": (30.0, 30.0, 20.0, 5.0),
    "GP1605B": (25.0, 15.0, 8.0, 0.5),
    "FFN100": (30.0, 23.0, 5.0, 0.5),
    "RM200-PT01": (25.0, 20.0, 8.0, 0.5),
    "RM200+BPT01": (25.0, 20.0, 8.0, 0.5),
    "PCNCT-CARD": (15.0, 10.0, 2.0, 0.3),
    "L24D50": (25.0, 15.0, 8.0, 0.3),
    "L24D65": (25.0, 15.0, 8.0, 0.3),
    "LNDS-1PK-D65": (25.0, 15.0, 8.0, 0.3),
    "PBT100-17": (25.0, 15.0, 8.0, 0.3),
    "STG201": (25.0, 15.0, 8.0, 0.5),
    "STG202": (25.0, 15.0, 8.0, 0.5),
    "LNDS-1PK-D50": (25.0, 15.0, 8.0, 0.3),
    "VCP-F25": (48.0, 48.0, 20.0, 3.0),
    "VCP-F26": (48.0, 48.0, 20.0, 3.0),
    "VCP-F27": (48.0, 48.0, 20.0, 3.0),
    "VCP-S25": (48.0, 48.0, 20.0, 3.0),
    "VCP-S26": (48.0, 48.0, 20.0, 3.0),
    "VCP-S27": (48.0, 48.0, 20.0, 3.0),
    "VH2023-BOOK": (48.0, 48.0, 20.0, 3.0),
    "VH2023": (48.0, 48.0, 20.0, 3.0),
    "VH2024-BOOK": (48.0, 48.0, 20.0, 3.0),
    "VH2024-PSC": (48.0, 48.0, 20.0, 3.0),
    "VH2024": (48.0, 48.0, 20.0, 3.0),
    "VH2025-BOOK": (48.0, 48.0, 20.0, 3.0),
    "VH2025": (48.0, 48.0, 20.0, 3.0),
    "VH2026-BOOK": (48.0, 48.0, 20.0, 3.0),
    "VH2026": (48.0, 48.0, 20.0, 3.0),
    "GB1504C": (30.0, 23.0, 5.0, 0.5),
    "GG1504C": (25.0, 15.0, 8.0, 0.5),
    "2017-042": (48.0, 48.0, 20.0, 3.0),
    "2017-043": (48.0, 48.0, 20.0, 3.0),
    "2017-040": (48.0, 48.0, 20.0, 3.0),
    "2017-041": (48.0, 48.0, 20.0, 3.0),
    "2017-039": (48.0, 48.0, 20.0, 3.0),
    "2022-044": (48.0, 48.0, 20.0, 3.0),
    "PSC-PS1755": (48.0, 48.0, 26.0, 15.0),
    "PTTC100": (48.0, 48.0, 20.0, 3.0),
    "VIEWPOINT-18": (30.0, 26.0, 5.0, 1.5),
    "VIEWPOINT-19": (30.0, 26.0, 5.0, 1.5),
}

# Pattern-based matches (small swatch-card style items) — all share the same
# dims/weight in the source sheet (15 x 15 x 3 cm, 0.01kg), except the
# "Replacement Pages" rule which is matched on the product NAME, not the code.
_PANTONE_SMALL = (15.0, 15.0, 3.0, 0.01)


def match_pantone_product(sku, name=""):
    """
    Look up a DEAR line item's SKU/name against the Pantone dims & weight
    table. Returns (length, width, height, weight_kg) if matched, else None.
    Exact product codes are checked first, then the pattern-based rules from
    the sheet (numeric-dash codes, PQ- codes, TPG/TSX/TPM/N suffixes, and
    "Replacement Page" products).
    """
    code = (sku or "").strip().upper()
    title = (name or "").strip().upper()

    if code and code in PANTONE_EXACT:
        return PANTONE_EXACT[code]

    if code:
        if code.startswith("PQ-"):
            return _PANTONE_SMALL
        if re.match(r"^\d+-.*TPG$", code):
            return _PANTONE_SMALL
        if re.match(r"^\d+-.*TSX$", code):
            return _PANTONE_SMALL
        if re.match(r"^\d+-.*TPM$", code):
            return _PANTONE_SMALL
        if re.match(r"^\d+-.*N$", code):
            return _PANTONE_SMALL
        if re.match(r"^\d+-", code):
            return _PANTONE_SMALL

    if "REPLACEMENT" in title and "PAGE" in title:
        return _PANTONE_SMALL

    return None


# AusPost Parcel Send import template header - exact 32 columns, verbatim.
AUSPOST_CSV_COLUMNS = [
    "Row type", "Sender account", "Payer account", "Recipient contact name",
    "Recipient business name", "Recipient address line 1", "Recipient address line 2",
    "Recipient address line 3", "Recipient suburb", "Recipient state",
    "Recipient postcode", "Send tracking email to recipient", "Recipient email address",
    "Recipient phone number", "Delivery/special instruction 1", "Special instruction 2",
    "Special instruction 3", "Sender reference 1 ", "Sender reference 2", "Product id",
    "Authority to leave", "Safe drop ", "Quantity", "Packaging type", "Weight",
    "Length", "Width", "Height", "Parcel contents", "Transit cover value",
    "Deliver wine to addressee only", "Schedule 8 or medicinal cannabis",
]

SERVICE_OPTIONS = {
    "Parcel Post (3D55)": "3D55",
    "Express Post (3J55)": "3J55",
}
DEFAULT_SERVICE_FALLBACK = SERVICE_OPTIONS["Parcel Post (3D55)"]

# International Parcel Send template - exact 55 columns, verbatim (some headers
# have trailing spaces).
AUSPOST_INTL_COLUMNS = [
    "Row type", "Sender account", "Payer account", "Sender business name",
    "Sender email address", "Sender phone number", "Recipient contact name",
    "Recipient business name", "Recipient country / region",
    "Recipient address line 1", "Recipient address line 2", "Recipient address line 3",
    "Recipient suburb", "Recipient state", "Recipient postcode ",
    "Send tracking email to recipient", "Recipient email address",
    "Recipient phone number", "Delivery/special instruction 1", "Special instruction 2",
    "Special instruction 3", "Sender reference 1 ", "Sender reference 2", "Product id",
    "Authority to leave", "Safe drop ", "Quantity", "Packaging type", "Weight",
    "Length", "Width", "Height", "Parcel contents", "Transit cover value",
    "Senders customs reference", "Comments", "Landed costs payer",
    "Importer's reference number", "Licence number", "Certificate number",
    "Invoice number", "Digital declaration", "Commercial value", "Reason for export",
    "Other reason for export", "Export declaration number", "Non-delivery preference",
    "Item - Quantity", "Item - Unit weight", "Item - Individual unit value (AUD)",
    "Item - Description", "Item - Origin", "Item - HS tariff code",
    "Deliver wine to addressee only", "Schedule 8 or medicinal cannabis",
]

# International sender + service settings (fixed per your business).
INTL_SENDER_BUSINESS = os.getenv("AUSPOST_INTL_SENDER_NAME", "Rocket Logistics")
INTL_SENDER_EMAIL = os.getenv("AUSPOST_INTL_SENDER_EMAIL", "logistics@rocketlog.com.au")
INTL_PRODUCT_ID = os.getenv("AUSPOST_INTL_PRODUCT_ID", "PTI7")
INTL_REASON_FOR_EXPORT = os.getenv("AUSPOST_INTL_REASON", "Commercial Sale of Goods (B2B)")
# Default country of origin for the customs item line (Pantone guides are
# manufactured/printed in the US).
INTL_ITEM_ORIGIN = os.getenv("AUSPOST_INTL_ORIGIN", "US")
# Fixed customs description + HS tariff code for Pantone guide books.
INTL_ITEM_DESCRIPTION = os.getenv("AUSPOST_INTL_ITEM_DESC", "Pantone Color Guide Book")
INTL_ITEM_HS_CODE = os.getenv("AUSPOST_INTL_HS_CODE", "9609100919")


def dims_for_weight(weight):
    try:
        w = float(weight)
    except (TypeError, ValueError):
        w = 0.0
    for limit, dims, label in DIM_TIERS:
        if w < limit:
            return dims[0], dims[1], dims[2], label
    if w <= 2.0:
        dims, label = DIM_TIERS[-1][1], DIM_TIERS[-1][2]
        return dims[0], dims[1], dims[2], label
    dims, label = DIM_TIER_OVER
    return dims[0], dims[1], dims[2], label


def compute_order_weight_and_dims(order):
    """
    Total shippable weight for the order's remaining line items, plus a
    matched Pantone box size if any line item is a recognised product. When
    several matched line items are on the one order, we use the dims of the
    single largest-volume matched item (the box has to fit the biggest piece).
    Returns (weight_kg, dims_tuple_or_None).
    """
    total_weight = 0.0
    best_match = None  # (volume, (l, w, h))
    for it in order.get("Line Items", []):
        qty = it.get("remaining_qty", 0)
        if qty <= 0:
            continue
        total_weight += qty * it.get("unit_weight_kg", 0.0)
        dims = it.get("pantone_dims")
        if dims:
            vol = dims[0] * dims[1] * dims[2]
            if best_match is None or vol > best_match[0]:
                best_match = (vol, dims)
    total_weight = float(round(max(total_weight, 0.1), 2))
    return total_weight, (best_match[1] if best_match else None)


def detect_requested_service(full_sale, order_block):
    """
    If DEAR has a carrier/shipping method recorded on the sale (customer
    requested Express, etc.), pre-select the matching AusPost service on
    Tab 2. Otherwise fall back to Parcel Post.
    """
    candidates = [
        full_sale.get("Carrier"),
        full_sale.get("ShippingMethod"),
        full_sale.get("CarrierService"),
        order_block.get("Carrier"),
        order_block.get("ShippingMethod"),
        (full_sale.get("ShippingAddress") or {}).get("Carrier"),
    ]
    text = " ".join(str(c) for c in candidates if c).strip().upper()
    if not text:
        return None
    # DEAR records this as a plain "Standard" / "Express" service level.
    if "EXPRESS" in text or "3J55" in text:
        return SERVICE_OPTIONS["Express Post (3J55)"]
    if "STANDARD" in text or "PARCEL" in text or "3D55" in text:
        return SERVICE_OPTIONS["Parcel Post (3D55)"]
    return None


# -------------------------------------------------------------------
# CIN7 CORE / DEAR API
# -------------------------------------------------------------------
def _cin7_get(path, params=None):
    try:
        res = _cin7_request("GET", path, params=params)
    except requests.exceptions.RequestException as e:
        st.error(f"Could not reach Cin7: {e}")
        return None
    if res is None:
        st.error(f"Cin7 API Error on {path}: no response after retries.")
        return None
    if res.status_code != 200:
        st.error(f"Cin7 API Error ({res.status_code}) on {path}: {res.text}")
        return None
    return res.json()


def _cin7_get_sale_raw(sale_id):
    """
    Thread-safe sale fetch for parallel use - does NOT call any st.* functions
    (Streamlit isn't safe to call from worker threads). Returns the JSON or None.
    Shares the same rate limiter as the paging loop above, so parallel detail
    fetches and saleList paging never combine to exceed DEAR's call budget.
    """
    try:
        res = _cin7_request("GET", "sale", params={"ID": sale_id})
        if res is not None and res.status_code == 200:
            return res.json()
    except requests.exceptions.RequestException:
        pass
    return None


def _has_authorised_pack(full_sale):
    """
    True if the sale has at least one fulfilment where BOTH Pick and Pack are
    AUTHORISED - i.e. it's genuinely picked and packed and ready to ship. This
    is the authoritative "ready" signal, more reliable than the combined status
    string on the saleList row.
    """
    for f in (full_sale.get("Fulfilments", []) or []):
        pick = str((f.get("Pick") or {}).get("Status", "")).strip().upper()
        pack = str((f.get("Pack") or {}).get("Status", "")).strip().upper()
        ship = str((f.get("Ship") or {}).get("Status", "")).strip().upper()
        if pick == "AUTHORISED" and pack == "AUTHORISED" and ship != "AUTHORISED":
            return True
    return False


def fetch_cin7_ready_orders():
    """
    Fetch sales that are packed but not yet shipped. DEAR puts the combined
    fulfilment status on each saleList row, so we filter there (fast) and only
    pull full detail for the ones we're keeping (for line items + address).
    """
    if not CIN7_ACCOUNT_ID or not CIN7_API_KEY:
        st.error("Missing CIN7_ACCOUNT_ID / CIN7_API_KEY in .env.")
        return []

    # Ask DEAR for recently-updated sales rather than paging the full 17k list
    # oldest-first. UpdatedSince surfaces orders actively being worked on.
    since = CIN7_UPDATED_SINCE
    if not since:
        cutoff = datetime.now() - timedelta(days=CIN7_LOOKBACK_DAYS)
        since = cutoff.strftime("%Y-%m-%dT00:00:00Z")

    detailed = []
    matched_summaries = []
    scanned = 0
    consecutive_failures = 0

    # NOTE: DEAR's saleList is NOT sorted by most-recently-updated first (confirmed
    # by testing — an order updated minutes earlier turned up on page 59, not
    # page 1). So we can't assume recent activity surfaces early; we have to
    # scan deep enough to cover your real volume within the lookback window.
    # This loop breaks out as soon as a page comes back short, so scanning
    # further costs nothing extra when there's nothing left to find.
    for page in range(1, 201):  # up to 20,000 sales in the UpdatedSince window
        data = _cin7_get("saleList", params={
            "Page": page, "Limit": 100, "UpdatedSince": since
        })
        if data is None:
            # A single page failing (timeout, transient 429 exhausting
            # retries, etc.) must NOT silently truncate the whole scan -
            # that would drop every later page (where a recently-updated
            # order can still be sitting, since DEAR doesn't sort saleList
            # by recency). Skip this page and keep going; only give up if
            # several pages in a row are failing, which means the API is
            # genuinely unreachable rather than a one-off blip.
            consecutive_failures += 1
            if consecutive_failures >= 5:
                st.warning(f"Stopping the scan after {consecutive_failures} consecutive "
                           f"page failures around page {page} - DEAR may be unreachable "
                           f"right now. Results below may be incomplete.")
                break
            continue
        consecutive_failures = 0
        batch = data.get("SaleList", [])
        if not batch:
            break
        scanned += len(batch)

        for s in batch:
            # Cheap pre-filter on the saleList row: skip clearly-irrelevant
            # sales (not packed at all, already shipped, wrong prefix) so we
            # don't pull full detail for thousands of orders. The authoritative
            # check (Pick + Pack both AUTHORISED) happens on the detail below.
            packing = str(s.get("CombinedPackingStatus", "")).strip().upper()
            shipping = str(s.get("CombinedShippingStatus", "")).strip().upper()
            order_no = str(s.get("OrderNumber", "")).strip()

            if packing in ("NOT AVAILABLE", "NOT PACKED"):
                continue
            if shipping == "SHIPPED":
                continue
            if ORDER_NUMBER_PREFIX and not order_no.upper().startswith(ORDER_NUMBER_PREFIX):
                continue

            matched_summaries.append(s)

        if len(batch) < 100:
            break

    # Most recently CHANGED first. DEAR's 'Updated' timestamp moves whenever the
    # sale is modified - including when it's picked or packed - so it reflects
    # real recent activity, unlike OrderDate which is often bulk-stamped.
    def _sort_key(s):
        return (str(s.get("Updated", "")), str(s.get("OrderNumber", "")))
    matched_summaries.sort(key=_sort_key, reverse=True)

    # Safety cap: don't pull detail for an unbounded number of candidates.
    if len(matched_summaries) > MAX_DETAIL_FETCHES:
        matched_summaries = matched_summaries[:MAX_DETAIL_FETCHES]

    # Pull full detail in PARALLEL - this is the slow part (one API call each),
    # so we run them concurrently instead of one-at-a-time. Keep only sales with
    # a fulfilment where BOTH Pick and Pack are AUTHORISED (picked & packed,
    # ready to ship).
    def _fetch_detail(summary):
        sid = summary.get("SaleID")
        if not sid:
            return None
        full = _cin7_get_sale_raw(sid)
        if full is None or not _has_authorised_pack(full):
            return None
        return parse_cin7_sale(full, summary)

    results = []
    with ThreadPoolExecutor(max_workers=DETAIL_FETCH_WORKERS) as pool:
        futures = {pool.submit(_fetch_detail, s): s for s in matched_summaries}
        for fut in as_completed(futures):
            try:
                parsed = fut.result()
            except Exception as e:
                log_error("fetch_detail", str(e))
                parsed = None
            if parsed:
                results.append(parsed)

    # Re-sort results most-recently-changed first (thread completion order is
    # arbitrary). Uses the sale's Updated timestamp.
    results.sort(key=lambda p: (str(p.get("_updated", "")), str(p.get("Order Name", ""))),
                 reverse=True)

    for parsed in results:
        detailed.append(parsed)
        try:
            save_order({
                "Order ID": parsed["Sale ID"],
                "Order Name": parsed["Order Name"],
                "Customer": parsed["Customer"],
                "Email": parsed["Email"],
                "Phone": parsed["Phone"],
                "Raw Address": parsed["Raw Address"],
                "Fulfillment Order ID": parsed["Sale ID"],
                "Line Items": [
                    {
                        "fo_line_item_id": li["line_id"],
                        "title": li["title"],
                        "remaining_qty": li["remaining_qty"],
                        "unit_weight_kg": li["unit_weight_kg"],
                        "sku": li["sku"],
                    } for li in parsed["Line Items"]
                ],
            })
        except Exception as e:
            log_error("save_order", str(e))

    st.caption(f"Scanned {scanned} recently-updated sales · "
               f"{len(matched_summaries)} candidates · "
               f"{len(detailed)} picked & packed, ready to ship"
               + (f" (prefix {ORDER_NUMBER_PREFIX})" if ORDER_NUMBER_PREFIX else ""))

    return detailed


def parse_cin7_sale(full_sale, summary):
    """
    Normalise a DEAR sale into the shape the rest of the app uses.
    Weight comes from the Pantone lookup table when the SKU is recognised,
    else the order line if present, else a 0.2kg default.
    """
    sale_id = full_sale.get("ID") or summary.get("SaleID")
    order_number = summary.get("OrderNumber") \
        or (full_sale.get("Order", {}) or {}).get("SaleOrderNumber") \
        or str(sale_id)

    customer = full_sale.get("Customer", "") or summary.get("Customer", "")
    email = full_sale.get("Email", "") or ""
    phone = full_sale.get("Phone", "") or ""

    ship_addr = full_sale.get("ShippingAddress", {}) or {}
    raw_address = {
        "name": ship_addr.get("DisplayAddressLine1") and customer or customer,
        "line1": ship_addr.get("Line1", "") or "",
        "line2": ship_addr.get("Line2", "") or "",
        "city": ship_addr.get("City", "") or ship_addr.get("Suburb", "") or "",
        "state": ship_addr.get("State", "") or "",
        "postcode": ship_addr.get("Postcode", "") or "",
        "country": ship_addr.get("Country", "") or "",
    }

    order_block = full_sale.get("Order", {}) or {}
    lines = order_block.get("Lines", []) or []

    # Flag whether Pack is already AUTHORISED on the ready fulfilment, so the
    # UI can warn when it isn't (packing will be auto-authorised on upload
    # instead of by someone physically confirming the box - see
    # _has_authorised_pack / authorise_pack_cin7_sale for the tradeoff).
    pack_already_authorised = False
    for f in (full_sale.get("Fulfilments", []) or []):
        pick_status = str((f.get("Pick") or {}).get("Status", "")).strip().upper()
        pack_status = str((f.get("Pack") or {}).get("Status", "")).strip().upper()
        ship_status = str((f.get("Ship") or {}).get("Status", "")).strip().upper()
        if pick_status == "AUTHORISED" and ship_status != "AUTHORISED":
            pack_already_authorised = (pack_status == "AUTHORISED")
            break

    # For partial packs we want the quantity actually PACKED, not ordered.
    # Sum packed quantity per SKU across the fulfilment Pack tasks; if a SKU
    # appears there, use the packed qty, else fall back to the ordered qty.
    packed_by_sku = {}
    for f in (full_sale.get("Fulfilments", []) or []):
        pack = f.get("Pack", {}) or {}
        for pl in (pack.get("Lines", []) or []):
            sku = pl.get("SKU", "") or pl.get("ProductID", "")
            if sku:
                packed_by_sku[sku] = packed_by_sku.get(sku, 0) + (pl.get("Quantity", 0) or 0)

    parsed_items = []
    for idx, li in enumerate(lines):
        sku = li.get("SKU", "") or li.get("ProductID", "")
        ordered_qty = li.get("Quantity", 0) or 0

        # Prefer the packed quantity when this SKU was packed; that's what's
        # physically in the box and what we'll actually ship.
        qty = packed_by_sku.get(sku, ordered_qty)
        if qty <= 0:
            continue

        title = li.get("Name", "") or li.get("SKU", "Product")

        # Look this product up in the Pantone dims/weight table. When matched,
        # its dims and weight take priority over DEAR's own (often blank)
        # weight field, since the table is the reliable source for these SKUs.
        pantone_match = match_pantone_product(sku, title)
        pantone_dims = None
        if pantone_match:
            p_l, p_w, p_h, p_wt = pantone_match
            weight = p_wt
            pantone_dims = (p_l, p_w, p_h)
        else:
            # DEAR's line weight field is ProductWeight (kg). It's often 0.0
            # when not maintained, so treat 0/None alike and fall back.
            weight = li.get("ProductWeight")
            if weight in (None, 0, 0.0):
                weight = li.get("Weight") or 0.2

        parsed_items.append({
            "sku": sku,
            # Unique per line even when two lines share a SKU (DEAR allows
            # duplicate/placeholder SKUs). Used as the widget key and match key.
            "line_id": f"{sku}#{idx}",
            "title": title,
            "remaining_qty": int(qty),
            "unit_weight_kg": round(float(weight), 3),
            "pantone_dims": pantone_dims,
        })

    if not parsed_items:
        return None

    addr_str = f"{raw_address['line1']}, {raw_address['city']} {raw_address['postcode']}".strip(", ")

    return {
        "Sale ID": sale_id,
        "Order Name": order_number,
        "Customer": customer or "N/A",
        "Email": email,
        "Phone": phone,
        "Address": addr_str,
        "Raw Address": raw_address,
        "Status": summary.get("Status", ""),
        "_order_date": summary.get("OrderDate", ""),
        "_updated": summary.get("Updated", ""),
        "_requested_service": detect_requested_service(full_sale, order_block),
        "_pack_already_authorised": pack_already_authorised,
        "Line Items": parsed_items,
    }


def authorise_pack_cin7_sale(sale_id):
    """
    ⚠️ EXPERIMENTAL - auto-authorises the Pack step so a Picked-only order
    can proceed straight to shipping without a person manually packing it
    in DEAR first. Built the same way the Ship fix worked - mirroring the
    real Pick lines' SKU/ProductID/Quantity into the Pack payload rather
    than guessing blank ones - but this is UNCONFIRMED against DEAR's
    actual API spec.

    This is materially riskier than the Ship fix: authorising Pack tells
    DEAR "this was physically packed correctly," which can move stock. A
    wrong payload here risks real inventory discrepancies, not just a
    blocked API call. Test on one low-stakes order first and check DEAR's
    stock movement for it afterward before relying on this for a batch.
    """
    sale = _cin7_get("sale", params={"ID": sale_id})
    if sale is None:
        return False, "Could not fetch sale details."

    fulfilments = sale.get("Fulfilments", []) or []
    target = None
    for f in fulfilments:
        pick_status = str((f.get("Pick") or {}).get("Status", "")).strip().upper()
        pack_status = str((f.get("Pack") or {}).get("Status", "")).strip().upper()
        ship_status = str((f.get("Ship") or {}).get("Status", "")).strip().upper()
        if pick_status == "AUTHORISED" and pack_status != "AUTHORISED" and ship_status != "AUTHORISED":
            target = f
            break

    if target is None:
        return False, "No fulfilment found that's Picked but not yet Packed."

    task_id = target.get("TaskID")
    if not task_id:
        return False, "Fulfilment has no TaskID."

    pick_lines = (target.get("Pick") or {}).get("Lines", []) or []
    if not pick_lines:
        return False, "No Pick lines to build the Pack from."

    pack = target.get("Pack", {}) or {}
    payload = dict(pack)
    payload["TaskID"] = task_id
    payload["Status"] = "AUTHORISED"
    payload["Lines"] = [
        {
            "ProductID": pl.get("ProductID"),
            "SKU": pl.get("SKU"),
            "Quantity": pl.get("Quantity"),
            "Box": "1",
            "Location": pl.get("Location"),
            "LocationID": pl.get("LocationID"),
        }
        for pl in pick_lines
    ]

    try:
        res = _cin7_request("POST", "sale/fulfilment/pack", json_body=payload)
    except requests.exceptions.RequestException as e:
        return False, f"Could not reach Cin7 to authorise packing: {e}"
    if res is None:
        return False, "DEAR pack error: no response after retries."
    if res.status_code in (200, 201):
        return True, "Pack task authorised in DEAR."
    return False, f"DEAR pack error ({res.status_code}): {res.text}"


def ship_cin7_sale(sale_id, carrier, tracking_number, tracking_url=""):
    """
    Complete the Ship task on the sale's EXISTING fulfilment.

    A sale can carry more than one fulfilment (e.g. an empty draft plus the real
    packed one). We must target the fulfilment whose Pick is AUTHORISED and Ship
    is still open - shipping the first fulfilment blindly creates the wrong
    record. We reference it by its existing TaskID and PUT the ship update so
    DEAR completes that task rather than creating a new fulfilment.

    If Pack isn't AUTHORISED yet (order was only Picked), this auto-authorises
    Pack first via authorise_pack_cin7_sale - see the warning on that function
    about the real risk of doing that automatically.
    """
    sale = _cin7_get("sale", params={"ID": sale_id})
    if sale is None:
        return False, "Could not fetch sale details."

    fulfilments = sale.get("Fulfilments", []) or []
    if not fulfilments:
        return False, "No fulfilment on this sale (is it picked in DEAR?)."

    # Target: Pick AUTHORISED, Ship not yet done. Pack may or may not be
    # AUTHORISED yet - handled below.
    target = None
    for f in fulfilments:
        pick_status = str((f.get("Pick") or {}).get("Status", "")).strip().upper()
        ship_status = str((f.get("Ship") or {}).get("Status", "")).strip().upper()
        if pick_status == "AUTHORISED" and ship_status != "AUTHORISED":
            target = f
            break

    if target is None:
        # Fall back to any fulfilment that has packed lines.
        for f in fulfilments:
            if (f.get("Pack") or {}).get("Lines"):
                target = f
                break

    if target is None:
        return False, "No fulfilment found to ship on this sale."

    pack_status = str((target.get("Pack") or {}).get("Status", "")).strip().upper()
    if pack_status != "AUTHORISED":
        ok, msg = authorise_pack_cin7_sale(sale_id)
        if not ok:
            return False, f"Could not auto-authorise packing before shipping: {msg}"
        # Re-fetch - the fulfilment's Pack/Ship blocks just changed.
        sale = _cin7_get("sale", params={"ID": sale_id})
        if sale is None:
            return False, "Packing was authorised, but could not re-fetch the sale to ship it."
        fulfilments = sale.get("Fulfilments", []) or []
        target = None
        for f in fulfilments:
            pick_status = str((f.get("Pick") or {}).get("Status", "")).strip().upper()
            ship_status = str((f.get("Ship") or {}).get("Status", "")).strip().upper()
            if pick_status == "AUTHORISED" and ship_status != "AUTHORISED":
                target = f
                break
        if target is None:
            return False, "Packing was authorised, but no fulfilment was found ready to ship afterward."

    task_id = target.get("TaskID")
    if not task_id:
        return False, "Fulfilment has no TaskID to target."

    ship = target.get("Ship", {}) or {}

    # DEAR's ship structure is one shipment line PER BOX (not per product), with
    # the carrier and tracking living inside that shipment line. This mirrors the
    # shape DEAR stores on a completed shipment. We ship as a single box.
    ship_date = datetime.now().strftime("%Y-%m-%dT00:00:00Z")

    # NOTE: You're clearing the stale placeholder shipment line manually in
    # DEAR before running this, so we don't do that automatically here -
    # this just builds and sends the real shipment directly.
    #
    # Ship lines are ONE PER BOX, not one per product. Confirmed by testing:
    # when two different SKUs were packed into the same box (both Pack lines
    # had Box="1"), building a Ship line per product created two lines both
    # claiming Box #1, and DEAR rejected it with "Box #1 is already added to
    # another shipment line" - a box number has to be unique across Ship
    # lines. Group by box instead, so a multi-SKU box still gets exactly one
    # shipment line.
    pack_lines = (target.get("Pack") or {}).get("Lines", []) or []
    if pack_lines:
        box_numbers = []
        seen = set()
        for pl in pack_lines:
            box_num = str(pl.get("Box") or "1")
            if box_num not in seen:
                seen.add(box_num)
                box_numbers.append(box_num)

        ship_lines = [
            {
                "Box": box_num,
                "Boxes": box_num,
                "ShipmentDate": ship_date,
                "Carrier": carrier,
                "TrackingNumber": tracking_number,
                "TrackingURL": tracking_url or "",
                "IsShipped": True,
            }
            for box_num in box_numbers
        ]
    else:
        # No Pack lines to go by (shouldn't normally happen) - fall back to
        # a single box-level line as before.
        ship_lines = [{
            "ShipmentDate": ship_date,
            "Carrier": carrier,
            "Box": "1",
            "Boxes": "1",
            "TrackingNumber": tracking_number,
            "TrackingURL": tracking_url or "",
            "IsShipped": True,
        }]

    # Send back the full Ship object (RequireBy, ShippingAddress,
    # ShippingNotes preserved) rather than a stripped-down payload.
    payload = dict(ship)
    payload["TaskID"] = task_id
    payload["Status"] = "AUTHORISED"
    payload["Lines"] = ship_lines

    try:
        res = _cin7_request("POST", "sale/fulfilment/ship", json_body=payload)
    except requests.exceptions.RequestException as e:
        return False, f"Could not reach Cin7 to ship: {e}"

    if res is None:
        return False, "DEAR ship error: no response after retries (rate limited?)."
    if res.status_code in (200, 201):
        return True, "Ship task completed in DEAR with tracking."
    return False, f"DEAR ship error ({res.status_code}): {res.text}"


# -------------------------------------------------------------------
# CSV EXPORT / IMPORT
# -------------------------------------------------------------------
def build_auspost_csv_row(entry):
    order = entry["order_data"]
    addr = order.get("Raw Address", {}) or {}

    row = {col: "" for col in AUSPOST_CSV_COLUMNS}
    row["Row type"] = "S"
    row["Sender account"] = AUSPOST_SENDER_ACCOUNT
    row["Payer account"] = AUSPOST_PAYER_ACCOUNT
    row["Recipient contact name"] = order.get("Customer", "")
    row["Recipient address line 1"] = addr.get("line1", "")
    row["Recipient address line 2"] = addr.get("line2", "")
    row["Recipient suburb"] = addr.get("city", "")
    row["Recipient state"] = addr.get("state", "")
    row["Recipient postcode"] = addr.get("postcode", "")
    row["Send tracking email to recipient"] = "Yes" if order.get("Email") else "No"
    row["Recipient email address"] = order.get("Email", "")
    row["Recipient phone number"] = order.get("Phone", "")
    # Order number is the match key on reupload (header has a trailing space).
    row["Sender reference 1 "] = order.get("Order Name", "")
    row["Product id"] = entry["service"]
    row["Quantity"] = 1
    row["Weight"] = entry["weight"]
    row["Length"] = entry["length"]
    row["Width"] = entry["width"]
    row["Height"] = entry["height"]
    # Generic contents - item count only, no product names/SKUs.
    unit_count = sum(i.get("dispatch_qty", 0) for i in entry["selected_items"])
    row["Parcel contents"] = f"{unit_count} item(s)"
    # Required declaration column - always No.
    row["Schedule 8 or medicinal cannabis"] = "No"
    return row


def build_export_dataframe(entries):
    rows = [build_auspost_csv_row(e) for e in entries]
    return pd.DataFrame(rows, columns=AUSPOST_CSV_COLUMNS)


COUNTRY_CODE_MAP = {
    "NEW ZEALAND": "NZ",
    "NZ": "NZ",
    "AUSTRALIA": "AU",
    "UNITED STATES": "US",
    "USA": "US",
    "UNITED STATES OF AMERICA": "US",
}


def normalise_country_code(raw_country):
    """
    AusPost's international template wants a country CODE (e.g. 'NZ'), not a
    full name. Map common names to codes; fall back to NZ if nothing's set,
    since that's the default destination for these shipments right now.
    """
    key = (raw_country or "").strip().upper()
    if key in COUNTRY_CODE_MAP:
        return COUNTRY_CODE_MAP[key]
    if not key:
        return "NZ"
    # Already looks like a 2-letter code - leave it as-is.
    if len(key) == 2:
        return key
    return raw_country


def build_intl_csv_row(entry):
    """Build one international Parcel Send row (55-col template)."""
    order = entry["order_data"]
    addr = order.get("Raw Address", {}) or {}

    unit_count = sum(i.get("dispatch_qty", 0) for i in entry["selected_items"])
    # Total declared value from the order lines if present, else blank.
    total_value = entry.get("customs_value", "")

    # Per-unit customs value = total value / quantity, not the total itself.
    unit_value = ""
    if total_value not in ("", None):
        try:
            tv = float(total_value)
            unit_value = round(tv / unit_count, 2) if unit_count else tv
        except (TypeError, ValueError):
            unit_value = total_value

    row = {col: "" for col in AUSPOST_INTL_COLUMNS}
    row["Row type"] = "s"
    row["Sender account"] = ""            # blank per requirement
    row["Payer account"] = ""             # blank per requirement
    row["Sender business name"] = INTL_SENDER_BUSINESS
    row["Sender email address"] = INTL_SENDER_EMAIL
    row["Sender phone number"] = ""       # not needed

    row["Recipient contact name"] = order.get("Customer", "")
    row["Recipient country / region"] = normalise_country_code(addr.get("country", ""))
    row["Recipient address line 1"] = addr.get("line1", "")
    row["Recipient address line 2"] = addr.get("line2", "")
    row["Recipient suburb"] = addr.get("city", "")
    row["Recipient state"] = addr.get("state", "")
    row["Recipient postcode "] = addr.get("postcode", "")  # trailing space header
    row["Send tracking email to recipient"] = "Yes" if order.get("Email") else "No"
    row["Recipient email address"] = order.get("Email", "")
    row["Recipient phone number"] = order.get("Phone", "")

    row["Sender reference 1 "] = order.get("Order Name", "")
    row["Product id"] = INTL_PRODUCT_ID
    row["Quantity"] = 1
    row["Weight"] = entry["weight"]
    row["Length"] = entry["length"]
    row["Width"] = entry["width"]
    row["Height"] = entry["height"]
    row["Parcel contents"] = ""

    # Customs / export declaration.
    row["Digital declaration"] = "No"
    row["Landed costs payer"] = "RECEIVER_PAYS"
    row["Reason for export"] = INTL_REASON_FOR_EXPORT
    row["Commercial value"] = "yes"

    # One generic customs item line covering the whole parcel.
    row["Item - Quantity"] = unit_count
    row["Item - Unit weight"] = entry["weight"]
    row["Item - Individual unit value (AUD)"] = unit_value
    row["Item - Description"] = INTL_ITEM_DESCRIPTION
    row["Item - Origin"] = INTL_ITEM_ORIGIN
    row["Item - HS tariff code"] = INTL_ITEM_HS_CODE

    row["Deliver wine to addressee only"] = "No"
    row["Schedule 8 or medicinal cannabis"] = "No"
    return row


def build_intl_dataframe(entries):
    rows = [build_intl_csv_row(e) for e in entries]
    return pd.DataFrame(rows, columns=AUSPOST_INTL_COLUMNS)


def find_column(df, *candidates):
    norm = {c.strip().lower(): c for c in df.columns}
    for cand in candidates:
        key = cand.strip().lower()
        if key in norm:
            return norm[key]
    for cand in candidates:
        key = cand.strip().lower()
        for norm_key, original in norm.items():
            if key in norm_key:
                return original
    return None


def parse_results_csv(file_bytes):
    """
    Read the AusPost results CSV (the 'past shipments' export) and return
    { order_number: {"tracking": connote, "url": tracking_url} }.
    """
    df = pd.read_csv(io.BytesIO(file_bytes), dtype=str, encoding="utf-8-sig").fillna("")

    ref_col = find_column(df, "Sender reference 1", "Sender reference 1 ", "Sender reference")
    track_col = find_column(df, "Connote", "Tracking number", "Article id", "Consignment number")
    url_col = find_column(df, "Tracking url", "Tracking URL", "Track link")

    if ref_col is None:
        return None, "Could not find a 'Sender reference 1' column in the uploaded CSV."
    if track_col is None:
        return None, "Could not find a tracking number column (looked for 'Connote' / 'Tracking number')."

    mapping = {}
    for _, r in df.iterrows():
        ref = str(r[ref_col]).strip()
        if not ref:
            continue
        tracking = str(r[track_col]).strip()
        if not tracking:
            continue
        url = str(r[url_col]).strip() if url_col else ""
        if not url:
            url = f"https://auspost.com.au/track/{tracking}"
        mapping[ref] = {"tracking": tracking, "url": url}

    return mapping, None


def log_to_csv(order_name, customer_name, tracking_no, tracking_url, items_dispatched):
    log_entry = {
        "Timestamp": [datetime.now().strftime("%Y-%m-%d %H:%M:%S")],
        "Order Name": [order_name],
        "Customer Name": [customer_name],
        "Dispatched Items": [items_dispatched],
        "Tracking Number": [tracking_no],
        "Tracking URL": [tracking_url],
    }
    df_new = pd.DataFrame(log_entry)
    if not os.path.isfile(HISTORY_FILE):
        df_new.to_csv(HISTORY_FILE, index=False)
    else:
        df_new.to_csv(HISTORY_FILE, mode='a', header=False, index=False)


def queue_order(order, items_to_dispatch, service_code, weight, length, width, height):
    st.session_state['csv_queue'].append({
        "order_data": order,
        "selected_items": items_to_dispatch,
        "service": service_code,
        "weight": weight,
        "length": length,
        "width": width,
        "height": height,
    })
    dispatch_map = {i['fo_line_item_id']: i['dispatch_qty'] for i in items_to_dispatch}
    for item in order['Line Items']:
        if item['fo_line_item_id'] in dispatch_map:
            item['remaining_qty'] -= dispatch_map[item['fo_line_item_id']]
    total_remaining = sum(item['remaining_qty'] for item in order['Line Items'])
    save_queue_to_disk()
    return total_remaining <= 0, "Added to CSV batch."


def clear_order_widget_state(order_key):
    stale_prefixes = (f"item_{order_key}_", f"wt_{order_key}", f"bulksel_{order_key}")
    for k in list(st.session_state.keys()):
        if k.startswith(stale_prefixes):
            del st.session_state[k]


# -------------------------------------------------------------------
# UI - line items keyed on fo_line_item_id (which holds the SKU for DEAR)
# -------------------------------------------------------------------
for order in st.session_state['orders_list']:
    for li in order['Line Items']:
        li.setdefault('fo_line_item_id', li.get('line_id') or li.get('sku') or li['title'])

st.sidebar.header("⚙️ Settings")

if not AUSPOST_SENDER_ACCOUNT:
    st.sidebar.warning("AUSPOST_SENDER_ACCOUNT not set in .env — CSV exports with a blank Sender account.")

# Default service applied when an order is first added to the batch and DEAR
# doesn't specify a requested carrier/service; adjustable per order on Tab 2.
default_service_choice = st.sidebar.selectbox(
    "Default service", list(SERVICE_OPTIONS.keys()), index=0, key="default_service")
DEFAULT_SERVICE_CODE = SERVICE_OPTIONS[default_service_choice]

if st.sidebar.button("🔄 Fetch Packed Orders from DEAR"):
    with st.spinner("Fetching packed sales from Cin7 Core..."):
        orders = fetch_cin7_ready_orders()
        for o in orders:
            for li in o['Line Items']:
                li.setdefault('fo_line_item_id', li.get('line_id') or li.get('sku') or li['title'])
        st.session_state['orders_list'] = orders
        st.success(f"Fetched {len(orders)} packed orders.")

tab_select, tab_export, tab_import = st.tabs([
    f"1️⃣ Select Orders ({len(st.session_state['orders_list'])})",
    f"2️⃣ Export CSV ({len(st.session_state['csv_queue'])})",
    "3️⃣ Import Tracking → DEAR",
])


# ===================================================================
# TAB 1 - SELECT ORDERS ONLY
# ===================================================================
with tab_select:
    st.subheader("Step 1: Select the orders to ship")

    if st.session_state['orders_list']:
        search_term = st.text_input("🔍 Search Order Number or Customer",
            placeholder="e.g. Q38642 or John Doe").strip().lower()
        if search_term:
            filtered_orders = [o for o in st.session_state['orders_list']
                if search_term in o['Order Name'].lower() or search_term in o['Customer'].lower()]
        else:
            filtered_orders = st.session_state['orders_list']

        if not filtered_orders:
            st.warning(f"No orders match '{search_term}'.")

        # IDs already in the batch, so we don't add duplicates.
        queued_ids = {e['order_data']['Sale ID'] for e in st.session_state['csv_queue']}

        if filtered_orders:
            bulk_cols = st.columns([2, 2, 4])
            if bulk_cols[0].button("☑️ Select all shown", key="bulk_select_all"):
                for o in filtered_orders:
                    if o['Sale ID'] not in queued_ids:
                        st.session_state[f"bulksel_{o['Sale ID']}"] = True
                st.rerun()
            if bulk_cols[1].button("⬜ Clear selection", key="bulk_clear_all"):
                for o in filtered_orders:
                    st.session_state[f"bulksel_{o['Sale ID']}"] = False
                st.rerun()

            selected_ids = [o['Sale ID'] for o in filtered_orders
                if st.session_state.get(f"bulksel_{o['Sale ID']}", False)
                and o['Sale ID'] not in queued_ids]

            if selected_ids:
                if bulk_cols[2].button(f"➕ Add {len(selected_ids)} selected order(s) to batch",
                                       key="add_selected", type="primary"):
                    by_id = {o['Sale ID']: o for o in st.session_state['orders_list']}
                    added = 0
                    for sid in selected_ids:
                        target = by_id.get(sid)
                        if not target:
                            continue
                        items = [{"fo_line_item_id": it['fo_line_item_id'], "title": it['title'],
                                  "dispatch_qty": it['remaining_qty']}
                                 for it in target['Line Items'] if it['remaining_qty'] > 0]
                        if not items:
                            continue

                        # Weight/dims: use the Pantone-matched product size when
                        # any line item is recognised, so the user doesn't have
                        # to pick a size manually. Falls back to the old
                        # weight-tier estimate when nothing matches.
                        auto_wt, matched_dims = compute_order_weight_and_dims(target)
                        if matched_dims:
                            length, width, height = matched_dims
                            dim_mode = "pantone"
                        else:
                            length, width, height, _ = dims_for_weight(auto_wt)
                            dim_mode = "auto"

                        # Service: use DEAR's requested carrier/service if we
                        # could detect one, otherwise default to Parcel Post.
                        service_code = target.get('_requested_service') or DEFAULT_SERVICE_FALLBACK

                        st.session_state['csv_queue'].append({
                            "order_data": target,
                            "selected_items": items,
                            "service": service_code,
                            "weight": auto_wt,
                            "length": length,
                            "width": width,
                            "height": height,
                            "dim_mode": dim_mode,        # "pantone" | "auto" until overridden
                            "pantone_dims": matched_dims,
                        })
                        st.session_state[f"bulksel_{sid}"] = False
                        added += 1
                    save_queue_to_disk()
                    st.success(f"Added {added} order(s) to the batch. Service/weight/dimensions "
                               f"are pre-filled where possible — adjust on the Export CSV tab if needed.")
                    st.rerun()

        st.write("---")

        for order in filtered_orders:
            order_key = str(order['Sale ID'])
            already = order['Sale ID'] in queued_ids
            hc = st.columns([1, 11])
            if already:
                hc[0].markdown("✅")
            else:
                hc[0].checkbox("Select", key=f"bulksel_{order_key}", label_visibility="collapsed")
            suffix = "  ·  _in batch_" if already else ""
            hc[1].markdown(f"### 📦 **{order['Order Name']}** — {order['Customer']}{suffix}")

            num_lines = len(order['Line Items'])
            total_units = sum(item['remaining_qty'] for item in order['Line Items'])
            changed = str(order.get('_updated', ''))[:16].replace("T", " ")
            changed_txt = f"  ·  _updated {changed}_" if changed else ""
            st.write(f"**Delivery:** {order['Address']}  ·  **{num_lines} line(s), {total_units} item(s)**{changed_txt}")
    else:
        st.info("No packed orders loaded. Click 'Fetch Packed Orders from DEAR' in the sidebar.")


# ===================================================================
# TAB 2 - CONFIGURE SERVICE / WEIGHT / DIMENSIONS, THEN DOWNLOAD
# ===================================================================
with tab_export:
    st.subheader("Step 2: Set service, weight & dimensions, then download the CSV")

    if st.session_state['csv_queue']:
        st.caption("Dimensions default automatically from the matched Pantone product, or from "
                   "weight if no product matched. Change weight and unmatched dims re-adjust; "
                   "pick a fixed size to override. Untick 'Include' to leave an order out.")

        bulk_cols = st.columns([2, 2, 8])
        if bulk_cols[0].button("☑️ Select all", key="exp_select_all"):
            for idx in range(len(st.session_state['csv_queue'])):
                st.session_state[f"exp_inc_{idx}"] = True
            st.rerun()
        if bulk_cols[1].button("⬜ Unselect all", key="exp_unselect_all"):
            for idx in range(len(st.session_state['csv_queue'])):
                st.session_state[f"exp_inc_{idx}"] = False
            st.rerun()

        header = st.columns([1, 3, 3, 3, 2, 3])
        header[0].markdown("*Include*")
        header[1].markdown("*Order*")
        header[2].markdown("*Service*")
        header[3].markdown("*Weight (kg)*")
        header[4].markdown("*Items*")
        header[5].markdown("*Dimensions*")

        flags = []
        service_names = list(SERVICE_OPTIONS.keys())
        dim_names = list(DIM_PRESETS.keys())

        for idx, entry in enumerate(st.session_state['csv_queue']):
            order = entry['order_data']
            row = st.columns([1, 3, 3, 3, 2, 3])

            flags.append(row[0].checkbox("inc", value=True, key=f"exp_inc_{idx}", label_visibility="collapsed"))
            row[1].write(order['Order Name'])
            row[1].caption(order['Customer'])

            # Service selector, defaulting to the entry's current service.
            cur_service_name = next((n for n, c in SERVICE_OPTIONS.items() if c == entry['service']), service_names[0])
            svc_name = row[2].selectbox("service", service_names,
                index=service_names.index(cur_service_name),
                key=f"exp_svc_{idx}", label_visibility="collapsed")
            entry['service'] = SERVICE_OPTIONS[svc_name]

            # Weight - editable.
            new_weight = row[3].number_input("wt", min_value=0.1, value=float(entry['weight']),
                step=0.1, key=f"exp_wt_{idx}", label_visibility="collapsed")

            unit_count = sum(i['dispatch_qty'] for i in entry['selected_items'])
            row[4].write(f"{unit_count}")

            # Dimensions: "Matched product" (when we found one in the Pantone
            # table), "Auto (from weight)", plus each fixed preset.
            dim_options = ["Auto (from weight)"] + dim_names
            has_pantone_match = bool(entry.get('pantone_dims'))
            if has_pantone_match:
                dim_options = ["Matched product (Pantone)"] + dim_options

            default_choice = "Matched product (Pantone)" if has_pantone_match else "Auto (from weight)"
            prev_choice = st.session_state.get(f"exp_dimchoice_{idx}", default_choice)
            dim_choice = row[5].selectbox("dim", dim_options,
                index=dim_options.index(prev_choice) if prev_choice in dim_options else 0,
                key=f"exp_dim_{idx}", label_visibility="collapsed")
            st.session_state[f"exp_dimchoice_{idx}"] = dim_choice

            # Apply weight + dimension resolution to the entry.
            entry['weight'] = float(round(new_weight, 2))
            if dim_choice == "Matched product (Pantone)" and has_pantone_match:
                l, w, h = entry['pantone_dims']
                entry['length'], entry['width'], entry['height'] = l, w, h
                row[5].caption(f"→ {l:g}x{w:g}x{h:g} (matched product)")
            elif dim_choice == "Auto (from weight)":
                l, w, h, tier = dims_for_weight(entry['weight'])
                entry['length'], entry['width'], entry['height'] = l, w, h
                row[5].caption(f"→ {int(l)}x{int(w)}x{int(h)} ({tier.split(' (')[0]})")
            else:
                l, w, h = DIM_PRESETS[dim_choice]
                entry['length'], entry['width'], entry['height'] = l, w, h

            # Customs value (AUD) - only relevant for international
            # shipments, feeds the "Commercial value" / per-unit value fields
            # on the intl CSV. Was previously never wired up to any input,
            # so it exported blank.
            addr = order.get("Raw Address", {}) or {}
            country = (addr.get("country") or "").strip().upper()
            if country and country not in ("AUSTRALIA", "AU"):
                cv_row = st.columns([1, 3, 3, 3, 2, 3])
                cv_row[1].caption(f"🌏 Customs value (AUD) for {order['Order Name']}")
                new_customs_value = cv_row[2].number_input(
                    "customs value", min_value=0.0,
                    value=float(entry.get('customs_value') or 0.0),
                    step=1.0, key=f"exp_customs_{idx}", label_visibility="collapsed")
                entry['customs_value'] = new_customs_value

        selected_entries = [e for e, keep in zip(st.session_state['csv_queue'], flags) if keep]
        save_queue_to_disk()

        st.write("---")
        c1, c2, c3 = st.columns([2, 2, 3])
        with c1:
            if selected_entries:
                df = build_export_dataframe(selected_entries)
                csv_bytes = df.to_csv(index=False).encode("utf-8-sig")
                fname = f"auspost_domestic_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                try:
                    with open(os.path.join(EXPORTS_DIR, fname), "wb") as f:
                        f.write(csv_bytes)
                except Exception as e:
                    log_error("export_csv", str(e))
                st.download_button(f"⬇️ Domestic CSV ({len(selected_entries)})",
                    data=csv_bytes, file_name=fname, mime="text/csv", type="primary")
                st.caption("Australian addresses.")
            else:
                st.info("Tick at least one order.")
        with c2:
            if selected_entries:
                idf = build_intl_dataframe(selected_entries)
                icsv = idf.to_csv(index=False).encode("utf-8-sig")
                ifname = f"auspost_international_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                try:
                    with open(os.path.join(EXPORTS_DIR, ifname), "wb") as f:
                        f.write(icsv)
                except Exception as e:
                    log_error("export_intl_csv", str(e))
                st.download_button(f"🌏 International CSV ({len(selected_entries)})",
                    data=icsv, file_name=ifname, mime="text/csv")
                st.caption(f"Sender: {INTL_SENDER_BUSINESS} · {INTL_PRODUCT_ID}.")
        with c3:
            if st.button("🗑️ Clear the whole batch"):
                st.session_state['csv_queue'] = []
                save_queue_to_disk()
                st.rerun()

        st.write("---")
        st.markdown(
            "**Next:** 1) Download CSV → 2) Import into AusPost Parcel Send, print labels → "
            "3) Export the results CSV from AusPost → 4) Upload it on the Import Tracking tab."
        )
    else:
        st.info("Batch is empty. Select orders on the Select Orders tab first.")


# ===================================================================
# TAB 3
# ===================================================================
with tab_import:
    st.subheader("Step 3: Upload AusPost results CSV → complete Ship in DEAR")
    st.markdown("Matched by **Sender reference 1** (order number). Matched orders get the "
                "Ship step completed in DEAR with the tracking number. **Before uploading, "
                "manually clear any existing box/package on the Ship step for these orders in "
                "DEAR** — a leftover placeholder line there causes a "
                "\"can't ship more than was packed\" error.")

    uploaded = st.file_uploader("AusPost results CSV", type=["csv"], key="results_upload")
    if uploaded is not None:
        mapping, err = parse_results_csv(uploaded.getvalue())
        if err:
            st.error(err)
        elif not mapping:
            st.warning("No usable rows (need a reference and a tracking number in each row).")
        else:
            st.success(f"Found tracking for {len(mapping)} reference(s).")
            queue_by_ref = {}
            for entry in st.session_state['csv_queue']:
                queue_by_ref.setdefault(entry['order_data']['Order Name'], []).append(entry)

            matched = [(ref, info) for ref, info in mapping.items() if ref in queue_by_ref]
            unmatched = [ref for ref in mapping if ref not in queue_by_ref]

            st.write(f"**Matched:** {len(matched)}  ·  **Unmatched CSV rows:** {len(unmatched)}")
            if unmatched:
                st.caption("Unmatched (skipped): " + ", ".join(unmatched[:20]) + ("…" if len(unmatched) > 20 else ""))

            if matched:
                st.dataframe(pd.DataFrame([{"Order": r, "Tracking": i["tracking"], "URL": i["url"]}
                    for r, i in matched]), use_container_width=True)

                if st.button(f"✅ Ship {len(matched)} orders in DEAR", type="primary"):
                    progress = st.progress(0.0)
                    status = st.container()
                    done, failed, completed_refs = 0, 0, []
                    for idx, (ref, info) in enumerate(matched):
                        for entry in queue_by_ref[ref]:
                            order = entry['order_data']
                            ok, msg = ship_cin7_sale(order['Sale ID'], DEFAULT_CARRIER, info['tracking'], info['url'])
                            if ok:
                                done += 1
                                completed_refs.append(ref)
                                status.success(f"{ref}: {msg} · {info['tracking']}")
                                items_str = ", ".join(f"{i['dispatch_qty']}x {i['title']}" for i in entry['selected_items'])
                                try:
                                    save_shipment(order['Order Name'], "", info['tracking'],
                                        entry['service'], "", "", entry['selected_items'])
                                    log_to_csv(order['Order Name'], order['Customer'],
                                        info['tracking'], info['url'], items_str)
                                except Exception as e:
                                    log_error("import_tracking", str(e))
                            else:
                                failed += 1
                                status.error(f"{ref}: {msg}")
                        progress.progress((idx + 1) / len(matched))

                    if completed_refs:
                        done_set = set(completed_refs)
                        st.session_state['csv_queue'] = [e for e in st.session_state['csv_queue']
                            if e['order_data']['Order Name'] not in done_set]
                        save_queue_to_disk()
                    st.info(f"Finished — {done} shipped in DEAR, {failed} failed.")
            else:
                st.warning("No tracking references match the current batch. Export from this session first.")