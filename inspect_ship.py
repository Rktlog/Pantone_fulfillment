"""
inspect_ship.py — dumps the raw Fulfilments (Pick/Pack/Ship) JSON for one
order, so we can see exactly what DEAR has recorded (quantities, box
breakdown, etc.) instead of guessing field names one error at a time.

Run:
    python inspect_ship.py Q38672
"""

import os
import sys
import json
import time
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

CIN7_ACCOUNT_ID = os.getenv("CIN7_ACCOUNT_ID")
CIN7_API_KEY = os.getenv("CIN7_API_KEY")
CIN7_BASE_URL = "https://inventory.dearsystems.com/ExternalApi/v2"


def headers():
    return {
        "api-auth-accountid": CIN7_ACCOUNT_ID,
        "api-auth-applicationkey": CIN7_API_KEY,
        "Content-Type": "application/json",
    }


def get_with_retry(path, params=None, max_retries=5):
    for attempt in range(max_retries):
        res = requests.get(f"{CIN7_BASE_URL}/{path}", headers=headers(), params=params, timeout=30)
        if res.status_code == 429:
            wait = float(res.headers.get("Retry-After", 5 * (attempt + 1)))
            print(f"  [rate limited - waiting {wait:.1f}s]")
            time.sleep(wait)
            continue
        time.sleep(1.1)  # basic pacing so we don't trip the limit ourselves
        return res
    return res


def find_sale_id(order_number, lookback_days=365):
    order_number = order_number.strip().upper()

    # Try the direct filter first (fast, if your account supports it).
    for param_name in ("SaleOrderNumber", "OrderNumber"):
        res = get_with_retry("saleList", params={param_name: order_number, "Page": 1, "Limit": 10})
        if res.status_code == 200:
            for s in res.json().get("SaleList", []):
                if str(s.get("OrderNumber", "")).strip().upper() == order_number:
                    print(f"[found via direct {param_name} filter]")
                    return s.get("SaleID")

    # Fall back to a windowed scan (same approach that worked in test_dear.py).
    cutoff = datetime.now() - timedelta(days=lookback_days)
    since = cutoff.strftime("%Y-%m-%dT00:00:00Z")
    print(f"[scanning saleList, UpdatedSince {since} ...]")
    for page in range(1, 300):
        res = get_with_retry("saleList", params={"Page": page, "Limit": 100, "UpdatedSince": since})
        if res.status_code != 200:
            print(f"  saleList page {page} error {res.status_code}: {res.text[:200]}")
            continue  # skip this page rather than aborting the whole scan
        batch = res.json().get("SaleList", [])
        if not batch:
            break
        for s in batch:
            if str(s.get("OrderNumber", "")).strip().upper() == order_number:
                print(f"[found on page {page}]")
                return s.get("SaleID")
        if len(batch) < 100:
            break
    return None


def main():
    order_number = sys.argv[1] if len(sys.argv) > 1 else None
    if not order_number:
        print("Usage: python inspect_ship.py <ORDER_NUMBER>")
        return

    if not CIN7_ACCOUNT_ID or not CIN7_API_KEY:
        print("Missing CIN7_ACCOUNT_ID / CIN7_API_KEY in .env")
        return

    print(f"Finding SaleID for {order_number} ...")
    sale_id = find_sale_id(order_number)
    if not sale_id:
        print(f"Could not find {order_number}.")
        return
    print(f"SaleID: {sale_id}\n")

    res = get_with_retry("sale", params={"ID": sale_id})
    if res.status_code != 200:
        print(f"Could not fetch sale detail: {res.status_code} {res.text[:300]}")
        return
    full = res.json()

    fulfilments = full.get("Fulfilments", []) or []
    print(f"=== {len(fulfilments)} Fulfilment(s) on this sale ===\n")
    for i, f in enumerate(fulfilments):
        print(f"--- Fulfilment #{i} ---")
        print(f"TaskID: {f.get('TaskID')}")
        for section in ("Pick", "Pack", "Ship"):
            block = f.get(section) or {}
            print(f"\n[{section}]")
            print(json.dumps(block, indent=2, default=str))
        print("\n" + "=" * 60 + "\n")

    # Also dump the order's ordered lines for comparison.
    order_block = full.get("Order", {}) or {}
    lines = order_block.get("Lines", []) or []
    print("=== Order lines (as ordered) ===")
    for l in lines:
        print(f"  SKU={l.get('SKU')}  Qty={l.get('Quantity')}  Name={l.get('Name')}")


if __name__ == "__main__":
    main()