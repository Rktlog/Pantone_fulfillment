"""
check_order.py — v2
Diagnoses why a specific DEAR order isn't showing up in the app's fetch.

Run:
    python check_order.py Q38672
"""

import os
import sys
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv

print("=== check_order.py v2 ===")
print(f"Running from: {os.path.abspath(__file__)}\n")

load_dotenv()

CIN7_ACCOUNT_ID = os.getenv("CIN7_ACCOUNT_ID")
CIN7_API_KEY = os.getenv("CIN7_API_KEY")
CIN7_BASE_URL = "https://inventory.dearsystems.com/ExternalApi/v2"

ORDER_NUMBER_PREFIX = os.getenv("CIN7_ORDER_PREFIX", "SQ").strip().upper()
CIN7_LOOKBACK_DAYS = int(os.getenv("CIN7_LOOKBACK_DAYS", "30"))
MAX_DETAIL_FETCHES = int(os.getenv("CIN7_MAX_DETAILS", "150"))


def headers():
    return {
        "api-auth-accountid": CIN7_ACCOUNT_ID,
        "api-auth-applicationkey": CIN7_API_KEY,
        "Content-Type": "application/json",
    }


def cin7_get(path, params=None):
    url = f"{CIN7_BASE_URL}/{path}"
    return requests.get(url, headers=headers(), params=params, timeout=30)


def find_summary_by_order_number(order_number, wide_search_days=365):
    order_number = order_number.strip().upper()

    for param_name in ("SaleOrderNumber", "OrderNumber"):
        res = cin7_get("saleList", params={param_name: order_number, "Page": 1, "Limit": 10})
        if res.status_code == 200:
            batch = res.json().get("SaleList", [])
            for s in batch:
                if str(s.get("OrderNumber", "")).strip().upper() == order_number:
                    print(f"[found via saleList?{param_name}=... filter]")
                    return s
    print("[direct filter didn't return it — falling back to a wide scan of saleList]")

    cutoff = datetime.now() - timedelta(days=wide_search_days)
    since = cutoff.strftime("%Y-%m-%dT00:00:00Z")
    for page in range(1, 200):
        res = cin7_get("saleList", params={"Page": page, "Limit": 100, "UpdatedSince": since})
        if res.status_code != 200:
            print(f"  saleList page {page} error {res.status_code}: {res.text[:200]}")
            break
        batch = res.json().get("SaleList", [])
        if not batch:
            break
        for s in batch:
            if str(s.get("OrderNumber", "")).strip().upper() == order_number:
                print(f"[found on saleList page {page} of the {wide_search_days}-day scan]")
                return s
        if len(batch) < 100:
            break
    return None


def main():
    order_number = sys.argv[1] if len(sys.argv) > 1 else None
    if not order_number:
        print("Usage: python check_order.py <ORDER_NUMBER>")
        return

    print(f"Argument received: {sys.argv}")
    print(f"Looking up order: {order_number}\n")

    if not CIN7_ACCOUNT_ID or not CIN7_API_KEY:
        print("Missing CIN7_ACCOUNT_ID / CIN7_API_KEY in .env — can't call DEAR.")
        return

    summary = find_summary_by_order_number(order_number)

    if summary is None:
        print(f"\n❌ Could not find {order_number} in saleList at all (even over a "
              f"365-day scan). Either the order number is wrong, or it's outside "
              f"that window too.")
        return

    sale_id = summary.get("SaleID")
    updated = summary.get("Updated", "")
    packing = str(summary.get("CombinedPackingStatus", "")).strip().upper()
    shipping = str(summary.get("CombinedShippingStatus", "")).strip().upper()
    dear_status = summary.get("Status", "")

    print("\n--- saleList summary ---")
    print(f"  SaleID:                  {sale_id}")
    print(f"  OrderNumber:             {summary.get('OrderNumber')}")
    print(f"  Status (DEAR):           {dear_status}")
    print(f"  Updated:                 {updated}")
    print(f"  CombinedPackingStatus:   {packing}")
    print(f"  CombinedShippingStatus:  {shipping}")

    print("\n--- app filter checks ---")

    cutoff = datetime.now() - timedelta(days=CIN7_LOOKBACK_DAYS)
    try:
        updated_dt = datetime.strptime(updated[:19], "%Y-%m-%dT%H:%M:%S")
        in_window = updated_dt >= cutoff
    except Exception:
        updated_dt = None
        in_window = None
    print(f"  CIN7_LOOKBACK_DAYS = {CIN7_LOOKBACK_DAYS}  (cutoff: {cutoff.strftime('%Y-%m-%d %H:%M')})")
    if in_window is None:
        print(f"  ⚠️  Could not parse Updated timestamp '{updated}' to compare.")
    elif in_window:
        print(f"  ✅ Updated ({updated}) is within the lookback window.")
    else:
        print(f"  ❌ Updated ({updated}) is OLDER than the lookback window — "
              f"MISSED by the UpdatedSince scan. Increase CIN7_LOOKBACK_DAYS in .env.")

    if packing in ("NOT AVAILABLE", "NOT PACKED"):
        print(f"  ❌ CombinedPackingStatus = '{packing}' — skipped (not packed yet).")
    else:
        print(f"  ✅ CombinedPackingStatus = '{packing}' — passes.")

    if shipping == "SHIPPED":
        print(f"  ❌ CombinedShippingStatus = 'SHIPPED' — skipped (already shipped).")
    else:
        print(f"  ✅ CombinedShippingStatus = '{shipping}' — passes.")

    order_no = str(summary.get("OrderNumber", "")).strip()
    if ORDER_NUMBER_PREFIX and not order_no.upper().startswith(ORDER_NUMBER_PREFIX):
        print(f"  ❌ ORDER_NUMBER_PREFIX = '{ORDER_NUMBER_PREFIX}' but order is '{order_no}' — skipped.")
    else:
        print(f"  ✅ Order number '{order_no}' matches prefix '{ORDER_NUMBER_PREFIX}' (or none set).")

    if not sale_id:
        print("\n  Could not fetch full detail — no SaleID on the summary.")
        return

    res = cin7_get("sale", params={"ID": sale_id})
    if res.status_code != 200:
        print(f"\n  ❌ Could not fetch full sale detail ({res.status_code}): {res.text[:300]}")
        return
    full = res.json()

    print(f"\n--- Full sale detail ---")
    print(f"  Full sale 'Status' field: {full.get('Status', '(not present)')}")

    print("\n--- Fulfilments on this sale ---")
    fulfilments = full.get("Fulfilments", []) or []
    if not fulfilments:
        print("  ❌ No Fulfilments at all — DEAR hasn't started picking/packing it.")
    ready_found = False
    for i, f in enumerate(fulfilments):
        pick = str((f.get("Pick") or {}).get("Status", "")).strip().upper()
        pack = str((f.get("Pack") or {}).get("Status", "")).strip().upper()
        ship = str((f.get("Ship") or {}).get("Status", "")).strip().upper()
        print(f"  Fulfilment #{i}:  Pick={pick or '(none)'}  Pack={pack or '(none)'}  Ship={ship or '(none)'}")
        if pick == "AUTHORISED" and pack == "AUTHORISED" and ship != "AUTHORISED":
            ready_found = True

    print()
    if ready_found:
        print("  ✅ SHOULD pass the app's readiness check.")
    else:
        print("  ❌ No fulfilment has BOTH Pick and Pack = AUTHORISED with Ship still open — this is why it's excluded.")

    print(f"\nMAX_DETAIL_FETCHES = {MAX_DETAIL_FETCHES} (cap on how many candidates get checked per fetch).")


if __name__ == "__main__":
    main()