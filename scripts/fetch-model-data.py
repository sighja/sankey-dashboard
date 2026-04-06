#!/usr/bin/env python3
"""
Fetch Grinn model scenario snapshots from Excel Online via Microsoft Graph API
using client credentials (app-only auth). Writes data.json for the dashboard.

Strategy to handle 504 timeouts on complex workbooks:
1. Get auth token
2. Create a persistent workbook session (retries with long waits)
3. "Warm up" the workbook by reading a tiny range first
4. Wait for Excel Online to finish loading/calculating
5. Read each scenario block individually
"""
import json, os, sys, time
from datetime import datetime, timezone
from urllib.request import Request, urlopen
from urllib.parse import quote
from urllib.error import HTTPError

# ── Config ──────────────────────────────────────────────────────────────
TENANT_ID     = os.environ["AZURE_TENANT_ID"]
CLIENT_ID     = os.environ["AZURE_CLIENT_ID"]
CLIENT_SECRET = os.environ["AZURE_CLIENT_SECRET"]
USER_ID       = os.environ["GRAPH_USER_ID"]
DRIVE_PATH    = os.environ.get("DRIVE_PATH", "Grinn/Grinn_v17_r41.xlsm")
SHEET         = os.environ.get("SHEET", "45_Combined")

GRAPH_BASE    = "https://graph.microsoft.com/v1.0"
TOKEN_URL     = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"

SCENARIOS     = ["Bull", "Base", "Bear", "Organic"]
SNAPSHOT_STARTS = {"Bull": 30, "Base": 59, "Bear": 88, "Organic": 114}
N_ROWS        = 24
YEARS         = ["2026", "2027", "2028", "2029", "2030"]

# ── Auth ────────────────────────────────────────────────────────────────
def get_app_token():
    body = (
        f"client_id={CLIENT_ID}"
        f"&client_secret={CLIENT_SECRET}"
        f"&scope=https%3A%2F%2Fgraph.microsoft.com%2F.default"
        f"&grant_type=client_credentials"
    ).encode()
    req = Request(TOKEN_URL, data=body, headers={"Content-Type": "application/x-www-form-urlencoded"})
    resp = json.loads(urlopen(req).read())
    return resp["access_token"]

# ── Graph helpers ───────────────────────────────────────────────────────
def workbook_url():
    return f"{GRAPH_BASE}/users/{USER_ID}/drive/root:/{DRIVE_PATH}:/workbook"

def graph_get(url, token, session_id=None, retries=6, base_wait=10):
    """GET with aggressive retry for 504s."""
    for attempt in range(retries):
        headers = {"Authorization": f"Bearer {token}"}
        if session_id:
            headers["workbook-session-id"] = session_id
        req = Request(url, headers=headers)
        try:
            return json.loads(urlopen(req, timeout=180).read())
        except HTTPError as e:
            if e.code in (504, 502, 503) and attempt < retries - 1:
                wait = base_wait * (attempt + 1)
                print(f"  ⏳ {e.code} error, retry {attempt+1}/{retries-1} (waiting {wait}s)...")
                time.sleep(wait)
                continue
            raise
        except Exception as e:
            if attempt < retries - 1:
                wait = base_wait * (attempt + 1)
                print(f"  ⏳ Error: {e}, retry {attempt+1}/{retries-1} (waiting {wait}s)...")
                time.sleep(wait)
                continue
            raise

def graph_post(url, token, body_dict, timeout=120):
    body = json.dumps(body_dict).encode()
    req = Request(url, data=body, headers={
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    })
    return json.loads(urlopen(req, timeout=timeout).read())

def create_session(token, retries=3):
    """Create a persistent session with retries."""
    url = f"{workbook_url()}/createSession"
    for attempt in range(retries):
        try:
            resp = graph_post(url, token, {"persistChanges": False}, timeout=180)
            return resp.get("id")
        except Exception as e:
            if attempt < retries - 1:
                wait = 15 * (attempt + 1)
                print(f"  ⚠️ Session attempt {attempt+1} failed: {e}")
                print(f"  Waiting {wait}s before retry...")
                time.sleep(wait)
            else:
                print(f"  ⚠️ All session attempts failed: {e}")
                return None

def read_range(token, session_id, sheet, address):
    encoded_sheet = quote(sheet, safe="")
    url = f"{workbook_url()}/worksheets('{encoded_sheet}')/range(address='{address}')"
    data = graph_get(url, token, session_id)
    return data.get("values", [])

def warm_up_workbook(token, session_id):
    """Read a single cell to force the workbook to load and calculate."""
    print("🔥 Warming up workbook (reading A1)...")
    encoded_sheet = quote(SHEET, safe="")
    url = f"{workbook_url()}/worksheets('{encoded_sheet}')/range(address='A1')"
    try:
        graph_get(url, token, session_id, retries=6, base_wait=15)
        print("  ✅ Workbook is responsive")
        return True
    except Exception as e:
        print(f"  ⚠️ Warm-up failed: {e}")
        return False

# ── Main ────────────────────────────────────────────────────────────────
def main():
    print("🔐 Authenticating (client credentials)...")
    token = get_app_token()
    print("✅ Token acquired")

    # Step 1: Create session (triggers workbook load)
    print("📂 Creating workbook session...")
    session_id = create_session(token)
    if session_id:
        print(f"  Session: {session_id[:20]}...")
    else:
        print("  No session (proceeding without)")

    # Step 2: Warm up — read a single cell to force Excel Online to open the workbook
    # The first access to a complex workbook often triggers recalculation and times out.
    # By warming up and waiting, subsequent reads are much faster.
    warm_up_workbook(token, session_id)
    print("⏳ Waiting 30s for workbook to stabilize...")
    time.sleep(30)

    # Step 3: Read each scenario
    output = {
        "updated": datetime.now(timezone.utc).isoformat(),
        "sheet": SHEET,
        "years": YEARS,
        "scenarios": {}
    }

    for scenario in SCENARIOS:
        start_row = SNAPSHOT_STARTS[scenario]
        end_row = start_row + N_ROWS - 1
        address = f"A{start_row}:F{end_row}"
        print(f"📊 Reading {SHEET}!{address} ({scenario})...")

        try:
            all_values = read_range(token, session_id, SHEET, address)
            print(f"  Got {len(all_values)} rows")
        except Exception as e:
            print(f"  ❌ Failed to read {scenario}: {e}")
            continue

        row_labels = []
        scenario_data = {}

        for yi, year in enumerate(YEARS):
            year_data = {}
            for offset in range(N_ROWS):
                if offset >= len(all_values):
                    break
                row = all_values[offset]
                label = str(row[0] or "").strip() if row[0] else f"row_{offset}"
                if yi == 0:
                    row_labels.append(label)
                year_data[label] = row[yi + 1] if (yi + 1) < len(row) else 0
            scenario_data[year] = year_data

        output["scenarios"][scenario] = {
            "labels": row_labels,
            "data": scenario_data
        }
        print(f"  ✅ {scenario}: {len(row_labels)} rows, labels: {row_labels[:5]}...")

        # Small delay between reads
        time.sleep(3)

    if not output["scenarios"]:
        print("❌ No scenarios were read successfully!")
        sys.exit(1)

    # Write output
    out_path = os.environ.get("OUTPUT_PATH", "data.json")
    with open(out_path, "w") as f:
        json.dump(output, f, indent=2)
    print(f"\n📁 Wrote {out_path} ({os.path.getsize(out_path)} bytes)")
    print(f"🕐 Updated: {output['updated']}")

if __name__ == "__main__":
    main()
