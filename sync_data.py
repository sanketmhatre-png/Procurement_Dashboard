"""
Sync the Procurement Dashboard data from Metabase into monthly .csv.gz files, plus an all-time summary.
  dn  -> Delivery Note line items (Metabase question DN_S),  dated by posting date,   saved in dn/
  bom -> BOM line items          (Metabase question BOM_S), dated by creation date, saved in bom/
  stock -> incoming stock ledger entries (Metabase question "stock Balnce"), from Apr 2026, saved in stock/

Runs in GitHub Actions (sync_data.yml). METABASE_URL defaults to SolarSquare's Metabase. Needs either:
  - METABASE_USERNAME and METABASE_PASSWORD (your normal Metabase login), or
  - METABASE_API_KEY (created by a Metabase admin)

    python sync_data.py              # all datasets: missing months + refresh the last 2 months
    python sync_data.py dn           # only delivery notes
    python sync_data.py bom --full   # only BOMs, re-export every month
    python sync_data.py stock        # only stock
"""
import csv, gzip, io, json, os, re, sys, time
from datetime import date, datetime, timezone
from pathlib import Path
import requests

METABASE_URL = (os.environ.get("METABASE_URL") or "https://metabase-lighthouse.solarsquare.in").rstrip("/")
API_KEY = os.environ.get("METABASE_API_KEY", "")
USERNAME = os.environ.get("METABASE_USERNAME", "")
PASSWORD = os.environ.get("METABASE_PASSWORD", "")
DATABASE_ID = 9                     # ERP (MySQL)
START = date(2025, 1, 1)
REFRESH_MONTHS = 2                  # current + previous month (catches records cancelled or changed since)

# ---------- The two datasets ----------
# Column names say which CSV columns feed the dashboard's breakdowns.
# "group": "customer" = customer group from the project ID (DN); "status" = workflow state (BOM).
DATASETS = {
    "dn": {
        "label": "DN",
        "out_dir": "dn",
        # Same SQL as Metabase question DN_S (#5728), one month at a time.
        "sql": """
SELECT dni.parent AS DN_NO, dn.posting_date, dn.customer AS Project, dn.workflow_state,
       dni.warehouse AS source_warehouse, dni.target_warehouse AS target_warehouse,
       dni.custom_item_category AS item_category, dni.custom_item_subcategory AS item_subcategory,
       dni.item_code, dni.item_name, dni.qty, dni.uom AS uohm, dni.rate, dni.amount,
       dn.status AS `Delivery Note Status`, dn.docstatus
FROM `tabDelivery Note Item` dni
LEFT JOIN `tabDelivery Note` dn ON dni.parent = dn.name
WHERE dn.docstatus = 1
  AND dn.posting_date >= '{start}' AND dn.posting_date < '{end}'
ORDER BY dn.posting_date DESC
""",
        "col": {"id": "DN_NO", "date": "posting_date", "project": "Project",
                "warehouse": "source_warehouse", "category": "item_category", "amount": "amount"},
        "group": "customer",
    },
    "bom": {
        "label": "BOM",
        "out_dir": "bom",
        # Same columns as Metabase question BOM_S (#5883), plus creation date and item category.
        "sql": """
SELECT b.name AS bom_id, DATE(b.creation) AS creation_date, b.project, b.item, b.item_name, b.quantity,
       b.warehouse, bi.name AS bom_item_name, it.custom_item_category AS item_category, bi.item_code,
       bi.item_name AS bom_item_item_name, bi.qty, bi.rate, bi.amount, b.workflow_state, b.docstatus
FROM tabBOM b
JOIN `tabBOM Item` bi ON b.name = bi.parent
LEFT JOIN tabItem it ON it.name = bi.item_code
WHERE b.creation >= '{start}' AND b.creation < '{end}'
ORDER BY b.creation DESC
""",
        "col": {"id": "bom_id", "date": "creation_date", "project": "project",
                "warehouse": "warehouse", "category": "item_category", "amount": "amount",
                "status": "workflow_state"},
        "group": "status",
    },
    "stock": {
        "label": "Stock",
        "out_dir": "stock",
        "start": date(2026, 4, 1),
        "summary": False,               # data only for now; the dashboard view comes later
        # Same SQL as Metabase question "stock Balnce" (#6126), one month (by creation) at a time.
        "sql": """
SELECT
    DATE(creation)          AS `Date`,
    warehouse,
    item_code,
    voucher_no              AS ID,
    qty_after_transaction   AS QTY,
    valuation_rate          AS AVG_Rate,
    stock_uom               AS UOM
FROM `tabStock Ledger Entry`
WHERE actual_qty >= 1
  AND creation >= '{start}' AND creation < '{end}'
ORDER BY warehouse, item_code, creation
""",
        "col": {"id": "ID", "date": "Date", "amount": None},
    },
}

# ---------- Metabase access ----------
HEADERS = {}

def auth_headers():
    """Use the API key if there is one, otherwise log in with email + password."""
    if API_KEY:
        return {"X-API-Key": API_KEY}
    if not (USERNAME and PASSWORD):
        sys.exit("Set METABASE_USERNAME and METABASE_PASSWORD (or METABASE_API_KEY).")
    r = requests.post(f"{METABASE_URL}/api/session",
                      json={"username": USERNAME, "password": PASSWORD}, timeout=60)
    if r.status_code in (400, 401):
        sys.exit("Metabase login failed: check METABASE_USERNAME / METABASE_PASSWORD. "
                 "If you sign in to Metabase with Google, this account has no password; use an API key.")
    r.raise_for_status()
    return {"X-Metabase-Session": r.json()["id"]}

def logout(headers):
    if "X-Metabase-Session" in headers:
        try:
            requests.delete(f"{METABASE_URL}/api/session", headers=headers, timeout=30)
        except Exception:
            pass

def fetch_csv(sql, first, nxt, attempts=3):
    query = {"database": DATABASE_ID, "type": "native",
             "native": {"query": sql.format(start=first, end=nxt)}}
    for i in range(attempts):
        try:
            r = requests.post(f"{METABASE_URL}/api/dataset/csv",
                              headers=HEADERS,
                              data={"query": json.dumps(query), "format_rows": "false"},
                              timeout=1200)
            r.raise_for_status()
            return r.content.decode("utf-8-sig")
        except Exception as e:
            if i == attempts - 1:
                raise
            print(f"  retrying after error: {e}")
            time.sleep(30)

# ---------- Helpers ----------
def months(start, end):
    y, m = start.year, start.month
    while (y, m) <= (end.year, end.month):
        first = date(y, m, 1)
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
        yield first, date(y, m, 1)

def norm(v, empty):
    v = " ".join((v or "").split())
    return v or empty

def num(v):
    try: return float(str(v).replace(",", "") or 0)
    except ValueError: return 0.0

def count_rows(text, amount_col):
    rows, amount = 0, 0.0
    for row in csv.DictReader(io.StringIO(text)):
        rows += 1
        if amount_col:
            amount += num(row.get(amount_col))
    return rows, round(amount, 2)

STATES = {"MH": "Maharashtra", "UP": "Uttar Pradesh", "MP": "Madhya Pradesh", "TN": "Tamil Nadu",
          "RJ": "Rajasthan", "GJ": "Gujarat", "DL": "Delhi", "TL": "Telangana", "TS": "Telangana",
          "KA": "Karnataka", "HR": "Haryana", "AP": "Andhra Pradesh", "CG": "Chhattisgarh",
          "PB": "Punjab", "WB": "West Bengal", "KL": "Kerala", "OR": "Odisha", "OD": "Odisha",
          "BR": "Bihar", "UK": "Uttarakhand", "JH": "Jharkhand", "GA": "Goa", "AS": "Assam"}
NO_STATE = "Not state-coded"
# Keep these rules identical to customerInfo() in index.html
def customer_info(c):
    c = (c or "").strip()
    m = re.match(r"^(RSSE|SSE)([A-Z]{2})", c)
    if m:
        return f"{m.group(1)} projects", STATES.get(m.group(2), m.group(2))
    u = c.upper()
    if u.startswith("SSHS"): return "SSHS projects", NO_STATE
    if u.startswith("CCNI"): return "CCNI projects", NO_STATE
    if u.startswith("SOLARSQUARE ENERGY"): return "Internal transfers", NO_STATE
    if c.endswith(" Customer"): return "Service customers", NO_STATE
    return "Other customers", NO_STATE

# ---------- All-time summary for the dashboard (<out_dir>/summary.json) ----------
def build_summary(cfg, out_dir, manifest):
    col = cfg["col"]
    dims = {k: [] for k in ("warehouse", "category", "group", "state")}
    index = {k: {} for k in dims}
    def idx(k, v):
        d = index[k]
        if v not in d:
            d[v] = len(dims[k]); dims[k].append(v)
        return d[v]
    months_ = sorted(manifest["months"])
    cube, doc_cube, cust_cache = {}, {}, {}
    first_date, last_date = None, None
    for mi, key in enumerate(months_):
        path = out_dir / manifest["months"][key]["file"]
        with gzip.open(path, "rt", encoding="utf-8", newline="") as f:
            for row in csv.DictReader(f):
                cust = row.get(col["project"], "")
                if cust not in cust_cache:
                    cust_cache[cust] = customer_info(cust)
                group, state = cust_cache[cust]
                if cfg["group"] == "status":
                    group = norm(row.get(col["status"]), "No status")
                w = idx("warehouse", norm(row.get(col["warehouse"]), "No warehouse"))
                c = idx("category", norm(row.get(col["category"]), "Uncategorised"))
                gi, si = idx("group", group), idx("state", state)
                doc = row.get(col["id"], "")
                cell = cube.get((mi, w, c, gi, si))
                if cell is None:
                    cell = cube[(mi, w, c, gi, si)] = [0, 0.0, set()]
                cell[0] += 1
                cell[1] += num(row.get(col["amount"]))
                cell[2].add(doc)
                doc_cube.setdefault((mi, w, gi, si), set()).add(doc)
                d = (row.get(col["date"]) or "")[:10]
                if d:
                    first_date = d if not first_date or d < first_date else first_date
                    last_date = d if not last_date or d > last_date else last_date
    return {
        "generated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "first_date": first_date, "last_date": last_date,
        "months": months_, "dims": dims,
        # cube rows: month, warehouse, category, group, state, line items, amount, documents (DNs or BOMs)
        "cube": [[*k, v[0], round(v[1]), len(v[2])] for k, v in cube.items()],
        # documents without the category split (exact counts when no category filter is on)
        "dn_cube": [[*k, len(v)] for k, v in doc_cube.items()],
    }

# ---------- Sync one dataset ----------
def sync(key, full):
    cfg = DATASETS[key]
    out_dir = Path(cfg["out_dir"])
    out_dir.mkdir(exist_ok=True)
    print(f"=== {cfg['label']} -> {out_dir}/ ===")
    manifest_path = out_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text()) if manifest_path.exists() else {"months": {}}
    all_months = list(months(cfg.get("start", START), date.today()))
    recent = {f"{m:%Y-%m}" for m, _ in all_months[-REFRESH_MONTHS:]}

    for first, nxt in all_months:
        mkey = f"{first:%Y-%m}"
        path = out_dir / f"{cfg['out_dir']}_{mkey}.csv.gz"
        if not full and path.exists() and mkey not in recent and mkey in manifest["months"]:
            continue
        t = time.time()
        text = fetch_csv(cfg["sql"], first, nxt)
        # mtime=0 keeps the file byte-identical when the data hasn't changed, so git sees no diff
        with open(path, "wb") as f, gzip.GzipFile(filename="", mode="wb", fileobj=f,
                                                  mtime=0, compresslevel=9) as gz:
            gz.write(text.encode("utf-8"))
        rows, amount = count_rows(text, cfg["col"]["amount"])
        manifest["months"][mkey] = {"file": path.name, "rows": rows, "amount": amount,
                                    "bytes": path.stat().st_size}
        print(f"{mkey}: {rows:>7,} rows  {path.stat().st_size/1e6:5.1f} MB  ({time.time()-t:.0f}s)")

    manifest["months"] = dict(sorted(manifest["months"].items()))
    manifest["last_refresh"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    manifest["total_rows"] = sum(m["rows"] for m in manifest["months"].values())
    manifest_path.write_text(json.dumps(manifest, indent=1))
    if cfg.get("summary", True):
        t = time.time()
        summary = build_summary(cfg, out_dir, manifest)
        (out_dir / "summary.json").write_text(json.dumps(summary, separators=(",", ":")))
        print(f"Summary: {len(summary['cube']):,} cells ({time.time()-t:.0f}s)")
    print(f"{cfg['label']} done: {manifest['total_rows']:,} rows across {len(manifest['months'])} months.\n")

def main():
    args = [a.lower() for a in sys.argv[1:]]
    full = "--full" in args
    which = next((a for a in args if a in DATASETS or a in ("all", "both")), "all")
    keys = list(DATASETS) if which in ("all", "both") else [which]   # "both" kept for older workflow files
    HEADERS.update(auth_headers())
    failed = []
    try:
        for key in keys:
            try:
                sync(key, full)
            except Exception as e:            # keep going so the other dataset still refreshes
                print(f"!! {DATASETS[key]['label']} sync failed: {e}\n")
                failed.append(DATASETS[key]["label"])
    finally:
        logout(HEADERS)
    if failed:
        sys.exit(f"Failed: {', '.join(failed)}. The rest was saved; re-run to retry.")

if __name__ == "__main__":
    main()
