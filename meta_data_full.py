import os
import requests
import pandas as pd
import time
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text, inspect


# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
ACCESS_TOKEN = os.environ["ACCESS_TOKEN"]
NEON_CONNECTION_STRING = os.environ["NEON_CONNECTION_STRING"]

AD_ACCOUNT_IDS = [
    "act_1937951709801590",
    "act_2111571239641157",
    "act_935022987639527",
    "act_1447779473228664",
    "act_2073146216601611"
]

END_DATE   = datetime.today().strftime("%Y-%m-%d")
START_DATE = (datetime.today() - timedelta(days=40)).strftime("%Y-%m-%d")

TABLE_NAME = "meta_ads_summary"

FIELDS = [
    "campaign_id", "campaign_name",
    "adset_id", "adset_name",
    "ad_id", "ad_name",
    "objective",
    "impressions", "reach", "frequency",
    "clicks", "ctr", "cpc", "cpm", "spend",
    "actions", "action_values",
    "cost_per_action_type",
    "purchase_roas",
    "results",
    "cost_per_result",
    "date_start", "date_stop",
    "catalog_segment_actions",
    "catalog_segment_value",
    "catalog_segment_value_mobile_purchase_roas",
    "catalog_segment_value_omni_purchase_roas",
    "catalog_segment_value_website_purchase_roas"
]

# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────
def safe_float(value):
    try:
        return float(value) if value else 0
    except (ValueError, TypeError):
        return 0


# ─────────────────────────────────────────────
# ASYNC JOB: CREATE (Ad-level insights)
# ─────────────────────────────────────────────
def create_async_job(account):
    url = f"https://graph.facebook.com/v18.0/{account}/insights"
    params = {
        "level": "ad",
        "time_increment": 1,
        "fields": ",".join(FIELDS),
        "time_range": f'{{"since":"{START_DATE}","until":"{END_DATE}"}}',
        "action_attribution_windows": "7d_click,1d_view",
        "action_report_time": "conversion",
        "access_token": ACCESS_TOKEN,
        "limit": 500,
    }
    response = requests.post(url, params=params)
    data = response.json()

    if "report_run_id" in data:
        job_id = data["report_run_id"]
        print(f"  ✅ Job created: {job_id}")
        return job_id
    else:
        print(f"  ❌ Failed to create job for {account}: {data}")
        return None


# ─────────────────────────────────────────────
# ASYNC JOB: POLL UNTIL COMPLETE
# ─────────────────────────────────────────────
def poll_job(job_id, timeout_minutes=30):
    url = f"https://graph.facebook.com/v18.0/{job_id}"
    params = {"access_token": ACCESS_TOKEN}

    start = time.time()
    poll_interval = 10

    while True:
        elapsed = time.time() - start
        if elapsed > timeout_minutes * 60:
            print(f"  ❌ Job {job_id} timed out after {timeout_minutes} minutes")
            return False

        response = requests.get(url, params=params)
        data = response.json()

        status = data.get("async_status", "unknown")
        pct    = data.get("async_percent_completion", 0)

        print(f"  ⏳ Status: {status} ({pct}% complete) — {int(elapsed)}s elapsed")

        if status == "Job Completed":
            return True
        elif status in ("Job Failed", "Job Skipped"):
            print(f"  ❌ Job failed with status: {status}")
            print(f"     Details: {data}")
            return False

        time.sleep(poll_interval)
        poll_interval = min(poll_interval + 5, 30)  # ramp up to 30s max


# ─────────────────────────────────────────────
# ASYNC JOB: FETCH RESULTS
# ─────────────────────────────────────────────
def fetch_job_results(job_id):
    url = f"https://graph.facebook.com/v18.0/{job_id}/insights"
    params = {
        "access_token": ACCESS_TOKEN,
        "limit": 500,
    }

    rows = []
    page = 0

    while True:
        response = requests.get(url, params=params)
        data = response.json()

        if "data" not in data:
            print(f"  ❌ Error fetching results: {data}")
            break

        rows.extend(data["data"])
        page += 1
        print(f"  Page {page}: {len(data['data'])} rows | Total: {len(rows)}")

        if "paging" in data and "next" in data["paging"]:
            url = data["paging"]["next"]
            params = {}
            time.sleep(1)
        else:
            break

    return rows


# ─────────────────────────────────────────────
# ORPHAN PURCHASE FETCH
# Fetches account-level daily purchase values
# including days when no ads were actively running.
# This captures delayed conversions attributed to
# paused/stopped campaigns via the conversion window.
# ─────────────────────────────────────────────
PIXEL_PURCHASE_FIELDS = [
    "ad_id", "ad_name",
    "adset_id", "adset_name",
    "campaign_id", "campaign_name",
    "actions", "action_values",
    "date_start", "date_stop",
]

def create_orphan_purchase_job(account):
    """
    Creates an async insights job at the AD level but with
    action_report_time=conversion and NO spend/impression filter.
    This returns rows for ads that received 0 spend but still had
    purchase conversions attributed to them on a given day.
    """
    url = f"https://graph.facebook.com/v18.0/{account}/insights"
    params = {
        "level": "ad",
        "time_increment": 1,
        "fields": ",".join(PIXEL_PURCHASE_FIELDS),
        "time_range": f'{{"since":"{START_DATE}","until":"{END_DATE}"}}',
        "action_attribution_windows": "7d_click,1d_view",
        "action_report_time": "conversion",
        # Key difference: filter to only rows that have purchase action values
        # but no spend constraint — Meta will return these "zero-spend" rows
        "filtering": '[{"field":"action_values","operator":"GREATER_THAN","value":0}]',
        "access_token": ACCESS_TOKEN,
        "limit": 500,
    }
    response = requests.post(url, params=params)
    data = response.json()

    if "report_run_id" in data:
        job_id = data["report_run_id"]
        print(f"  ✅ Orphan purchase job created: {job_id}")
        return job_id
    else:
        print(f"  ⚠️  Could not create orphan job for {account}: {data}")
        return None


def fetch_orphan_purchases(account):
    """
    Directly queries (synchronously) per-day purchase action_values
    at the account level. Used as a fallback/supplement to capture
    purchase value on days when campaigns had zero impressions/spend.

    Returns a dict: { (ad_id, date_start) -> purchase_value }
    """
    print(f"\n  🔍 Fetching orphan purchases for {account}…")

    url = f"https://graph.facebook.com/v18.0/{account}/insights"
    params = {
        "level": "ad",
        "time_increment": 1,
        "fields": "ad_id,ad_name,adset_id,adset_name,campaign_id,campaign_name,actions,action_values,spend,impressions,date_start,date_stop",
        "time_range": f'{{"since":"{START_DATE}","until":"{END_DATE}"}}',
        "action_attribution_windows": "7d_click,1d_view",
        "action_report_time": "conversion",
        "access_token": ACCESS_TOKEN,
        "limit": 500,
    }

    orphan_rows = []
    page = 0

    while True:
        response = requests.get(url, params=params)
        data = response.json()

        if "data" not in data:
            print(f"  ⚠️  Orphan fetch error for {account}: {data}")
            break

        for row in data["data"]:
            spend = safe_float(row.get("spend", 0))
            impressions = safe_float(row.get("impressions", 0))

            # We only care about rows where there was NO active delivery
            # but there IS some action_value (purchase value)
            if spend == 0 and impressions == 0:
                purchase_value = _extract_purchase_value_from_actions(row.get("action_values", []))
                if purchase_value > 0:
                    row["_orphan_purchase_value"] = purchase_value
                    row["account_id"] = account
                    orphan_rows.append(row)

        page += 1
        print(f"    Page {page}: {len(data['data'])} rows scanned | Orphan rows found so far: {len(orphan_rows)}")

        if "paging" in data and "next" in data["paging"]:
            url = data["paging"]["next"]
            params = {}
            time.sleep(1)
        else:
            break

    print(f"  ✅ {len(orphan_rows)} orphan rows (purchase value with zero spend/impressions)")
    return orphan_rows


def _extract_purchase_value_from_actions(action_values_list):
    """
    Given a list like [{"action_type": "offsite_conversion.fb_pixel_purchase", "value": "1234.56"}, ...]
    returns the total purchase value across all purchase-related action types.
    """
    purchase_action_types = {
        "offsite_conversion.fb_pixel_purchase",
        "omni_purchase",
        "onsite_web_purchase",
        "onsite_web_app_purchase",
        "web_in_store_purchase",
        "web_app_in_store_purchase",
        "purchase",
    }
    total = 0.0
    for item in (action_values_list or []):
        if item.get("action_type") in purchase_action_types:
            total += safe_float(item.get("value", 0))
    return total


# ─────────────────────────────────────────────
# MAIN FETCH LOOP
# ─────────────────────────────────────────────
all_data = []

# Step 1: Create all jobs
print("=" * 60)
print("STEP 1: Creating async jobs")
print("=" * 60)

jobs = {}
for account in AD_ACCOUNT_IDS:
    print(f"\nCreating job for: {account}")
    job_id = create_async_job(account)
    if job_id:
        jobs[account] = job_id

# Step 2: Poll all jobs until complete
print(f"\n{'=' * 60}")
print("STEP 2: Waiting for jobs to complete")
print("=" * 60)

completed_jobs = {}
for account, job_id in jobs.items():
    print(f"\nPolling job for {account} (id: {job_id})")
    success = poll_job(job_id)
    if success:
        completed_jobs[account] = job_id
        print(f"  ✅ Job complete!")
    else:
        print(f"  ❌ Job failed, skipping {account}")

# Step 3: Fetch results
print(f"\n{'=' * 60}")
print("STEP 3: Fetching results")
print("=" * 60)

for account, job_id in completed_jobs.items():
    print(f"\nFetching results for {account}")
    rows = fetch_job_results(job_id)
    for row in rows:
        row["account_id"] = account
    all_data.extend(rows)
    print(f"  ✅ {len(rows)} rows fetched")

print(f"\n📊 Total rows from active-day jobs: {len(all_data)}")

# Step 4: Fetch orphan purchase rows (zero-spend days with purchase value)
print(f"\n{'=' * 60}")
print("STEP 4: Fetching orphan purchases (inactive days with purchase value)")
print("=" * 60)

orphan_data = []
for account in AD_ACCOUNT_IDS:
    orphan_rows = fetch_orphan_purchases(account)
    orphan_data.extend(orphan_rows)

print(f"\n📊 Total orphan rows: {len(orphan_data)}")

# Merge: build a set of (ad_id, date_start) already in all_data to avoid duplicates
existing_keys = set()
for row in all_data:
    ad_id = row.get("ad_id", "")
    date  = row.get("date_start", "")
    if ad_id and date:
        existing_keys.add((ad_id, date))

new_orphans_added = 0
for row in orphan_data:
    key = (row.get("ad_id", ""), row.get("date_start", ""))
    if key not in existing_keys:
        all_data.append(row)
        existing_keys.add(key)
        new_orphans_added += 1

print(f"✅ {new_orphans_added} new orphan rows merged (non-duplicate)")
print(f"📊 Grand total rows: {len(all_data)}")

if not all_data:
    print("❌ No data returned. Exiting.")
    exit()


# ─────────────────────────────────────────────
# FLATTEN
# ─────────────────────────────────────────────
def flatten(row):
    # 'results' comes back as a list of dicts:
    # [{'indicator': 'profile_visit_view', 'values': [{'value': '539'}]}]
    results_raw = row.get("results")
    if isinstance(results_raw, list) and results_raw:
        try:
            row["result"] = safe_float(results_raw[0]["values"][0]["value"])
            indicator = results_raw[0].get("indicator", "")
            if indicator:
                # Extract the action_type from indicator, e.g., 'omni_purchase' from 'catalog_segment_actions:omni_purchase'
                action_type = indicator.split(":", 1)[-1] if ":" in indicator else indicator
                row["result_indicator"] = indicator  # Optional, to store the indicator
                # For result_value, first try catalog-specific, then standard
                catalog_key = f"{action_type}_catalog_value"
                standard_key = f"{action_type}_value"
                row["result_value"] = safe_float(row.get(catalog_key, row.get(standard_key, 0)))
            else:
                row["result_value"] = 0
        except (KeyError, IndexError):
            row["result"] = 0
            row["result_value"] = 0
    else:
        row["result"] = 0
        row["result_value"] = 0

    # 'cost_per_result' also comes back as a list of dicts
    cpr_raw = row.get("cost_per_result")
    if isinstance(cpr_raw, list) and cpr_raw:
        try:
            row["cost_per_result_value"] = safe_float(cpr_raw[0]["values"][0]["value"])
        except (KeyError, IndexError):
            row["cost_per_result_value"] = 0
    else:
        row["cost_per_result_value"] = 0

    # Flatten actions
    if row.get("actions"):
        for a in row["actions"]:
            col = a["action_type"].replace(".", "_")
            row[col] = a.get("value")

    # Flatten action values
    if row.get("action_values"):
        for a in row["action_values"]:
            col = a["action_type"].replace(".", "_")
            row[col + "_value"] = a.get("value")

    # Flatten catalog_segment_actions (counts)
    if row.get("catalog_segment_actions"):
        for a in row["catalog_segment_actions"]:
            col = a["action_type"].replace(".", "_")
            row[col + "_catalog"] = a.get("value")

    # Flatten catalog_segment_value (values)
    if row.get("catalog_segment_value"):
        for a in row["catalog_segment_value"]:
            col = a["action_type"].replace(".", "_")
            row[col + "_catalog_value"] = a.get("value")

    # Flatten catalog ROAS metrics
    roas_fields = [
        "catalog_segment_value_mobile_purchase_roas",
        "catalog_segment_value_omni_purchase_roas",
        "catalog_segment_value_website_purchase_roas"
    ]
    for field in roas_fields:
        raw = row.get(field)
        if isinstance(raw, list) and raw:
            try:
                row[field.replace("value_", "")] = safe_float(raw[0]["value"])
            except (KeyError, IndexError):
                row[field.replace("value_", "")] = 0

    # Flatten purchase ROAS
    if row.get("purchase_roas"):
        for r in row["purchase_roas"]:
            row["purchase_roas"] = r.get("value")

    # Flatten cost per action
    if row.get("cost_per_action_type"):
        for c in row["cost_per_action_type"]:
            col = c["action_type"].replace(".", "_")
            row["cost_per_" + col] = c.get("value")

    return row


processed = [flatten(r) for r in all_data]

for r in processed:
    r.pop("actions", None)
    r.pop("action_values", None)
    r.pop("cost_per_action_type", None)
    r.pop("results", None)
    r.pop("cost_per_result", None)
    r.pop("catalog_segment_actions", None)
    r.pop("catalog_segment_value", None)
    r.pop("catalog_segment_value_mobile_purchase_roas", None)
    r.pop("catalog_segment_value_omni_purchase_roas", None)
    r.pop("catalog_segment_value_website_purchase_roas", None)

df = pd.DataFrame(processed)
df.columns = [c.replace(".", "_") for c in df.columns]

if df.empty:
    print("❌ DataFrame is empty. Exiting.")
    exit()

# ─────────────────────────────────────────────
# CALCULATE TOTAL PURCHASE VALUE & ROAS
# ─────────────────────────────────────────────
purchase_value_columns = [
    "offsite_conversion_fb_pixel_purchase_value",
    "omni_purchase_value",
    "onsite_web_purchase_value",
    "onsite_web_app_purchase_value",
    "web_in_store_purchase_value",
    "web_app_in_store_purchase_value",
    "purchase_value",
    "omni_purchase_catalog_value",  # Added for catalog
    # Add others if needed, e.g., "mobile_purchase_catalog_value"
]

def calculate_total_purchase_value(row):
    # Prefer the pre-computed _orphan_purchase_value for orphan rows
    # (already extracted from action_values before flattening)
    orphan_val = safe_float(row.get("_orphan_purchase_value", 0))
    if orphan_val > 0:
        return orphan_val

    return sum(
        safe_float(row[col])
        for col in purchase_value_columns
        if col in row and pd.notna(row[col])
    )

df["total_purchase_value"] = df.apply(calculate_total_purchase_value, axis=1)

# Drop internal helper column if present
if "_orphan_purchase_value" in df.columns:
    df.drop(columns=["_orphan_purchase_value"], inplace=True)

def calculate_roas(row):
    spend = safe_float(row.get("spend", 0))
    pv    = safe_float(row.get("total_purchase_value", 0))
    return pv / spend if spend > 0 and pv > 0 else 0

df["calculated_roas"] = df.apply(calculate_roas, axis=1)

# Flag orphan rows for transparency in reporting
df["is_orphan_row"] = (
    df["spend"].apply(safe_float).eq(0) &
    df["impressions"].apply(lambda x: safe_float(x) if pd.notna(x) else 0).eq(0) &
    df["total_purchase_value"].gt(0)
).astype(int)

# ─────────────────────────────────────────────
# DATA QUALITY CHECKS
# ─────────────────────────────────────────────
print("\n" + "=" * 60)
print("DATA QUALITY CHECKS")
print("=" * 60)

result_sum     = df["result"].astype(float).sum()
non_zero_count = (df["result"].astype(float) != 0).sum()

print(f"\n✅ Result column (from native 'results' field):")
print(f"   Non-zero rows : {non_zero_count}")
print(f"   Total results : {result_sum:.0f}")

print(f"\n✅ Result Value (e.g., total revenue from results):")
print(f"   Rows with value : {(df['result_value'] > 0).sum()}")
print(f"   Total           : ₹{df['result_value'].sum():.2f}")

print(f"\n💰 Purchase Value:")
print(f"   Rows with value : {(df['total_purchase_value'] > 0).sum()}")
print(f"   Total           : ₹{df['total_purchase_value'].sum():.2f}")

orphan_pv = df[df["is_orphan_row"] == 1]["total_purchase_value"].sum()
orphan_count = df["is_orphan_row"].sum()
print(f"\n🔍 Orphan Purchase Rows (zero spend/impressions but has purchase value):")
print(f"   Orphan rows     : {orphan_count}")
print(f"   Orphan PV total : ₹{orphan_pv:.2f}")

print(f"\n📈 ROAS:")
roas_df = df[df["calculated_roas"] > 0]
if not roas_df.empty:
    print(f"   Rows with ROAS  : {len(roas_df)}")
    print(f"   Average ROAS    : {roas_df['calculated_roas'].mean():.2f}x")
    print(f"   Max ROAS        : {roas_df['calculated_roas'].max():.2f}x")
else:
    print("   No rows with ROAS > 0")

print(f"\n💵 Spend:")
print(f"   Total : ₹{df['spend'].astype(float).sum():.2f}")

print(f"\n📋 Objectives: {df['objective'].dropna().unique().tolist()}")

print(f"\n📊 Result Breakdown by Objective:")
for obj in df["objective"].dropna().unique():
    obj_df     = df[df["objective"] == obj]
    result_sum = obj_df["result"].astype(float).sum()
    print(f"   {obj}: {result_sum:.0f} results across {len(obj_df)} rows")

print(f"\n🏆 Top 10 Ads by Result (verify against Ads Manager):")
top = (
    df[df["result"].astype(float) > 0]
    .assign(result_num=lambda x: x["result"].astype(float))
    .nlargest(10, "result_num")
    [["ad_name", "campaign_name", "result", "result_value", "cost_per_result_value", "spend", "date_start"]]
)
print(top.to_string(index=False))

print("\n" + "=" * 60)

# ─────────────────────────────────────────────
# UPSERT TO NEON
# ─────────────────────────────────────────────
def add_missing_columns(engine, table_name, df_columns):
    inspector = inspect(engine)
    if not inspector.has_table(table_name):
        return
    existing = {col["name"] for col in inspector.get_columns(table_name)}
    missing  = set(df_columns) - existing
    if missing:
        print(f"\n📊 Adding {len(missing)} missing columns…")
        with engine.begin() as conn:
            for col in missing:
                try:
                    conn.execute(text(f'ALTER TABLE {table_name} ADD COLUMN "{col}" TEXT'))
                    print(f"  ✓ {col}")
                except Exception as e:
                    print(f"  ✗ {col}: {e}")

engine = create_engine(NEON_CONNECTION_STRING)

with engine.connect() as conn:
    table_exists = conn.execute(text("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables WHERE table_name = :table
        )
    """), {"table": TABLE_NAME}).scalar()

if table_exists:
    add_missing_columns(engine, TABLE_NAME, df.columns)
    with engine.begin() as conn:
        conn.execute(
            text(f"DELETE FROM {TABLE_NAME} WHERE date_start >= :start AND date_start <= :end"),
            {"start": START_DATE, "end": END_DATE}
        )
        df.to_sql(TABLE_NAME, conn, if_exists="append", index=False)
    print(f"\n✅ {len(df)} rows upserted into '{TABLE_NAME}'")
else:
    with engine.begin() as conn:
        df.to_sql(TABLE_NAME, conn, if_exists="replace", index=False)
    print(f"\n✅ Table '{TABLE_NAME}' created with {len(df)} rows")

print(f"\n🎉 Done!")
print(f"   'result'                → matches Ads Manager Results column exactly (count)")
print(f"   'result_value'          → value (e.g., revenue) of the results, now including catalog segment values")
print(f"   'cost_per_result_value' → matches Ads Manager Cost per Result column")
print(f"   'total_purchase_value'  → sum of all pixel and catalog purchase values")
print(f"   'calculated_roas'       → total_purchase_value / spend")
print(f"   'is_orphan_row'         → 1 if row has purchase value but zero spend/impressions (inactive campaign day)")
