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
START_DATE = (datetime.today() - timedelta(days=7)).strftime("%Y-%m-%d")

TABLE_NAME = "meta_ads_region"

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
# ASYNC JOB: CREATE
# ─────────────────────────────────────────────
def create_async_job(account):
    url = f"https://graph.facebook.com/v18.0/{account}/insights"
    params = {
        "level": "ad",
        "time_increment": 1,
        "breakdowns": "region",              # ← Region breakdown
        "fields": ",".join(FIELDS),
        "time_range": f'{{"since":"{START_DATE}","until":"{END_DATE}"}}',
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
        poll_interval = min(poll_interval + 5, 30)


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
# MAIN FETCH LOOP
# ─────────────────────────────────────────────
all_data = []

print("=" * 60)
print("STEP 1: Creating async jobs")
print("=" * 60)

jobs = {}
for account in AD_ACCOUNT_IDS:
    print(f"\nCreating job for: {account}")
    job_id = create_async_job(account)
    if job_id:
        jobs[account] = job_id

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

print(f"\n📊 Total rows fetched: {len(all_data)}")

if not all_data:
    print("❌ No data returned. Exiting.")
    exit()


# ─────────────────────────────────────────────
# FLATTEN
# ─────────────────────────────────────────────
def flatten(row):
    results_raw = row.get("results")
    if isinstance(results_raw, list) and results_raw:
        try:
            row["result"] = safe_float(results_raw[0]["values"][0]["value"])
            indicator = results_raw[0].get("indicator", "")
            if indicator:
                action_type = indicator.split(":", 1)[-1] if ":" in indicator else indicator
                row["result_indicator"] = indicator
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

    cpr_raw = row.get("cost_per_result")
    if isinstance(cpr_raw, list) and cpr_raw:
        try:
            row["cost_per_result_value"] = safe_float(cpr_raw[0]["values"][0]["value"])
        except (KeyError, IndexError):
            row["cost_per_result_value"] = 0
    else:
        row["cost_per_result_value"] = 0

    if row.get("actions"):
        for a in row["actions"]:
            col = a["action_type"].replace(".", "_")
            row[col] = a.get("value")

    if row.get("action_values"):
        for a in row["action_values"]:
            col = a["action_type"].replace(".", "_")
            row[col + "_value"] = a.get("value")

    if row.get("catalog_segment_actions"):
        for a in row["catalog_segment_actions"]:
            col = a["action_type"].replace(".", "_")
            row[col + "_catalog"] = a.get("value")

    if row.get("catalog_segment_value"):
        for a in row["catalog_segment_value"]:
            col = a["action_type"].replace(".", "_")
            row[col + "_catalog_value"] = a.get("value")

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

    if row.get("purchase_roas"):
        for r in row["purchase_roas"]:
            row["purchase_roas"] = r.get("value")

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
    "omni_purchase_catalog_value",
]

def calculate_total_purchase_value(row):
    return sum(
        safe_float(row[col])
        for col in purchase_value_columns
        if col in row and pd.notna(row[col])
    )

df["total_purchase_value"] = df.apply(calculate_total_purchase_value, axis=1)

def calculate_roas(row):
    spend = safe_float(row.get("spend", 0))
    pv    = safe_float(row.get("total_purchase_value", 0))
    return pv / spend if spend > 0 and pv > 0 else 0

df["calculated_roas"] = df.apply(calculate_roas, axis=1)

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

print(f"\n✅ Result Value:")
print(f"   Rows with value : {(df['result_value'] > 0).sum()}")
print(f"   Total           : ₹{df['result_value'].sum():.2f}")

print(f"\n💰 Purchase Value:")
print(f"   Rows with value : {(df['total_purchase_value'] > 0).sum()}")
print(f"   Total           : ₹{df['total_purchase_value'].sum():.2f}")

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

print(f"\n📋 Objectives: {df['objective'].unique().tolist()}")

# Region specific breakdown
print(f"\n🗺️  Top 20 Regions by Spend:")
if "region" in df.columns:
    region_summary = (
        df.groupby("region")["spend"]
        .apply(lambda x: x.astype(float).sum())
        .reset_index()
        .sort_values("spend", ascending=False)
        .head(20)
    )
    print(region_summary.to_string(index=False))

print(f"\n📊 Result Breakdown by Objective:")
for obj in df["objective"].unique():
    obj_df     = df[df["objective"] == obj]
    result_sum = obj_df["result"].astype(float).sum()
    print(f"   {obj}: {result_sum:.0f} results across {len(obj_df)} rows")

print(f"\n🏆 Top 10 Ads by Result:")
top = (
    df[df["result"].astype(float) > 0]
    .assign(result_num=lambda x: x["result"].astype(float))
    .nlargest(10, "result_num")
    [["ad_name", "campaign_name", "region", "result", "result_value", "cost_per_result_value", "spend", "date_start"]]
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

print(f"\n🎉 Done! (Region Breakdown)")
print(f"   Breakdown column  : 'region'")
print(f"   'result'                → matches Ads Manager Results column exactly (count)")
print(f"   'result_value'          → value (e.g., revenue) of the results")
print(f"   'cost_per_result_value' → matches Ads Manager Cost per Result column")
print(f"   'total_purchase_value'  → sum of all pixel and catalog purchase values")
print(f"   'calculated_roas'       → total_purchase_value / spend")
