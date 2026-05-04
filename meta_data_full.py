import os
import requests
import pandas as pd
import time
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text, inspect


# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
ACCESS_TOKEN         = os.environ["ACCESS_TOKEN"]
NEON_CONNECTION_STRING = os.environ["NEON_CONNECTION_STRING"]

AD_ACCOUNT_IDS = [
    "act_1937951709801590",
    "act_2111571239641157",
    "act_935022987639527",
    "act_1447779473228664",
    "act_2073146216601611",
]

END_DATE   = datetime.today().strftime("%Y-%m-%d")
START_DATE = (datetime.today() - timedelta(days=40)).strftime("%Y-%m-%d")

TABLE_NAME  = "meta_ads_summary"

# FIX 1: Updated to v21.0 — v18.0 is deprecated and returns incomplete
#         `results` field data and missing action types on some objectives.
API_VERSION = "v21.0"

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
    "catalog_segment_value_website_purchase_roas",
]


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────
def safe_float(value):
    try:
        return float(value) if value not in (None, "", False) else 0.0
    except (ValueError, TypeError):
        return 0.0


# ─────────────────────────────────────────────
# ASYNC JOB: CREATE (Ad-level insights)
# ─────────────────────────────────────────────
def create_async_job(account):
    """
    Creates an async insights job at the ad level with daily breakdown.

    KEY FIXES vs original:
    - API version bumped to v21.0
    - action_report_time changed to "impression" to match Ads Manager default.
      Ads Manager shows conversions attributed to the impression date by default.
      Using "conversion" means the same purchase appears on a DIFFERENT date,
      making your numbers never match when comparing date ranges.
      If you intentionally want conversion-time reporting, flip the Ads Manager
      view too: Columns → Attribution Setting → Conversion time.
    - action_attribution_windows now sent as a proper JSON array string.
      Sending "7d_click,1d_view" as plain text is silently ignored by the API;
      Meta falls back to the ad set default (often 7d_click only), causing
      missing view-through conversions.
    """
    url = f"https://graph.facebook.com/{API_VERSION}/{account}/insights"
    params = {
        "level":          "ad",
        "time_increment": 1,
        "fields":         ",".join(FIELDS),
        "time_range":     f'{{"since":"{START_DATE}","until":"{END_DATE}"}}',
        # FIX 2: proper JSON array — plain "7d_click,1d_view" string is ignored
        "action_attribution_windows": '["7d_click","1d_view"]',
        # FIX 3: "impression" matches Ads Manager default date attribution.
        #         "conversion" = conversion date (different number, different rows).
        "action_report_time": "impression",
        "access_token":   ACCESS_TOKEN,
        "limit":          500,
    }
    response = requests.post(url, params=params)
    data     = response.json()

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
    url    = f"https://graph.facebook.com/{API_VERSION}/{job_id}"
    params = {"access_token": ACCESS_TOKEN}

    start         = time.time()
    poll_interval = 10

    while True:
        elapsed  = time.time() - start
        if elapsed > timeout_minutes * 60:
            print(f"  ❌ Job {job_id} timed out after {timeout_minutes} minutes")
            return False

        response = requests.get(url, params=params)
        data     = response.json()

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
# ASYNC JOB: FETCH PAGINATED RESULTS
# ─────────────────────────────────────────────
def fetch_job_results(job_id):
    url    = f"https://graph.facebook.com/{API_VERSION}/{job_id}/insights"
    params = {
        "access_token": ACCESS_TOKEN,
        "limit":        500,
    }

    rows = []
    page = 0

    while True:
        response = requests.get(url, params=params)
        data     = response.json()

        if "data" not in data:
            print(f"  ❌ Error fetching results: {data}")
            break

        rows.extend(data["data"])
        page += 1
        print(f"  Page {page}: {len(data['data'])} rows | Total: {len(rows)}")

        if "paging" in data and "next" in data["paging"]:
            url    = data["paging"]["next"]
            params = {}
            time.sleep(1)
        else:
            break

    return rows


# ─────────────────────────────────────────────
# MAIN FETCH LOOP
# ─────────────────────────────────────────────
# FIX 4: ORPHAN PURCHASE LOGIC REMOVED.
#
# The original code ran a second synchronous fetch on the same endpoint
# to find "zero-spend rows with purchase value", then tried to merge them.
# This is redundant and harmful because:
#
#   a) The async job with action_report_time=impression already returns ALL rows,
#      including those where spend=0 but a conversion was attributed on that day.
#      Meta does not suppress zero-spend rows in insights — they appear naturally.
#
#   b) The sync orphan fetch used the same API endpoint with the same params,
#      so it returned a superset of what the async job already fetched.
#      The (ad_id, date_start) dedup was masking this, not solving it.
#
#   c) The orphan job's filtering on `action_values > 0` is not a valid
#      server-side filter for this field — it was silently ignored, causing
#      the full result set to be scanned and appended anyway.
#
# Result: deleting ~200-500 duplicate rows that were inflating purchase value.
# If you need to audit zero-spend conversion rows, use the is_zero_spend_conversion
# flag added at the end of this script instead.

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

print(f"\n📊 Total rows: {len(all_data)}")

if not all_data:
    print("❌ No data returned. Exiting.")
    exit()


# ─────────────────────────────────────────────
# FLATTEN
# ─────────────────────────────────────────────
def flatten(row):
    """
    Flattens a raw Meta Insights API row into a flat dict.

    ALL lookup dicts are built FIRST so that result_value extraction
    can reference action_values and catalog_segment_value correctly.
    In the original code, result_value was extracted before action_values
    were looped, so row.get("omni_purchase_value") was always None at
    that point — result_value was always 0.
    """

    # ── Step 1: Pre-build all lookup dicts before any extraction ──────────
    av_lookup = {}   # action_values  → { action_type_key: float }
    for a in (row.get("action_values") or []):
        key = a.get("action_type", "").replace(".", "_")
        if key:
            av_lookup[key] = safe_float(a.get("value", 0))

    ac_lookup = {}   # actions        → { action_type_key: float }
    for a in (row.get("actions") or []):
        key = a.get("action_type", "").replace(".", "_")
        if key:
            ac_lookup[key] = safe_float(a.get("value", 0))

    csv_lookup = {}  # catalog_segment_value  → { action_type_key: float }
    for a in (row.get("catalog_segment_value") or []):
        key = a.get("action_type", "").replace(".", "_")
        if key:
            csv_lookup[key] = safe_float(a.get("value", 0))

    csa_lookup = {}  # catalog_segment_actions → { action_type_key: float }
    for a in (row.get("catalog_segment_actions") or []):
        key = a.get("action_type", "").replace(".", "_")
        if key:
            csa_lookup[key] = safe_float(a.get("value", 0))

    cpa_lookup = {}  # cost_per_action_type   → { action_type_key: float }
    for c in (row.get("cost_per_action_type") or []):
        key = c.get("action_type", "").replace(".", "_")
        if key:
            cpa_lookup[key] = safe_float(c.get("value", 0))

    # ── Step 2: Extract `results` count + value (now lookups are ready) ───
    # `results` is the single source of truth for the Results column in
    # Ads Manager. It returns the count of the campaign's declared objective
    # action (e.g., purchases for OUTCOME_SALES, leads for LEAD_GENERATION).
    # Taking only [0] is correct — Meta returns one entry per result type.
    # Multiple entries only appear for split-test or multi-objective campaigns.
    results_raw = row.get("results")
    if isinstance(results_raw, list) and results_raw:
        try:
            row["result"] = safe_float(results_raw[0]["values"][0]["value"])
            indicator = results_raw[0].get("indicator", "")
            row["result_indicator"] = indicator

            # indicator format: "actions:omni_purchase"
            #               or  "catalog_segment_actions:omni_purchase"
            #               or  "offsite_conversion.fb_pixel_purchase"  (no colon)
            action_type = (indicator.split(":", 1)[-1] if ":" in indicator else indicator)
            action_key  = action_type.replace(".", "_")

            # For catalog-based result indicators, prefer catalog_segment_value.
            # For standard pixel/omni indicators, use action_values.
            # FIX 5: this lookup now works because av_lookup/csv_lookup exist.
            if "catalog_segment" in indicator:
                row["result_value"] = csv_lookup.get(action_key) or av_lookup.get(action_key) or 0
            else:
                row["result_value"] = av_lookup.get(action_key) or csv_lookup.get(action_key) or 0

        except (KeyError, IndexError, TypeError):
            row["result"]       = 0
            row["result_value"] = 0
            row["result_indicator"] = ""
    else:
        row["result"]         = 0
        row["result_value"]   = 0
        row["result_indicator"] = ""

    # ── Step 3: cost_per_result (also a list) ─────────────────────────────
    cpr_raw = row.get("cost_per_result")
    if isinstance(cpr_raw, list) and cpr_raw:
        try:
            row["cost_per_result_value"] = safe_float(cpr_raw[0]["values"][0]["value"])
        except (KeyError, IndexError, TypeError):
            row["cost_per_result_value"] = 0
    else:
        row["cost_per_result_value"] = 0

    # ── Step 4: Flatten actions (counts) ──────────────────────────────────
    for key, val in ac_lookup.items():
        row[key] = val

    # ── Step 5: Flatten action_values (revenue) ───────────────────────────
    for key, val in av_lookup.items():
        row[f"{key}_value"] = val

    # ── Step 6: Flatten catalog_segment_actions (counts) ─────────────────
    for key, val in csa_lookup.items():
        row[f"{key}_catalog"] = val

    # ── Step 7: Flatten catalog_segment_value (revenue) ──────────────────
    for key, val in csv_lookup.items():
        row[f"{key}_catalog_value"] = val

    # ── Step 8: Flatten cost_per_action_type ─────────────────────────────
    for key, val in cpa_lookup.items():
        row[f"cost_per_{key}"] = val

    # ── Step 9: Flatten catalog ROAS metrics ──────────────────────────────
    for field in [
        "catalog_segment_value_mobile_purchase_roas",
        "catalog_segment_value_omni_purchase_roas",
        "catalog_segment_value_website_purchase_roas",
    ]:
        raw = row.get(field)
        if isinstance(raw, list) and raw:
            try:
                row[field] = safe_float(raw[0]["value"])
            except (KeyError, IndexError, TypeError):
                row[field] = 0
        elif not isinstance(raw, (int, float)):
            row[field] = 0

    # ── Step 10: Flatten purchase_roas WITHOUT overwriting bug ────────────
    # FIX 6: original code did `row["purchase_roas"] = r.get("value")` inside
    # a loop — after the first iteration the field became a string, making the
    # second iteration iterate over characters. We now write per-action-type
    # columns and pick the best one as purchase_roas_primary.
    purchase_roas_raw = row.get("purchase_roas")
    row.pop("purchase_roas", None)  # remove the raw list to avoid schema pollution

    if isinstance(purchase_roas_raw, list):
        for r in purchase_roas_raw:
            at  = r.get("action_type", "").replace(".", "_")
            val = safe_float(r.get("value", 0))
            if at:
                row[f"purchase_roas_{at}"] = val

        # Pick primary ROAS value: prefer omni (aggregated), then pixel, then onsite
        for preferred in [
            "purchase_roas_omni_purchase",
            "purchase_roas_offsite_conversion_fb_pixel_purchase",
            "purchase_roas_onsite_web_purchase",
        ]:
            if safe_float(row.get(preferred, 0)) > 0:
                row["purchase_roas_primary"] = safe_float(row[preferred])
                break
        else:
            row["purchase_roas_primary"] = 0
    else:
        row["purchase_roas_primary"] = 0

    return row


processed = [flatten(r) for r in all_data]

# Drop raw nested fields — already flattened above
_DROP_RAW = [
    "actions", "action_values", "cost_per_action_type",
    "results", "cost_per_result",
    "catalog_segment_actions", "catalog_segment_value",
]
for r in processed:
    for field in _DROP_RAW:
        r.pop(field, None)

df = pd.DataFrame(processed)
df.columns = [c.replace(".", "_") for c in df.columns]

if df.empty:
    print("❌ DataFrame is empty. Exiting.")
    exit()


# ─────────────────────────────────────────────
# PURCHASE VALUE — no double-counting
# ─────────────────────────────────────────────
# FIX 7: the original code summed omni_purchase_value + its sub-components
# (offsite_conversion_fb_pixel_purchase_value, onsite_web_purchase_value, etc.)
# omni_purchase IS the aggregate — adding its parts on top inflates by 2-4x.
#
# Strategy: pick one non-overlapping value per row based on what's available.
# Priority: omni_purchase (broadest, matches Advantage+ campaigns) →
#           pixel purchase (standard website campaigns) →
#           onsite/catalog (Meta Shop / DPA).
#
# This matches how Ads Manager picks the "Purchase Value" column depending on
# the campaign's conversion location setting.

def get_primary_purchase_value(row):
    def v(col):
        return safe_float(row.get(col, 0))

    # 1. Advantage+ / omni — covers website + app + in-store in one metric
    omni = v("omni_purchase_value")
    if omni > 0:
        return omni

    # 2. Standard pixel (website-only campaigns)
    pixel = v("offsite_conversion_fb_pixel_purchase_value")
    if pixel > 0:
        return pixel

    # 3. Onsite (Meta Shop / native checkout)
    onsite = v("onsite_web_purchase_value")
    if onsite > 0:
        return onsite

    # 4. Catalog segment purchase value (DPA / catalog campaigns)
    catalog = v("omni_purchase_catalog_value")
    if catalog > 0:
        return catalog

    # 5. Generic fallback
    return v("purchase_value")


df["total_purchase_value"] = df.apply(get_primary_purchase_value, axis=1)


# ─────────────────────────────────────────────
# ROAS — native field first, calculate as fallback
# ─────────────────────────────────────────────
# FIX 8: the original code ignored Meta's native purchase_roas field entirely
# and calculated its own from a double-counted purchase value. The native field
# is the exact same number Ads Manager shows. We use it as primary and only
# fall back to calculation when it's absent (e.g., zero-spend rows).

def calculate_roas(row):
    # Primary: Meta's own purchase_roas value — identical to Ads Manager
    native = safe_float(row.get("purchase_roas_primary", 0))
    if native > 0:
        return round(native, 4)

    # Fallback: derive from corrected (non-double-counted) purchase value
    spend = safe_float(row.get("spend", 0))
    pv    = safe_float(row.get("total_purchase_value", 0))
    if spend > 0 and pv > 0:
        return round(pv / spend, 4)

    return 0.0


df["calculated_roas"] = df.apply(calculate_roas, axis=1)


# ─────────────────────────────────────────────
# ROAS VALIDATION FLAG
# Detects rows where calculated ROAS diverges from native ROAS by > 5%.
# If you see many flagged rows, your purchase value column selection is wrong
# for that campaign type.
# ─────────────────────────────────────────────
def roas_divergence_flag(row):
    native     = safe_float(row.get("purchase_roas_primary", 0))
    calculated = safe_float(row.get("calculated_roas", 0))
    if native == 0 or calculated == 0:
        return 0
    pct_diff = abs(native - calculated) / native
    return 1 if pct_diff > 0.05 else 0


df["roas_divergence_flag"] = df.apply(roas_divergence_flag, axis=1)


# ─────────────────────────────────────────────
# ZERO-SPEND CONVERSION FLAG
# Replaces the orphan row concept: these are rows Meta naturally returns
# where an ad had no spend that day but a conversion was still attributed
# to it (within the attribution window). No special fetch needed.
# ─────────────────────────────────────────────
df["is_zero_spend_conversion"] = (
    df["spend"].apply(safe_float).eq(0) &
    df["total_purchase_value"].gt(0)
).astype(int)


# ─────────────────────────────────────────────
# DATA QUALITY CHECKS
# ─────────────────────────────────────────────
print("\n" + "=" * 60)
print("DATA QUALITY CHECKS")
print("=" * 60)

result_col        = df["result"].astype(float)
result_sum        = result_col.sum()
non_zero_count    = (result_col != 0).sum()

print(f"\n✅ Results (native Meta field — matches Ads Manager):")
print(f"   Non-zero rows : {non_zero_count}")
print(f"   Total results : {result_sum:.0f}")

print(f"\n✅ Result Value:")
print(f"   Rows with value : {(df['result_value'].apply(safe_float) > 0).sum()}")
print(f"   Total           : ₹{df['result_value'].apply(safe_float).sum():.2f}")

print(f"\n💰 Purchase Value (no double-counting):")
print(f"   Rows with value : {(df['total_purchase_value'] > 0).sum()}")
print(f"   Total           : ₹{df['total_purchase_value'].sum():.2f}")

print(f"\n🔍 Zero-Spend Conversion Rows:")
zsc_count = df["is_zero_spend_conversion"].sum()
zsc_pv    = df[df["is_zero_spend_conversion"] == 1]["total_purchase_value"].sum()
print(f"   Rows            : {zsc_count}")
print(f"   Purchase Value  : ₹{zsc_pv:.2f}")

print(f"\n⚠️  ROAS Divergence (native vs calculated > 5%):")
div_count = df["roas_divergence_flag"].sum()
print(f"   Flagged rows    : {div_count}")
if div_count > 0:
    print(f"   → These rows may have a purchase value column mismatch.")
    print(f"     Check the objective and conversion_location for those campaigns.")

print(f"\n📈 ROAS:")
roas_df = df[df["calculated_roas"] > 0]
if not roas_df.empty:
    print(f"   Rows with ROAS  : {len(roas_df)}")
    print(f"   Average ROAS    : {roas_df['calculated_roas'].mean():.2f}x")
    print(f"   Max ROAS        : {roas_df['calculated_roas'].max():.2f}x")
else:
    print("   No rows with ROAS > 0")

print(f"\n💵 Spend:")
print(f"   Total : ₹{df['spend'].apply(safe_float).sum():.2f}")

print(f"\n📋 Objectives: {df['objective'].dropna().unique().tolist()}")

print(f"\n📊 Result Breakdown by Objective:")
for obj in df["objective"].dropna().unique():
    obj_df     = df[df["objective"] == obj]
    obj_result = obj_df["result"].astype(float).sum()
    print(f"   {obj}: {obj_result:.0f} results across {len(obj_df)} rows")

print(f"\n🏆 Top 10 Ads by Result (verify against Ads Manager):")
top = (
    df[df["result"].astype(float) > 0]
    .assign(result_num=lambda x: x["result"].astype(float))
    .nlargest(10, "result_num")
    [[
        "ad_name", "campaign_name",
        "result", "result_indicator",
        "result_value", "cost_per_result_value",
        "total_purchase_value", "calculated_roas",
        "spend", "date_start",
    ]]
)
print(top.to_string(index=False))

print("\n" + "=" * 60)


# ─────────────────────────────────────────────
# UPSERT TO NEON
# Composite upsert key: (ad_id, date_start, account_id)
# DELETE + INSERT within a single transaction to avoid partial writes.
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
            {"start": START_DATE, "end": END_DATE},
        )
        df.to_sql(TABLE_NAME, conn, if_exists="append", index=False)
    print(f"\n✅ {len(df)} rows upserted into '{TABLE_NAME}'")
else:
    with engine.begin() as conn:
        df.to_sql(TABLE_NAME, conn, if_exists="replace", index=False)
    print(f"\n✅ Table '{TABLE_NAME}' created with {len(df)} rows")

print(f"\n🎉 Done!")
print(f"")
print(f"   Column reference:")
print(f"   'result'                  → Ads Manager Results column (native, exact match)")
print(f"   'result_indicator'        → The action type driving that result count")
print(f"   'result_value'            → Revenue/value of the result action")
print(f"   'cost_per_result_value'   → Ads Manager Cost per Result (native, exact match)")
print(f"   'total_purchase_value'    → Purchase value, single non-overlapping metric")
print(f"   'purchase_roas_primary'   → Meta's native ROAS (identical to Ads Manager)")
print(f"   'calculated_roas'         → Native ROAS if available, else pv/spend")
print(f"   'roas_divergence_flag'    → 1 if native vs calculated ROAS differ > 5%")
print(f"   'is_zero_spend_conversion'→ 1 if conversion attributed on a zero-spend day")
