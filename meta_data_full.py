import os
import requests
import pandas as pd
import time
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text, inspect


# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
ACCESS_TOKEN           = os.environ["ACCESS_TOKEN"]
NEON_CONNECTION_STRING = os.environ["NEON_CONNECTION_STRING"]

AD_ACCOUNT_IDS = [
    "act_1937951709801590",
    "act_2111571239641157",
    "act_935022987639527",
    "act_1447779473228664",
    "act_2073146216601611",
]

END_DATE   = datetime.today().strftime("%Y-%m-%d")
START_DATE = (datetime.today() - timedelta(days=10)).strftime("%Y-%m-%d")

TABLE_NAME  = "meta_ads_summary"
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
    # ── Video metrics ──────────────────────────────────────────────────────
    # These are top-level array fields — NOT part of the `actions` array.
    # Each returns a list of {action_type, value} objects (one per video type).
    # Must be requested explicitly; they are silently absent otherwise.
    "video_play_actions",              # Total video plays (any duration)
    "video_p25_watched_actions",       # Reached 25% of video
    "video_p50_watched_actions",       # Reached 50% of video
    "video_p75_watched_actions",       # Reached 75% of video
    "video_p100_watched_actions",      # Watched to completion (100%)
    "video_thruplay_watched_actions",  # ThruPlay: ≥15s watched, or full video if <15s
    "video_avg_time_watched_actions",  # Average watch time in seconds
    "video_continuous_2_sec_watched_actions",  # 2-second continuous views
]


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────
def safe_float(value):
    try:
        return float(value) if value not in (None, "", False) else 0.0
    except (ValueError, TypeError):
        return 0.0


def extract_video_field(raw):
    """
    Video fields return a list like:
      [{"action_type": "video_view", "value": "1234"}, ...]
    We sum all entries so that if Meta ever returns multiple action_type
    entries (e.g. split by placement), they're collapsed into one number.
    Returns 0.0 if the field is absent or malformed.
    """
    if not isinstance(raw, list) or not raw:
        return 0.0
    total = 0.0
    for entry in raw:
        total += safe_float(entry.get("value", 0))
    return total


# ─────────────────────────────────────────────
# ASYNC JOB: CREATE (Ad-level insights)
# ─────────────────────────────────────────────
def create_async_job(account):
    """
    Creates an async insights job at the ad level with daily breakdown.

    Notes:
    - API version v21.0 (v18.0 is deprecated, returns incomplete results/actions).
    - action_report_time=impression matches Ads Manager default date attribution.
      Using "conversion" shifts numbers to the conversion date — numbers won't
      match Ads Manager unless you also switch the Ads Manager view.
    - action_attribution_windows sent as a proper JSON array.
      Plain "7d_click,1d_view" string is silently ignored by Meta's API.
    """
    url = f"https://graph.facebook.com/{API_VERSION}/{account}/insights"
    params = {
        "level":                      "ad",
        "time_increment":             1,
        "fields":                     ",".join(FIELDS),
        "time_range":                 f'{{"since":"{START_DATE}","until":"{END_DATE}"}}',
        "action_attribution_windows": '["7d_click","1d_view"]',
        "action_report_time":         "impression",
        "access_token":               ACCESS_TOKEN,
        "limit":                      500,
    }
    response = requests.post(url, params=params)

    if not response.content:
        print(f"  ❌ Empty response from API (HTTP {response.status_code})")
        return None

    try:
        data = response.json()
    except requests.exceptions.JSONDecodeError:
        print(f"  ❌ Non-JSON response (HTTP {response.status_code}): {response.text[:200]!r}")
        return None

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
        elapsed = time.time() - start
        if elapsed > timeout_minutes * 60:
            print(f"  ❌ Job {job_id} timed out after {timeout_minutes} minutes")
            return False

        response = requests.get(url, params=params)

        if not response.content:
            print(f"  ⚠️  Empty response (HTTP {response.status_code}) — retrying in {poll_interval}s…")
            time.sleep(poll_interval)
            poll_interval = min(poll_interval + 5, 30)
            continue

        try:
            data = response.json()
        except requests.exceptions.JSONDecodeError:
            print(f"  ⚠️  Non-JSON response (HTTP {response.status_code}): {response.text[:200]!r} — retrying…")
            time.sleep(poll_interval)
            poll_interval = min(poll_interval + 5, 30)
            continue

        if "error" in data:
            err = data["error"]
            print(f"  ❌ API error: {err.get('message', err)}")
            return False

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

        if not response.content:
            print(f"  ❌ Empty response (HTTP {response.status_code})")
            break

        try:
            data = response.json()
        except requests.exceptions.JSONDecodeError:
            print(f"  ❌ Non-JSON response (HTTP {response.status_code}): {response.text[:200]!r}")
            break

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

# ─── TEMPORARY VIDEO DEBUG ───────────────────────────────────────
print("\n🔍 VIDEO DEBUG — checking raw API response")

video_fields_to_check = [
    "video_play_actions",
    "video_p25_watched_actions",
    "video_p50_watched_actions",
    "video_p75_watched_actions",
    "video_p100_watched_actions",
    "video_thruplay_watched_actions",
    "video_avg_time_watched_actions",
    "video_continuous_2_sec_watched_actions",
]

# Check first 5 rows
for i, row in enumerate(all_data[:5]):
    print(f"\n  Row {i} | ad_id: {row.get('ad_id')} | date: {row.get('date_start')}")
    for f in video_fields_to_check:
        val = row.get(f)
        print(f"    {f}: {val}")

# Count how many rows have ANY video data at all
rows_with_video = sum(
    1 for row in all_data
    if any(row.get(f) for f in video_fields_to_check)
)
print(f"\n  Rows with at least one video field present: {rows_with_video} / {len(all_data)}")
# ─────────────────────────────────────────────────────────────────


# ─────────────────────────────────────────────
# FLATTEN
# ─────────────────────────────────────────────
def flatten(row):
    """
    Flattens a raw Meta Insights API row into a flat dict.

    ALL lookup dicts are built FIRST so that result_value extraction
    can reference action_values and catalog_segment_value correctly.
    Video fields are extracted separately since they are top-level
    array fields, not nested inside the `actions` array.
    """

    # ── Step 1: Pre-build all lookup dicts before any extraction ──────────
    av_lookup = {}
    for a in (row.get("action_values") or []):
        key = a.get("action_type", "").replace(".", "_")
        if key:
            av_lookup[key] = safe_float(a.get("value", 0))

    ac_lookup = {}
    for a in (row.get("actions") or []):
        key = a.get("action_type", "").replace(".", "_")
        if key:
            ac_lookup[key] = safe_float(a.get("value", 0))

    csv_lookup = {}
    for a in (row.get("catalog_segment_value") or []):
        key = a.get("action_type", "").replace(".", "_")
        if key:
            csv_lookup[key] = safe_float(a.get("value", 0))

    csa_lookup = {}
    for a in (row.get("catalog_segment_actions") or []):
        key = a.get("action_type", "").replace(".", "_")
        if key:
            csa_lookup[key] = safe_float(a.get("value", 0))

    cpa_lookup = {}
    for c in (row.get("cost_per_action_type") or []):
        key = c.get("action_type", "").replace(".", "_")
        if key:
            cpa_lookup[key] = safe_float(c.get("value", 0))

    # ── Step 2: Extract `results` count + value ───────────────────────────
    results_raw = row.get("results")
    if isinstance(results_raw, list) and results_raw:
        try:
            row["result"] = safe_float(results_raw[0]["values"][0]["value"])
            indicator = results_raw[0].get("indicator", "")
            row["result_indicator"] = indicator

            action_type = (indicator.split(":", 1)[-1] if ":" in indicator else indicator)
            action_key  = action_type.replace(".", "_")

            if "catalog_segment" in indicator:
                row["result_value"] = csv_lookup.get(action_key) or av_lookup.get(action_key) or 0
            else:
                row["result_value"] = av_lookup.get(action_key) or csv_lookup.get(action_key) or 0

        except (KeyError, IndexError, TypeError):
            row["result"]           = 0
            row["result_value"]     = 0
            row["result_indicator"] = ""
    else:
        row["result"]           = 0
        row["result_value"]     = 0
        row["result_indicator"] = ""

    # ── Step 3: cost_per_result ───────────────────────────────────────────
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

    # ── Step 10: Flatten purchase_roas ────────────────────────────────────
    purchase_roas_raw = row.get("purchase_roas")
    row.pop("purchase_roas", None)

    if isinstance(purchase_roas_raw, list):
        for r in purchase_roas_raw:
            at  = r.get("action_type", "").replace(".", "_")
            val = safe_float(r.get("value", 0))
            if at:
                row[f"purchase_roas_{at}"] = val

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

    # ── Step 11: Flatten video metrics ───────────────────────────────────
    # Video fields are separate top-level array fields from Meta's API.
    # They are NOT inside the `actions` array and must be handled separately.
    # extract_video_field() sums all entries in case Meta returns multiple
    # action_type sub-entries (e.g. split by placement type).
    #
    # Column naming convention:
    #   video_plays              → total plays (any duration)
    #   video_views_p25          → 25% completion views
    #   video_views_p50          → 50% completion views
    #   video_views_p75          → 75% completion views
    #   video_views_p100         → 100% completion (watched to end)
    #   video_thruplay_views     → ThruPlay (≥15s, or full if video <15s)
    #   video_avg_watch_time_sec → average watch time in seconds
    #   video_2sec_continuous    → 2-second continuous video views
    row["video_plays"]              = extract_video_field(row.get("video_play_actions"))
    row["video_views_p25"]          = extract_video_field(row.get("video_p25_watched_actions"))
    row["video_views_p50"]          = extract_video_field(row.get("video_p50_watched_actions"))
    row["video_views_p75"]          = extract_video_field(row.get("video_p75_watched_actions"))
    row["video_views_p100"]         = extract_video_field(row.get("video_p100_watched_actions"))
    row["video_thruplay_views"]     = extract_video_field(row.get("video_thruplay_watched_actions"))
    row["video_avg_watch_time_sec"] = extract_video_field(row.get("video_avg_time_watched_actions"))
    row["video_2sec_continuous"]    = extract_video_field(row.get("video_continuous_2_sec_watched_actions"))

    # ── Step 12: Derived video metrics ────────────────────────────────────
    # video_hook_rate: % of people who reached 25% of the video out of all plays.
    # A useful creative quality signal — high hook rate = strong opening.
    plays = safe_float(row.get("video_plays", 0))
    row["video_hook_rate_pct"] = round((row["video_views_p25"] / plays * 100), 2) if plays > 0 else 0.0

    # video_completion_rate: % of plays that reached 100%.
    row["video_completion_rate_pct"] = round((row["video_views_p100"] / plays * 100), 2) if plays > 0 else 0.0

    return row


processed = [flatten(r) for r in all_data]

# Drop raw nested fields — already flattened above
_DROP_RAW = [
    "actions", "action_values", "cost_per_action_type",
    "results", "cost_per_result",
    "catalog_segment_actions", "catalog_segment_value",
    # Drop raw video arrays — values already extracted into flat columns above
    "video_play_actions",
    "video_p25_watched_actions",
    "video_p50_watched_actions",
    "video_p75_watched_actions",
    "video_p100_watched_actions",
    "video_thruplay_watched_actions",
    "video_avg_time_watched_actions",
    "video_continuous_2_sec_watched_actions",
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
# Priority: omni (Advantage+) → pixel (standard website) →
#           onsite (Meta Shop) → catalog (DPA) → generic fallback.
def get_primary_purchase_value(row):
    def v(col):
        return safe_float(row.get(col, 0))

    omni = v("omni_purchase_value")
    if omni > 0:
        return omni

    pixel = v("offsite_conversion_fb_pixel_purchase_value")
    if pixel > 0:
        return pixel

    onsite = v("onsite_web_purchase_value")
    if onsite > 0:
        return onsite

    catalog = v("omni_purchase_catalog_value")
    if catalog > 0:
        return catalog

    return v("purchase_value")


df["total_purchase_value"] = df.apply(get_primary_purchase_value, axis=1)


# ─────────────────────────────────────────────
# ROAS — native field first, calculate as fallback
# ─────────────────────────────────────────────
def calculate_roas(row):
    native = safe_float(row.get("purchase_roas_primary", 0))
    if native > 0:
        return round(native, 4)

    spend = safe_float(row.get("spend", 0))
    pv    = safe_float(row.get("total_purchase_value", 0))
    if spend > 0 and pv > 0:
        return round(pv / spend, 4)

    return 0.0


df["calculated_roas"] = df.apply(calculate_roas, axis=1)


# ─────────────────────────────────────────────
# ROAS DIVERGENCE FLAG
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

result_col     = df["result"].astype(float)
result_sum     = result_col.sum()
non_zero_count = (result_col != 0).sum()

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
    print(f"   → Check objective and conversion_location for those campaigns.")

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

# ── Video metrics summary ──────────────────────────────────────────────────
print(f"\n🎬 Video Metrics Summary:")
video_cols = [
    "video_plays", "video_views_p25", "video_views_p50",
    "video_views_p75", "video_views_p100", "video_thruplay_views",
    "video_2sec_continuous",
]
for col in video_cols:
    if col in df.columns:
        total    = df[col].apply(safe_float).sum()
        nonzero  = (df[col].apply(safe_float) > 0).sum()
        print(f"   {col:<30} total: {total:>12,.0f}   rows with data: {nonzero}")

if "video_avg_watch_time_sec" in df.columns:
    avg_watch = df[df["video_avg_watch_time_sec"].apply(safe_float) > 0]["video_avg_watch_time_sec"].apply(safe_float).mean()
    print(f"   {'video_avg_watch_time_sec':<30} avg across rows with data: {avg_watch:.1f}s")

if "video_hook_rate_pct" in df.columns:
    avg_hook = df[df["video_hook_rate_pct"] > 0]["video_hook_rate_pct"].mean()
    print(f"   {'video_hook_rate_pct':<30} avg (plays→25% reached): {avg_hook:.1f}%")

if "video_completion_rate_pct" in df.columns:
    avg_comp = df[df["video_completion_rate_pct"] > 0]["video_completion_rate_pct"].mean()
    print(f"   {'video_completion_rate_pct':<30} avg (plays→100% watched): {avg_comp:.1f}%")

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

print(f"\n🎬 Top 10 Ads by Video Plays:")
if "video_plays" in df.columns:
    top_video = (
        df[df["video_plays"].apply(safe_float) > 0]
        .assign(plays_num=lambda x: x["video_plays"].apply(safe_float))
        .nlargest(10, "plays_num")
        [[
            "ad_name", "campaign_name",
            "video_plays", "video_views_p25", "video_views_p50",
            "video_views_p75", "video_views_p100",
            "video_thruplay_views",
            "video_hook_rate_pct", "video_completion_rate_pct",
            "spend", "date_start",
        ]]
    )
    print(top_video.to_string(index=False))
else:
    print("   No video play data returned (non-video campaign).")

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
print(f"   'result'                    → Ads Manager Results column (native, exact match)")
print(f"   'result_indicator'          → The action type driving that result count")
print(f"   'result_value'              → Revenue/value of the result action")
print(f"   'cost_per_result_value'     → Ads Manager Cost per Result (native, exact match)")
print(f"   'total_purchase_value'      → Purchase value, single non-overlapping metric")
print(f"   'purchase_roas_primary'     → Meta's native ROAS (identical to Ads Manager)")
print(f"   'calculated_roas'           → Native ROAS if available, else pv/spend")
print(f"   'roas_divergence_flag'      → 1 if native vs calculated ROAS differ > 5%")
print(f"   'is_zero_spend_conversion'  → 1 if conversion attributed on a zero-spend day")
print(f"   --- Video ---")
print(f"   'video_plays'               → Total video plays (any duration)")
print(f"   'video_views_p25'           → Views reaching 25% of video")
print(f"   'video_views_p50'           → Views reaching 50% of video")
print(f"   'video_views_p75'           → Views reaching 75% of video")
print(f"   'video_views_p100'          → Views watching to completion (100%)")
print(f"   'video_thruplay_views'      → ThruPlay views (≥15s or full video if <15s)")
print(f"   'video_avg_watch_time_sec'  → Average watch time in seconds")
print(f"   'video_2sec_continuous'     → 2-second continuous video views")
print(f"   'video_hook_rate_pct'       → % of plays that reached 25% (creative hook signal)")
print(f"   'video_completion_rate_pct' → % of plays that reached 100%")
