import os
import requests
import pandas as pd
import time
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
ACCESS_TOKEN = os.environ["ACCESS_TOKEN"]

NEON_CONNECTION_STRING = os.environ["NEON_CONNECTION_STRING"]

AD_ACCOUNT_IDS = [
    "act_1937951709801590",
    "act_2111571239641157",
    "act_935022987639527",
    "act_1447779473228664"
]

END_DATE   = datetime.today().strftime("%Y-%m-%d")
START_DATE = "2026-01-01"

TABLE_NAME = "meta_ads_region"

FIELDS = [
    "campaign_id", "campaign_name",
    "adset_id", "adset_name",
    "ad_id", "ad_name",
    "objective",
    "impressions", "reach", "frequency",
    "clicks", "ctr", "cpc", "cpm", "spend",
    "actions", "action_values",
    "purchase_roas",
    "date_start", "date_stop"
]

# ─────────────────────────────────────────────
# FETCH
# ─────────────────────────────────────────────
all_data = []

for account in AD_ACCOUNT_IDS:
    print(f"Fetching Region for {account} | {START_DATE} → {END_DATE}")

    base_url = f"https://graph.facebook.com/v18.0/{account}/insights"
    params = {
        "level": "ad",
        "time_increment": 1,
        "fields": ",".join(FIELDS),
        "breakdowns": "region",
        "time_range": f"{{'since':'{START_DATE}','until':'{END_DATE}'}}",
        "access_token": ACCESS_TOKEN,
        "limit": 500
    }

    while True:
        response = requests.get(base_url, params=params)
        data = response.json()

        if "data" not in data:
            print("Error:", data)
            break

        for row in data["data"]:
            row["account_id"] = account
            all_data.append(row)

        if "paging" in data and "next" in data["paging"]:
            base_url = data["paging"]["next"]
            params = {}
            time.sleep(1)
        else:
            break

print(f"\n📊 Total rows fetched: {len(all_data)}")

# ─────────────────────────────────────────────
# FLATTEN
# ─────────────────────────────────────────────
# Enhanced objective mapping
OBJECTIVE_TO_RESULT_ACTION = {
    "LINK_CLICKS":          "link_click",
    "OUTCOME_TRAFFIC":      "link_click",
    "OUTCOME_ENGAGEMENT":   "post_engagement",
    "OUTCOME_AWARENESS":    "reach",
    "OUTCOME_LEADS":        "lead",
    "OUTCOME_SALES":        "offsite_conversion.fb_pixel_purchase",
    "OUTCOME_APP_PROMOTION":"app_install",
    "POST_ENGAGEMENT":      "post_engagement",
    "PAGE_LIKES":           "like",
    "EVENT_RESPONSES":      "event_response",
    "MESSAGES":             "onsite_conversion.messaging_conversation_started_7d",
    "CONVERSIONS":          "offsite_conversion.fb_pixel_purchase",
    "CATALOG_SALES":        "offsite_conversion.fb_pixel_purchase",
    "STORE_VISITS":         "omni_store_visit",
    "REACH":                "reach",
    "BRAND_AWARENESS":      "reach",
    "VIDEO_VIEWS":          "video_view",
}

def extract_result(row):
    """Return the primary result value based on the campaign objective."""
    objective = row.get("objective", "")
    target_action = OBJECTIVE_TO_RESULT_ACTION.get(objective)
    actions = row.get("actions") or []

    # Try to find the exact target action
    if target_action:
        for a in actions:
            if a["action_type"] == target_action:
                value = a.get("value")
                try:
                    return float(value) if value else 0
                except (ValueError, TypeError):
                    return 0
        
        # Fallback: for sales objectives, try omni_purchase
        if objective in ["OUTCOME_SALES", "CONVERSIONS", "CATALOG_SALES"]:
            for a in actions:
                if a["action_type"] == "omni_purchase":
                    value = a.get("value")
                    try:
                        return float(value) if value else 0
                    except (ValueError, TypeError):
                        return 0

    # Special case: if objective is OUTCOME_AWARENESS, use reach from main metrics
    if objective in ["OUTCOME_AWARENESS", "REACH", "BRAND_AWARENESS"]:
        reach = row.get("reach")
        try:
            return float(reach) if reach else 0
        except (ValueError, TypeError):
            return 0

    # Generic fallback: return the first action value if objective unknown
    if actions:
        value = actions[0].get("value")
        try:
            return float(value) if value else 0
        except (ValueError, TypeError):
            return 0
    
    return 0  # Default to 0 if no result found

def flatten(row):
    # Extract the result first
    row["result"] = extract_result(row)
    
    # Flatten actions
    if row.get("actions"):
        for a in row["actions"]:
            row[a["action_type"]] = a.get("value")
    
    # Flatten action values
    if row.get("action_values"):
        for a in row["action_values"]:
            row[a["action_type"] + "_value"] = a.get("value")
    
    # Flatten purchase ROAS
    if row.get("purchase_roas"):
        for r in row["purchase_roas"]:
            row["purchase_roas"] = r.get("value")
    
    return row

processed = [flatten(r) for r in all_data]

# Clean up nested structures
for r in processed:
    r.pop("actions", None)
    r.pop("action_values", None)

df = pd.DataFrame(processed)

if df.empty:
    print("❌ No data returned. Exiting.")
    exit()

# Clean column names
df.columns = [c.replace(".", "_") for c in df.columns]

# Verify result column exists and show stats
if 'result' in df.columns:
    print(f"✅ Result column created successfully")
    print(f"   - Non-zero results: {(df['result'] != 0).sum()}")
    print(f"   - Total result value: {df['result'].astype(float).sum():.2f}")
else:
    print("⚠️  Warning: Result column not found in dataframe")

# Show unique objectives
print(f"\n📋 Unique objectives found: {df['objective'].unique().tolist()}")

# ─────────────────────────────────────────────
# UPSERT TO NEON
# ─────────────────────────────────────────────
from sqlalchemy import inspect

def add_missing_columns(engine, table_name, df_columns):
    """Add any missing columns to the table before inserting."""
    inspector = inspect(engine)
    if not inspector.has_table(table_name):
        return  # Table doesn't exist yet, will be created
    
    existing_cols = {col['name'] for col in inspector.get_columns(table_name)}
    missing_cols = set(df_columns) - existing_cols
    
    if missing_cols:
        print(f"📊 Adding {len(missing_cols)} missing columns...")
        with engine.begin() as conn:
            for col in missing_cols:
                try:
                    sql = f'ALTER TABLE {table_name} ADD COLUMN "{col}" TEXT'
                    conn.execute(text(sql))
                    print(f"  ✓ {col}")
                except Exception as e:
                    print(f"  ✗ {col}: {e}")

engine = create_engine(NEON_CONNECTION_STRING)

# Check if table exists
with engine.connect() as conn:
    result = conn.execute(text("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables
            WHERE table_name = :table
        )
    """), {"table": TABLE_NAME})
    table_exists = result.scalar()

# Add missing columns if table exists
if table_exists:
    add_missing_columns(engine, TABLE_NAME, df.columns)

# Upsert data
if table_exists:
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
