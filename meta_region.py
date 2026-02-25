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

def safe_float(value):
    """Safely convert value to float."""
    try:
        return float(value) if value else 0
    except (ValueError, TypeError):
        return 0

def flatten(row):
    # Extract the result first
    row["result"] = extract_result(row)
    
    # Flatten actions - FIXED: properly handle action types
    if row.get("actions"):
        for a in row["actions"]:
            action_type = a["action_type"].replace(".", "_")  # Replace dots with underscores
            row[action_type] = a.get("value")
    
    # Flatten action values - FIXED: properly handle value types and naming
    if row.get("action_values"):
        for a in row["action_values"]:
            action_type = a["action_type"].replace(".", "_")  # Replace dots with underscores
            row[action_type + "_value"] = a.get("value")
    
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

# ─────────────────────────────────────────────
# CALCULATE TOTAL PURCHASE VALUE & ROAS
# ─────────────────────────────────────────────
# List of all possible purchase value columns from Meta Ads
purchase_value_columns = [
    'offsite_conversion_fb_pixel_purchase_value',
    'omni_purchase_value',
    'onsite_web_purchase_value',
    'onsite_web_app_purchase_value',
    'web_in_store_purchase_value',
    'web_app_in_store_purchase_value',
    'purchase_value'
]

# Create total_purchase_value column by summing all purchase value sources
def calculate_total_purchase_value(row):
    total = 0
    for col in purchase_value_columns:
        if col in row and pd.notna(row[col]):
            total += safe_float(row[col])
    return total

df['total_purchase_value'] = df.apply(calculate_total_purchase_value, axis=1)

# Calculate ROAS (Return on Ad Spend)
def calculate_roas(row):
    spend = safe_float(row.get('spend', 0))
    purchase_value = safe_float(row.get('total_purchase_value', 0))
    
    if spend > 0 and purchase_value > 0:
        return purchase_value / spend
    return 0

df['calculated_roas'] = df.apply(calculate_roas, axis=1)

# Verify result column exists and show stats
print("\n" + "="*60)
print("DATA QUALITY CHECKS")
print("="*60)

if 'result' in df.columns:
    print(f"✅ Result column created successfully")
    print(f"   - Non-zero results: {(df['result'] != 0).sum()}")
    print(f"   - Total result value: {df['result'].astype(float).sum():.2f}")

# Show purchase value stats
print(f"\n💰 Purchase Value Analysis:")
print(f"   - Rows with purchase value: {(df['total_purchase_value'] > 0).sum()}")
print(f"   - Total purchase value: ${df['total_purchase_value'].sum():.2f}")
if (df['total_purchase_value'] > 0).sum() > 0:
    print(f"   - Average purchase value (non-zero): ${df[df['total_purchase_value'] > 0]['total_purchase_value'].mean():.2f}")

# Show ROAS stats
print(f"\n📈 ROAS Analysis:")
print(f"   - Rows with ROAS > 0: {(df['calculated_roas'] > 0).sum()}")
if (df['calculated_roas'] > 0).sum() > 0:
    print(f"   - Average ROAS (non-zero): {df[df['calculated_roas'] > 0]['calculated_roas'].mean():.2f}x")
    print(f"   - Max ROAS: {df['calculated_roas'].max():.2f}x")

# Show spend stats
print(f"\n💵 Spend Analysis:")
print(f"   - Total spend: ${df['spend'].astype(float).sum():.2f}")
print(f"   - Rows with spend: {(df['spend'].astype(float) > 0).sum()}")

# Show which purchase value columns have data
print(f"\n📊 Purchase Value Columns with Data:")
for col in purchase_value_columns:
    if col in df.columns:
        count = (pd.to_numeric(df[col], errors='coerce') > 0).sum()
        total = pd.to_numeric(df[col], errors='coerce').sum()
        if count > 0:
            print(f"   ✓ {col}: {count} rows, ${total:.2f}")

# Show unique objectives
print(f"\n📋 Unique objectives found: {df['objective'].unique().tolist()}")

print("\n" + "="*60)

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

print(f"\n🎉 Script completed successfully!")
print(f"   - New columns: 'total_purchase_value' and 'calculated_roas'")
