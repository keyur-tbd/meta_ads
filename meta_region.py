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
START_DATE = "01-01-2026"

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

# ─────────────────────────────────────────────
# FLATTEN
# ─────────────────────────────────────────────
# Maps Meta objective → the action_type Meta counts as the primary result
OBJECTIVE_TO_RESULT_ACTION = {
    "LINK_CLICKS":          "link_click",
    "OUTCOME_TRAFFIC":      "link_click",
    "OUTCOME_ENGAGEMENT":   "post_engagement",
    "OUTCOME_AWARENESS":    "reach",
    "OUTCOME_LEADS":        "lead",
    "OUTCOME_SALES":        "offsite_conversion.fb_pixel_purchase",
    "OUTCOME_APP_PROMOTION":"app_install",
}

def extract_result(row):
    """Return the primary result value based on the campaign objective."""
    objective = row.get("objective", "")
    target_action = OBJECTIVE_TO_RESULT_ACTION.get(objective)
    actions = row.get("actions") or []

    if target_action:
        for a in actions:
            if a["action_type"] == target_action:
                return a.get("value")
        # Fallback: for OUTCOME_SALES try omni_purchase if pixel purchase not found
        if objective == "OUTCOME_SALES":
            for a in actions:
                if a["action_type"] == "omni_purchase":
                    return a.get("value")

    # Generic fallback: return the first action value if objective unknown
    if actions:
        return actions[0].get("value")
    return None

def flatten(row):
    row["result"] = extract_result(row)
    if row.get("actions"):
        for a in row["actions"]:
            row[a["action_type"]] = a.get("value")
    if row.get("action_values"):
        for a in row["action_values"]:
            row[a["action_type"] + "_value"] = a.get("value")
    if row.get("purchase_roas"):
        for r in row["purchase_roas"]:
            row["purchase_roas"] = r.get("value")
    return row

processed = [flatten(r) for r in all_data]
for r in processed:
    r.pop("actions", None)
    r.pop("action_values", None)

df = pd.DataFrame(processed)

if df.empty:
    print("No data returned. Exiting.")
    exit()

df.columns = [c.replace(".", "_") for c in df.columns]

# ─────────────────────────────────────────────
# UPSERT TO NEON
# ─────────────────────────────────────────────
engine = create_engine(NEON_CONNECTION_STRING)

with engine.connect() as conn:
    result = conn.execute(text("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables
            WHERE table_name = :table
        )
    """), {"table": TABLE_NAME})
    table_exists = result.scalar()

if table_exists:
    with engine.begin() as conn:
        conn.execute(
            text(f"DELETE FROM {TABLE_NAME} WHERE date_start >= :start AND date_start <= :end"),
            {"start": START_DATE, "end": END_DATE}
        )
        df.to_sql(TABLE_NAME, conn, if_exists="append", index=False)
    print(f"✅ {len(df)} rows upserted into '{TABLE_NAME}'")
else:
    with engine.begin() as conn:
        df.to_sql(TABLE_NAME, conn, if_exists="replace", index=False)
    print(f"✅ Table '{TABLE_NAME}' created with {len(df)} rows")
