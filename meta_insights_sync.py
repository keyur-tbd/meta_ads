"""
Meta Ads Insights -> Supabase (PostgreSQL) sync
===============================================
Pulls ad-level daily insights for every account in AD_ACCOUNT_IDS through the
async insights API and writes them to one of three tables, chosen with
--breakdown:

  --breakdown none        -> meta_ads_summary      key (account_id, ad_id, date_start)
  --breakdown age_gender  -> meta_ads_age_gender   key (..., age, gender)
  --breakdown region      -> meta_ads_region       key (..., region)

Column naming is the same as the previous Neon scripts (actions are
flattened to their action_type, action_values get a `_value` suffix,
catalog segments a `_catalog` / `_catalog_value` suffix, cost per action a
`cost_per_` prefix, plus the derived result / purchase / ROAS / video
columns). Numeric columns are DOUBLE PRECISION, ids are TEXT, dates are DATE.

Date range
  Default   : today minus LOOKBACK_DAYS (10)  ->  today
  Backfill  : START_DATE / END_DATE (YYYY-MM-DD); fetched in CHUNK_DAYS windows.

Writes are per account and per window in a single transaction: the window is
deleted for that account and re-inserted, so an account whose job failed keeps
its existing rows. The process exits 1 if any account/window failed.

Environment
  ACCESS_TOKEN, SUPABASE_DB_URL, AD_ACCOUNT_IDS (optional, comma separated),
  META_API_VERSION (default v21.0), LOOKBACK_DAYS, START_DATE, END_DATE,
  CHUNK_DAYS (default 31)
"""

import os
import re
import sys
import math
import time
import argparse
import logging
from datetime import datetime, timedelta, date

import requests
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
ACCESS_TOKEN  = os.environ.get("ACCESS_TOKEN") or os.environ.get("META_ACCESS_TOKEN", "")
DB_URL        = os.environ.get("SUPABASE_DB_URL") or os.environ.get("DATABASE_URL", "")
API_VERSION   = os.getenv("META_API_VERSION", "v21.0")
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "10"))
CHUNK_DAYS    = int(os.getenv("CHUNK_DAYS", "31"))
POLL_TIMEOUT_MIN = int(os.getenv("POLL_TIMEOUT_MINUTES", "45"))

DEFAULT_ACCOUNTS = [
    "act_1937951709801590",
    "act_2111571239641157",
    "act_935022987639527",
    "act_1447779473228664",
    "act_2073146216601611",
]
AD_ACCOUNT_IDS = [
    a.strip() if a.strip().startswith("act_") else f"act_{a.strip()}"
    for a in os.getenv("AD_ACCOUNT_IDS", ",".join(DEFAULT_ACCOUNTS)).split(",")
    if a.strip()
]

BREAKDOWNS = {
    "none":       {"table": "meta_ads_summary",    "params": None,         "cols": []},
    "age_gender": {"table": "meta_ads_age_gender", "params": "age,gender", "cols": ["age", "gender"]},
    "region":     {"table": "meta_ads_region",     "params": "region",     "cols": ["region"]},
}

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
    # Video metrics: top-level array fields, absent unless requested
    "video_play_actions",
    "video_p25_watched_actions",
    "video_p50_watched_actions",
    "video_p75_watched_actions",
    "video_p100_watched_actions",
    "video_thruplay_watched_actions",
    "video_avg_time_watched_actions",
    "video_continuous_2_sec_watched_actions",
]

VIDEO_FIELDS = {
    "video_plays":              "video_play_actions",
    "video_views_p25":          "video_p25_watched_actions",
    "video_views_p50":          "video_p50_watched_actions",
    "video_views_p75":          "video_p75_watched_actions",
    "video_views_p100":         "video_p100_watched_actions",
    "video_thruplay_views":     "video_thruplay_watched_actions",
    "video_avg_watch_time_sec": "video_avg_time_watched_actions",
    "video_2sec_continuous":    "video_continuous_2_sec_watched_actions",
}

# Raw nested fields that are flattened and then dropped
RAW_FIELDS = [
    "actions", "action_values", "cost_per_action_type",
    "results", "cost_per_result",
    "catalog_segment_actions", "catalog_segment_value",
    *VIDEO_FIELDS.values(),
]

# Columns stored as TEXT; every other column is DOUBLE PRECISION
TEXT_COLUMNS = {
    "account_id", "campaign_id", "campaign_name", "adset_id", "adset_name",
    "ad_id", "ad_name", "objective", "result_indicator",
    "age", "gender", "region", "country", "dma", "placement",
    "publisher_platform", "platform_position", "device_platform", "impression_device",
}
DATE_COLUMNS = {"date_start", "date_stop"}
INT_COLUMNS  = {"roas_divergence_flag", "is_zero_spend_conversion"}


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────
def safe_float(value):
    try:
        if value is None or value == "" or value is False:
            return 0.0
        x = float(value)
        return 0.0 if math.isnan(x) else x
    except (ValueError, TypeError):
        return 0.0


def sum_entries(raw):
    """Sum the `value` of a list of {action_type, value} dicts."""
    if not isinstance(raw, list):
        return 0.0
    return sum(safe_float(e.get("value", 0)) for e in raw if isinstance(e, dict))


def lookup(raw):
    out = {}
    for a in (raw or []):
        key = str(a.get("action_type", "")).replace(".", "_")
        if key:
            out[key] = safe_float(a.get("value", 0))
    return out


_COL_RE = re.compile(r"[^a-z0-9_]")


def norm_col(name: str) -> str:
    return _COL_RE.sub("_", str(name).lower().replace(".", "_"))


# ─────────────────────────────────────────────
# HTTP with retries
# ─────────────────────────────────────────────
class MetaAPIError(Exception):
    pass


RETRYABLE_CODES = {1, 2, 4, 17, 32, 613, 80000, 80004}  # transient / rate limit


def api_call(method, url, params=None, retries=6):
    delay = 15
    last = None
    for attempt in range(1, retries + 1):
        try:
            resp = requests.request(method, url, params=params, timeout=120)
        except requests.RequestException as e:
            last = f"network error: {e}"
        else:
            if resp.content:
                try:
                    data = resp.json()
                except ValueError:
                    data = None
                    last = f"non-JSON response (HTTP {resp.status_code}): {resp.text[:200]!r}"
                if data is not None:
                    err = data.get("error") if isinstance(data, dict) else None
                    if not err:
                        return data
                    code = err.get("code")
                    last = f"API error {code}/{err.get('error_subcode')}: {err.get('message')}"
                    if code not in RETRYABLE_CODES and resp.status_code < 500:
                        raise MetaAPIError(last)
            else:
                last = f"empty response (HTTP {resp.status_code})"
        log.warning(f"    {last} - retry {attempt}/{retries} in {delay}s")
        time.sleep(delay)
        delay = min(delay * 2, 120)
    raise MetaAPIError(f"giving up: {last}")


# ─────────────────────────────────────────────
# ASYNC INSIGHTS JOB
# ─────────────────────────────────────────────
def create_async_job(account, since, until, breakdown):
    """
    - action_report_time=impression matches the Ads Manager default.
    - action_attribution_windows must be a JSON array; a plain string is ignored.
    """
    url = f"https://graph.facebook.com/{API_VERSION}/{account}/insights"
    params = {
        "level":                      "ad",
        "time_increment":             1,
        "fields":                     ",".join(FIELDS),
        "time_range":                 f'{{"since":"{since}","until":"{until}"}}',
        "action_attribution_windows": '["7d_click","1d_view"]',
        "action_report_time":         "impression",
        "access_token":               ACCESS_TOKEN,
        "limit":                      500,
    }
    if breakdown["params"]:
        params["breakdowns"] = breakdown["params"]
    data = api_call("POST", url, params)
    job_id = data.get("report_run_id")
    if not job_id:
        raise MetaAPIError(f"no report_run_id in response: {data}")
    return job_id


def poll_job(job_id):
    url = f"https://graph.facebook.com/{API_VERSION}/{job_id}"
    start, interval = time.time(), 10
    while True:
        elapsed = time.time() - start
        if elapsed > POLL_TIMEOUT_MIN * 60:
            raise MetaAPIError(f"job {job_id} timed out after {POLL_TIMEOUT_MIN} minutes")
        data   = api_call("GET", url, {"access_token": ACCESS_TOKEN})
        status = data.get("async_status", "unknown")
        pct    = data.get("async_percent_completion", 0)
        log.info(f"    {status} ({pct}%) - {int(elapsed)}s")
        if status == "Job Completed":
            return
        if status in ("Job Failed", "Job Skipped"):
            raise MetaAPIError(f"job {job_id} ended with status {status}: {data}")
        time.sleep(interval)
        interval = min(interval + 5, 30)


def fetch_job_results(job_id):
    url    = f"https://graph.facebook.com/{API_VERSION}/{job_id}/insights"
    params = {"access_token": ACCESS_TOKEN, "limit": 500}
    rows   = []
    while True:
        data = api_call("GET", url, params)
        if "data" not in data:
            raise MetaAPIError(f"unexpected results payload: {str(data)[:300]}")
        rows.extend(data["data"])
        nxt = (data.get("paging") or {}).get("next")
        if not nxt:
            return rows
        url, params = nxt, None
        time.sleep(0.5)


# ─────────────────────────────────────────────
# FLATTEN
# ─────────────────────────────────────────────
def flatten(row):
    av  = lookup(row.get("action_values"))
    ac  = lookup(row.get("actions"))
    csv = lookup(row.get("catalog_segment_value"))
    csa = lookup(row.get("catalog_segment_actions"))
    cpa = lookup(row.get("cost_per_action_type"))

    # results (native Ads Manager "Results")
    row["result"], row["result_value"], row["result_indicator"] = 0.0, 0.0, ""
    results_raw = row.get("results")
    if isinstance(results_raw, list) and results_raw:
        try:
            row["result"] = safe_float(results_raw[0]["values"][0]["value"])
            indicator = results_raw[0].get("indicator", "") or ""
            row["result_indicator"] = indicator
            action_type = indicator.split(":", 1)[-1] if ":" in indicator else indicator
            key = action_type.replace(".", "_")
            if "catalog_segment" in indicator:
                row["result_value"] = csv.get(key) or av.get(key) or 0.0
            else:
                row["result_value"] = av.get(key) or csv.get(key) or 0.0
        except (KeyError, IndexError, TypeError):
            pass

    row["cost_per_result_value"] = 0.0
    cpr_raw = row.get("cost_per_result")
    if isinstance(cpr_raw, list) and cpr_raw:
        try:
            row["cost_per_result_value"] = safe_float(cpr_raw[0]["values"][0]["value"])
        except (KeyError, IndexError, TypeError):
            pass

    for k, v in ac.items():
        row[k] = v
    for k, v in av.items():
        row[f"{k}_value"] = v
    for k, v in csa.items():
        row[f"{k}_catalog"] = v
    for k, v in csv.items():
        row[f"{k}_catalog_value"] = v
    for k, v in cpa.items():
        row[f"cost_per_{k}"] = v

    for field in (
        "catalog_segment_value_mobile_purchase_roas",
        "catalog_segment_value_omni_purchase_roas",
        "catalog_segment_value_website_purchase_roas",
    ):
        raw = row.get(field)
        if isinstance(raw, list) and raw:
            try:
                row[field] = safe_float(raw[0]["value"])
            except (KeyError, IndexError, TypeError):
                row[field] = 0.0
        elif not isinstance(raw, (int, float)):
            row[field] = 0.0

    purchase_roas_raw = row.pop("purchase_roas", None)
    row["purchase_roas_primary"] = 0.0
    if isinstance(purchase_roas_raw, list):
        for r in purchase_roas_raw:
            at = str(r.get("action_type", "")).replace(".", "_")
            if at:
                row[f"purchase_roas_{at}"] = safe_float(r.get("value", 0))
        for preferred in (
            "purchase_roas_omni_purchase",
            "purchase_roas_offsite_conversion_fb_pixel_purchase",
            "purchase_roas_onsite_web_purchase",
        ):
            if safe_float(row.get(preferred, 0)) > 0:
                row["purchase_roas_primary"] = safe_float(row[preferred])
                break

    for col, field in VIDEO_FIELDS.items():
        row[col] = sum_entries(row.get(field))
    plays = row["video_plays"]
    row["video_hook_rate_pct"]       = round(row["video_views_p25"]  / plays * 100, 2) if plays > 0 else 0.0
    row["video_completion_rate_pct"] = round(row["video_views_p100"] / plays * 100, 2) if plays > 0 else 0.0

    for field in RAW_FIELDS:
        row.pop(field, None)
    return row


def primary_purchase_value(row):
    for col in ("omni_purchase_value", "offsite_conversion_fb_pixel_purchase_value",
                "onsite_web_purchase_value", "omni_purchase_catalog_value"):
        v = safe_float(row.get(col, 0))
        if v > 0:
            return v
    return safe_float(row.get("purchase_value", 0))


def build_dataframe(raw_rows, account):
    processed = []
    for r in raw_rows:
        r["account_id"] = account
        processed.append(flatten(r))
    df = pd.DataFrame(processed)
    df.columns = [norm_col(c) for c in df.columns]

    for col in ("impressions", "reach", "frequency", "clicks", "ctr", "cpc", "cpm", "spend"):
        if col in df.columns:
            df[col] = df[col].apply(safe_float)
        else:
            df[col] = 0.0

    df["total_purchase_value"] = df.apply(primary_purchase_value, axis=1)

    def roas(row):
        native = safe_float(row.get("purchase_roas_primary", 0))
        if native > 0:
            return round(native, 4)
        spend, pv = safe_float(row.get("spend", 0)), safe_float(row.get("total_purchase_value", 0))
        return round(pv / spend, 4) if spend > 0 and pv > 0 else 0.0
    df["calculated_roas"] = df.apply(roas, axis=1)

    def divergence(row):
        native, calc = safe_float(row.get("purchase_roas_primary", 0)), safe_float(row.get("calculated_roas", 0))
        if native == 0 or calc == 0:
            return 0
        return 1 if abs(native - calc) / native > 0.05 else 0
    df["roas_divergence_flag"]     = df.apply(divergence, axis=1)
    df["is_zero_spend_conversion"] = (df["spend"].eq(0) & df["total_purchase_value"].gt(0)).astype(int)
    return df


# ─────────────────────────────────────────────
# DATABASE
# ─────────────────────────────────────────────
def col_type(name: str) -> str:
    if name in TEXT_COLUMNS:
        return "TEXT"
    if name in DATE_COLUMNS:
        return "DATE"
    if name in INT_COLUMNS:
        return "INTEGER"
    return "DOUBLE PRECISION"


BASE_COLUMNS = [
    "account_id", "campaign_id", "campaign_name", "adset_id", "adset_name",
    "ad_id", "ad_name", "objective", "date_start", "date_stop",
    "impressions", "reach", "frequency", "clicks", "ctr", "cpc", "cpm", "spend",
    "result", "result_indicator", "result_value", "cost_per_result_value",
    "purchase_roas_primary", "total_purchase_value", "calculated_roas",
    "roas_divergence_flag", "is_zero_spend_conversion",
    *VIDEO_FIELDS.keys(), "video_hook_rate_pct", "video_completion_rate_pct",
]


def connect_db():
    if not DB_URL:
        raise SystemExit("SUPABASE_DB_URL is not set")
    dsn = DB_URL
    if "sslmode=" not in dsn:
        dsn += ("&" if "?" in dsn else "?") + "sslmode=require"
    conn = psycopg2.connect(dsn)
    with conn.cursor() as cur:
        cur.execute("SET statement_timeout = '600s'")
    conn.commit()
    return conn


def ensure_table(conn, table, key_cols):
    cols = BASE_COLUMNS + [c for c in key_cols if c not in BASE_COLUMNS]
    defs = ",\n    ".join(f'"{c}" {col_type(c)}' for c in cols)
    key  = ", ".join(f'"{c}"' for c in key_cols)
    with conn.cursor() as cur:
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS "{table}" (
                {defs},
                synced_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)
        cur.execute(f'ALTER TABLE "{table}" ADD COLUMN IF NOT EXISTS synced_at TIMESTAMPTZ NOT NULL DEFAULT NOW()')
        for c in key_cols:
            cur.execute(f'ALTER TABLE "{table}" ADD COLUMN IF NOT EXISTS "{c}" {col_type(c)}')
        cur.execute(f'CREATE UNIQUE INDEX IF NOT EXISTS "{table}_uniq" ON "{table}" ({key})')
        cur.execute(f'CREATE INDEX IF NOT EXISTS "{table}_date_idx" ON "{table}" (date_start)')
    conn.commit()


def ensure_schema(conn):
    """Create all three tables (used by neon_to_supabase.py)."""
    for b in BREAKDOWNS.values():
        ensure_table(conn, b["table"], ["account_id", "ad_id", "date_start", *b["cols"]])


def existing_columns(conn, table) -> dict:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT column_name, data_type FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s
        """, (table,))
        return dict(cur.fetchall())


def ensure_columns(conn, table, columns):
    existing = existing_columns(conn, table)
    missing  = [c for c in columns if c not in existing]
    if not missing:
        return
    with conn.cursor() as cur:
        for c in missing:
            cur.execute(f'ALTER TABLE "{table}" ADD COLUMN IF NOT EXISTS "{c}" {col_type(c)}')
    conn.commit()
    log.info(f"  Added {len(missing)} new column(s) to {table}: {sorted(missing)}")


def to_db_value(v, data_type):
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return None
    if data_type in ("double precision", "numeric", "real"):
        return safe_float(v)
    if data_type in ("integer", "bigint", "smallint"):
        return int(safe_float(v))
    if data_type == "date":
        return date.fromisoformat(str(v)[:10])
    return str(v)


def write_account_window(conn, table, key_cols, account, since, until, df):
    ensure_columns(conn, table, list(df.columns))
    types = existing_columns(conn, table)
    cols  = [c for c in df.columns if c in types]
    records = [
        tuple(to_db_value(row[c], types[c]) for c in cols)
        for row in df.to_dict("records")
    ]
    collist = ", ".join(f'"{c}"' for c in cols)
    keylist = ", ".join(f'"{c}"' for c in key_cols)
    with conn.cursor() as cur:
        cur.execute(
            f'DELETE FROM "{table}" WHERE account_id = %s AND date_start BETWEEN %s AND %s',
            (account, since, until),
        )
        deleted = cur.rowcount
        if records:
            execute_values(
                cur,
                f'INSERT INTO "{table}" ({collist}) VALUES %s ON CONFLICT ({keylist}) DO NOTHING',
                records, page_size=1000,
            )
    conn.commit()
    log.info(f"  {table}: {account} {since}..{until}: replaced {deleted} rows with {len(records)}")


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
def get_date_range():
    start_env, end_env = os.getenv("START_DATE", "").strip(), os.getenv("END_DATE", "").strip()
    if start_env or end_env:
        if not start_env:
            raise SystemExit("END_DATE given without START_DATE")
        start = date.fromisoformat(start_env)
        end   = date.fromisoformat(end_env) if end_env else date.today()
        if start > end:
            raise SystemExit(f"START_DATE {start} is after END_DATE {end}")
        return start, end
    return date.today() - timedelta(days=LOOKBACK_DAYS), date.today()


def date_chunks(start, end, size):
    cur = start
    while cur <= end:
        nxt = min(cur + timedelta(days=size - 1), end)
        yield cur.isoformat(), nxt.isoformat()
        cur = nxt + timedelta(days=1)


def summarize(df, breakdown):
    log.info(f"  rows={len(df)}  spend={df['spend'].sum():,.2f}  "
             f"results={df['result'].sum():,.0f}  purchase_value={df['total_purchase_value'].sum():,.2f}  "
             f"video_plays={df['video_plays'].sum():,.0f}")
    for c in breakdown["cols"]:
        if c in df.columns:
            log.info(f"  {c}: {sorted(df[c].dropna().unique().tolist())[:20]}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--breakdown", choices=BREAKDOWNS.keys(), default="none")
    args = ap.parse_args()
    breakdown = BREAKDOWNS[args.breakdown]
    table     = breakdown["table"]
    key_cols  = ["account_id", "ad_id", "date_start", *breakdown["cols"]]

    if not ACCESS_TOKEN:
        raise SystemExit("ACCESS_TOKEN is not set")

    start, end = get_date_range()
    log.info("=" * 60)
    log.info(f"Meta Ads -> Supabase | table={table} | {len(AD_ACCOUNT_IDS)} accounts | {start} -> {end} | {API_VERSION}")

    conn = connect_db()
    ensure_table(conn, table, key_cols)

    errors = []
    for since, until in date_chunks(start, end, CHUNK_DAYS):
        log.info(f"── Window {since} -> {until}")
        jobs = {}
        for account in AD_ACCOUNT_IDS:
            try:
                jobs[account] = create_async_job(account, since, until, breakdown)
                log.info(f"  {account}: job {jobs[account]} created")
            except Exception as e:
                log.error(f"  {account}: create job FAILED: {e}")
                errors.append(f"{account} {since}..{until} create: {e}")

        for account, job_id in jobs.items():
            try:
                log.info(f"  {account}: polling job {job_id}")
                poll_job(job_id)
                raw = fetch_job_results(job_id)
                log.info(f"  {account}: {len(raw)} rows fetched")
                if raw:
                    df = build_dataframe(raw, account)
                    summarize(df, breakdown)
                else:
                    df = pd.DataFrame(columns=key_cols)
                write_account_window(conn, table, key_cols, account, since, until, df)
            except Exception as e:
                conn.rollback()
                log.error(f"  {account}: FAILED: {e}")
                errors.append(f"{account} {since}..{until}: {e}")

    conn.close()
    if errors:
        log.error(f"Finished with {len(errors)} error(s):")
        for e in errors:
            log.error(f"  - {e}")
        sys.exit(1)
    log.info("All accounts synced successfully")


if __name__ == "__main__":
    main()
