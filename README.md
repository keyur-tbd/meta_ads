# Meta Ads → Supabase sync

Daily GitHub Actions job that pulls ad-level daily insights from the Meta Marketing API (async insights jobs) and writes them to Supabase (Postgres).

One script, `meta_insights_sync.py`, feeds three tables depending on `--breakdown`:

| `--breakdown` | Table | Unique key |
|---|---|---|
| `none` | `meta_ads_summary` | account_id, ad_id, date_start |
| `age_gender` | `meta_ads_age_gender` | account_id, ad_id, date_start, age, gender |
| `region` | `meta_ads_region` | account_id, ad_id, date_start, region |

Column names are unchanged from the old Neon tables (flattened `actions`, `_value` for action values, `_catalog` / `_catalog_value` for catalog segments, `cost_per_` for cost per action, plus `result`, `result_indicator`, `result_value`, `cost_per_result_value`, `total_purchase_value`, `purchase_roas_primary`, `calculated_roas`, `roas_divergence_flag`, `is_zero_spend_conversion` and the `video_*` columns). Numeric columns are `double precision`, ids are `text`, `date_start` / `date_stop` are `date`. New action types appearing in the API are added as columns automatically.

## Secrets

| Secret | Value |
|---|---|
| `SUPABASE_DB_URL` | `postgresql://postgres.<ref>:<password>@aws-0-<region>.pooler.supabase.com:5432/postgres` — use the **session pooler** host (IPv4); the direct `db.<ref>.supabase.co` host is IPv6-only and unreachable from GitHub runners |
| `META_ACCESS_TOKEN` | Marketing API token. Use a System User token so it does not expire every 60 days |
| `NEON_CONNECTION_STRING` | only needed by the one-off Neon backfill |

Optional env: `AD_ACCOUNT_IDS` (comma separated, overrides the list in the script), `META_API_VERSION` (default `v21.0`), `LOOKBACK_DAYS` (default 10), `CHUNK_DAYS` (default 31).

## Runs

* **Scheduled**: daily at 03:56 UTC; re-syncs the last 10 days up to today for all three tables, so 7-day-click attribution catches up.
* **Backfill from the API**: Actions → *Meta Ads → Supabase Daily Sync* → *Run workflow*, fill `start_date` / `end_date` (and optionally limit `breakdowns`). Ranges are fetched in 31-day windows, one async job per account per window, and each account/window is committed on its own.
* **Backfill from Neon**: Actions → *Backfill Meta Ads tables from Neon → Supabase*. Copies the historic rows once with type coercion (Neon columns are mostly text); rows already in Supabase are kept unless `overwrite` is `true`.

Each account/window is written in one transaction (delete that account's rows in the window, insert the fresh ones), so an account whose job fails keeps its previous rows. The job exits non-zero if anything failed.

## Local run

```
pip install -r requirements.txt
# .env with ACCESS_TOKEN and SUPABASE_DB_URL
python meta_insights_sync.py --breakdown none
START_DATE=2025-01-01 END_DATE=2025-03-31 python meta_insights_sync.py --breakdown region
```
