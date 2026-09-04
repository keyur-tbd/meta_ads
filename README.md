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

## Disk guard (shared across every pipeline)

This repo writes to a Supabase volume shared with the Business Central sync and
the GRN schedulers. Before it writes, it asks the database whether it is
allowed. **If you get an email titled `[WARN]` or `[STOP] Supabase disk`, start
here.**

```sql
-- this pipeline genuinely needs more room, and the volume has space:
UPDATE etl_disk_policy SET budget_gb = 30 WHERE pipeline = 'marketplace';

-- you resized the Supabase volume (do this EVERY time you resize):
UPDATE etl_disk_policy SET budget_gb = 100 WHERE pipeline = '_disk';

-- someone else should get the emails:
UPDATE etl_alert_config SET recipients = ARRAY['birbal@thebakersdozen.in'];
```

A `[STOP]` means this pipeline is refusing to write until you do one of those.
Nothing is lost: it stops before writing, and the next run continues.

`etl_alerts.py` is **identical in every pipeline repo** - do not add per-repo
logic to it. Everything configurable lives in Postgres (`etl_disk_policy`,
`etl_alert_config`), so budgets, thresholds and recipients change with an
`UPDATE` and no deploy, for all pipelines at once.

Two behaviours worth knowing:

- **It fails OPEN.** If the guard cannot run - no credentials in that step, the
  database unreachable - it logs an error and lets the pipeline continue. A
  guard that breaks a working pipeline is worse than one that cannot check.
  Grep the logs for `Disk guard could not run` if you suspect it is asleep.
- **Budgets grow themselves** into genuinely unallocated volume space, so a
  pipeline that is legitimately growing is not blocked by a number somebody
  guessed months ago. It can never grow past the volume ceiling, so this is
  not a way of turning the guard off.

Full documentation, including how the budgets were sized:
https://github.com/keyur-tbd/bc-supabase-sync#disk-alerts-and-auto-budgeting---start-here-if-you-got-an-email

## Birbal reads these tables (shared across every pipeline)

Since 2026-09-03 the Supabase project this writes to also backs **Birbal**
(`birbal-tbdai/birbal-mission-control`), the app the business asks questions in
plain language. Birbal never reads `public` directly: it reads one `select *`
view per table in a separate `warehouse` schema, plus a dictionary row per table
that tells it what the columns mean. Two consequences for this repo.

**A new table, or a new column, is invisible to Birbal until somebody exposes
it.** A view freezes its column list at CREATE time, and the exposure list is an
array inside a function - so nothing errors anywhere. The table simply does not
exist as far as the business is concerned, and an answer quietly leaves the new
column out. After applying the DDL this repo prints, run as `postgres`:

```sql
select app.sync_warehouse_views();   -- mirror new tables and columns
select app.sync_role_grants();       -- re-grant: the mirror drops grants
```

and add or update that table's row in `warehouse.warehouse_meta`. A column
nobody described there is a column Birbal will not use correctly.

**Never DROP or rename a table this pipeline owns.** A `warehouse` view depends
on it, so a plain `DROP` fails and `DROP ... CASCADE` deletes Birbal's view
without a word - that is how the BC sync went red on 2026-09-04. Add columns;
never replace tables. The writes themselves are safe: the
`delete window; insert` pair runs in one transaction, so a reader never sees
the gap.

**This sync applies its own DDL**: `ensure_columns()` runs
`alter table ... add column if not exists` whenever Meta returns a field it has
not seen, so columns appear in `public` unattended and stay invisible to Birbal
until the mirror above is rebuilt. Check for drift after any Meta API change.

Exposed today: `meta_ads_summary`, `meta_ads_age_gender` and `meta_ads_region` -
all three this sync writes.

Full contract, and the checks to run after a schema change:
https://github.com/keyur-tbd/bc-supabase-sync#who-reads-this-database-birbal
