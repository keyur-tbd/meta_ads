"""
Meta Ads status + creatives -> Supabase
=======================================
The insights sync (meta_insights_sync.py) carries numbers only: no on/off state and
no picture. This script reads, for every account in AD_ACCOUNT_IDS:

  meta_ads_entities   one row per campaign / ad set / ad: its name, parents and
                      Meta's status right now (effective_status = what is actually
                      delivering, e.g. CAMPAIGN_PAUSED; configured_status = the
                      switch on the object itself), plus each ad's creative id and
                      shareable preview link.            key (level, id)
  meta_ads_creatives  one row per creative: type, video id, title / body, Instagram
                      permalink and a small JPEG thumbnail (longest side
                      THUMB_PX) so Birbal can show it without hot-linking Meta's
                      CDN, whose URLs are signed and expire within days.
                                                         key (creative_id)

A creative never changes once made (editing an ad makes a new creative), so a
thumbnail is downloaded once. Thumbnails are fetched for creatives of ads that are
live, or that delivered in the last THUMB_DAYS days.

Status is a snapshot: rows are upserted every run and never deleted, so an object
Meta no longer lists (deleted ones cannot be listed) keeps its last known status;
synced_at says when it was last seen.

Environment: as meta_insights_sync.py, plus THUMB_PX (default 320) and
THUMB_DAYS (default 180).
"""

import hashlib
import io
import json
import logging
import os
import sys

import requests
from PIL import Image
from psycopg2.extras import execute_values

from etl_alerts import guard
from meta_insights_sync import (ACCESS_TOKEN, AD_ACCOUNT_IDS, API_VERSION, api_call,
                                check_token, connect_db)

log = logging.getLogger(__name__)

THUMB_PX   = int(os.getenv("THUMB_PX", "320"))
THUMB_DAYS = int(os.getenv("THUMB_DAYS", "180"))
GRAPH      = f"https://graph.facebook.com/{API_VERSION}"

# Every effective_status each level accepts (a value a level does not know is an
# "Invalid parameter"). Without the filter the edges leave out archived objects, and the
# ads that ran last quarter are exactly those. DELETED is refused ("Cannot request deleted
# objects"), so a deleted object keeps the last status it was seen with, or none.
CAMPAIGN_STATUSES = ["ACTIVE", "PAUSED", "ARCHIVED", "IN_PROCESS", "WITH_ISSUES"]
ADSET_STATUSES    = CAMPAIGN_STATUSES + ["CAMPAIGN_PAUSED"]
AD_STATUSES       = ADSET_STATUSES + ["ADSET_PAUSED", "PENDING_REVIEW", "DISAPPROVED", "PREAPPROVED",
                                      "PENDING_BILLING_INFO"]

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS meta_ads_entities (
    level              TEXT NOT NULL,          -- campaign | adset | ad
    id                 TEXT NOT NULL,
    account_id         TEXT NOT NULL,
    name               TEXT,
    campaign_id        TEXT,
    adset_id           TEXT,
    effective_status   TEXT,
    configured_status  TEXT,
    creative_id        TEXT,                   -- ads only
    preview_url        TEXT,                   -- ads only: Meta's shareable preview link
    updated_time       TIMESTAMPTZ,
    synced_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (level, id)
);
CREATE INDEX IF NOT EXISTS meta_ads_entities_creative_idx ON meta_ads_entities (creative_id);

CREATE TABLE IF NOT EXISTS meta_ads_creatives (
    creative_id        TEXT PRIMARY KEY,
    account_id         TEXT NOT NULL,
    object_type        TEXT,                   -- VIDEO, PHOTO, SHARE, ...
    video_id           TEXT,
    title              TEXT,
    body               TEXT,
    instagram_url      TEXT,
    story_id           TEXT,                   -- page post behind the ad, if any
    thumb              BYTEA,                  -- JPEG, longest side THUMB_PX
    thumb_sha          TEXT,
    thumb_w            INTEGER,
    thumb_h            INTEGER,
    thumb_error        TEXT,
    synced_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
-- Supabase grants anon/authenticated write on new public tables; RLS with no
-- policy shuts that door (the sync connects as the owner and is not affected).
ALTER TABLE meta_ads_entities  ENABLE ROW LEVEL SECURITY;
ALTER TABLE meta_ads_creatives ENABLE ROW LEVEL SECURITY;
"""


def paged(path, params):
    """Every row of a Graph API edge, following paging.next."""
    url = f"{GRAPH}/{path}"
    params = {**params, "access_token": ACCESS_TOKEN, "limit": 500}
    while url:
        data = api_call("GET", url, params=params)
        yield from data.get("data", [])
        url = (data.get("paging") or {}).get("next")
        params = None                       # the next URL carries them


def fetch_entities(account):
    rows = []
    for c in paged(f"{account}/campaigns", {"fields": "id,name,effective_status,configured_status,updated_time",
                                            "effective_status": json.dumps(CAMPAIGN_STATUSES)}):
        rows.append(("campaign", c["id"], account, c.get("name"), c["id"], None,
                     c.get("effective_status"), c.get("configured_status"), None, None, c.get("updated_time")))
    for s in paged(f"{account}/adsets", {"fields": "id,name,campaign_id,effective_status,configured_status,updated_time",
                                         "effective_status": json.dumps(ADSET_STATUSES)}):
        rows.append(("adset", s["id"], account, s.get("name"), s.get("campaign_id"), s["id"],
                     s.get("effective_status"), s.get("configured_status"), None, None, s.get("updated_time")))
    for a in paged(f"{account}/ads", {"fields": "id,name,campaign_id,adset_id,effective_status,configured_status,"
                                                "updated_time,preview_shareable_link,creative{id}",
                                      "effective_status": json.dumps(AD_STATUSES)}):
        rows.append(("ad", a["id"], account, a.get("name"), a.get("campaign_id"), a.get("adset_id"),
                     a.get("effective_status"), a.get("configured_status"),
                     (a.get("creative") or {}).get("id"), a.get("preview_shareable_link"), a.get("updated_time")))
    return rows


def upsert_entities(conn, rows):
    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO meta_ads_entities
                (level, id, account_id, name, campaign_id, adset_id, effective_status,
                 configured_status, creative_id, preview_url, updated_time)
            VALUES %s
            ON CONFLICT (level, id) DO UPDATE SET
                account_id = EXCLUDED.account_id, name = EXCLUDED.name,
                campaign_id = EXCLUDED.campaign_id, adset_id = EXCLUDED.adset_id,
                effective_status = EXCLUDED.effective_status,
                configured_status = EXCLUDED.configured_status,
                creative_id = EXCLUDED.creative_id, preview_url = EXCLUDED.preview_url,
                updated_time = EXCLUDED.updated_time, synced_at = NOW()
        """, rows, page_size=1000)
    conn.commit()


def creatives_wanted(conn):
    """Creative ids worth a thumbnail that do not have one yet (or whose download
    failed): ads live now, or with delivery in the last THUMB_DAYS days."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT e.creative_id, e.account_id
              FROM meta_ads_entities e
              LEFT JOIN meta_ads_creatives c ON c.creative_id = e.creative_id
             WHERE e.level = 'ad' AND e.creative_id IS NOT NULL
               AND (c.creative_id IS NULL OR (c.thumb IS NULL AND c.synced_at < NOW() - INTERVAL '1 day'))
               AND (e.effective_status = 'ACTIVE'
                    OR EXISTS (SELECT 1 FROM meta_ads_summary s
                                WHERE s.ad_id = e.id AND s.date_start >= CURRENT_DATE - %s
                                  AND (COALESCE(s.spend, 0) > 0 OR COALESCE(s.impressions, 0) > 0)))
        """, (THUMB_DAYS,))
        return cur.fetchall()


def make_thumb(url):
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    img = Image.open(io.BytesIO(r.content))
    img = img.convert("RGB")
    img.thumbnail((THUMB_PX, THUMB_PX))
    out = io.BytesIO()
    img.save(out, "JPEG", quality=82, optimize=True)
    data = out.getvalue()
    return data, hashlib.sha1(data).hexdigest(), img.width, img.height


def fetch_creatives(wanted):
    """Creative details for (creative_id, account_id) pairs, 50 ids per call."""
    rows = []
    account_of = dict(wanted)
    ids = list(account_of)
    for i in range(0, len(ids), 50):
        batch = ids[i:i + 50]
        data = api_call("GET", f"{GRAPH}/", params={
            "ids": ",".join(batch), "access_token": ACCESS_TOKEN,
            "thumbnail_width": THUMB_PX, "thumbnail_height": THUMB_PX,
            "fields": "id,object_type,video_id,title,body,image_url,thumbnail_url,"
                      "instagram_permalink_url,effective_object_story_id"})
        for cid in batch:
            c = data.get(cid) or {}
            thumb = sha = w = h = err = None
            # thumbnail_url honours the requested size and exists for video, image and
            # catalog ads alike; image_url (full size) is the fallback
            for url in (c.get("thumbnail_url"), c.get("image_url")):
                if not url:
                    continue
                try:
                    thumb, sha, w, h = make_thumb(url)
                    err = None
                    break
                except Exception as e:
                    err = f"{type(e).__name__}: {e}"[:300]
            if thumb is None and err is None:
                err = "no thumbnail_url or image_url"
            rows.append((cid, account_of[cid], c.get("object_type"), c.get("video_id"), c.get("title"),
                         c.get("body"), c.get("instagram_permalink_url"), c.get("effective_object_story_id"),
                         thumb, sha, w, h, err))
    return rows


def upsert_creatives(conn, rows):
    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO meta_ads_creatives
                (creative_id, account_id, object_type, video_id, title, body, instagram_url,
                 story_id, thumb, thumb_sha, thumb_w, thumb_h, thumb_error)
            VALUES %s
            ON CONFLICT (creative_id) DO UPDATE SET
                account_id = EXCLUDED.account_id, object_type = EXCLUDED.object_type,
                video_id = EXCLUDED.video_id, title = EXCLUDED.title, body = EXCLUDED.body,
                instagram_url = EXCLUDED.instagram_url, story_id = EXCLUDED.story_id,
                thumb = COALESCE(EXCLUDED.thumb, meta_ads_creatives.thumb),
                thumb_sha = COALESCE(EXCLUDED.thumb_sha, meta_ads_creatives.thumb_sha),
                thumb_w = COALESCE(EXCLUDED.thumb_w, meta_ads_creatives.thumb_w),
                thumb_h = COALESCE(EXCLUDED.thumb_h, meta_ads_creatives.thumb_h),
                thumb_error = EXCLUDED.thumb_error, synced_at = NOW()
        """, rows, page_size=200)
    conn.commit()


def main():
    if not ACCESS_TOKEN:
        raise SystemExit("ACCESS_TOKEN is not set")
    check_token()
    guard("marketplace")

    conn = connect_db()
    with conn.cursor() as cur:
        cur.execute(SCHEMA_SQL)
    conn.commit()

    errors = []
    for account in AD_ACCOUNT_IDS:
        try:
            rows = fetch_entities(account)
            upsert_entities(conn, rows)
            n = {lvl: sum(1 for r in rows if r[0] == lvl) for lvl in ("campaign", "adset", "ad")}
            live = sum(1 for r in rows if r[0] == "ad" and r[6] == "ACTIVE")
            log.info(f"  {account}: {n['campaign']} campaigns, {n['adset']} ad sets, {n['ad']} ads ({live} active)")
        except Exception as e:
            conn.rollback()
            log.error(f"  {account}: status FAILED: {e}")
            errors.append(f"{account} status: {e}")

    try:
        wanted = creatives_wanted(conn)
        log.info(f"Creatives needing a thumbnail: {len(wanted)}")
        for i in range(0, len(wanted), 200):          # commit as we go
            rows = fetch_creatives(wanted[i:i + 200])
            upsert_creatives(conn, rows)
            bad = sum(1 for r in rows if r[8] is None)
            log.info(f"  stored {len(rows)} creatives ({bad} without a thumbnail)")
    except Exception as e:
        conn.rollback()
        log.error(f"Creatives FAILED: {e}")
        errors.append(f"creatives: {e}")

    conn.close()
    if errors:
        log.error(f"Finished with {len(errors)} error(s):")
        for e in errors:
            log.error(f"  - {e}")
        sys.exit(1)
    log.info("Status and creatives synced")


if __name__ == "__main__":
    main()
