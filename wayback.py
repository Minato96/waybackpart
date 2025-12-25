#!/usr/bin/env python3
"""
taaft_wayback_timeseries_final.py

• ONE target URL
• ALL Wayback snapshots (2015–2025)
• Era-agnostic DOM parsing
• Multi-selector fallback per field
• Rate-limited, retried, checkpointed
• Append-only CSV
"""

import asyncio
import aiohttp
import pandas as pd
import time
import json
import random
import os
from typing import Optional, List, Dict
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from tqdm import tqdm

# ================= CONFIG =================

TARGET_URL = "https://theresanaiforthat.com/"
BASE_URL = "https://theresanaiforthat.com/"

OUT_CSV = "taaft_tools_timeseries.csv"
CHECKPOINT_FILE = "wayback_checkpoint.json"

WAYBACK_CDX_URL = "https://web.archive.org/cdx/search/cdx"
WAYBACK_FROM = "2015"
WAYBACK_TO = "2025"
WAYBACK_FILTER = "statuscode:200"

REQUESTS_PER_MIN = 60
TIMEOUT = 30
MAX_RETRIES = 10
BACKOFF_BASE = 1.7
SAVE_EVERY = 50

# ================= RATE LIMITER =================

class RateLimiter:
    def __init__(self, rate):
        self.capacity = rate
        self.tokens = rate
        self.last = time.time()
        self.lock = asyncio.Lock()

    async def acquire(self):
        while True:
            async with self.lock:
                now = time.time()
                elapsed = now - self.last
                self.tokens = min(
                    self.capacity,
                    self.tokens + elapsed * (self.capacity / 60)
                )
                self.last = now
                if self.tokens >= 1:
                    self.tokens -= 1
                    return
            await asyncio.sleep(0.05)

RATE_LIMITER = RateLimiter(REQUESTS_PER_MIN)

# ================= HELPERS =================

def to_int(x):
    try:
        x = x.replace(",", "").strip()
        return int(x)
    except:
        return None

def to_float(x):
    try:
        return float(x)
    except:
        return None

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Mozilla/5.0 (X11; Linux x86_64)",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"
]

def headers():
    return {"User-Agent": random.choice(USER_AGENTS)}

def first_text(li, selectors):
    for sel in selectors:
        el = li.select_one(sel)
        if el and el.get_text(strip=True):
            return el.get_text(strip=True)
    return None

def first_attr(li, selectors, attr):
    for sel in selectors:
        el = li.select_one(sel)
        if el and el.get(attr):
            return el.get(attr)
    return None

def has_any_class(li, names):
    cls = set(li.get("class", []))
    return any(n in cls for n in names)

# ================= NETWORK =================

async def fetch(session, url):
    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            await RATE_LIMITER.acquire()
            async with session.get(url, headers=headers(), timeout=TIMEOUT) as r:
                if r.status == 200:
                    return await r.text(errors="ignore"), None
                if r.status in (429, 500, 502, 503, 504):
                    await asyncio.sleep(BACKOFF_BASE ** attempt)
                else:
                    return None, f"HTTP {r.status}"
        except Exception as e:
            last_err = e
            await asyncio.sleep(BACKOFF_BASE ** attempt)
    return None, str(last_err)

async def query_cdx(session):
    params = {
        "url": TARGET_URL,
        "from": WAYBACK_FROM,
        "to": WAYBACK_TO,
        "output": "json",
        "filter": WAYBACK_FILTER,
        "collapse": "timestamp"
    }
    q = WAYBACK_CDX_URL + "?" + "&".join(f"{k}={v}" for k,v in params.items())
    text, err = await fetch(session, q)
    if err:
        raise RuntimeError(err)
    data = json.loads(text)
    return [
        {
            "timestamp": row[1],
            "snapshot_url": f"https://web.archive.org/web/{row[1]}/{TARGET_URL}"
        }
        for row in data[1:]
    ]

# ================= PARSER =================

def parse_snapshot(html, ts, snap_url) -> List[Dict]:
    soup = BeautifulSoup(html, "html.parser")
    rows = []

    for li in soup.select("li.li[data-id]"):
        r = {
            "snapshot_timestamp": ts,
            "snapshot_url": snap_url,
            "tool_id": li.get("data-id"),
            "tool_name": li.get("data-name"),
            "internal_link": first_attr(li, ["a.ai_link"], "href"),
            "external_link": first_attr(li, ["a.external_ai_link"], "href"),
            "task_name": first_text(li, ["a.task_label"]),
            "use_case": first_text(li, ["a.use_case"]),
            "saves": to_int(first_text(li, [".saves"])),
            "comments": to_int(first_text(li, [".comments"])),
            "rating": to_float(first_text(li, [".average_rating", ".stats_left"])),
            "views": to_int(first_text(li, [".stats_views span", ".views_count"])),
            "pricing_text": first_text(li, [".ai_launch_date"]),
            "release_relative": first_text(li, [".released .relative"]),
            "icon_url": first_attr(li, ["img.taaft_icon", "img"], "src"),
            "has_video": has_any_class(li, ["has_video"]),
            "has_comment": has_any_class(li, ["has_comment"]),
            "is_verified": has_any_class(li, ["verified"]),
            "raw_classes": " ".join(li.get("class", [])),
            "share_slug": first_attr(li, [".share_ai"], "data-slug"),
            "error": None
        }

        if r["internal_link"]:
            r["internal_link"] = urljoin(BASE_URL, r["internal_link"])
        if r["external_link"]:
            r["external_link"] = urljoin(BASE_URL, r["external_link"])

        rows.append(r)

    return rows

# ================= CHECKPOINT =================

def load_checkpoint():
    if not os.path.exists(CHECKPOINT_FILE):
        return set()
    with open(CHECKPOINT_FILE) as f:
        return set(json.load(f))

def save_checkpoint(done):
    with open(CHECKPOINT_FILE + ".tmp", "w") as f:
        json.dump(list(done), f)
    os.replace(CHECKPOINT_FILE + ".tmp", CHECKPOINT_FILE)

# ================= MAIN =================

async def main():
    async with aiohttp.ClientSession() as session:
        snapshots = await query_cdx(session)
        processed = load_checkpoint()

        snap_bar = tqdm(total=len(snapshots), desc="Snapshots")
        row_bar = tqdm(desc="Rows")

        buffer = []

        for snap in snapshots:
            ts = snap["timestamp"]
            if ts in processed:
                snap_bar.update(1)
                continue

            html, err = await fetch(session, snap["snapshot_url"])
            if html:
                rows = parse_snapshot(html, ts, snap["snapshot_url"])
                buffer.extend(rows)
                row_bar.update(len(rows))

            processed.add(ts)
            snap_bar.update(1)

            if len(buffer) >= SAVE_EVERY:
                pd.DataFrame(buffer).to_csv(
                    OUT_CSV,
                    mode="a",
                    header=not os.path.exists(OUT_CSV),
                    index=False
                )
                buffer.clear()
                save_checkpoint(processed)

        if buffer:
            pd.DataFrame(buffer).to_csv(
                OUT_CSV,
                mode="a",
                header=not os.path.exists(OUT_CSV),
                index=False
            )
            save_checkpoint(processed)

        snap_bar.close()
        row_bar.close()

if __name__ == "__main__":
    asyncio.run(main())
