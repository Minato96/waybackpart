#!/usr/bin/env python3
"""
taaft_wayback_timeseries_FINAL_LOCKED.py

• Homepage cards across ALL Wayback eras (2015–2025+)
• STRICT extraction for quantitative metrics (no guessing)
• Heuristic extraction ONLY for pricing & release
• Progress bars, retries, rate limit, waits, checkpoint
• Monotonic: new logic only ADDS fallbacks
"""

import asyncio
import aiohttp
import pandas as pd
import time
import json
import random
import os
import re
from typing import List, Dict, Optional
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from tqdm import tqdm

# ================= CONFIG =================

TARGET_URL = "https://theresanaiforthat.com/"
BASE_URL = "https://theresanaiforthat.com/"

OUT_CSV = "taaft_tools_timeseries2.csv"
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
SNAPSHOT_DELAY = 0.3

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

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Mozilla/5.0 (X11; Linux x86_64)",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
]

PRICE_RE = re.compile(r"(free|from\s*\$\d+|\$\d+)", re.I)
RELATIVE_RE = re.compile(r"\b\d+\s*(h|d|w|m|y)\s*ago\b", re.I)

def headers():
    return {"User-Agent": random.choice(USER_AGENTS)}

def safe_int(txt):
    try:
        return int(txt.replace(",", "").strip())
    except:
        return None

def safe_float(txt):
    try:
        return float(txt.strip())
    except:
        return None

def first_text(li, selectors: List[str]) -> Optional[str]:
    for sel in selectors:
        el = li.select_one(sel)
        if el:
            txt = el.get_text(strip=True)
            if txt:
                return txt
    return None

def first_attr(li, selectors: List[str], attr: str) -> Optional[str]:
    for sel in selectors:
        el = li.select_one(sel)
        if el and el.get(attr):
            return el.get(attr)
    return None

# ================= STRICT METRIC EXTRACTORS =================
# (NO GUESSING — ONLY REAL TAGS)

def extract_saves(li):
    el = li.select_one(".saves")
    return safe_int(el.text) if el else None

def extract_comments(li):
    for sel in [
        ".comments"
    ]:
        el = li.select_one(sel)
        if el:
            return safe_int(el.text)
    return None

def extract_rating(li):
    # 1️⃣ Canonical (2024–2025)
    for sel in [
        ".average_rating"
    ]:
        el = li.select_one(sel)
        if el:
            return safe_float(el.text)

    # 2️⃣ 2023 stats_right pattern (e.g. JungGPT)
    for sel in [
        ".stats .stats_right",
    ]:
        el = li.select_one(sel)
        if el:
            txt = el.get_text(" ", strip=True)
            # match exactly one decimal rating like 5.0, 4.3, etc.
            m = re.search(r"\b\d\.\d\b", txt)
            if m:
                return float(m.group(0))

    return None


def extract_views(li):
    for sel in [
        ".stats_views span",
        ".views_count",
        ".views_count_count"
    ]:
        el = li.select_one(sel)
        if el:
            return safe_int(el.text)
    return None

# ================= SEMANTIC (SAFE) EXTRACTORS =================

def extract_pricing(li):
    # Newest / structured
    txt = first_text(li, [".ai_launch_date"])
    if txt:
        return txt

    # Early 2023
    txt = first_text(li, [".tags .tag.price"])
    if txt:
        return txt

    # Late 2022
    txt = first_text(li, [".tags .price"])
    if txt:
        return txt

    return None

def extract_release(li):
    # 2024–2025 relative release
    txt = first_text(li, [".released .relative"])
    if txt:
        return txt

    # Older absolute release
    txt = first_text(li, [".available_starting"])
    if txt:
        return txt.replace("Released", "").strip()

    # Attribute-based (2025)
    if li.get("data-release"):
        return li.get("data-release")

    return None

def extract_tags(li):
    tags = []
    for el in li.select(".tags span"):
        t = el.get_text(strip=True)
        if t and not PRICE_RE.search(t):
            tags.append(t)
    return ",".join(tags) if tags else None

# ================= NETWORK =================

async def fetch(session, url, context=""):
    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            await RATE_LIMITER.acquire()
            async with session.get(url, headers=headers(), timeout=TIMEOUT) as r:
                if r.status == 200:
                    return await r.text(errors="ignore"), None
                wait = BACKOFF_BASE ** attempt
                tqdm.write(f"[retry {attempt}] {context} HTTP {r.status} → wait {wait:.1f}s")
                await asyncio.sleep(wait)
        except Exception as e:
            last_err = e
            wait = BACKOFF_BASE ** attempt
            tqdm.write(f"[retry {attempt}] {context} exception → wait {wait:.1f}s")
            await asyncio.sleep(wait)
    return None, str(last_err)

async def query_cdx(session):
    q = (
        f"{WAYBACK_CDX_URL}?url={TARGET_URL}"
        f"&from={WAYBACK_FROM}&to={WAYBACK_TO}"
        f"&output=json&filter={WAYBACK_FILTER}&collapse=timestamp"
    )
    text, _ = await fetch(session, q, "CDX")
    data = json.loads(text)
    return [
        {
            "timestamp": row[1],
            "snapshot_url": f"https://web.archive.org/web/{row[1]}/{TARGET_URL}"
        }
        for row in data[1:]
    ]

# ================= PARSER =================

def parse_snapshot(html, ts, snap_url):
    soup = BeautifulSoup(html, "html.parser")
    rows = []

    for li in soup.select("li.li"):
        r = {
            "snapshot_timestamp": ts,
            "snapshot_url": snap_url,
            "tool_id": li.get("data-id"),
            "tool_name": li.get("data-name") or first_text(li, ["a.ai_link"]),
            "raw_classes": " ".join(li.get("class", [])),
            "internal_link": None,
            "external_link": None,
            "tags": extract_tags(li),
            "pricing_text": extract_pricing(li),
            "release_text": extract_release(li),
            "saves": extract_saves(li),
            "comments": extract_comments(li),
            "rating": extract_rating(li),
            "views": extract_views(li),
            "has_video": "has_video" in li.get("class", []),
            "has_comment": "has_comment" in li.get("class", []),
            "is_verified": "verified" in li.get("class", []),
            "is_pinned": "is_pinned" in li.get("class", []),
            "share_slug": first_attr(li, [".share_ai"], "data-slug"),
        }

        internal = first_attr(li, ["a.ai_link[href*='/ai/']"], "href")
        external = first_attr(
            li,
            ["a.external_ai_link", "a.ai_link[target='_blank']"],
            "href"
        )

        if internal:
            r["internal_link"] = urljoin(BASE_URL, internal)
        if external:
            r["external_link"] = urljoin(BASE_URL, external)

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
        done = load_checkpoint()

        snap_bar = tqdm(total=len(snapshots), desc="Snapshots")
        row_bar = tqdm(desc="Rows")

        buffer = []

        for snap in snapshots:
            ts = snap["timestamp"]
            if ts in done:
                snap_bar.update(1)
                continue

            html, _ = await fetch(session, snap["snapshot_url"], f"snapshot {ts}")
            if html:
                rows = parse_snapshot(html, ts, snap["snapshot_url"])
                buffer.extend(rows)
                row_bar.update(len(rows))

            done.add(ts)
            snap_bar.update(1)
            snap_bar.set_postfix(buf=len(buffer))

            if len(buffer) >= SAVE_EVERY:
                pd.DataFrame(buffer).to_csv(
                    OUT_CSV,
                    mode="a",
                    header=not os.path.exists(OUT_CSV),
                    index=False
                )
                buffer.clear()
                save_checkpoint(done)

            await asyncio.sleep(SNAPSHOT_DELAY)

        if buffer:
            pd.DataFrame(buffer).to_csv(
                OUT_CSV,
                mode="a",
                header=not os.path.exists(OUT_CSV),
                index=False
            )
            save_checkpoint(done)

        snap_bar.close()
        row_bar.close()

if __name__ == "__main__":
    asyncio.run(main())
