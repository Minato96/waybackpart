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
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# ================= CONFIG =================

TARGET_URLS = [
    "https://theresanaiforthat.com"
]

BASE_URL = "https://theresanaiforthat.com/"

OUT_CSV = "ai_tools_feautered.csv"
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
WAYBACK_RENDER_RETRIES = 4
WAYBACK_RETRY_SLEEP = 6  # seconds

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

# ================= SELENIUM RENDERER =================

def create_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")

    return webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=chrome_options
    )

def fetch_wayback_rendered_html(driver, url, timeout=45):
    for attempt in range(1, WAYBACK_RENDER_RETRIES + 1):
        try:
            driver.get(url)
            wait = WebDriverWait(driver, timeout)

            # 1️⃣ wait ONLY for page shell
            wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))

            # 2️⃣ short, non-blocking check for ul.tasks
            short_wait = WebDriverWait(driver, 8)
            try:
                short_wait.until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "ul.tasks"))
                )
            except:
                # ul.tasks genuinely not present → NOT an error
                return driver.page_source

            # 3️⃣ ul.tasks exists → now stabilize
            prev = -1
            stable_rounds = 0
            for _ in range(20):
                lis = driver.find_elements(By.CSS_SELECTOR, "ul.tasks > li.li")
                count = len(lis)

                if count == prev and count > 0:
                    stable_rounds += 1
                    if stable_rounds >= 2:
                        break
                else:
                    stable_rounds = 0

                prev = count
                time.sleep(0.5)

            return driver.page_source

        except Exception as e:
            tqdm.write(
                f"[Wayback retry {attempt}/{WAYBACK_RENDER_RETRIES}] "
                f"{url} → waiting {WAYBACK_RETRY_SLEEP}s"
            )
            time.sleep(WAYBACK_RETRY_SLEEP)

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

def extract_video(li):
    if "has_video" not in li.get("class", []):
        return False

    wrapper = li.select_one(".main_video_embed_wrapper")
    if not wrapper:
        return False

    iframe = wrapper.select_one("iframe")
    poster = wrapper.select_one(".video_poster")
    views = wrapper.select_one(".views_count_count")

    thumbnail = None
    if poster and poster.get("style"):
        m = re.search(r"url\(['\"]?(.*?)['\"]?\)", poster.get("style"))
        if m:
            thumbnail = m.group(1)

    return {
        "title": iframe.get("title") if iframe else None,
        "embed_url": iframe.get("src") if iframe else None,
        "provider": (
            "mediadelivery"
            if iframe and "mediadelivery" in iframe.get("src", "")
            else None
        ),
        "thumbnail": thumbnail,
        "views": safe_int(views.text) if views else None
    }

def extract_featured_internal_link(li):
    el = li.select_one("a.ai_link.new_tab[data-fid]")
    if el and el.get("href"):
        return urljoin(BASE_URL, el.get("href"))
    return None

def extract_comments_json(li):
    if "has_comment" not in li.get("class", []):
        return False

    comments = []
    for c in li.select(".comment"):
        comments.append({
            "comment_id": c.get("data-id"),
            "user": {
                "user_id": c.get("data-user"),
                "username": first_text(c, [".user_name"]),
                "profile_url": first_attr(c, [".user_card"], "href")
            },
            "date": first_text(c, [".comment_date a"]),
            "body": first_text(c, [".comment_body"]),
            "upvotes": safe_int(first_text(c, [".comment_upvote"])) or 0
        })

    return comments if comments else False



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

async def query_cdx(session, target_url):
    q = (
        f"{WAYBACK_CDX_URL}?url={target_url}"
        f"&from={WAYBACK_FROM}&to={WAYBACK_TO}"
        f"&output=json&filter={WAYBACK_FILTER}&collapse=timestamp"
    )
    text, _ = await fetch(session, q, f"CDX {target_url}")
    data = json.loads(text)
    return [
        {
            "timestamp": row[1],
            "snapshot_url": f"https://web.archive.org/web/{row[1]}/{target_url}",
            "source_url": target_url
        }
        for row in data[1:]
    ]


# ================= PARSER =================

def parse_snapshot(html, ts, snap_url,source_url):
    soup = BeautifulSoup(html, "html.parser")
    rows = []

    task_ul = soup.select_one("ul.tasks")
    if not task_ul:
        return []

    for li in task_ul.select("> li.li"):
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
            "video": extract_video(li),
            "comments_json": extract_comments_json(li),
            "featured_internal_link": extract_featured_internal_link(li),
            "is_verified": "verified" in li.get("class", []),
            "is_pinned": "is_pinned" in li.get("class", []),
            "share_slug": first_attr(li, [".share_ai"], "data-slug"),
            "source_url": source_url,
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
        snapshots = []
        cdx_bar = tqdm(TARGET_URLS, desc="CDX URLs")
        for url in cdx_bar:
            cdx_bar.set_postfix(url=url.split("/")[-2])
            snaps = await query_cdx(session, url)
            snapshots.extend(snaps)

        # sort AFTER collecting all snapshots
        snapshots.sort(key=lambda x: (x["source_url"], x["timestamp"]))

        done = load_checkpoint()

        snap_bar = tqdm(total=len(snapshots), desc="Snapshots")
        row_bar = tqdm(desc="Rows")

        buffer = []
        
        driver = create_driver()
        for snap in snapshots:
            ts = snap["timestamp"]
            key = f"{snap['source_url']}|{ts}"

            if key in done:
                snap_bar.update(1)
                continue

            html = fetch_wayback_rendered_html(driver, snap["snapshot_url"])
            if html:
                rows = parse_snapshot(
                        html,
                        ts,
                        snap["snapshot_url"],
                        snap["source_url"]
                    )
                buffer.extend(rows)
                row_bar.update(len(rows))

            done.add(key)
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

        driver.quit()

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
