#!/usr/bin/env python3
"""
async_wayback_scraper_with_progress.py

Same as before but with:
 - progress bars (tqdm) for originals processed and snapshots saved
 - buffer flush/save every 50 snapshot rows
"""

import asyncio
import aiohttp
import pandas as pd
import time
import re
import json
import random
import os
from typing import Optional, List, Tuple
from urllib.parse import urljoin
from html import unescape
# third-party progress bar
from tqdm import tqdm

# ---------------- CONFIG ----------------
INPUT_CSV = "taaft_tools_2015_2025.csv"
URL_COLUMN = "tool_url"
OUT_CSV = "ai_wayback_async_out_2024_2.csv"
PROXIES_FILE = "proxies.txt"     # optional, one proxy per line: http://ip:port or socks5://ip:port
USE_TOR = False                  # if True, your local tor must be running (socks5://127.0.0.1:9050)
TOR_PROXY = "socks5://127.0.0.1:9050"

# Wayback/CDX
WAYBACK_CDX_URL = "https://web.archive.org/cdx/search/cdx"
WAYBACK_FROM = "2024"
WAYBACK_TO = "2024"
WAYBACK_FILTER = "statuscode:200"
BASE_URL = "https://theresanaiforthat.com/"

# Rate and concurrency:
REQUESTS_PER_MIN = 60           # global requests/minute (CDX + snapshot fetches). increase only if you have proxies/multiple IPs
CONCURRENCY = 12                # number of simultaneous HTTP fetch tasks (increase with more proxies)
MIN_SECONDS_BETWEEN = 60.0 / REQUESTS_PER_MIN

# Resilience:
MAX_RETRY_ATTEMPTS = 12
BACKOFF_BASE = 1.8
TIMEOUT = 30                    # per-request timeout (seconds)

# Save/resume:
SAVE_EVERY = 50                 # flush every N snapshot rows (you asked for 50)
CHECKPOINT_FILE = "wayback_checkpoint.json"  # stores processed keys set (saves progress fast)

# Columns (keeps your parsing outputs + wayback fields)

COLUMNS = [
    "name",
    "link",
    "tool_link",
    "description",
    "use_case",
    "pricing_model",
    "saves",
    "rating",
    "number_of_ratings",
    "number_of_comments",
    "versions_count",
    "versions",
    "pros_count",
    "cons_count",
    "pros",        # joined string for human-readable CSV
    "cons",        # joined string for human-readable CSV
    "pros_json",   # OPTIONAL: JSON array (analytics-friendly)
    "cons_json",    # OPTIONAL: JSON array (analytics-friendly)
    "also_searched",
    "also_searched_json",
    "also_searched_count",
    "rank_text",
    "comments_count",
    "comments_json",
    "task_label_name",
    "task_label_url",
    "faq_count",
    "faq_json",
    "featured_cards_count",
    "featured_cards_json",
    "tools_count",
    "tools_json",
    "error"
]

# ----------------------------------------

# load proxies into a cycle list
def load_proxies_list(path: str):
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        lines = [l.strip() for l in f if l.strip()]
    return lines

PROXIES = load_proxies_list(PROXIES_FILE)
proxy_cycle = None
if PROXIES:
    from itertools import cycle
    proxy_cycle = cycle(PROXIES)

# global rate limiter: simple token bucket using asyncio (fixed to start inside loop)
class RateLimiter:
    def __init__(self, rate_per_minute: int):
        self.tokens = rate_per_minute
        self.capacity = rate_per_minute
        self.lock = asyncio.Lock()
        self._last = time.time()
        self._refill_task = None  # will hold the background task once started

    async def _refill_loop(self):
        while True:
            await asyncio.sleep(1.0)
            async with self.lock:
                now = time.time()
                elapsed = now - self._last
                add = elapsed * (self.capacity / 60.0)
                if add >= 1.0:
                    self.tokens = min(self.capacity, self.tokens + add)
                    self._last = now

    async def start(self):
        """Start the refill background task — call this from inside a running event loop."""
        if self._refill_task is None:
            self._refill_task = asyncio.create_task(self._refill_loop())

    async def acquire(self):
        while True:
            async with self.lock:
                if self.tokens >= 1.0:
                    self.tokens -= 1.0
                    return
            await asyncio.sleep(0.05)

# We'll instantiate RATE_LIMITER inside main() (so it's created when event loop is running)
RATE_LIMITER = None

# helper: choose headers with rotating UA
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"
]
def random_headers():
    return {"User-Agent": random.choice(USER_AGENTS)}

# robust fetch with retries (aiohttp)
async def robust_fetch(session: aiohttp.ClientSession, url: str, proxy: Optional[str]=None) -> Tuple[Optional[str], Optional[str]]:
    global RATE_LIMITER
    last_exc = None
    for attempt in range(1, MAX_RETRY_ATTEMPTS + 1):
        try:
            await RATE_LIMITER.acquire()
            headers = random_headers()
            timeout = aiohttp.ClientTimeout(total=TIMEOUT)
            kwargs = {"timeout": timeout, "headers": headers}
            if proxy:
                kwargs["proxy"] = proxy
            async with session.get(url, **kwargs) as resp:
                status = resp.status
                if status == 200:
                    text = await resp.text(errors="ignore")
                    return text, None
                if status == 404:
                    return None, "HTTP 404"
                if status in (429, 502, 503, 504):
                    wait = (BACKOFF_BASE ** attempt) + random.random()
                    print(f"[robust_fetch] {status} {url} -> backoff {wait:.1f}s (attempt {attempt})")
                    await asyncio.sleep(wait)
                    last_exc = Exception(f"HTTP {status}")
                    continue
                if 500 <= status < 600:
                    wait = (BACKOFF_BASE ** attempt) + random.random()
                    await asyncio.sleep(wait)
                    last_exc = Exception(f"HTTP {status}")
                    continue
                return None, f"HTTP {status}"
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            wait = (BACKOFF_BASE ** attempt) + random.random()
            print(f"[robust_fetch] network error attempt {attempt} for {url}: {e}; sleep {wait:.1f}s")
            await asyncio.sleep(wait)
            last_exc = e
            continue
    return None, f"Max retries exceeded: {last_exc}"

# CDX query (returns list of {"timestamp","snapshot_url"})
async def query_cdx(session: aiohttp.ClientSession, orig_url: str, proxy: Optional[str]=None) -> Tuple[List[dict], Optional[str]]:
    params = {
        "url": orig_url,
        "from": WAYBACK_FROM,
        "to": WAYBACK_TO,
        "output": "json",
        "filter": WAYBACK_FILTER,
        "collapse": "timestamp"
    }
    # build safe URL
    q = WAYBACK_CDX_URL + "?" + "&".join([f"{k}={aiohttp.helpers.quote(str(v))}" for k,v in params.items()])
    text, err = await robust_fetch(session, q, proxy)
    if err:
        return [], err
    try:
        data = json.loads(text)
        snaps = []
        for row in data[1:]:
            if len(row) >= 2:
                ts = row[1]
                snap_url = f"https://web.archive.org/web/{ts}/{orig_url}"
                snaps.append({"timestamp": ts, "snapshot_url": snap_url})
        return snaps, None
    except Exception as e:
        return [], f"cdx parse error: {e}"

# ---- your parse logic (sync but runs within async wrapper) ----
from bs4 import BeautifulSoup
def safe_find_text(tag, selector_class=None, recursive=False):
    if tag is None:
        return None
    if selector_class:
        tag = tag.find(class_=selector_class)
        if tag is None:
            return None
    txt = tag.find(string=True, recursive=recursive)
    if txt:
        return txt.strip()
    return tag.get_text(" ", strip=True) if tag else None

def to_int_or_none(s):
    if s is None:
        return None
    s = str(s).strip()
    s_clean = s.strip("() ").replace(",", "")
    if s_clean == "":
        return None
    if s_clean.isdigit():
        return int(s_clean)
    try:
        if "." in s_clean:
            return float(s_clean)
    except ValueError:
        pass
    return s


def to_float_or_none(s):
    if s is None:
        return None
    s = str(s).strip()
    s_clean = s.strip("() ").replace(",", "")
    if s_clean == "":
        return None
    if s_clean.isfloat():
        return float(s_clean)
    try:
        if "." in s_clean:
            return float(s_clean)
    except ValueError:
        pass
    return s

def parse_page_to_record(html: str, original_url: str, snapshot_ts: str, snapshot_url: str):
    record = dict.fromkeys(COLUMNS, None)
    record["original_url"] = original_url
    record["snapshot_timestamp"] = snapshot_ts
    record["snapshot_url"] = snapshot_url
    record["link"] = snapshot_url
    try:
        soup = BeautifulSoup(html, "html.parser")

        # name
        h1 = soup.find("h1", class_="title_inner")
        record["name"] = safe_find_text(h1, None, recursive=False)

        # -------------------------
        # tool link ("Use tool")
        # -------------------------
        tool_a = soup.find("a", id="ai_top_link")
        if tool_a and tool_a.get("href"):
            href = tool_a.get("href")
            # normalize into absolute unless it's already external
            record["tool_link"] = urljoin(BASE_URL, href) if href.startswith("/") else href
        else:
            record["tool_link"] = None


        desc_block = soup.find("div", class_="description")
        if not desc_block:
            # fallback: sometimes class order may differ, so catch using selector
            desc_block = soup.select_one("div.description")

        if desc_block:
            # get the raw HTML text with <br> converted to newline
            desc_html = desc_block.decode_contents()

            # Replace <br> tags manually with newline
            desc_html = desc_html.replace("<br>", "\n").replace("<br/>", "\n").replace("<br />", "\n")

            # Now strip HTML tags using BeautifulSoup again (quick clean-up)
            temp_soup = BeautifulSoup(desc_html, "html.parser")
            cleaned_desc = temp_soup.get_text("\n", strip=True)

            record["description"] = cleaned_desc if cleaned_desc else None
        else:
            record["description"] = None


        # -------------------------
        # use_case extraction
        # -------------------------
        use_case_div = soup.find("div", id="use_case")
        if use_case_div:
            record["use_case"] = use_case_div.get_text(" ", strip=True)
        else:
            record["use_case"] = None

        # pricing
        pricing_divs = soup.select("span.tag.price")
        pricing_texts = []
        for div in pricing_divs:
            t = div.find(string=True, recursive=False)
            if not t:
                t = div.get_text(" ", strip=True)
            pricing_texts.append(t.strip() if t else None)
        mapping_keys = ["pricing_model"]
        for i, k in enumerate(mapping_keys):
            if i < len(pricing_texts):
                record[k] = pricing_texts[i]

        # saves
        saves_div = soup.find("div", class_="saves")
        record["saves"] = safe_find_text(saves_div, None, recursive=False)

        # rating (4.7)
        rating_anchor = soup.find("a", class_="rating_top")
        if rating_anchor:
            # first span is star, second has rating + nested ratings_count
            second_span = rating_anchor.select_one("span:nth-of-type(2)")
            record["rating"] = safe_find_text(second_span, None, recursive=False)

        # number_of_ratings (the (11) -> 11)
        ratings_count_span = soup.find("span", class_="ratings_count")
        rc_text = safe_find_text(ratings_count_span, None, recursive=False)
        # strip parentheses and coerce to int if possible
        record["number_of_ratings"] = to_int_or_none(rc_text.strip("() ")) if rc_text else None

        # number_of_comments
        comments_anchor = soup.find("a", class_="comments")
        comments_text = safe_find_text(comments_anchor, None, recursive=False)
        record["number_of_comments"] = to_int_or_none(comments_text)

        # -------------------------
        # versions extraction (list of {version, date, changelog})
        # -------------------------
        versions_list = []
        # select all release blocks (both current and hidden ones)
        for vdiv in soup.select("div.version"):
            try:
                # header and children
                header = vdiv.find("div", class_="version_header")
                ver_num_div = header.find("div", class_="version_number") if header else None
                changelog_title_div = header.find("div", class_="changelog_title") if header else None

                # version text (e.g., "Intuo ... v4.2") -> extract "v4.2"
                ver_text = None
                if ver_num_div:
                    ver_text = ver_num_div.get_text(" ", strip=True)
                # fallback to id attribute if text missing
                if not ver_text:
                    vid = vdiv.get("id")  # e.g., "release-v4.2"
                    if vid:
                        # try to take last dash part
                        ver_text = vid.split("-")[-1]
                # regex find version token like v1 or v1.2.3
                ver_token = None
                if ver_text:
                    m = re.search(r"\bv\d+(?:\.\d+)*\b", ver_text)
                    if m:
                        ver_token = m.group(0)
                    else:
                        # if nothing, fallback to last word (may still be useful)
                        ver_token = ver_text.split()[-1]

                # date string (as shown in the changelog_title div)
                date_text = None
                if changelog_title_div:
                    date_text = changelog_title_div.get_text(" ", strip=True)
                # try to canonicalize to ISO date (YYYY-MM-DD) using pandas (falls back to raw text)
                date_iso = None
                if date_text:
                    try:
                        dt = pd.to_datetime(date_text, errors="coerce")
                        if not pd.isna(dt):
                            date_iso = dt.strftime("%Y-%m-%d")
                        else:
                            date_iso = date_text  # keep original if parsing failed
                    except Exception:
                        date_iso = date_text

                # changelog text (the free text in version_changelog)
                changelog_div = vdiv.find("div", class_="version_changelog")
                changelog_text = changelog_div.get_text(" ", strip=True) if changelog_div else None

                # only append if we have some info (avoid empty junk)
                if ver_token or date_iso or changelog_text:
                    versions_list.append({
                        "version": ver_token,
                        "date": date_iso,
                        "changelog": changelog_text
                    })
            except Exception as e:
                # don't let one funky release kill everything
                print(f"[versions parsing] skipped a block due to: {e}")
                continue

        # final assignment to the record
        record["versions_count"] = len(versions_list)
        # store JSON string (safe for CSV); analytics team can parse this JSON later
        record["versions"] = json.dumps(versions_list, ensure_ascii=False)


        pros_cons_section = soup.find("section", id="pros-and-cons")
        if pros_cons_section:

            pros_items = []
            cons_items = []

            pros_block = pros_cons_section.find("div", class_="pac-info-item-pros")
            if pros_block:
                for d in pros_block.find_all("div", recursive=False):
                    text = d.get_text(" ", strip=True)
                    if text:
                        pros_items.append(text)

            cons_block = pros_cons_section.find("div", class_="pac-info-item-cons")
            if cons_block:
                for d in cons_block.find_all("div", recursive=False):
                    text = d.get_text(" ", strip=True)
                    if text:
                        cons_items.append(text)

            record["pros_count"] = len(pros_items)
            record["cons_count"] = len(cons_items)

            record["pros"] = " || ".join(pros_items) if pros_items else None
            record["cons"] = " || ".join(cons_items) if cons_items else None

            record["pros_json"] = json.dumps(pros_items, ensure_ascii=False) if pros_items else None
            record["cons_json"] = json.dumps(cons_items, ensure_ascii=False) if cons_items else None

        else:
            record["pros_count"] = None
            record["cons_count"] = None
            record["pros"] = None
            record["cons"] = None
            record["pros_json"] = None
            record["cons_json"] = None

            
        also_div = soup.find("div", class_="also_searched_inner")
        if also_div:
            # extract all <a> tags inside the container
            als = [a.get_text(" ", strip=True) for a in also_div.find_all("a")]

            record["also_searched"] = " || ".join(als) if als else None
            record["also_searched_json"] = json.dumps(als, ensure_ascii=False) if als else None
            record["also_searched_count"] = len(als) if als else 0
        else:
            record["also_searched"] = None
            record["also_searched_json"] = None
            record["also_searched_count"] = None
        
        # -------------------------
        # leaderboard rank & score
        # -------------------------
        rank_bottom = soup.find("span", class_="bottom")
        if rank_bottom:
            # remove the # symbol text
            rank_symbol = rank_bottom.find("span", class_="rank_symbol")
            if rank_symbol:
                rank_symbol.extract()

            rank_text = rank_bottom.get_text(" ", strip=True)
            record["rank_text"] = rank_text if rank_text else None
        else:
            record["rank_text"] = None

        # -------------------------
        # comments (all, nested) -> comments_json (list of nested dicts) + comments_count
        # -------------------------
        wrappers = soup.find_all("div", class_="comment-wrapper")
        comments_map = {}   # id -> dict (with children list)
        parent_of = {}      # id -> parent_id

        for w in wrappers:
            c = w.find("div", class_="comment")
            if not c:
                continue
            cid = c.get("data-id")
            if not cid:
                # skip if no id (rare)
                continue

            # user id (data-user on comment), username, profile url
            user_id = c.get("data-user")
            user_name_tag = c.find("div", class_="user_name")
            user_name = user_name_tag.get_text(strip=True) if user_name_tag else None
            profile_a = c.find("a", class_="user_card")
            user_profile = urljoin(BASE_URL, profile_a.get("href")) if profile_a and profile_a.get("href") else None

            # date (text inside .comment_date > a). Normalize to yyyy-mm-dd if possible
            date_tag = c.find("div", class_="comment_date")
            date_text = None
            date_iso = None
            if date_tag:
                at = date_tag.find("a")
                if at:
                    date_text = at.get_text(" ", strip=True)
                    try:
                        dt = pd.to_datetime(date_text, errors="coerce")
                        if not pd.isna(dt):
                            date_iso = dt.strftime("%Y-%m-%d")
                        else:
                            date_iso = date_text
                    except Exception:
                        date_iso = date_text

            # rating: count .star-full inside comment_rating
            rating_wrap = c.find("div", class_="comment_rating")
            rating_val = None
            if rating_wrap:
                rating_val = len(rating_wrap.select(".star-full"))

            # body text
            body_tag = c.find("div", class_="comment_body")
            body_text = body_tag.get_text(" ", strip=True) if body_tag else None

            # upvotes/downvotes (extract ints)
            up_tag = c.find("span", class_="comment_upvote")
            up_val = None
            if up_tag:
                ut = up_tag.get_text(" ", strip=True)
                m2 = re.search(r"(\d+)", ut)
                if m2:
                    try:
                        up_val = int(m2.group(1))
                    except Exception:
                        up_val = None

            # Build initial comment dict (children empty list)
            comments_map[cid] = {
                "id": cid,
                "user_id": user_id,
                "user_name": user_name,
                "user_profile": user_profile,
                "date": date_iso,
                "date_raw": date_text,
                "rating": rating_val,
                "body": body_text,
                "upvotes": up_val,
                "children": []
            }

            # determine parent comment-wrapper (immediate parent wrapper)
            parent_wrapper = w.find_parent("div", class_="comment-wrapper")
            if parent_wrapper:
                # parent_wrapper may contain its own comment; find parent's comment div
                parent_comment = parent_wrapper.find("div", class_="comment")
                if parent_comment and parent_comment.get("data-id"):
                    parent_of[cid] = parent_comment.get("data-id")
                else:
                    parent_of[cid] = None
            else:
                parent_of[cid] = None

        # now link children to parents (linear assembly)
        for cid, pdata in list(parent_of.items()):
            pid = pdata
            if pid and pid in comments_map:
                comments_map[pid]["children"].append(comments_map[cid])

        # collect roots (top-level comments)
        roots = []
        for cid, comment_obj in comments_map.items():
            if not parent_of.get(cid):
                roots.append(comment_obj)

        # final assignment
        record["comments_count"] = len(comments_map)
        # store JSON string (nested comments). analytics can parse this.
        record["comments_json"] = json.dumps(roots, ensure_ascii=False) if roots else json.dumps([], ensure_ascii=False)

        # -------------------------
        # single task_label extraction (name + link)
        # -------------------------
        
        tlabel = soup.find("a", class_="task_label")
        if tlabel:
            # name
            record["task_label_name"] = tlabel.get_text(" ", strip=True)

            # absolute link
            href = tlabel.get("href")
            record["task_label_url"] = urljoin(BASE_URL, href) if href else None
        else:
            record["task_label_name"] = None
            record["task_label_url"] = None
    


        # -------------------------
        # FAQ / Q&A extraction -> faq_json (list of {question, answer}) + faq_count
        # -------------------------
        faq_section = soup.find("section", id="faq")
        if faq_section:
            qa_items = []
            # select all faq-info blocks except the ask_question one
            for item in faq_section.select("div.faq-info"):
                # skip the "ask_question" block if it has that class
                if "ask_question" in (item.get("class") or []):
                    continue

                q_tag = item.find("div", class_="faq-info-title")
                a_tag = item.find("div", class_="faq-info-description")

                question = q_tag.get_text(" ", strip=True) if q_tag else None
                answer = a_tag.get_text(" ", strip=True) if a_tag else None

                # only add if at least question or answer exists
                if question or answer:
                    qa_items.append({"question": question, "answer": answer})

            record["faq_count"] = len(qa_items)
            record["faq_json"] = json.dumps(qa_items, ensure_ascii=False) if qa_items else json.dumps([], ensure_ascii=False)
        else:
            record["faq_count"] = None
            record["faq_json"] = None
       

       #-------------------------
       # Featured cards and all tools extraction
       #-------------------------
        featured_block = soup.find("div", class_="in_content_featured")
        cards_data = []

        if featured_block:
            cards = featured_block.select("li.standalone")

            for li in cards:
                # ids / attributes
                ai_id = li.get("data-id")
                name = li.get("data-name")
                task_id = li.get("data-task_id")
                fid = li.get("data-fid")
                featured = li.get("data-featured") == "true"

                # internal tool link
                tool_a = li.find("a", class_="ai_link")
                tool_link = tool_a.get("href") if tool_a and tool_a.get("href") else None

                # external website
                external_link = unescape(li.get("data-url")) if li.get("data-url") else None

                # task
                task_a = li.find("a", class_="task_label")
                task_name = task_a.get_text(" ", strip=True) if task_a else None
                task_link = urljoin(BASE_URL, task_a.get("href")) if task_a else None

                # description
                desc_tag = li.find("div", class_="short_desc")
                description = desc_tag.get_text(" ", strip=True) if desc_tag else None

                # saves
                saves_tag = li.find("div", class_="saves")
                saves = to_int_or_none(saves_tag.get_text(strip=True)) if saves_tag else None

                # rating
                rating_tag = li.find("div", class_="average_rating")
                rating = (rating_tag.get_text(strip=True)) if rating_tag else None

                ratings_count = None
                rc_tag = li.find("span", class_="ratings_count")
                if rc_tag:
                    ratings_count = to_int_or_none(rc_tag.get_text(strip=True))

                #comments_count
                comments_tag = li.find("div", class_="comments")
                comments = to_int_or_none(comments_tag.get_text(strip=True)) if comments_tag else None

                # pricing
                price_tag = li.find("a", class_="ai_launch_date")
                pricing = price_tag.get_text(" ", strip=True) if price_tag else None

                # screenshot
                img = li.find("img", class_="ai_image")
                screenshot = urljoin(BASE_URL, img.get("src")) if img else None

                cards_data.append({
                    "id": ai_id,
                    "name": name,
                    "tool_link": tool_link,
                    "external_link": external_link,
                    "task_name": task_name,
                    "task_link": task_link,
                    "description": description,
                    "saves": saves,
                    "average_rating": rating,
                    "ratings_count": ratings_count,
                    "pricing": pricing,
                    "comments_count": comments,
                    "screenshot": screenshot,
                    "featured": featured,
                    "task_id": task_id,
                    "fid": fid
                })

        record["featured_cards_count"] = len(cards_data)
        record["featured_cards_json"] = json.dumps(cards_data, ensure_ascii=False)

        #-------------------------
        # All tools extraction
        #-------------------------
        cards_data = []

        cards = soup.find_all("li", attrs={"data-id": True, "data-name": True})

        for li in cards:
            ai_id = li.get("data-id")
            name = li.get("data-name")
            task = li.get("data-task")
            task_id = li.get("data-task_id")
            task_slug = li.get("data-task_slug")

            # internal tool link
            tool_a = li.find("a", class_="ai_link")
            tool_link = tool_a.get("href") if tool_a and tool_a.get("href") else None

            # external website
            external_link = unescape(li.get("data-url")) if li.get("data-url") else None

            # task label
            task_a = li.find("a", class_="task_label")
            task_name = task_a.get_text(" ", strip=True) if task_a else task
            task_link = urljoin(BASE_URL, task_a.get("href")) if task_a else None

            # description
            desc_tag = li.find("div", class_="short_desc")
            description = desc_tag.get_text(" ", strip=True) if desc_tag else None

            # saves
            saves_tag = li.find("div", class_="saves")
            saves = to_int_or_none(saves_tag.get_text(strip=True)) if saves_tag else None

            #comments_count
            comments_tag = li.find("div", class_="comments")
            comments = to_int_or_none(comments_tag.get_text(strip=True)) if comments_tag else None

            # pricing
            price_tag = li.find("a", class_="ai_launch_date")
            pricing = price_tag.get_text(" ", strip=True) if price_tag else None

            # icon
            img = li.find("img")
            icon = urljoin(BASE_URL, img.get("src")) if img and img.get("src") else None

            cards_data.append({
                "id": ai_id,
                "name": name,
                "tool_link": tool_link,
                "external_link": external_link,
                "task_name": task_name,
                "task_link": task_link,
                "description": description,
                "saves": saves,
                "comments_count": comments,
                "pricing": pricing,
                "icon": icon,
                "task_id": task_id,
                "task_slug": task_slug
            })

        record["tools_count"] = len(cards_data)
        record["tools_json"] = json.dumps(cards_data, ensure_ascii=False)


    except Exception as e:
        print(f"[get_cols] unexpected error on : {e}")

    return record

# async worker for one original URL
async def process_original(orig_url: str, session: aiohttp.ClientSession, processed_set: set, buffer: list, lock: asyncio.Lock):
    proxy = None
    if USE_TOR:
        proxy = TOR_PROXY
    elif proxy_cycle:
        proxy = next(proxy_cycle)

    snaps, err = await query_cdx(session, orig_url, proxy)
    if err:
        rec = dict.fromkeys(COLUMNS, None)
        rec["original_url"] = orig_url
        rec["error"] = f"wayback cdx error: {err}"
        async with lock:
            buffer.append(rec)
        return

    if not snaps:
        rec = dict.fromkeys(COLUMNS, None)
        rec["original_url"] = orig_url
        rec["error"] = "no 2025 snapshots"
        async with lock:
            buffer.append(rec)
        return

    for snap in snaps:
        ts = snap["timestamp"]
        snap_url = snap["snapshot_url"]
        key = orig_url + "||" + ts
        if key in processed_set:
            continue
        resp_text, fetch_err = await robust_fetch(session, snap_url, proxy)
        if resp_text:
            rec = parse_page_to_record(resp_text, orig_url, ts, snap_url)
            if not rec.get("error"):
                rec["error"] = None
        else:
            rec = dict.fromkeys(COLUMNS, None)
            rec["original_url"] = orig_url
            rec["snapshot_timestamp"] = ts
            rec["snapshot_url"] = snap_url
            rec["error"] = f"fetch snapshot error: {fetch_err}"
        async with lock:
            buffer.append(rec)
            processed_set.add(key)

# save buffer atomically using pandas (append mode)
async def flush_buffer(buffer: list, snapshots_bar: tqdm):
    if not buffer:
        return
    df_new = pd.DataFrame(buffer, columns=COLUMNS)
    # atomic write: read existing, concat, write back
    if os.path.exists(OUT_CSV):
        df_old = pd.read_csv(OUT_CSV, dtype=str)
    else:
        df_old = pd.DataFrame(columns=COLUMNS)
    df_concat = pd.concat([df_old, df_new], ignore_index=True)
    df_concat.to_csv(OUT_CSV, index=False)
    # update progress bar for snapshots saved
    try:
        snapshots_bar.total += len(df_new)
        snapshots_bar.update(len(df_new))
    except Exception:
        pass
    buffer.clear()

# checkpoint processed set quickly (to resume faster)
def save_checkpoint(processed_set: set):
    tmp = list(processed_set)
    with open(CHECKPOINT_FILE + ".tmp", "w", encoding="utf-8") as f:
        json.dump(tmp, f)
    os.replace(CHECKPOINT_FILE + ".tmp", CHECKPOINT_FILE)

def load_checkpoint() -> set:
    if not os.path.exists(CHECKPOINT_FILE):
        return set()
    with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
        arr = json.load(f)
    return set(arr)

# main orchestrator
async def main():
    global RATE_LIMITER
    RATE_LIMITER = RateLimiter(REQUESTS_PER_MIN)
    await RATE_LIMITER.start()

    if not os.path.exists(INPUT_CSV):
        print("Input CSV not found:", INPUT_CSV); return

    df_in = pd.read_csv(INPUT_CSV, dtype=str)
    if URL_COLUMN not in df_in.columns:
        if "url" in df_in.columns:
            urls = df_in["url"].astype(str).tolist()
        else:
            raise SystemExit(f"URL column '{URL_COLUMN}' not found. Columns: {df_in.columns.tolist()}")
    else:
        urls = df_in[URL_COLUMN].astype(str).tolist()

    processed = load_checkpoint() if os.path.exists(CHECKPOINT_FILE) else set()
    print(f"Loaded checkpoint: {len(processed)} snapshots already processed")

    total = len(urls)
    # initialize tqdm bars
    originals_bar = tqdm(total=total, desc="Originals", unit="url")
    snapshots_bar = tqdm(total=0, desc="Snapshots saved", unit="rows")

    conn = aiohttp.TCPConnector(limit=CONCURRENCY, force_close=True)
    timeout = aiohttp.ClientTimeout(total=TIMEOUT)
    async with aiohttp.ClientSession(connector=conn, timeout=timeout) as session:
        lock = asyncio.Lock()
        buffer = []
        tasks = []
        sem = asyncio.Semaphore(CONCURRENCY)
        i = 0
        for orig in urls:
            orig = orig.strip()
            if not orig:
                originals_bar.update(1)
                continue
            i += 1
            await sem.acquire()
            # create wrapped to ensure per-original progress update and semaphore release
            async def wrapped(orig_url):
                try:
                    await process_original(orig_url, session, processed, buffer, lock)
                finally:
                    originals_bar.update(1)
                    sem.release()
            t = asyncio.create_task(wrapped(orig))
            tasks.append(t)

            # periodic flush & checkpoint (if buffer big enough)
            async with lock:
                if len(buffer) >= SAVE_EVERY:
                    await flush_buffer(buffer, snapshots_bar)
                    save_checkpoint(processed)
                    print(f"\nFlushed {SAVE_EVERY} rows; checkpoint saved ({len(processed)} processed)\n")

        # wait for all tasks to finish
        await asyncio.gather(*tasks)

        # final flush
        async with lock:
            await flush_buffer(buffer, snapshots_bar)
            save_checkpoint(processed)
            print("Final flush done; checkpoint saved")

    originals_bar.close()
    snapshots_bar.close()

if __name__ == "__main__":
    asyncio.run(main())
