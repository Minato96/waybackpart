#!/usr/bin/env python3
"""
unified_wayback_async_scraper.py

• NO CDX
• Reads missing_wayback_urls directly
• Year-aware parsing (2023 / 2024 / 2025)
• Schema-locked (predefined columns)
• Async, retry-safe, resumable
"""

import asyncio
import aiohttp
import pandas as pd
import time
import re
import json
import random
import os
from tqdm import tqdm
from typing import Optional, Tuple
from bs4 import BeautifulSoup
from urllib.parse import urljoin

# ================= CONFIG =================

INPUT_CSV = "missing_wayback_urls_2023_2025.csv"
WAYBACK_COLUMN = "missing_wayback_urls"
OUT_CSV = "ai_wayback_unified.csv"

BASE_URL = "https://theresanaiforthat.com/"
REQUESTS_PER_MIN = 60
CONCURRENCY = 12
TIMEOUT = 30
SAVE_EVERY = 50
CHECKPOINT_FILE = "wayback_unified_checkpoint.json"

# ================= SCHEMAS =================
# ⬇️ EXACTLY copy your column lists here ⬇️

COLUMNS_2025 = [
    "name",
    "link",
    "tool_link",
    "description",
    "use_case",
    "pricing_model",
    "paid_options_from",
    "billing_frequency",
    "refund_policy",
    "discount_label",
    "discount_value",
    "discount_code",
    "views",
    "saves",
    "author_date",
    "rating",
    "number_of_ratings",
    "number_of_comments",
    "versions_count",
    "socials",
    "socials_json",
    "socials_links_json",
    "socials_count",
    "author_name",
    "author_username",
    "author_profile_url",
    "author_bio",
    "author_socials_json",
    "author_socials_count",
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
    "model_types",
    "model_types_json",
    "model_types_count",
    "modalities_inputs",        # human-readable joined string
    "modalities_outputs",
    "modalities_inputs_json",   # JSON array as string
    "modalities_outputs_json",
    "modalities_inputs_count",
    "modalities_outputs_count",
    "ai_lists_count",
    "ai_lists_json",
    "leaderboard_rank",
    "leaderboard_score",
    "comments_count",
    "comments_json",
    "video_views",
    "task_label_name",
    "task_label_url",
    "video_views_number",
    "visit_site_text",
    "visit_site_link",
    "iframe_src",
    "iframe_data_src",
    "faq_count",
    "faq_json",
    "top_alternative_count",
    "top_alternative_json",
    "featured_items_count",
    "featured_items_json"
]

COLUMNS_2024 = [
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
    "cards_count",
    "cards_json"
]

COLUMNS_2023 = [
    "snapshot_timestamp",
    "snapshot_url",
    "use_case_name",
    "use_case_category",
    "use_case_created_date",
    "link",
    "tool_link",
    "rank_task_name",
    "rank_full_text",
    "rank_number",
    "rank_label",
    "tags",              # human-readable joined string
    "tags_json",         # JSON array of tags
    "tags_count",        # number of tags
    "tag_price",          # pricing text (Free, Free + from $20/mo, etc.)
    "description",        # human-readable joined text
    "description_json",   # list of paragraphs (analytics / NLP)
    "description_length",  # optional but VERY useful
    "listings_count",
    "listings_json",
    "also_searched_count",
    "also_searched_json"
]

GLOBAL_COLUMNS = sorted(
    set(COLUMNS_2023)
  | set(COLUMNS_2024)
  | set(COLUMNS_2025)
  | {
        "snapshot_url",
        "snapshot_timestamp",
        "snapshot_year",
        "_schema",
        "error"
    }
)

# ================= RATE LIMITER =================

class RateLimiter:
    def __init__(self, rpm):
        self.capacity = rpm
        self.tokens = rpm
        self.last = time.time()
        self.lock = asyncio.Lock()

    async def acquire(self):
        while True:
            async with self.lock:
                now = time.time()
                delta = now - self.last
                self.tokens = min(self.capacity, self.tokens + delta * (self.capacity / 60))
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
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"
]

def headers():
    return {"User-Agent": random.choice(USER_AGENTS)}

def extract_year_ts(url: str) -> Tuple[Optional[str], Optional[str]]:
    m = re.search(r"/web/(\d{14})/", url)
    if not m:
        return None, None
    ts = m.group(1)
    return ts[:4], ts

def init_record(year: str):
    r = dict.fromkeys(GLOBAL_COLUMNS, None)
    r["_schema"] = year
    r["snapshot_year"] = year
    return r

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
    try:
        return float(s_clean)
    except ValueError:
        return s


# ================= HTTP =================

async def fetch(session, url):
    last = None
    for attempt in range(1, 8):
        try:
            await RATE_LIMITER.acquire()
            async with session.get(url, headers=headers(), timeout=TIMEOUT) as r:
                if r.status == 200:
                    return await r.text(errors="ignore"), None
                if r.status in (429, 502, 503):
                    await asyncio.sleep(1.8 ** attempt)
                    continue
                return None, f"HTTP {r.status}"
        except Exception as e:
            last = e
            await asyncio.sleep(1.8 ** attempt)
    return None, str(last)

# ================= PARSER DISPATCH =================
# ⚠️ THESE FUNCTIONS WRAP YOUR EXISTING PARSERS

def parse_2023(html: str, snapshot_url: str):
    year, ts = extract_year_ts(snapshot_url)
    record = init_record("2023")
    record["snapshot_url"] = snapshot_url
    record["snapshot_timestamp"] = ts

    record["link"] = snapshot_url
    try:
        soup = BeautifulSoup(html, "html.parser")

        h1 = soup.find("h1")
        if h1:
            # -------- use case name (Chatting) --------
            use_case_a = h1.select_one(".rank_task_name a.use_case_top")
            record["use_case_name"] = use_case_a.get_text(" ", strip=True) if use_case_a else None

            # -------- created date (30 Nov 2022) --------
            date_span = h1.select_one(".rank_task_name .launch_date_top")
            date_raw = date_span.get_text(" ", strip=True) if date_span else None

            # normalize date if possible
            if date_raw:
                try:
                    dt = pd.to_datetime(date_raw, errors="coerce")
                    record["use_case_created_date"] = (
                        dt.strftime("%Y-%m-%d") if not pd.isna(dt) else date_raw
                    )
                except Exception:
                    record["use_case_created_date"] = date_raw
            else:
                record["use_case_created_date"] = None

            # -------- category (ChatGPT) --------
            # second direct div under h1
            category_divs = h1.find_all("div", recursive=False)
            record["use_case_category"] = (
                category_divs[1].get_text(" ", strip=True)
                if len(category_divs) > 1
                else None
            )
        else:
            record["use_case_name"] = None
            record["use_case_category"] = None
            record["use_case_created_date"] = None

        rank_a = soup.find("a", class_="rank_inner")
        if rank_a:
            # task name
            task_span = rank_a.find("span", class_="rank_task_name")
            record["rank_task_name"] = task_span.get_text(" ", strip=True) if task_span else None

            # bottom rank text
            bottom_span = rank_a.find("span", class_="bottom")
            bottom_text = bottom_span.get_text(" ", strip=True) if bottom_span else None

            record["rank_full_text"] = bottom_text  # "#40 most recent"

            # extract number + label separately
            if bottom_text:
                # remove #
                clean = bottom_text.replace("#", "").strip()
                parts = clean.split(" ", 1)

                record["rank_number"] = to_int_or_none(parts[0]) if parts else None
                record["rank_label"] = parts[1] if len(parts) > 1 else None
            else:
                record["rank_number"] = None
                record["rank_label"] = None
        else:
            record["rank_task_name"] = None
            record["rank_full_text"] = None
            record["rank_number"] = None
            record["rank_label"] = None

        #--------------
        # tags
        #--------------
        tags_div = soup.find("div", class_="tags")

        if tags_div:
            tags_list = []
            price_text = None

            for span in tags_div.find_all("span", class_="tag"):
                # price tag (special case)
                if "price" in span.get("class", []):
                    price_text = span.get_text(" ", strip=True)
                else:
                    txt = span.get_text(" ", strip=True)
                    if txt:
                        tags_list.append(txt)

            record["tags_count"] = len(tags_list)
            record["tags"] = " || ".join(tags_list) if tags_list else None
            record["tags_json"] = json.dumps(tags_list, ensure_ascii=False) if tags_list else None
            record["tag_price"] = price_text
        else:
            record["tags_count"] = None
            record["tags"] = None
            record["tags_json"] = None
            record["tag_price"] = None

        #------------
        #description
        #------------
        desc_div = soup.find("div", class_="description")

        if desc_div:
            # remove method block if present
            method_div = desc_div.find("div", class_="method")
            if method_div:
                method_div.decompose()

            # extract paragraph texts
            paragraphs = [
                p.get_text(" ", strip=True)
                for p in desc_div.find_all("p")
                if p.get_text(strip=True)
            ]

            # final assignments
            record["description"] = "\n\n".join(paragraphs) if paragraphs else None
            record["description_json"] = json.dumps(paragraphs, ensure_ascii=False) if paragraphs else None
            record["description_length"] = len(record["description"]) if record["description"] else 0
        else:
            record["description"] = None
            record["description_json"] = None
            record["description_length"] = None


        items = []

        for li in soup.select("li.li"):
            item = {}

            # ---------- NAME + INTERNAL LINK ----------
            ai_link = li.select_one("a.ai_link")
            if ai_link:
                item["name"] = ai_link.get_text(" ", strip=True)
                item["internal_link"] = urljoin(BASE_URL, ai_link.get("href"))
            else:
                item["name"] = None
                item["internal_link"] = None

            # ---------- EXTERNAL LINK ----------
            ext = li.select_one("a.external_ai_link")
            item["external_link"] = ext.get("href") if ext else None

            # ---------- DOMAIN (format 1 only, optional) ----------
            domain = li.select_one(".domain")
            item["domain"] = domain.get_text(strip=True).strip("()") if domain else None

            # ---------- USE CASE ----------
            use_case = li.select_one(".use_case")
            item["use_case"] = use_case.get_text(" ", strip=True) if use_case else None

            # ---------- TASK ----------
            task = li.select_one("a.task_label")
            if task:
                item["task_name"] = task.get_text(" ", strip=True)
                item["task_link"] = urljoin(BASE_URL, task.get("href"))
            else:
                item["task_name"] = None
                item["task_link"] = None

            # ---------- TAGS + PRICE ----------
            tags = []
            price = None
            for t in li.select(".tags .tag"):
                if "price" in t.get("class", []):
                    price = t.get_text(strip=True)
                else:
                    tags.append(t.get_text(strip=True))

            item["tags"] = tags if tags else None
            item["price_label"] = price

            # ---------- RELEASE DATE ----------
            date_div = li.select_one(".available_starting")
            if date_div:
                date_text = date_div.get_text(" ", strip=True)
                item["release_date"] = date_text
            else:
                item["release_date"] = None

            # ---------- SAVES ----------
            saves = li.select_one(".saves")
            item["saves"] = int(saves.get_text(strip=True)) if saves and saves.get_text(strip=True).isdigit() else None

            # ---------- COMMENTS ----------
            comments = li.select_one(".comments")
            item["comments"] = int(comments.get_text(strip=True)) if comments else None

            # ---------- RATING ----------
            rating = li.select_one(".average_rating")
            item["rating"] = rating.get_text(strip=True) if rating else None

            # ---------- PRICING ----------
            pricing = li.select_one(".ai_launch_date")
            item["pricing_text"] = pricing.get_text(" ", strip=True) if pricing else None

            # ---------- IMAGE ----------
            img = li.select_one("img")
            item["image"] = img.get("src") if img else None

            items.append(item)
        record["listings_count"] = len(items)
        record["listings_json"] = json.dumps(items, ensure_ascii=False)


        #--------------
        #also searched for
        #--------------
        also_block = soup.find("div", class_="also_searched")

        if also_block:
            items = []

            for a in also_block.select(".also_searched_inner a[href]"):
                text = a.get_text(strip=True)
                href = a.get("href")

                items.append({
                    "query": text,
                    "link": urljoin(BASE_URL, href)
                })

            record["also_searched_count"] = len(items)
            record["also_searched_json"] = json.dumps(items, ensure_ascii=False)

        else:
            record["also_searched_count"] = 0
            record["also_searched_json"] = json.dumps([], ensure_ascii=False)

    except Exception as e:
        print(f"[get_cols] unexpected error on : {e}")



    return record

def parse_2024(html: str, snapshot_url: str):
    year, ts = extract_year_ts(snapshot_url)
    record = init_record("2024")
    record["snapshot_url"] = snapshot_url
    record["snapshot_timestamp"] = ts

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
        pricing_divs = soup.find_all("span", class_="tag_price")
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
                tool_link = urljoin(BASE_URL, tool_a.get("href")) if tool_a else None

                # external website
                ext_a = li.find("a", class_="external_ai_link")
                external_link = urljoin(BASE_URL, ext_a.get("href")) if ext_a else None

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

            # internal tool page
            tool_a = li.find("a", class_="ai_link")
            tool_link = urljoin(BASE_URL, tool_a.get("href")) if tool_a else None

            # external website
            ext_a = li.find("a", class_="external_ai_link")
            external_link = urljoin(BASE_URL, ext_a.get("href")) if ext_a else None

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

def parse_2025(html: str, snapshot_url: str):
    year, ts = extract_year_ts(snapshot_url)
    record = init_record("2025")
    record["snapshot_url"] = snapshot_url
    record["snapshot_timestamp"] = ts

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


        desc_block = soup.select_one("div.description.ai_description")
        if not desc_block:
            # fallback: sometimes class order may differ, so catch using selector
            desc_block = soup.select_one("div.description.ai_description")

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
        pricing_divs = soup.find_all("div", class_="pricing-value")
        pricing_texts = []
        for div in pricing_divs:
            t = div.find(string=True, recursive=False)
            if not t:
                t = div.get_text(" ", strip=True)
            pricing_texts.append(t.strip() if t else None)
        mapping_keys = ["pricing_model", "paid_options_from", "billing_frequency", "refund_policy"]
        for i, k in enumerate(mapping_keys):
            if i < len(pricing_texts):
                record[k] = pricing_texts[i]

        # -------------------------
        # TAAFT discount row extraction
        # -------------------------
        discount_row = soup.find("div", id="taaft_pricing_row")
        if discount_row:
            # label: "TAAFT Discount"
            label_tag = discount_row.find("div", class_="pricing-label")
            record["discount_label"] = label_tag.get_text(" ", strip=True) if label_tag else None

            # value: "20% off"
            value_tag = discount_row.find("div", class_="pricing-value")
            value_text = None
            code_text = None
            if value_tag:
                # example: '20% off with code "TAAFT"'
                # extract raw text
                full_value = value_tag.get_text(" ", strip=True)

                # the value part BEFORE the "with code"
                m_val = re.search(r'^([^"]+?)\s*with code', full_value)
                if m_val:
                    value_text = m_val.group(1).strip()
                else:
                    # fallback: first part
                    value_text = full_value

                # extract discount code from <span class="pricing-code">
                code_span = value_tag.find("span", class_="pricing-code")
                if code_span:
                    # usually text: with code "TAAFT"
                    m_code = re.search(r'"([^"]+)"', code_span.get_text(" ", strip=True))
                    if m_code:
                        code_text = m_code.group(1).strip()

            record["discount_value"] = value_text
            record["discount_code"] = code_text

        else:
            record["discount_label"] = None
            record["discount_value"] = None
            record["discount_code"] = None


        # views (safe chaining)
        stats_inner = soup.find("div", class_="stats_inner")
        views_div = stats_inner.find("div", class_="stats_opens") if stats_inner else None
        record["views"] = safe_find_text(views_div, None, recursive=False)

        # saves
        saves_span = soup.find("span", class_="save_button_text")
        record["saves"] = safe_find_text(saves_span, None, recursive=False)

        # -------------------------
        # author date extraction (span#author_date_inner)
        # -------------------------
        date_span = soup.find("span", id="author_date_inner")
        if date_span:
            # remove SVG and get only text
            record["author_date"] = date_span.get_text(" ", strip=True)
        else:
            record["author_date"] = None


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

        socials_div = soup.find("div", class_="social_icons")
        if socials_div:
            social_names = []
            social_links = []

            for a in socials_div.find_all("a", href=True):
                href = a["href"].strip()
                social_links.append(href)

                # platform name from img alt, fallback to domain
                img = a.find("img", class_="social_url")
                if img and img.get("alt"):
                    social_names.append(img["alt"].strip().lower())
                else:
                    # fallback: extract domain keyword (x / youtube / tiktok / discord)
                    # simple domain keyword map
                    h = href.lower()
                    if "x.com" in h:
                        social_names.append("twitter")
                    elif "youtube" in h:
                        social_names.append("youtube")
                    elif "discord" in h:
                        social_names.append("discord")
                    elif "tiktok" in h:
                        social_names.append("tiktok")
                    else:
                        social_names.append("other")

            record["socials_count"] = len(social_names) if social_names else 0
            record["socials"] = " || ".join(social_names) if social_names else None
            record["socials_json"] = json.dumps(social_names, ensure_ascii=False) if social_names else None
            record["socials_links_json"] = json.dumps(social_links, ensure_ascii=False) if social_links else None

        else:
            record["socials_count"] = None
            record["socials"] = None
            record["socials_json"] = None
            record["socials_links_json"] = None


        # -------------------------
        # author details extraction (from .user_profile_main)
        # -------------------------
        author_wrap = soup.find("div", class_="user_profile_main")
        if author_wrap:
            # name (display name)
            name_tag = author_wrap.find("h3", class_="user_display_name")
            if name_tag:
                a_tag = name_tag.find("a")
                record["author_name"] = a_tag.get_text(" ", strip=True) if a_tag else name_tag.get_text(" ", strip=True)
            else:
                record["author_name"] = None

            # username (e.g. @marketminion)
            uname_tag = author_wrap.find("a", class_="username")
            record["author_username"] = uname_tag.get_text(strip=True) if uname_tag else None

            # profile URL (absolute)
            record["author_profile_url"] = urljoin(BASE_URL, uname_tag.get("href")) if uname_tag and uname_tag.get("href") else None

            # bio
            bio_tag = author_wrap.find("p", class_="user_bio")
            record["author_bio"] = bio_tag.get_text(" ", strip=True) if bio_tag else None

            # socials (same pattern as before)
            social_block = author_wrap.find("div", class_="social_icons")
            socials = []
            if social_block:
                for a in social_block.find_all("a", href=True):
                    href = a.get("href")
                    img = a.find("img")
                    platform = img.get("alt").strip().lower() if img and img.get("alt") else None
                    socials.append({
                        "platform": platform,
                        "link": href
                    })

            record["author_socials_json"] = json.dumps(socials, ensure_ascii=False) if socials else None
            record["author_socials_count"] = len(socials) if socials else 0

        else:
            record["author_name"] = None
            record["author_username"] = None
            record["author_profile_url"] = None
            record["author_bio"] = None
            record["author_socials_json"] = None
            record["author_socials_count"] = None


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
            # get only the actual list elements (ignore the "View X more" button)
            pros_items = [e.get_text(" ", strip=True) for e in pros_cons_section.select(".pac-info-item-pros .pac-elem")]
            cons_items = [e.get_text(" ", strip=True) for e in pros_cons_section.select(".pac-info-item-cons .pac-elem")]

            # convert empty lists -> None so CSV stays clean, or keep 0 counts if you prefer
            record["pros_count"] = len(pros_items) if pros_items else 0
            record["cons_count"] = len(cons_items) if cons_items else 0

            # human readable joined string (choose a delimiter that won't appear in items)
            record["pros"] = " || ".join(pros_items) if pros_items else None
            record["cons"] = " || ".join(cons_items) if cons_items else None

            # analytics-friendly JSON (easy to parse later)
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
        # model types extraction (list of <span class="model">)
        # -------------------------
        model_row = soup.find("span", class_="model_type_row")
        if model_row:
            models = []
            for span in model_row.find_all("span", class_="model"):
                txt = span.get_text(" ", strip=True)
                if txt:
                    models.append(txt)

            record["model_types"] = " || ".join(models) if models else None
            record["model_types_json"] = json.dumps(models, ensure_ascii=False) if models else None
            record["model_types_count"] = len(models) if models else 0
        else:
            record["model_types"] = None
            record["model_types_json"] = None
            record["model_types_count"] = None


        inputs_list = []
        outputs_list = []

        block = soup.find("div", id="modalities") or soup.find("div", class_="modalities_cont")
        if block:
            # input block: prefer .input_modalities, fallback to searching for "Inputs:" label
            input_block = block.find("div", class_="input_modalities") or block.select_one(".modalities_cont .input_modalities")
            if not input_block:
                # fallback: find the area that contains the text 'Inputs' then grab following svg/title tokens
                candidates = block.find_all(string=lambda s: s and "inputs" in s.lower())
                if candidates:
                    # try to get parent element that contains icons
                    parent = candidates[0].parent
                    input_block = parent

            if input_block:
                # prefer svg <title> values
                for svg in input_block.find_all("svg"):
                    title = svg.find("title")
                    if title and title.get_text(strip=True):
                        tok = title.get_text(strip=True)
                        if tok:
                            inputs_list.append(tok.strip())

                # fallback: pick inline text nodes after label (exclude the word "Inputs")
                inline_texts = [t.strip() for t in input_block.find_all(string=True) if t.strip()]
                for t in inline_texts:
                    if t.lower().startswith("inputs"):
                        continue
                    if t not in inputs_list:
                        inputs_list.append(t)

            # output block: similar logic
            output_block = block.find("div", class_="output_modalities") or block.select_one(".modalities_cont .output_modalities")
            if not output_block:
                candidates = block.find_all(string=lambda s: s and "outputs" in s.lower())
                if candidates:
                    parent = candidates[0].parent
                    output_block = parent

            if output_block:
                for svg in output_block.find_all("svg"):
                    title = svg.find("title")
                    if title and title.get_text(strip=True):
                        tok = title.get_text(strip=True)
                        if tok:
                            outputs_list.append(tok.strip())

                inline_texts = [t.strip() for t in output_block.find_all(string=True) if t.strip()]
                for t in inline_texts:
                    if t.lower().startswith("outputs"):
                        continue
                    if t not in outputs_list:
                        outputs_list.append(t)

        # Normalize tokens to canonical categories (small inline mapping, no new defs)
        def _norm(tok):
            if not tok:
                return None
            s = tok.strip().lower()
            return s

        # apply normalization + dedupe preserving order
        seen = set()
        final_inputs = []
        for tok in inputs_list:
            nm = _norm(tok)
            if nm and nm not in seen:
                seen.add(nm)
                final_inputs.append(nm)

        seen = set()
        final_outputs = []
        for tok in outputs_list:
            nm = _norm(tok)
            if nm and nm not in seen:
                seen.add(nm)
                final_outputs.append(nm)

        # assign to record
        record["modalities_inputs_count"] = len(final_inputs) if final_inputs else 0
        record["modalities_outputs_count"] = len(final_outputs) if final_outputs else 0

        record["modalities_inputs"] = " || ".join(final_inputs) if final_inputs else None
        record["modalities_outputs"] = " || ".join(final_outputs) if final_outputs else None

        record["modalities_inputs_json"] = json.dumps(final_inputs, ensure_ascii=False) if final_inputs else None
        record["modalities_outputs_json"] = json.dumps(final_outputs, ensure_ascii=False) if final_outputs else None

        # -------------------------
        # leaderboard rank & score
        # -------------------------
        leader = soup.find("div", class_="leaderboard_score")
        if leader:
            # rank
            rank_tag = leader.find("a", class_="title")
            if rank_tag:
                # "#18" → strip() keeps "#18"
                record["leaderboard_rank"] = rank_tag.get_text(strip=True)
            else:
                record["leaderboard_rank"] = None

            # score
            score_tag = leader.find("span", class_="score")
            if score_tag:
                score_text = score_tag.get_text(strip=True)
                record["leaderboard_score"] = to_int_or_none(score_text)
            else:
                record["leaderboard_score"] = None
        else:
            record["leaderboard_rank"] = None
            record["leaderboard_score"] = None

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

            # karma (like "🙏 3 karma") → extract first integer if present
            karma_tag = c.find("span", class_="user_karma")
            karma_val = None
            if karma_tag:
                kt = karma_tag.get_text(" ", strip=True)
                m = re.search(r"(\d+)", kt)
                if m:
                    try:
                        karma_val = int(m.group(1))
                    except Exception:
                        karma_val = None

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

            # version they commented for (e.g., @9.6.0)
            version_tag = c.find("div", class_="comment_for_version")
            version_text = version_tag.get_text(" ", strip=True) if version_tag else None

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
                "karma": karma_val,
                "date": date_iso,
                "date_raw": date_text,
                "version": version_text,
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
        # embedded video / visit site / views extraction
        # -------------------------
        # views (text + numeric)
        views_span = soup.find("span", class_="views_count_count")
        if views_span:
            vtext = views_span.get_text(" ", strip=True)  # e.g. "136,248"
            record["video_views"] = vtext
            # numeric value
            vn = vtext.replace(",", "").strip()
            record["video_views_number"] = to_int_or_none(vn)
        else:
            record["video_views"] = None
            record["video_views_number"] = None

        # visit site link (domain text and href)
        visit_a = soup.find("a", class_="visit_ai_website_link")
        if visit_a and visit_a.get("href"):
            record["visit_site_text"] = visit_a.get_text(" ", strip=True)
            href = visit_a.get("href").strip()
            record["visit_site_link"] = urljoin(BASE_URL, href) if href.startswith("/") else href
        else:
            record["visit_site_text"] = None
            record["visit_site_link"] = None

        # iframe srcs (data-src and src attributes)
        iframe = soup.find("iframe")
        if iframe:
            data_src = iframe.get("data-src")
            src_attr = iframe.get("src")
            record["iframe_data_src"] = urljoin(BASE_URL, data_src) if data_src else None
            record["iframe_src"] = urljoin(BASE_URL, src_attr) if src_attr else None
        else:
            record["iframe_data_src"] = None
            record["iframe_src"] = None

        # -------------------------
        # ai lists (many per page) -> ai_lists_json + ai_lists_count
        # -------------------------
        lists = []
        # select blocks that look like the cards (has_cover or ai_list)
        for box in soup.find_all("div", class_="ai_list"):
            try:
                # if it's a wrapper with has_cover class, background-image in style
                data_id = box.get("data-id") or None

                # link + title
                a = box.find("a", class_="list_href")
                list_href = urljoin(BASE_URL, a.get("href")) if a and a.get("href") else None
                title_tag = a.find(class_="ai_list_title") if a else box.find(class_="ai_list_title")
                title = title_tag.get_text(" ", strip=True) if title_tag else None

                # background image (from style attr on box)
                bg_style = box.get("style") or ""
                bg_url = None
                m = re.search(r'url\((.*?)\)', bg_style)
                if m:
                    raw = m.group(1).strip().strip('"').strip("'")
                    bg_url = urljoin(BASE_URL, raw) if raw and not raw.startswith("data:") else raw

                # subscriber count (follow_counter) and tools_count
                follow_counter_tag = box.select_one(".follow_counter")
                follow_counter = to_int_or_none(follow_counter_tag.get_text(strip=True)) if follow_counter_tag else None

                tools_counter_tag = box.select_one(".tools_counter")
                tools_counter = to_int_or_none(tools_counter_tag.get_text(strip=True)) if tools_counter_tag else None

                # number of tools maybe in .tools_counter or .tools_counter sibling (fallback)
                # icons: collect all icon images inside .ai_list_icons
                icons = []
                for img in box.select(".ai_list_icons img.taaft_icon"):
                    alt = img.get("alt") or None
                    src = img.get("src") or None
                    if src:
                        src = urljoin(BASE_URL, src)
                    icons.append({"alt": alt, "src": src})

                # author (in footer)
                author_block = box.find("div", class_="ai_list_author")
                author_name = None
                author_profile = None
                if author_block:
                    a2 = author_block.find("a", class_="user_card")
                    if a2:
                        # name may be in .user_name
                        name_tag = a2.find("div", class_="user_name") or a2.find("span", class_="user_name")
                        author_name = name_tag.get_text(" ", strip=True) if name_tag else a2.get_text(" ", strip=True)
                        href = a2.get("href")
                        author_profile = urljoin(BASE_URL, href) if href else None

                lists.append({
                    "data_id": data_id,
                    "title": title,
                    "list_link": list_href,
                    "background_image": bg_url,
                    "subscribers": follow_counter,
                    "tools_count": tools_counter,
                    "icons": icons,
                    "author_name": author_name,
                    "author_profile": author_profile
                })
            except Exception as e:
                # skip one bad box but continue
                print(f"[ai_list parse] skipped one list due to: {e}")
                continue

        record["ai_lists_count"] = len(lists) if lists else 0
        record["ai_lists_json"] = json.dumps(lists, ensure_ascii=False) if lists else json.dumps([], ensure_ascii=False)

        # -------------------------
        # single task_label extraction (name + link)
        # -------------------------
        stats_task = soup.find("div", class_="stats")
        if stats_task:
            tlabel = stats_task.find("a", class_="task_label")
            if tlabel:
                # name
                record["task_label_name"] = tlabel.get_text(" ", strip=True)

                # absolute link
                href = tlabel.get("href")
                record["task_label_url"] = urljoin(BASE_URL, href) if href else None
            else:
                record["task_label_name"] = None
                record["task_label_url"] = None
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
        # -------------------------
        # extract multiple <li class="li ..."> blocks -> top_alternative_json + top_alternative_count
        # -------------------------
        li_items = []
        # find all <li> elements with a data-id and class containing 'li' (handles many variants)
        for li in soup.find_all("li", attrs={"data-id": True}):
            try:
                data_id = li.get("data-id")
                data_name = li.get("data-name")
                data_task = li.get("data-task")
                data_task_id = li.get("data-task_id")
                data_url = li.get("data-url")
                data_release = li.get("data-release")

                # ai details: link to AI page and current version inside .ai_link.new_tab
                ai_link_tag = li.find("a", class_="ai_link")
                ai_page = urljoin(BASE_URL, ai_link_tag.get("href")) if ai_link_tag and ai_link_tag.get("href") else None
                ai_title = ai_link_tag.get_text(" ", strip=True) if ai_link_tag else None
                current_version_tag = ai_link_tag.find(class_="current_version") if ai_link_tag else None
                current_version = current_version_tag.get_text(" ", strip=True) if current_version_tag else None

                # short description
                short_desc_tag = li.find("div", class_="short_desc")
                short_desc = short_desc_tag.get_text(" ", strip=True) if short_desc_tag else None

            
                # open / status text (span.open_ai etc.)
                status_tag = li.find("span", class_="open_ai")
                status = status_tag.get_text(" ", strip=True) if status_tag else None

                # views / saves / rating (from .stats or .views_count_count etc.)
                # try the li-level stats first (common pattern)
                stats_anchor = li.find("a", class_="stats")
                views = None
                saves = None
                rating = None
                if stats_anchor:
                    views_tag = stats_anchor.select_one(".stats_views span") or stats_anchor.select_one(".stats_views")
                    if views_tag:
                        # get inner number text, fallback to span text
                        views_text = views_tag.get_text(" ", strip=True)
                        # try to extract digits (handles commas)
                        m = re.search(r"(\d[\d,]*)", views_text)
                        views = to_int_or_none(m.group(1).replace(",", "")) if m else to_int_or_none(views_text)
                    saves_tag = stats_anchor.select_one(".saves")
                    if saves_tag:
                        saves = to_int_or_none(saves_tag.get_text(" ", strip=True))
                    rating_tag = stats_anchor.select_one(".average_rating")
                    if rating_tag:
                        # rating often like: <span class="star star-full"></span>3.8
                        # extract the numeric part
                        rt = rating_tag.get_text(" ", strip=True)
                        m2 = re.search(r"(\d+(?:\.\d+)?)", rt)
                        rating = float(m2.group(1)) if m2 else None

                # video / visit site snippet (if present)
                visit_link_tag = li.find("a", class_="visit_ai_website_link")
                visit_site = visit_link_tag.get_text(" ", strip=True) if visit_link_tag else None
                visit_site_link = visit_link_tag.get("href") if visit_link_tag and visit_link_tag.get("href") else None
                if visit_site_link:
                    visit_site_link = urljoin(BASE_URL, visit_site_link) if visit_site_link.startswith("/") else visit_site_link

                # video views (alternative location)
                vid_views_tag = li.select_one(".views_count_count")
                video_views = None
                if vid_views_tag:
                    raw_v = vid_views_tag.get_text(" ", strip=True)
                    video_views = to_int_or_none(raw_v.replace(",", "")) if raw_v else None

                # capture any iframe srcs under this li (list)
                iframe_srcs = []
                for iframe in li.find_all("iframe"):
                    src = iframe.get("src") or iframe.get("data-src")
                    if src:
                        iframe_srcs.append(urljoin(BASE_URL, src))

                # changelog / author info inside .version_changelog (if exists)
                changelog = li.find("div", class_="version_changelog")
                changelog_author = None
                changelog_body = None
                changelog_upvotes = None
                if changelog:
                    # author name may be inside .user_name under changelog
                    auth = changelog.find("div", class_="user_name")
                    changelog_author = auth.get_text(" ", strip=True) if auth else None
                    body = changelog.find("div", class_="changelog_body")
                    changelog_body = body.get_text(" ", strip=True) if body else None
                    up_tag = changelog.find("span", class_="changelog_upvote")
                    if up_tag:
                        changelog_upvotes = to_int_or_none(up_tag.get_text(" ", strip=True))

                # price / launch link area
                price_tag = li.find("a", class_="ai_launch_date")
                price_text = price_tag.get_text(" ", strip=True) if price_tag else None

                # released relative text
                released_tag = li.find("div", class_="released")
                released_rel = None
                if released_tag:
                    rel = released_tag.find("span", class_="relative")
                    released_rel = rel.get_text(" ", strip=True) if rel else None

                # icon src
                icon_img = li.find("img", class_="taaft_icon")
                icon_src = urljoin(BASE_URL, icon_img.get("src")) if icon_img and icon_img.get("src") else None
                icon_alt = icon_img.get("alt") if icon_img and icon_img.get("alt") else None

                li_items.append({
                    "data_id": data_id,
                    "data_name": data_name,
                    "data_task": data_task,
                    "data_task_id": data_task_id,
                    "data_url": data_url,
                    "data_release": data_release,
                    "ai_page": ai_page,
                    "ai_title": ai_title,
                    "current_version": current_version,
                    "short_desc": short_desc,
                    "status": status,
                    "views": views,
                    "saves": saves,
                    "rating": rating,
                    "visit_site": visit_site,
                    "visit_site_link": visit_site_link,
                    "video_views": video_views,
                    "iframe_srcs": iframe_srcs,
                    "changelog_author": changelog_author,
                    "changelog_body": changelog_body,
                    "changelog_upvotes": changelog_upvotes,
                    "price_text": price_text,
                    "released_relative": released_rel,
                    "icon_alt": icon_alt,
                    "icon_src": icon_src
                })
            except Exception as e:
                # skip this li if it fails but continue
                print(f"[li parse] skipped an li due to: {e}")
                continue

        record["top_alternative_count"] = len(li_items)
        record["top_alternative_json"] = json.dumps(li_items, ensure_ascii=False) if li_items else json.dumps([], ensure_ascii=False)

       # -------------------------
        # featured / list li cards (tf_xyz3 etc.)
        # -------------------------
        featured_items = []

        for li in soup.find_all("li", attrs={"data-id": True}):
            try:
                # basic attributes from <li>
                item_id = li.get("data-id")
                name_attr = li.get("data-name")
                task_attr = li.get("data-task")
                task_id = li.get("data-task_id")
                task_slug = li.get("data-task_slug")
                featured = li.get("data-featured") == "true"
                release_attr = li.get("data-release")

                # AI page link + name + version
                ai_link = li.find("a", class_="ai_link")
                ai_name = None
                ai_page = None
                version = None
                if ai_link:
                    ai_page = ai_link.get("href")
                    ai_page = urljoin(BASE_URL, ai_page) if ai_page else None

                    name_span = ai_link.find("span")
                    ai_name = name_span.get_text(" ", strip=True) if name_span else None

                    version_span = ai_link.find("span", class_="current_version")
                    version = version_span.get_text(" ", strip=True) if version_span else None

                # external website
                ext = li.find("a", class_="external_ai_link")
                external_url = ext.get("href") if ext and ext.get("href") else None

                # short description
                desc_tag = li.find("div", class_="short_desc")
                short_desc = desc_tag.get_text(" ", strip=True) if desc_tag else None

                # task label (name + link)
                task_label_tag = li.find("a", class_="task_label")
                task_label_name = task_label_tag.get_text(" ", strip=True) if task_label_tag else None
                task_label_url = (
                    urljoin(BASE_URL, task_label_tag.get("href"))
                    if task_label_tag and task_label_tag.get("href")
                    else None
                )

                # released relative date
                released_relative = None
                released_tag = li.find("div", class_="released")
                if released_tag:
                    rel = released_tag.find("span", class_="relative")
                    released_relative = rel.get_text(" ", strip=True) if rel else None

                # pricing
                price_tag = li.find("a", class_="ai_launch_date")
                pricing = price_tag.get_text(" ", strip=True) if price_tag else None

                # stats (views, saves, rating)
                views = None
                saves = None
                rating = None

                stats = li.find("a", class_="stats")
                if stats:
                    views_tag = stats.select_one(".stats_views span")
                    if views_tag:
                        views = to_int_or_none(views_tag.get_text(strip=True).replace(",", ""))

                    saves_tag = stats.select_one(".saves")
                    if saves_tag:
                        saves = to_int_or_none(saves_tag.get_text(strip=True))

                    rating_tag = stats.select_one(".average_rating")
                    if rating_tag:
                        m = re.search(r"(\d+(?:\.\d+)?)", rating_tag.get_text(" ", strip=True))
                        rating = float(m.group(1)) if m else None

                featured_items.append({
                    "id": item_id,
                    "name": ai_name or name_attr,
                    "version": version,
                    "ai_page": ai_page,
                    "external_url": external_url,
                    "short_desc": short_desc,
                    "task": task_attr,
                    "task_id": task_id,
                    "task_slug": task_slug,
                    "task_label_name": task_label_name,
                    "task_label_url": task_label_url,
                    "featured": featured,
                    "release_info": release_attr,
                    "released_relative": released_relative,
                    "pricing": pricing,
                    "views": views,
                    "saves": saves,
                    "rating": rating
                })

            except Exception as e:
                print(f"[featured li parse] skipped one due to: {e}")
                continue

        record["featured_items_count"] = len(featured_items)
        record["featured_items_json"] = (
            json.dumps(featured_items, ensure_ascii=False)
            if featured_items
            else json.dumps([], ensure_ascii=False)
        )

    except Exception as e:
        print(f"[get_cols] unexpected error on {snapshot_url}: {e}")

    return record

def dispatch_parse(html, snapshot_url):
    year, _ = extract_year_ts(snapshot_url)
    try:
        if year == "2023":
            return parse_2023(html, snapshot_url)
        if year == "2024":
            return parse_2024(html, snapshot_url)
        if year == "2025":
            return parse_2025(html, snapshot_url)
        r = init_record(year)
        r["snapshot_url"] = snapshot_url
        r["error"] = f"unsupported year {year}"
        return r
    except Exception as e:
        r = init_record(year)
        r["snapshot_url"] = snapshot_url
        r["error"] = str(e)
        return r

# ================= MAIN =================

async def main():
    df = pd.read_csv(INPUT_CSV, dtype=str)
    if WAYBACK_COLUMN not in df.columns:
        raise SystemExit("missing_wayback_urls column not found")

    urls = df[WAYBACK_COLUMN].dropna().astype(str).tolist()
    processed = set(json.load(open(CHECKPOINT_FILE))) if os.path.exists(CHECKPOINT_FILE) else set()

    conn = aiohttp.TCPConnector(limit=CONCURRENCY, force_close=True)
    async with aiohttp.ClientSession(connector=conn) as session:

        buffer = []
        lock = asyncio.Lock()
        sem = asyncio.Semaphore(CONCURRENCY)
        remaining = len(urls) - len(processed)
        bar = tqdm(
            total=remaining,
            desc="Snapshots",
            mininterval=0.5,
            smoothing=0.1,
            dynamic_ncols=True
        )



        async def worker(url):
            if url in processed:
                return

            async with sem:
                html, err = await fetch(session, url)

                if html:
                    rec = dispatch_parse(html, url)
                else:
                    y, ts = extract_year_ts(url)
                    rec = init_record(y)
                    rec["snapshot_url"] = url
                    rec["snapshot_timestamp"] = ts
                    rec["error"] = err

                async with lock:
                    buffer.append(rec)
                    processed.add(url)
                    bar.update(1)

                    if len(buffer) >= SAVE_EVERY:
                        df_tmp = pd.DataFrame(buffer, columns=GLOBAL_COLUMNS)
                        if os.path.exists(OUT_CSV):
                            df_old = pd.read_csv(OUT_CSV, dtype=str)
                            df_tmp = pd.concat([df_old, df_tmp], ignore_index=True)
                        df_tmp.to_csv(OUT_CSV, index=False)
                        buffer.clear()
                        json.dump(list(processed), open(CHECKPOINT_FILE, "w"))


        queue = asyncio.Queue()

        for u in urls:
            await queue.put(u)

        async def queue_worker():
            while not queue.empty():
                url = await queue.get()
                try:
                    await worker(url)
                finally:
                    queue.task_done()

        workers = [asyncio.create_task(queue_worker()) for _ in range(CONCURRENCY)]
        await queue.join()

        for w in workers:
            w.cancel()

        if buffer:
            df_new = pd.DataFrame(buffer, columns=GLOBAL_COLUMNS)
            if os.path.exists(OUT_CSV):
                df_old = pd.read_csv(OUT_CSV, dtype=str)
                df_new = pd.concat([df_old, df_new], ignore_index=True)

            df_new.to_csv(OUT_CSV, index=False)

        json.dump(list(processed), open(CHECKPOINT_FILE, "w"))
        bar.close()

if __name__ == "__main__":
    asyncio.run(main())
