# ============================================================
# DAILY UPDATE (robust):
# - Scrape only current + previous month /period/ pages
# - Wait + scroll for lazy-loaded cards
# - Canonicalize tool URLs to /ai/<slug>
# - Append only truly new URLs into taaft_tools_2015_2025.csv
# - Optional debug dumps when pages look blocked/empty
# ============================================================

import os
import re
import time
import pandas as pd
from datetime import datetime
from urllib.parse import urlsplit, urlunsplit

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# ---------------- CONFIG ----------------
BASE = "https://theresanaiforthat.com"
CSV_PATH = "taaft_tools_2015_2025.csv"

HEADLESS = True
SETTLE_SEC = 0.3              # used only for tiny settle after scroll
PAGE_DELAY_SEC = 0.5
MAX_PAGES_CAP = 30

# keep current + previous month (update manually)
PERIOD_SLUGS = ["just-released"]

# Debug: save HTML for the first page of each period if no anchors found
DEBUG_DUMP_ON_EMPTY = True
DEBUG_DIR = "taaft_debug_html"
# --------------------------------------


# ---------------- DRIVER ----------------
def build_driver(headless: bool = True) -> webdriver.Chrome:
    opts = Options()

    if headless:
        opts.add_argument("--headless=new")

    # Stability / performance
    opts.add_argument("--window-size=1920,1200")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")

    # Reduce “automation obviousness” (helps sometimes)
    opts.add_argument("--lang=en-US")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    opts.add_argument(
        "user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    )

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=opts)

    # Hide navigator.webdriver
    try:
        driver.execute_cdp_cmd(
            "Page.addScriptToEvaluateOnNewDocument",
            {"source": "Object.defineProperty(navigator,'webdriver',{get:()=>undefined});"},
        )
    except Exception:
        pass

    driver.set_page_load_timeout(45)
    return driver


# ---------------- URL HELPERS ----------------
def normalize_url(u: str) -> str:
    parts = urlsplit(u)
    return urlunsplit((parts.scheme, parts.netloc, parts.path.rstrip("/"), "", ""))


def canonical_tool_url(u: str) -> str:
    """
    Accept anything containing /ai/<slug> and canonicalize to:
      https://theresanaiforthat.com/ai/<slug>
    This survives site changes like /ai/<slug>/reviews or query params.
    """
    parts = urlsplit(u)
    m = re.search(r"(/ai/[a-z0-9\-]+)", parts.path, re.I)
    if not m:
        return urlunsplit((parts.scheme, parts.netloc, parts.path.rstrip("/"), "", ""))
    return urlunsplit((parts.scheme, parts.netloc, m.group(1).rstrip("/"), "", ""))


def guess_name_from_context(text: str, url: str) -> str:
    text = (text or "").strip()
    if text and not re.fullmatch(r"(read more|view|open|details|visit|learn more)", text, re.I):
        return re.sub(r"\s+", " ", text)

    m = re.search(r"/ai/([^/]+)/?$", url)
    if m:
        return re.sub(r"[-_]+", " ", m.group(1)).strip().title()
    return url


# ---------------- PAGE HARVESTING ----------------
def harvest_listing_links_js(driver) -> list[dict]:
    """
    Collect all anchors containing /ai/ from the DOM.
    We do NOT enforce strict /ai/<slug> shape in JS; we canonicalize in Python.
    """
    script = r"""
    const out = [];
    const anchors = Array.from(document.querySelectorAll('a[href*="/ai/"]'));
    const seen = new Set();

    function abs(u){
      try { return new URL(u, window.location.origin).href; }
      catch(e){ return null; }
    }

    for (const a of anchors){
      const href = abs(a.getAttribute("href") || "");
      if(!href) continue;
      if(seen.has(href)) continue;
      seen.add(href);

      let label = (a.textContent||'').trim()
               || a.getAttribute('aria-label')
               || a.getAttribute('title')
               || '';

      if(!label){
        const card = a.closest('article, .card, .tool-card, .listing__item, li, .box, .result, .entry');
        if(card){
          const h = card.querySelector('h1,h2,h3,.title,.tool-title');
          if(h) label = (h.textContent||'').trim();
        }
      }
      out.push({name: label, url: href});
    }
    return out;
    """
    return driver.execute_script(script)


def scroll_to_load(driver, max_rounds: int = 8, pause: float = 0.8) -> None:
    """
    Scroll until the page height stops growing (lazy-loading trigger).
    """
    last_h = driver.execute_script("return document.body.scrollHeight") or 0
    for _ in range(max_rounds):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(pause)
        new_h = driver.execute_script("return document.body.scrollHeight") or 0
        if new_h <= last_h + 5:
            break
        last_h = new_h
    time.sleep(SETTLE_SEC)


def dump_debug_html(period_slug: str, p: int, html: str) -> None:
    if not DEBUG_DUMP_ON_EMPTY:
        return
    os.makedirs(DEBUG_DIR, exist_ok=True)
    fp = os.path.join(DEBUG_DIR, f"debug_{period_slug}_p{p}.html")
    with open(fp, "w", encoding="utf-8") as f:
        f.write(html or "")


def scrape_just_released(driver, existing_urls: set[str]) -> pd.DataFrame:
    new_rows: list[dict] = []
    wait = WebDriverWait(driver, 20)

    page_url = f"{BASE}/just-released/"
    driver.get(page_url)

    # Wait for real cards, not navbar junk
    wait.until(lambda d: len(
        d.find_elements(By.CSS_SELECTOR, 'article a[href*="/ai/"]')
    ) >= 5)

    # Aggressive scroll for infinite load
    scroll_to_load(driver, max_rounds=20, pause=1.0)

    items = harvest_listing_links_js(driver)

    for it in items:
        raw_url = it.get("url") or ""
        if "/ai/" not in raw_url:
            continue

        url = canonical_tool_url(raw_url)
        if url in existing_urls:
            continue

        name = guess_name_from_context(it.get("name", ""), url)

        existing_urls.add(url)
        new_rows.append({
            "tool_name": name,
            "tool_url": url,
            "period": "just-released"
        })

    return pd.DataFrame(new_rows)


# ---------------- MAIN ----------------
def main():
    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(f"Missing {CSV_PATH}")

    df = pd.read_csv(CSV_PATH)
    df.columns = [c.strip() for c in df.columns]

    if "tool_url" not in df.columns:
        raise ValueError("CSV must contain column 'tool_url'")

    if "tool_name" not in df.columns:
        # not fatal; just ensure it exists
        df["tool_name"] = ""

    df["tool_url"] = df["tool_url"].astype(str).map(canonical_tool_url)

    prev_n = len(df)
    existing_urls = set(df["tool_url"].dropna().astype(str).tolist())
    print(f"[BASE] rows before: {prev_n}")

    driver = build_driver(headless=HEADLESS)
    all_new = []
    try:
        for slug in PERIOD_SLUGS:
            print(f"\nScraping /period/{slug}/ ...")
            df_new = scrape_just_released(driver, existing_urls)
            print(f"New URLs from {slug}: {len(df_new)}")
            all_new.append(df_new)
    finally:
        driver.quit()

    new_df = pd.concat(all_new, ignore_index=True) if all_new else pd.DataFrame()

    # Assign year (simple rule: current year; adjust if you cross Jan and want previous year)
    year_now = datetime.now().year

    if len(new_df):
        new_df.insert(0, "year", year_now)

        # produce output with only the columns you want appended
        append_df = new_df[["year", "tool_name", "tool_url"]].copy()
        append_df["tool_url"] = append_df["tool_url"].astype(str).map(canonical_tool_url)
        append_df["tool_name"] = append_df["tool_name"].astype(str)

        out = pd.concat([df, append_df], ignore_index=True)
        out["tool_url"] = out["tool_url"].astype(str).map(canonical_tool_url)
        out["tool_name"] = out["tool_name"].astype(str)

        # dedupe by URL (keep first existing)
        out = out.drop_duplicates(subset=["tool_url"], keep="first").reset_index(drop=True)

        out.to_csv(CSV_PATH, index=False)

        added = len(append_df)
        print(f"\n[UPDATE] added {added} new tools. final rows: {len(out)}. saved -> {CSV_PATH}")

        # Print sample
        print("\n[NEW SAMPLE]")
        print(append_df.head(50).to_string(index=False))
    else:
        print("\n[UPDATE] added 0 new tools (no changes).")
        if HEADLESS:
            print("If you *expect* new tools today, try running once with HEADLESS=False to test for headless blocking.")
        if DEBUG_DUMP_ON_EMPTY:
            print(f"If warnings occurred, inspect HTML dumps in: {os.path.abspath(DEBUG_DIR)}")


if __name__ == "__main__":
    main()
