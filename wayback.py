import os
import time
import json
import random
import logging
import pandas as pd
import requests
from collections import Counter
from urllib.parse import urlsplit, urlunsplit
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

# ---------------- CONFIG ----------------
TIMEMAP_CDX = "https://web.archive.org/web/timemap/cdx"

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")

CSV_IN  = "also_searched_generated_urls_task_audit.csv"
CSV_OUT = "also_searched_generated_urls_task_audit_wayback.csv"

SLEEP_BETWEEN = 0.03
CHECKPOINT_EVERY = 50      # ✅ changed to 50
MAX_RETRIES = 6
TIMEOUT = 100

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": UA})

WAYBACK_OUT_COLS = [
    "generated_search_links",
    "wayback_blue",
    "wayback_orange",
    "wayback_other",
    "wayback_unknown",
    "wayback_total_numeric_status",
    "wayback_blue_urls",          # JSON string
    "wayback_blue_urls_count",
]


MAX_BLUE_URLS_TO_STORE = None

# ---------------- LOGGING ----------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("wayback-audit")


def normalize_variants(url: str) -> list[str]:
    p = urlsplit(url)
    base = urlunsplit((p.scheme or "https", p.netloc, p.path.rstrip("/"), "", ""))

    variants = [
        base,
        base + "/",
        base.replace("https://", "http://"),
        base.replace("https://", "http://") + "/",
    ]

    out, seen = [], set()
    for v in variants:
        if v not in seen:
            seen.add(v)
            out.append(v)
    return out


def _safe_int(x):
    try:
        return int(x)
    except Exception:
        return None


def wayback_status_breakdown(url: str, session, *, match_type="exact"):
    sess = session   # reuse session for efficiency
    headers = {"User-Agent": UA}

    seen_keys = set()
    status_counter = Counter()
    ok_snapshots = []
    unknown = 0
    found_any = False


    for variant in normalize_variants(url):
        params = {
            "url": variant,
            "matchType": match_type,
            "fl": "timestamp,original,statuscode",
        }

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                r = sess.get(TIMEMAP_CDX, params=params, headers=headers, timeout=TIMEOUT)
                if r.status_code == 429:
                    raise RuntimeError("RATE_LIMIT")
                if r.status_code in (500, 502, 503, 504):
                    raise RuntimeError(f"SERVER_ERROR_{r.status_code}")

                r.raise_for_status()

                if not r.text.strip():
                    break

                for line in r.text.splitlines():
                    parts = line.split()
                    if len(parts) < 2:
                        continue

                    ts, original = parts[0], parts[1]
                    status_raw = parts[2] if len(parts) >= 3 else None

                    key = (ts, original)
                    if key in seen_keys:
                        continue
                    seen_keys.add(key)

                    status = _safe_int(status_raw)
                    if status is None:
                        unknown += 1
                        continue

                    status_counter[status] += 1
                    if 200 <= status < 400:
                        ok_snapshots.append(
                            f"https://web.archive.org/web/{ts}/{original}"
                        )
                found_any = True


                break

            except RuntimeError as e:
                msg = str(e)

                # 🔁 Infinite retry on rate limit
                if msg == "RATE_LIMIT":
                    sleep_s = random.uniform(20, 60)
                    log.warning(f"429 rate limit — sleeping {sleep_s:.1f}s and retrying: {url}")
                    time.sleep(sleep_s)
                    continue

                # 🔁 Limited retry on server errors
                if attempt < MAX_RETRIES:
                    sleep_s = (0.7 * (2 ** (attempt - 1))) * random.uniform(0.7, 1.3)
                    time.sleep(min(sleep_s, 30))
                    continue

                log.warning(f"Wayback failed after retries: {url} ({variant})")
                break

        if found_any:
            break   # 👈 STOP trying other URL variants


    blue = sum(v for k, v in status_counter.items() if 200 <= k < 400)
    orange = sum(v for k, v in status_counter.items() if 400 <= k < 500)
    other = sum(v for k, v in status_counter.items() if k < 200 or k >= 500)

    snapshots_ok = sorted(set(ok_snapshots))
    if MAX_BLUE_URLS_TO_STORE is not None:
        snapshots_ok = snapshots_ok[:MAX_BLUE_URLS_TO_STORE]

    return {
        "blue": blue,
        "orange": orange,
        "other": other,
        "unknown_status": unknown,
        "total_numeric_status": blue + orange + other,
        "snapshots_ok": snapshots_ok,
    }

def process_row(idx, url):
    if not url or url.lower() in ("nan", "none"):
        return idx, {
            "wayback_blue": 0,
            "wayback_orange": 0,
            "wayback_other": 0,
            "wayback_unknown": 0,
            "wayback_total_numeric_status": 0,
            "wayback_blue_urls": "",
            "wayback_blue_urls_count": 0,
        }

    try:
        res = wayback_status_breakdown(url, SESSION)
        return idx, {
            "wayback_blue": res["blue"],
            "wayback_orange": res["orange"],
            "wayback_other": res["other"],
            "wayback_unknown": res["unknown_status"],
            "wayback_total_numeric_status": res["total_numeric_status"],
            "wayback_blue_urls": json.dumps(res["snapshots_ok"], ensure_ascii=False),
            "wayback_blue_urls_count": len(res["snapshots_ok"]),
        }
    except Exception as e:
        log.error(f"Row failed idx={idx} url={url} -> {e}")
        return idx, {
            "wayback_blue": 0,
            "wayback_orange": 0,
            "wayback_other": 0,
            "wayback_unknown": 0,
            "wayback_total_numeric_status": 0,
            "wayback_blue_urls": "",
            "wayback_blue_urls_count": 0,
        }


def main():
    df_in = pd.read_csv(CSV_IN)
    df_in.columns = [c.strip() for c in df_in.columns]

    if "generated_search_links" not in df_in.columns:
        raise ValueError("Input CSV must contain 'generated_search_links'")

    # Load or create clean Wayback output
    if os.path.exists(CSV_OUT):
        df_out = pd.read_csv(CSV_OUT)
        log.info("Resuming from existing Wayback output file")
    else:
        df_out = pd.DataFrame(columns=WAYBACK_OUT_COLS)
        log.info("Created new Wayback output file")

    done_urls = set(df_out["generated_search_links"]) if not df_out.empty else set()

    # Build todo list from INPUT only
    todo = [
        (idx, str(url).strip())
        for idx, url in df_in["generated_search_links"].items()
        if str(url).strip() and str(url).strip() not in done_urls
    ]

    log.info(f"Total URLs: {len(df_in)} | Remaining: {len(todo)}")

    processed = 0
    MAX_WORKERS = 6

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [
            executor.submit(process_row, idx, url)
            for idx, url in todo
        ]

        for future in tqdm(
            as_completed(futures),
            total=len(futures),
            desc="Wayback audit",
            unit="url"
        ):
            idx, result = future.result()

            raw_url = df_in.at[idx, "generated_search_links"]

            if pd.isna(raw_url):
                continue

            url = str(raw_url).strip()
            if not url or url.lower() == "nan":
                continue

            url = url.rstrip("/")



            row = {"generated_search_links": df_in.at[idx, "generated_search_links"]}
            row.update(result)

            df_out = pd.concat(
                [df_out, pd.DataFrame([row])],
                ignore_index=True
            )

            processed += 1
            if processed % CHECKPOINT_EVERY == 0:
                df_out.to_csv(CSV_OUT, index=False)
                log.info(f"Checkpoint saved ({processed} rows)")

        # Final safety net: ensure one row per generated_search_links
    df_out = df_out.drop_duplicates(
        subset=["generated_search_links"],
        keep="first"
    )

    df_out.to_csv(CSV_OUT, index=False)
    log.info("Audit complete — final Wayback file saved")




if __name__ == "__main__":
    main()
