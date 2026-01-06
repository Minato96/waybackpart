import os
import time
import random
import logging
import pandas as pd
import requests
from collections import Counter
from urllib.parse import urlsplit, urlunsplit
from tqdm import tqdm

# ---------------- CONFIG ----------------
TIMEMAP_CDX = "https://web.archive.org/web/timemap/cdx"

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")

CSV_IN  = "ai_tools_progress_04012026.csv"
CSV_OUT = "task_audit.csv"

SLEEP_BETWEEN = 0.03
CHECKPOINT_EVERY = 50      # ✅ changed to 50
MAX_RETRIES = 6
TIMEOUT = 30

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": UA})


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

    for variant in normalize_variants(url):
        params = {
            "url": variant,
            "matchType": match_type,
            "fl": "timestamp,original,statuscode",
        }

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                r = sess.get(TIMEMAP_CDX, params=params, headers=headers, timeout=TIMEOUT)
                if r.status_code in (429, 500, 502, 503, 504):
                    raise RuntimeError(f"Transient HTTP {r.status_code}")
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

                break

            except Exception as e:
                if attempt == MAX_RETRIES:
                    log.warning(f"Wayback failed after retries: {url} ({variant})")
                sleep_s = 0.7 * (0.7 + 0.6 * random.random())
                time.sleep(min(sleep_s, 15))

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


def main():
    if os.path.exists(CSV_OUT):
        df = pd.read_csv(CSV_OUT)
        log.info("Resuming from existing output file")
    else:
        df = pd.read_csv(CSV_IN)
        log.info("Loaded fresh input file")

    df.columns = [c.strip() for c in df.columns]

    required_col = "task_label_url"
    if required_col not in df.columns:
        raise ValueError(f"Missing column: {required_col}")

    out_cols = [
        "wayback_blue",
        "wayback_orange",
        "wayback_other",
        "wayback_unknown",
        "wayback_total_numeric_status",
        "wayback_blue_urls",
        "wayback_blue_urls_count",
    ]
    for c in out_cols:
        if c not in df.columns:
            df[c] = pd.NA

    todo = df.index[df["wayback_blue"].isna()].tolist()
    log.info(f"Total rows: {len(df)} | Remaining: {len(todo)}")

    processed = 0

    for idx in tqdm(todo, desc="Wayback audit", unit="url"):
        url = str(df.at[idx, required_col]).strip()

        if not url or url.lower() in ("nan", "none"):
            df.loc[idx, out_cols] = [0, 0, 0, 0, 0, "", 0]
            continue

        try:
            res = wayback_status_breakdown(url, SESSION)

            df.at[idx, "wayback_blue"] = res["blue"]
            df.at[idx, "wayback_orange"] = res["orange"]
            df.at[idx, "wayback_other"] = res["other"]
            df.at[idx, "wayback_unknown"] = res["unknown_status"]
            df.at[idx, "wayback_total_numeric_status"] = res["total_numeric_status"]

            blue_urls = res["snapshots_ok"]
            df.at[idx, "wayback_blue_urls"] = "\n".join(blue_urls)
            df.at[idx, "wayback_blue_urls_count"] = len(blue_urls)

        except Exception as e:
            log.error(f"Row failed idx={idx} url={url} -> {e}")
            df.loc[idx, out_cols] = [0, 0, 0, 0, 0, "", 0]

        processed += 1
        if processed % CHECKPOINT_EVERY == 0:
            df.to_csv(CSV_OUT, index=False)
            log.info(f"Checkpoint saved ({processed} rows)")

        time.sleep(SLEEP_BETWEEN)

    df.to_csv(CSV_OUT, index=False)
    log.info("Audit complete — final file saved")


if __name__ == "__main__":
    main()
