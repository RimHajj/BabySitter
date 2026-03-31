#!/usr/bin/env python3
"""
UR Housing Chuo Ward Monitor
============================
Monitors https://www.ur-net.go.jp/chintai/kanto/tokyo/area/102.html for
available units and sends an HTML email summary when new or newly-available
listings are detected.

Usage:
  python ur_monitor.py --once     # single check, then exit
  python ur_monitor.py --status   # print DB contents, no scraping
  python ur_monitor.py            # loop forever (CHECK_INTERVAL_MINUTES)

Required env vars (only for email; scraping always runs):
  GMAIL_ADDRESS       - sender Gmail address
  GMAIL_APP_PASSWORD  - Gmail App Password (not your account password)
  NOTIFY_EMAIL        - recipient address

Optional env vars:
  CHECK_INTERVAL_MINUTES  - loop sleep time (default 60)
"""

import argparse
import hashlib
import json
import logging
import os
import re
import smtplib
import sqlite3
import sys
import time
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any, Optional

# ── Constants ─────────────────────────────────────────────────────────────────
TARGET_URL = "https://www.ur-net.go.jp/chintai/kanto/tokyo/area/102.html"
BASE_URL   = "https://www.ur-net.go.jp"
DB_FILE    = "ur_chuo_listings.db"
LOG_FILE   = "ur_monitor.log"
LAST_RUN_FILE = "last_run.txt"

# Substrings that indicate an available unit in Japanese or English
AVAILABLE_MARKERS = (
    "空室あり", "申込受付中", "即入居可", "受付中",
    "空き", "available", "vacant",
)

# ── Env config ────────────────────────────────────────────────────────────────
GMAIL_ADDRESS      = os.environ.get("GMAIL_ADDRESS", "")
GMAIL_APP_PASSWORD = os.environ.get("GMAIL_APP_PASSWORD", "")
NOTIFY_EMAIL       = os.environ.get("NOTIFY_EMAIL", "")
CHECK_INTERVAL_MINUTES = int(os.environ.get("CHECK_INTERVAL_MINUTES", "60"))

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════════════

def make_id(*parts: str) -> str:
    """Stable 16-hex-char ID from field values."""
    combined = "|".join(p.strip() for p in parts)
    return hashlib.sha1(combined.encode("utf-8")).hexdigest()[:16]


def status_is_available(status: str) -> bool:
    return any(m in status for m in AVAILABLE_MARKERS)


def abs_url(href: str) -> str:
    if not href:
        return ""
    return href if href.startswith("http") else BASE_URL + href


# ═══════════════════════════════════════════════════════════════════════════════
# Database
# ═══════════════════════════════════════════════════════════════════════════════

def init_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS listings (
            id            TEXT PRIMARY KEY,
            property_name TEXT NOT NULL,
            address       TEXT,
            room_number   TEXT,
            floor_plan    TEXT,
            rent          TEXT,
            status        TEXT,
            detail_url    TEXT,
            first_seen    TEXT NOT NULL,
            last_seen     TEXT NOT NULL,
            prev_status   TEXT
        );
        CREATE TABLE IF NOT EXISTS runs (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            run_time       TEXT NOT NULL,
            listings_found INTEGER DEFAULT 0,
            new_listings   INTEGER DEFAULT 0,
            status_changes INTEGER DEFAULT 0,
            error          TEXT
        );
    """)
    conn.commit()
    return conn


def upsert_listing(conn: sqlite3.Connection, listing: dict) -> tuple:
    """
    Insert or update one listing row.
    Returns (is_new: bool, became_available: bool).
    """
    existing = conn.execute(
        "SELECT status FROM listings WHERE id = ?", (listing["id"],)
    ).fetchone()
    now = datetime.utcnow().isoformat()

    if existing is None:
        conn.execute(
            """INSERT INTO listings
               (id, property_name, address, room_number, floor_plan,
                rent, status, detail_url, first_seen, last_seen, prev_status)
               VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (
                listing["id"], listing["property_name"], listing["address"],
                listing["room_number"], listing["floor_plan"], listing["rent"],
                listing["status"], listing["detail_url"], now, now, None,
            ),
        )
        conn.commit()
        return True, False

    prev = existing["status"] or ""
    became_available = (
        not status_is_available(prev)
        and status_is_available(listing["status"])
    )
    conn.execute(
        """UPDATE listings SET
               property_name=?, address=?, room_number=?, floor_plan=?,
               rent=?, status=?, detail_url=?, last_seen=?, prev_status=?
           WHERE id=?""",
        (
            listing["property_name"], listing["address"],
            listing["room_number"], listing["floor_plan"],
            listing["rent"], listing["status"], listing["detail_url"],
            now, prev, listing["id"],
        ),
    )
    conn.commit()
    return False, became_available


# ═══════════════════════════════════════════════════════════════════════════════
# Scraping  (Playwright + network interception, with HTML fallback)
# ═══════════════════════════════════════════════════════════════════════════════

def scrape_listings() -> list[dict]:
    """
    Load the UR area page with Playwright, intercept JSON API responses,
    and extract property listings.  Falls back to HTML DOM parsing if no
    usable JSON is captured.
    """
    from playwright.sync_api import sync_playwright
    from bs4 import BeautifulSoup

    captured: list[dict] = []   # {"url": ..., "body": ...}
    final_html = ""

    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
        )
        ctx = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            locale="ja-JP",
            extra_http_headers={"Accept-Language": "ja,en;q=0.9"},
        )
        page = ctx.new_page()

        # ── Intercept every JSON response from the UR domain ──────────────────
        def on_response(resp):
            try:
                if resp.status != 200:
                    return
                ct = resp.headers.get("content-type", "")
                if "json" not in ct:
                    return
                url = resp.url
                if "ur-net.go.jp" not in url:
                    return
                body = resp.json()
                captured.append({"url": url, "body": body})
                log.debug("Captured JSON from %s", url)
            except Exception:
                pass

        page.on("response", on_response)

        # ── Load the target page ──────────────────────────────────────────────
        log.info("Loading %s", TARGET_URL)
        page.goto(TARGET_URL, wait_until="networkidle", timeout=90_000)

        # Wait a little longer for deferred JS to settle
        page.wait_for_timeout(3_000)

        # ── Collect pages if pagination exists ────────────────────────────────
        for page_num in range(1, 21):   # max 20 pages
            final_html += page.content()
            log.debug("Collected HTML page %d", page_num)

            # Look for a "next page" control
            next_sel = (
                "a:has-text('次'), "
                "a:has-text('次へ'), "
                "a[aria-label*='次'], "
                ".pagination .next:not(.disabled), "
                "li.next:not(.disabled) a"
            )
            nxt = page.locator(next_sel)
            if nxt.count() == 0:
                break
            try:
                nxt.first.click()
                page.wait_for_load_state("networkidle", timeout=20_000)
                page.wait_for_timeout(2_000)
            except Exception as exc:
                log.debug("Pagination click failed: %s", exc)
                break

        browser.close()

    # ── 1. Try captured JSON responses ────────────────────────────────────────
    for item in captured:
        listings = _parse_json_blob(item["body"])
        if listings:
            log.info("Parsed %d listings from JSON: %s", len(listings), item["url"])
            return listings

    # ── 2. Fallback: parse rendered HTML ─────────────────────────────────────
    log.info("No usable JSON API response; falling back to HTML parsing")
    soup = BeautifulSoup(final_html, "lxml")
    listings = _parse_html(soup)
    log.info("Parsed %d listings from HTML", len(listings))
    return listings


# ── JSON parsing ──────────────────────────────────────────────────────────────

def _parse_json_blob(data: Any) -> list[dict]:
    """
    Try to extract a list of listings from an arbitrary JSON blob.
    Returns [] if the blob doesn't look like property data.
    """
    items: list = []

    if isinstance(data, list):
        items = data
    elif isinstance(data, dict):
        # Try well-known top-level keys first
        for key in ("bukken", "result", "data", "list", "items",
                    "properties", "rooms", "heya", "chinryo"):
            if key in data and isinstance(data[key], list):
                items = data[key]
                break
        # Generic fallback: first list-of-dicts value
        if not items:
            for val in data.values():
                if isinstance(val, list) and val and isinstance(val[0], dict):
                    items = val
                    break

    if not items:
        return []

    listings = []
    for item in items:
        if not isinstance(item, dict):
            continue
        parsed = _extract_json_item(item)
        if parsed:
            listings.append(parsed)
    return listings


def _get(d: dict, *keys: str) -> str:
    for k in keys:
        v = d.get(k)
        if v is not None and str(v).strip():
            return str(v).strip()
    return ""


def _extract_json_item(item: dict) -> Optional[dict]:
    name = _get(
        item,
        "bukken_name", "bukkenName", "name", "property_name",
        "propertyName", "bukken_nm", "danchi_name",
    )
    if not name:
        return None

    address    = _get(item, "address", "jusho", "location", "addr", "jusho_nm")
    room_num   = _get(item, "room_number", "roomNumber", "room", "heya", "room_no", "heya_no")
    floor_plan = _get(item, "madori", "floor_plan", "floorPlan", "room_type",
                      "roomType", "layout", "madori_nm")
    rent       = _get(item, "rent", "yachin", "chinryo", "price", "amount", "yachin_kingaku")
    status     = _get(item, "status", "availability", "kushitsu",
                      "room_status", "akibeya_status")
    url        = _get(item, "url", "detail_url", "detailUrl", "link", "href", "detail_link")

    return {
        "id":            make_id(name, address, room_num, floor_plan),
        "property_name": name,
        "address":       address,
        "room_number":   room_num,
        "floor_plan":    floor_plan,
        "rent":          rent,
        "status":        status,
        "detail_url":    abs_url(url),
    }


# ── HTML parsing ──────────────────────────────────────────────────────────────

# Ordered list of CSS selectors to try for "one building block"
_BLOCK_SELECTORS = [
    "section.is-property",
    "div.bukken-cassette",
    "div.cassette",
    "div.bukken",
    "li.bukken-item",
    "div[class*='cassette']",
    "div[class*='bukken']",
    "section[class*='property']",
    "li[class*='property']",
    "article",
]

# Ordered list of CSS selectors to try for "one room row inside a block"
_ROW_SELECTORS = [
    "tr.room-row",
    "tr[class*='room']",
    "tr[class*='heya']",
    "tbody tr",
    "tr",
    "div[class*='room-item']",
    "li[class*='room']",
]


def _txt(el) -> str:
    return el.get_text(" ", strip=True) if el else ""


def _first_txt(parent, *selectors) -> str:
    for sel in selectors:
        el = parent.select_one(sel)
        if el:
            return _txt(el)
    return ""


def _first_href(parent) -> str:
    a = parent.select_one("a[href]")
    return abs_url(a["href"]) if a else ""


def _parse_html(soup) -> list[dict]:
    # ── Find building blocks ──────────────────────────────────────────────────
    blocks = []
    used_sel = ""
    for sel in _BLOCK_SELECTORS:
        found = soup.select(sel)
        if found:
            blocks = found
            used_sel = sel
            break

    if not blocks:
        log.warning("No property blocks found in rendered HTML")
        snippet = soup.get_text(" ", strip=True)[:400]
        log.debug("Page text snippet: %s", snippet)
        return []

    log.info("Block selector '%s' → %d blocks", used_sel, len(blocks))

    listings = []
    for block in blocks:
        prop_name = _first_txt(
            block,
            "[class*='name']", "[class*='title']",
            "h2", "h3", "h4",
            ".bukken-name", ".property-name",
        )
        address = _first_txt(
            block,
            "[class*='address']", "[class*='jusho']",
            "[class*='location']", "address",
        )

        # ── Try to find individual room rows ──────────────────────────────────
        rows = []
        for row_sel in _ROW_SELECTORS:
            rows = block.select(row_sel)
            if rows:
                break

        if rows:
            for row in rows:
                cells = row.select("td, [class*='cell']")
                if len(cells) < 2:
                    continue
                cell_texts = [_txt(c) for c in cells]

                room_number = ""
                floor_plan  = ""
                rent        = ""
                status      = ""

                for t in cell_texts:
                    if re.search(r"\d+\s*(号室|棟|階)", t):
                        room_number = room_number or t
                    elif re.search(r"^[0-9０-９]+[LDKSRldksr]+$|[LDKSRldksr]{1,5}", t):
                        floor_plan = floor_plan or t
                    elif re.search(r"[\d,，]+\s*円|万円", t):
                        rent = rent or t
                    elif any(m in t for m in ("空室", "入居可", "受付", "募集停止", "空き", "満室")):
                        status = status or t

                if not (floor_plan or rent or room_number):
                    continue

                listings.append({
                    "id":            make_id(prop_name, address, room_number, floor_plan),
                    "property_name": prop_name,
                    "address":       address,
                    "room_number":   room_number,
                    "floor_plan":    floor_plan,
                    "rent":          rent,
                    "status":        status,
                    "detail_url":    _first_href(row),
                })

        else:
            # No rows → single entry for the whole building card
            rent_text   = _first_txt(block, "[class*='rent']", "[class*='yachin']", "[class*='price']")
            status_text = _first_txt(block, "[class*='status']", "[class*='kushitsu']", "[class*='availability']")
            floor_text  = _first_txt(block, "[class*='madori']", "[class*='floor']", "[class*='plan']")

            if prop_name:
                listings.append({
                    "id":            make_id(prop_name, address, floor_text),
                    "property_name": prop_name,
                    "address":       address,
                    "room_number":   "",
                    "floor_plan":    floor_text,
                    "rent":          rent_text,
                    "status":        status_text,
                    "detail_url":    _first_href(block),
                })

    return listings


# ═══════════════════════════════════════════════════════════════════════════════
# Email notification
# ═══════════════════════════════════════════════════════════════════════════════

def send_email(new_listings: list[dict], status_changed: list[dict]) -> None:
    if not (GMAIL_ADDRESS and GMAIL_APP_PASSWORD and NOTIFY_EMAIL):
        log.warning(
            "Email env vars not set (GMAIL_ADDRESS / GMAIL_APP_PASSWORD / NOTIFY_EMAIL). "
            "Skipping notification."
        )
        return

    total = len(new_listings) + len(status_changed)
    subject = (
        f"[UR中央区] {total}件の新着・空室情報 "
        f"| {datetime.now().strftime('%Y-%m-%d %H:%M')}"
    )

    def table_html(rows: list[dict], heading: str) -> str:
        if not rows:
            return ""
        tr_html = ""
        for r in rows:
            link = f'<a href="{r["detail_url"]}">詳細</a>' if r["detail_url"] else "—"
            tr_html += (
                f"<tr>"
                f"<td>{r['property_name']}</td>"
                f"<td>{r['address']}</td>"
                f"<td>{r['room_number']}</td>"
                f"<td>{r['floor_plan']}</td>"
                f"<td>{r['rent']}</td>"
                f'<td style="color:#276749;font-weight:bold">{r["status"]}</td>'
                f"<td>{link}</td>"
                f"</tr>"
            )
        return f"""
        <h2 style="color:#2b6cb0;margin-top:24px">{heading}</h2>
        <table cellpadding="7" cellspacing="0"
               style="border-collapse:collapse;width:100%;font-size:13px;
                      border:1px solid #bee3f8">
          <thead style="background:#ebf8ff;text-align:left">
            <tr>
              <th>物件名</th><th>住所</th><th>部屋番号</th>
              <th>間取り</th><th>家賃</th><th>空室状況</th><th>詳細</th>
            </tr>
          </thead>
          <tbody>{tr_html}</tbody>
        </table>"""

    body_html = f"""
    <html>
    <body style="font-family:sans-serif;max-width:960px;margin:auto;padding:20px;color:#1a202c">
      <h1 style="color:#1a365d">🏠 UR賃貸 中央区 空室情報アラート</h1>
      <p>チェック日時: <strong>{datetime.now().strftime('%Y年%m月%d日 %H:%M')}</strong></p>
      <p>対象ページ: <a href="{TARGET_URL}">{TARGET_URL}</a></p>
      {table_html(new_listings,     "🆕 新着物件")}
      {table_html(status_changed,   "✅ 空室に変わった物件")}
      <hr style="margin-top:32px">
      <p style="color:#718096;font-size:11px">
        このメールは自動送信されています（UR賃貸モニター）。
      </p>
    </body>
    </html>"""

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = GMAIL_ADDRESS
    msg["To"]      = NOTIFY_EMAIL
    msg.attach(MIMEText(body_html, "html", "utf-8"))

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=30) as srv:
            srv.login(GMAIL_ADDRESS, GMAIL_APP_PASSWORD)
            srv.sendmail(GMAIL_ADDRESS, [NOTIFY_EMAIL], msg.as_bytes())
        log.info("Email sent → %s | %s", NOTIFY_EMAIL, subject)
    except Exception as exc:
        log.error("Failed to send email: %s", exc)


# ═══════════════════════════════════════════════════════════════════════════════
# Core check cycle
# ═══════════════════════════════════════════════════════════════════════════════

def run_check() -> None:
    run_time = datetime.utcnow().isoformat()
    log.info("=" * 60)
    log.info("Check started at %s", run_time)

    conn = init_db()
    new_list: list[dict]     = []
    changed_list: list[dict] = []
    error_msg: Optional[str] = None

    try:
        listings = scrape_listings()
        log.info("Total listings scraped: %d", len(listings))

        for lst in listings:
            is_new, became_avail = upsert_listing(conn, lst)
            tag = ""
            if is_new:
                new_list.append(lst)
                tag = "NEW"
            elif became_avail:
                changed_list.append(lst)
                tag = "NOW AVAILABLE"
            if tag:
                log.info(
                    "  [%s] %s | %s | %s | %s",
                    tag, lst["property_name"], lst["floor_plan"],
                    lst["rent"], lst["status"],
                )

        log.info(
            "Result → new=%d, status_changed=%d",
            len(new_list), len(changed_list),
        )

        if new_list or changed_list:
            send_email(new_list, changed_list)

        conn.execute(
            "INSERT INTO runs (run_time, listings_found, new_listings, status_changes) "
            "VALUES (?,?,?,?)",
            (run_time, len(listings), len(new_list), len(changed_list)),
        )

    except Exception as exc:
        log.error("Check failed: %s", exc, exc_info=True)
        error_msg = str(exc)
        conn.execute(
            "INSERT INTO runs (run_time, error) VALUES (?,?)",
            (run_time, error_msg),
        )

    finally:
        conn.commit()
        conn.close()

    # Always write last_run.txt (keeps GitHub Actions workflow alive)
    with open(LAST_RUN_FILE, "w", encoding="utf-8") as fh:
        fh.write(datetime.utcnow().isoformat() + "\n")

    log.info("Check complete.")


# ═══════════════════════════════════════════════════════════════════════════════
# --status mode
# ═══════════════════════════════════════════════════════════════════════════════

def print_status() -> None:
    conn = init_db()
    rows = conn.execute(
        "SELECT * FROM listings ORDER BY property_name, room_number"
    ).fetchall()

    if not rows:
        print("Database is empty — no listings stored yet.")
        conn.close()
        return

    W = 80
    print("\n" + "=" * W)
    print(f"{'物件名':<22} {'間取り':<8} {'家賃':<12} {'状況':<16} {'最終確認(UTC)'}")
    print("=" * W)
    for r in rows:
        print(
            f"{str(r['property_name'])[:22]:<22} "
            f"{str(r['floor_plan'])[:8]:<8} "
            f"{str(r['rent'])[:12]:<12} "
            f"{str(r['status'])[:16]:<16} "
            f"{str(r['last_seen'])[:19]}"
        )
    print("=" * W)
    print(f"Total: {len(rows)} listings\n")

    runs = conn.execute(
        "SELECT * FROM runs ORDER BY id DESC LIMIT 5"
    ).fetchall()
    if runs:
        print("Recent runs (latest first):")
        for run in runs:
            err = f" ❌ {run['error']}" if run["error"] else ""
            print(
                f"  {run['run_time']}  "
                f"found={run['listings_found']}  "
                f"new={run['new_listings']}  "
                f"changed={run['status_changes']}"
                f"{err}"
            )
    conn.close()


# ═══════════════════════════════════════════════════════════════════════════════
# Entry point
# ═══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    parser = argparse.ArgumentParser(
        description="UR Housing Chuo Ward Monitor"
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run a single check and exit",
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="Print current listings from DB without checking",
    )
    args = parser.parse_args()

    if args.status:
        print_status()
        return

    if args.once:
        run_check()
        return

    log.info(
        "Starting continuous monitoring loop (interval=%d min)",
        CHECK_INTERVAL_MINUTES,
    )
    while True:
        run_check()
        log.info("Sleeping %d minutes ...", CHECK_INTERVAL_MINUTES)
        time.sleep(CHECK_INTERVAL_MINUTES * 60)


if __name__ == "__main__":
    main()
