#!/usr/bin/env python3
"""SmartSitter.jp scraper – finds babysitters in Tokyo 23 wards near Harumi."""

import argparse
import datetime as dt
import email.mime.multipart
import email.mime.text
import html
import math
import os
import re
import smtplib
import sqlite3
import ssl
import sys
import time

import requests
from playwright.sync_api import sync_playwright, TimeoutError as PwTimeout

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
BASE = "https://smartsitter.jp"
HARUMI_LAT, HARUMI_LON = 35.6544, 139.7855
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sitters.db")
GSI_API = "https://msearch.gsi.go.jp/address-search/AddressSearch"
GMAIL_SMTP_HOST = "smtp.gmail.com"
GMAIL_SMTP_PORT = 465  # SSL

TOKYO_23_WARDS = {
    13101: "千代田区", 13102: "中央区", 13103: "港区", 13104: "新宿区",
    13105: "文京区", 13106: "台東区", 13107: "墨田区", 13108: "江東区",
    13109: "品川区", 13110: "目黒区", 13111: "大田区", 13112: "世田谷区",
    13113: "渋谷区", 13114: "中野区", 13115: "杉並区", 13116: "豊島区",
    13117: "北区",  13118: "荒川区", 13119: "板橋区", 13120: "練馬区",
    13121: "足立区", 13122: "葛飾区", 13123: "江戸川区",
}
WARD_NAMES = set(TOKYO_23_WARDS.values())

STATION_RE = re.compile(
    r"(?:最寄り?駅|最寄駅)[：:\s]*([^\s、。,.(）)]+?)(?:駅|$)|"
    r"([^\s、。,.(）)]+?)駅(?:より|から|まで|徒歩|周辺)"
)
TIME_RE = re.compile(
    r"(\d{1,2})[時:：](\d{0,2})?\s*[～〜~ー−-]\s*(\d{1,2})[時:：](\d{0,2})?"
)

# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def init_db(path: str = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS sitters (
            id             INTEGER PRIMARY KEY,
            tagline        TEXT,
            location_text  TEXT,
            service_area   TEXT,
            station_parsed TEXT,
            lat            REAL,
            lon            REAL,
            distance_km    REAL,
            profile_url    TEXT,
            self_intro     TEXT,
            first_seen     TEXT,
            last_seen      TEXT
        );
        CREATE TABLE IF NOT EXISTS availability (
            sitter_id   INTEGER,
            date        TEXT,
            status      TEXT,
            noted_hours TEXT,
            scraped_at  TEXT,
            PRIMARY KEY (sitter_id, date, scraped_at),
            FOREIGN KEY (sitter_id) REFERENCES sitters(id)
        );
        CREATE TABLE IF NOT EXISTS geocode_cache (
            query     TEXT PRIMARY KEY,
            lat       REAL,
            lon       REAL,
            cached_at TEXT
        );
    """)
    conn.commit()
    return conn


def upsert_sitter(conn, sid, tagline, location_text, service_area,
                  station_parsed, lat, lon, dist, profile_url, self_intro):
    now = dt.datetime.now().isoformat()
    row = conn.execute("SELECT id FROM sitters WHERE id=?", (sid,)).fetchone()
    if row:
        conn.execute(
            """UPDATE sitters
               SET tagline=?, location_text=?, service_area=?, station_parsed=?,
                   lat=?, lon=?, distance_km=?, profile_url=?, self_intro=?, last_seen=?
               WHERE id=?""",
            (tagline, location_text, service_area, station_parsed,
             lat, lon, dist, profile_url, self_intro, now, sid),
        )
    else:
        conn.execute(
            """INSERT INTO sitters
               (id, tagline, location_text, service_area, station_parsed,
                lat, lon, distance_km, profile_url, self_intro, first_seen, last_seen)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            (sid, tagline, location_text, service_area, station_parsed,
             lat, lon, dist, profile_url, self_intro, now, now),
        )


def insert_availability(conn, sid, date_str, status, noted_hours):
    now = dt.datetime.now().isoformat()
    conn.execute(
        """INSERT OR REPLACE INTO availability
           (sitter_id, date, status, noted_hours, scraped_at)
           VALUES (?,?,?,?,?)""",
        (sid, date_str, status, noted_hours, now),
    )


def is_known(conn, sid) -> bool:
    """True if this sitter has been seen before (profile already geocoded/filtered)."""
    return conn.execute(
        "SELECT 1 FROM sitters WHERE id=?", (sid,)
    ).fetchone() is not None


def is_newly_seen(conn, sid) -> bool:
    """True when first_seen == last_seen (inserted for the first time this run)."""
    return conn.execute(
        "SELECT 1 FROM sitters WHERE id=? AND first_seen=last_seen", (sid,)
    ).fetchone() is not None

# ---------------------------------------------------------------------------
# Geocoding
# ---------------------------------------------------------------------------

def geocode_gsi(query: str, conn: sqlite3.Connection, verbose: bool = False):
    cached = conn.execute(
        "SELECT lat, lon FROM geocode_cache WHERE query=?", (query,)
    ).fetchone()
    if cached:
        if verbose:
            print(f"  [geocode] cache: {query} -> {cached}")
        return cached
    try:
        resp = requests.get(GSI_API, params={"q": query}, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        if data:
            lon, lat = data[0]["geometry"]["coordinates"]
            conn.execute(
                "INSERT OR REPLACE INTO geocode_cache VALUES (?,?,?,?)",
                (query, lat, lon, dt.datetime.now().isoformat()),
            )
            conn.commit()
            if verbose:
                print(f"  [geocode] {query} -> ({lat:.4f}, {lon:.4f})")
            return (lat, lon)
    except Exception as exc:
        if verbose:
            print(f"  [geocode] failed for '{query}': {exc}")
    return None


def haversine(lat1, lon1, lat2, lon2) -> float:
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2
         + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2))
         * math.sin(dlon / 2) ** 2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

# ---------------------------------------------------------------------------
# Text-parsing helpers
# ---------------------------------------------------------------------------

def parse_station(text: str) -> str | None:
    if not text:
        return None
    m = STATION_RE.search(text)
    if m:
        return (m.group(1) or m.group(2)).strip()
    return None


def parse_time_range(text: str) -> str | None:
    if not text:
        return None
    m = TIME_RE.search(text)
    if m:
        h1, m1, h2, m2 = m.groups()
        t1 = f"{int(h1):02d}:{int(m1 or 0):02d}"
        t2 = f"{int(h2):02d}:{int(m2 or 0):02d}"
        return f"{t1}–{t2}"
    return None


def matches_23_wards(location_text: str, service_area: str,
                     self_intro: str) -> tuple[bool, str]:
    combined = f"{location_text or ''} {service_area or ''} {self_intro or ''}"
    matched = [w for w in WARD_NAMES if w in combined]
    if matched:
        return True, ", ".join(matched)
    if "東京都" in combined and "23区" in combined:
        return True, "23区 (general mention)"
    return False, ""

# ---------------------------------------------------------------------------
# Scraping
# ---------------------------------------------------------------------------

def collect_sitter_ids_from_listing(
    page, city_id: int, max_pages: int | None, verbose: bool
) -> list[tuple[int, str]]:
    """Return [(sitter_id, tagline), ...] from all listing pages for one ward."""
    results: list[tuple[int, str]] = []
    pg = 1
    ward_name = TOKYO_23_WARDS.get(city_id, str(city_id))
    while True:
        if max_pages and pg > max_pages:
            break
        url = (f"{BASE}/sitters?purpose=babysitter"
               f"&state_id=13000&city_id={city_id}&accepting_orders=true&page={pg}")
        if verbose:
            print(f"  [list] {ward_name} p{pg}: {url}")
        elif pg % 10 == 0:
            print(f"  [list] {ward_name} page {pg}…", flush=True)
        loaded = False
        for attempt in range(3):
            try:
                page.goto(url, timeout=30_000, wait_until="domcontentloaded")
                page.wait_for_timeout(1_500)
                loaded = True
                break
            except Exception as e:
                if verbose:
                    print(f"  [list] attempt {attempt+1} failed: {e}")
                time.sleep(5 * (attempt + 1))
        if not loaded:
            if verbose:
                print(f"  [list] giving up on {url}, skipping ward")
            break

        INACTIVE_MARKERS = ("休止", "受付停止", "受け付けておりません", "お休み中")

        # Extract sitter IDs, taglines, and listing-card availability in one JS pass
        card_data = page.evaluate("""() => {
            const results = [];
            const seen = new Set();
            document.querySelectorAll('a[href*="/sitters/"]').forEach(a => {
                const m = a.href.match(/\\/sitters\\/(\\d+)/);
                if (!m) return;
                const sid = parseInt(m[1]);
                if (seen.has(sid)) return;
                seen.add(sid);

                // Walk up to find the sitter card container
                let card = a;
                for (let i = 0; i < 6; i++) {
                    if (!card.parentElement) break;
                    card = card.parentElement;
                    if (card.querySelectorAll('a[href*="/sitters/"]').length === 1) break;
                }

                const cardText = card.innerText || '';
                const tagline = cardText.trim().split('\\n')[0].slice(0, 200);

                results.push({ sid, tagline, cardText });
            });
            return results;
        }""")

        total_cards = len(card_data or [])
        found = 0
        skipped_inactive = 0
        for item in (card_data or []):
            sid       = item["sid"]
            tagline   = item["tagline"]
            card_text = item["cardText"]

            if any(marker in card_text for marker in INACTIVE_MARKERS):
                skipped_inactive += 1
                if verbose:
                    print(f"    skip inactive sitter {sid}")
                continue
            results.append((sid, tagline))
            found += 1

        if verbose or pg % 10 == 0:
            print(f"  [list] p{pg}: {found} sitters ({skipped_inactive} inactive skipped)",
                  flush=True)

        # Stop when the page returned no sitter cards at all.
        if total_cards == 0:
            if verbose:
                print(f"  [list] no cards on p{pg}, done with {ward_name}")
            break
        if not page.query_selector(f"a[href*='page={pg + 1}']"):
            if verbose:
                print(f"  [list] no next page, done with {ward_name} at p{pg}")
            break
        pg += 1
    return results


def scrape_profile(page, sid: int, verbose: bool) -> dict | None:
    url = f"{BASE}/sitters/{sid}?purpose=babysitter&state_id=13000"
    for attempt in range(3):
        try:
            page.goto(url, timeout=30_000, wait_until="networkidle")
            page.evaluate("""async () => {
                const step = 300;
                for (let y = 0; y < document.body.scrollHeight; y += step) {
                    window.scrollTo(0, y);
                    await new Promise(r => setTimeout(r, 80));
                }
                document.querySelectorAll('img[data-src]').forEach(img => {
                    img.src = img.dataset.src;
                    if (img.dataset.srcset) img.srcset = img.dataset.srcset;
                });
            }""")
            page.wait_for_timeout(1_500)
            break
        except Exception as e:
            if verbose:
                print(f"  [profile] attempt {attempt+1} failed for {sid}: {e}")
            if attempt == 2:
                return None
            time.sleep(5 * (attempt + 1))

    # Dump raw HTML on first profile when verbose so we can inspect the
    # calendar structure if availability still shows as empty.
    if verbose and not getattr(scrape_profile, "_dumped", False):
        dump_path = os.path.join(os.path.dirname(DB_PATH), "debug_profile.html")
        with open(dump_path, "w", encoding="utf-8") as fh:
            fh.write(page.content())
        print(f"  [debug] HTML dumped to {dump_path}")
        scrape_profile._dumped = True

    try:
        body_text = page.inner_text("body")
    except Exception:
        body_text = ""

    # Tagline / display name
    tagline = ""
    for sel in ["h1", "h2", ".profile-title", ".sitter-name"]:
        el = page.query_selector(sel)
        if el:
            t = el.inner_text().strip()[:200]
            if t:
                tagline = t
                break

    # Location line (first short line mentioning a prefecture)
    location_text = ""
    for line in body_text.split("\n"):
        line = line.strip()
        if any(p in line for p in ("東京都", "神奈川県", "千葉県", "埼玉県")) \
                and len(line) < 50:
            location_text = line
            break

    # Service area
    service_area = ""
    m = re.search(r"対応エリア[：:\s]*(.+?)(?:\n|$)", body_text)
    if m:
        service_area = m.group(1).strip()[:200]

    # Self-introduction (used for station/hour parsing)
    self_intro = ""
    m2 = re.search(
        r"(?:自己紹介|プロフィール|はじめまして|ご覧いただき)(.{0,2000})",
        body_text, re.DOTALL,
    )
    if m2:
        self_intro = m2.group(0)[:2000]
    elif len(body_text) > 200:
        self_intro = body_text[:2000]

    # Calendar – next 14 days
    # The site uses <img src="...calendar-status-icon-[available|unavailable]-....png">
    # inside <td> cells. Each table has a "2026年 X月" month header.
    today = dt.date.today()
    availability: dict[str, str] = {}

    # Exact class names from live HTML dump:
    #   square-all-available = ○ fully available
    #   square-conditinal    = △ 相談可  (site typo: "conditinal")
    #   square-past          = ー past / unavailable  → skip
    # Date number is in class "penguin-sitter-calendar-body-column-day"
    # Month header is in class "penguin-sitter-calendar-title"
    raw_cells = page.evaluate("""() => {
        const results = [];

        document.querySelectorAll(
            '.penguin-sitter-calendar-body-column-content-square-all-available,' +
            '.penguin-sitter-calendar-body-column-content-square-conditinal'
        ).forEach(el => {
            const cls = el.className || '';
            const status = cls.includes('conditinal') ? 'partial' : 'available';

            // Date lives in .penguin-sitter-calendar-body-column-day within
            // the same .penguin-sitter-calendar-body-column ancestor
            const col = el.closest('.penguin-sitter-calendar-body-column');
            if (!col) return;

            const dayEl = col.querySelector('.penguin-sitter-calendar-body-column-day');
            const day = dayEl ? parseInt((dayEl.innerText || '').trim()) : null;
            if (!day || day < 1 || day > 31) return;

            // Month from nearest .penguin-sitter-calendar-title
            const titleEl = el.closest('[class*="penguin-sitter-calendar"]')
                              ?.querySelector('.penguin-sitter-calendar-title');
            const titleText = titleEl ? titleEl.innerText : '';
            const m = titleText.match(/(\\d{4})年\\s*(\\d{1,2})月/);
            let year  = m ? parseInt(m[1]) : null;
            let month = m ? parseInt(m[2]) : null;

            // Fallback: walk up to find year/month text
            if (!year) {
                let node = col;
                for (let i = 0; i < 12; i++) {
                    node = node.parentElement;
                    if (!node) break;
                    const mm = (node.innerText || '').match(/(\\d{4})年\\s*(\\d{1,2})月/);
                    if (mm) { year = parseInt(mm[1]); month = parseInt(mm[2]); break; }
                }
            }

            results.push({ status, day, year, month });
        });
        return results;
    }""")

    for cell in (raw_cells or []):
        try:
            day_num = cell.get("day")
            year    = cell.get("year")
            month   = cell.get("month")
            status  = cell.get("status")
            if not day_num or not status:
                continue

            if year and month:
                try:
                    cdate = dt.date(year, month, day_num)
                except ValueError:
                    continue
            else:
                cdate = None
                for mo in range(3):
                    base = today.replace(day=1) + dt.timedelta(days=32 * mo)
                    try:
                        candidate = base.replace(day=day_num)
                        if (candidate - today).days >= 0:
                            cdate = candidate
                            break
                    except ValueError:
                        continue
                if cdate is None:
                    continue

            delta = (cdate - today).days
            if 0 <= delta <= 14:
                key = cdate.isoformat()
                if key not in availability:
                    availability[key] = status
        except Exception:
            continue

    if verbose:
        print(f"    calendar: {len(availability)} days parsed "
              f"({sum(1 for v in availability.values() if v == 'available')} available, "
              f"{sum(1 for v in availability.values() if v == 'partial')} partial)")

    return {
        "tagline": tagline,
        "location_text": location_text,
        "service_area": service_area,
        "self_intro": self_intro,
        "availability": availability,
        "profile_url": f"{BASE}/sitters/{sid}?purpose=babysitter",
    }

# ---------------------------------------------------------------------------
# Email formatting & sending
# ---------------------------------------------------------------------------

def _day_label(date_str: str) -> str:
    d = dt.date.fromisoformat(date_str)
    fmt = "%a %#m/%#d" if sys.platform == "win32" else "%a %-m/%-d"
    return d.strftime(fmt)


STATUS_ICON = {"available": "○", "partial": "△", "unavailable": "×"}
STATUS_COLOR = {"available": "#2e7d32", "partial": "#e65100", "unavailable": "#9e9e9e"}


def build_email_html(sitters_data: list[dict], run_date: dt.date) -> str:
    # Build the summary table at the top
    summary_rows = ""
    for i, s in enumerate(sitters_data, 1):
        tagline = html.escape(s["tagline"][:60] or "(no tagline)")
        url = html.escape(s["profile_url"])
        dist = s["distance_km"]
        avail_count = sum(1 for v in s.get("availability", {}).values()
                          if v in ("available", "partial"))
        summary_rows += (
            f"<tr style='background:{'#f9f9f9' if i % 2 else '#fff'}'>"
            f"<td style='padding:6px 10px'>{i}</td>"
            f"<td style='padding:6px 10px'>"
            f"<a href='{url}' style='color:#1a73e8'>{tagline}</a></td>"
            f"<td style='padding:6px 10px;text-align:center'>{dist:.1f} km</td>"
            f"<td style='padding:6px 10px;text-align:center'>"
            f"{'–' if not avail_count else avail_count}</td>"
            f"</tr>"
        )

    summary_table = f"""
    <table style='border-collapse:collapse;width:100%;font-size:13px;
                  border:1px solid #ddd;margin-bottom:24px'>
      <thead>
        <tr style='background:#1a73e8;color:#fff'>
          <th style='padding:8px 10px;text-align:left'>#</th>
          <th style='padding:8px 10px;text-align:left'>Sitter</th>
          <th style='padding:8px 10px;text-align:center'>Distance</th>
          <th style='padding:8px 10px;text-align:center'>Available days</th>
        </tr>
      </thead>
      <tbody>{summary_rows}</tbody>
    </table>"""

    # Build individual sitter cards
    rows_html = ""
    for i, s in enumerate(sitters_data, 1):
        tagline = html.escape(s["tagline"][:80] or "(no tagline)")
        dist = s["distance_km"]
        url = html.escape(s["profile_url"])
        avail = {k: v for k, v in s.get("availability", {}).items()
                 if v in ("available", "partial")}
        noted_hours = html.escape(s.get("noted_hours") or "")

        avail_rows = ""
        for date_str in sorted(avail.keys())[:14]:
            status = avail[date_str]
            icon = STATUS_ICON.get(status, "?")
            color = STATUS_COLOR.get(status, "#000")
            label = _day_label(date_str)
            time_cell = noted_hours or "–"
            avail_rows += (
                f"<tr>"
                f"<td style='padding:3px 10px;color:{color};font-weight:bold;"
                f"font-size:14px'>{icon}</td>"
                f"<td style='padding:3px 10px'>{label}</td>"
                f"<td style='padding:3px 10px;color:#555'>{time_cell}</td>"
                f"</tr>"
            )

        avail_block = (
            f"<table style='border-collapse:collapse;margin:6px 0;font-size:13px'>"
            f"<tr style='color:#888;font-size:11px'>"
            f"<th style='padding:2px 10px;text-align:left'>Status</th>"
            f"<th style='padding:2px 10px;text-align:left'>Date</th>"
            f"<th style='padding:2px 10px;text-align:left'>Hours</th></tr>"
            f"{avail_rows}</table>"
        ) if avail_rows else "<p style='color:#aaa;font-size:12px;margin:6px 0'>No availability data</p>"

        rows_html += f"""
        <div style='border:1px solid #ddd;border-radius:6px;padding:14px 16px;
                    margin-bottom:12px;background:#fafafa'>
          <div style='font-size:13px;color:#999;margin-bottom:2px'>#{i} &nbsp;·&nbsp; {dist:.1f} km</div>
          <div style='font-size:15px;font-weight:bold;margin-bottom:8px'>
            <a href='{url}' style='color:#1a73e8;text-decoration:none'>{tagline}</a>
          </div>
          {avail_block}
          <div style='font-size:11px;margin-top:8px;color:#aaa'>
            <a href='{url}' style='color:#aaa'>{url}</a>
          </div>
        </div>
        """

    return f"""<!DOCTYPE html>
<html lang='ja'>
<head><meta charset='utf-8'></head>
<body style='font-family:sans-serif;max-width:640px;margin:0 auto;padding:16px'>
  <h2 style='color:#1a73e8'>SmartSitter – New / Available Sitters Near Harumi</h2>
  <p style='color:#555;font-size:13px'>
    Scraped {run_date.strftime('%Y-%m-%d')} &nbsp;|&nbsp;
    {len(sitters_data)} sitter(s) within 5 km &nbsp;|&nbsp;
    Next 14 days shown
  </p>
  {rows_html}
  <hr style='border:none;border-top:1px solid #eee;margin-top:24px'>
  <p style='font-size:11px;color:#aaa'>Generated by smartsitter-scraper</p>
</body>
</html>"""


def build_email_plain(sitters_data: list[dict], run_date: dt.date) -> str:
    lines = [
        f"SmartSitter – New/Available Sitters Near Harumi",
        f"Scraped: {run_date}  |  {len(sitters_data)} sitter(s) within 5 km",
        "",
    ]
    for s in sitters_data:
        lines.append(f"■ {s['tagline'][:80] or '(no tagline)'}")
        lines.append(f"  {s['distance_km']:.1f} km from Harumi, Chuo-ku")
        avail = {k: v for k, v in s.get("availability", {}).items()
                 if v in ("available", "partial")}
        if avail:
            lines.append("  Date       Status  Hours")
            for date_str in sorted(avail.keys())[:14]:
                status = avail[date_str]
                icon = STATUS_ICON.get(status, "?")
                label = _day_label(date_str)
                hours = s.get("noted_hours") or ""
                lines.append(f"  {label:<10} {icon}       {hours}")
        else:
            lines.append("  (no availability data)")
        lines.append(f"  {s['profile_url']}")
        lines.append("")
    return "\n".join(lines)


def send_gmail(gmail_address: str, app_password: str,
               sitters_data: list[dict], run_date: dt.date,
               verbose: bool = False) -> bool:
    subject = (f"SmartSitter: {len(sitters_data)} sitter(s) near Harumi "
               f"[{run_date}]")
    html_body = build_email_html(sitters_data, run_date)
    plain_body = build_email_plain(sitters_data, run_date)

    msg = email.mime.multipart.MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = gmail_address
    msg["To"] = gmail_address
    msg.attach(email.mime.text.MIMEText(plain_body, "plain", "utf-8"))
    msg.attach(email.mime.text.MIMEText(html_body, "html", "utf-8"))

    ctx = ssl.create_default_context()
    try:
        with smtplib.SMTP_SSL(GMAIL_SMTP_HOST, GMAIL_SMTP_PORT, context=ctx) as smtp:
            smtp.login(gmail_address, app_password)
            smtp.sendmail(gmail_address, gmail_address, msg.as_bytes())
        if verbose:
            print(f"[email] sent to {gmail_address} ({len(html_body)} bytes HTML)")
        return True
    except smtplib.SMTPAuthenticationError:
        print("[email] Authentication failed – check GMAIL_ADDRESS and GMAIL_APP_PASSWORD")
        return False
    except Exception as exc:
        print(f"[email] send error: {exc}")
        return False

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="SmartSitter.jp Tokyo sitter scraper")
    parser.add_argument("--dry-run", action="store_true",
                        help="Scrape only, skip email notification")
    parser.add_argument("--verbose", action="store_true",
                        help="Show filter reasoning and geocoding details")
    parser.add_argument("--test", action="store_true",
                        help="Force send email ignoring newness/availability check")
    parser.add_argument("--max-pages", type=int, default=None,
                        help="Max listing pages per ward (default: unlimited)")
    parser.add_argument("--ward", type=str, default=None,
                        help="Limit scraping to a single ward name e.g. '中央区'")
    parser.add_argument("--db", default=DB_PATH, help="SQLite database path")
    args = parser.parse_args()

    gmail_address = os.environ.get("GMAIL_ADDRESS", "")
    gmail_app_password = os.environ.get("GMAIL_APP_PASSWORD", "")
    if not args.dry_run and not (gmail_address and gmail_app_password):
        print("Warning: GMAIL_ADDRESS or GMAIL_APP_PASSWORD not set. "
              "Use --dry-run or set both env vars.")

    conn = init_db(args.db)
    today = dt.date.today()
    print(f"[start] Scraping smartsitter.jp – {today}")

    # ── Target wards ─────────────────────────────────────────────────────────
    # Scrape only these 5 wards. 中央区 also captures sitters based anywhere
    # in Tokyo who explicitly accept requests in Chuo ward.
    TARGET_WARDS = {13101: "千代田区", 13102: "中央区",
                    13103: "港区",    13104: "新宿区", 13108: "江東区"}

    print("[wards] geocoding ward centres …")
    ward_order: list[tuple[int, str, float]] = []
    for city_id, ward_name in TARGET_WARDS.items():
        wc = geocode_gsi(f"東京都{ward_name}", conn, args.verbose)
        d = haversine(HARUMI_LAT, HARUMI_LON, wc[0], wc[1]) if wc else float("inf")
        ward_order.append((city_id, ward_name, d))
    ward_order.sort(key=lambda x: x[2])

    if args.ward:
        ward_order = [(c, w, d) for c, w, d in ward_order if w == args.ward]
        if not ward_order:
            print(f"[error] Ward '{args.ward}' not in target list: "
                  f"{', '.join(TARGET_WARDS.values())}")
            sys.exit(1)
    print(f"[wards] {len(ward_order)} ward(s) to scrape (closest first):")
    for city_id, ward_name, d in ward_order:
        print(f"  {ward_name:6s}  {d:.1f} km")

    seen_ids: dict[int, str] = {}           # sid -> best tagline
    ward_membership: dict[int, set[str]] = {}  # sid -> wards it appeared under

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        ctx = browser.new_context(
            locale="ja-JP",
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
        )
        page = ctx.new_page()

        # ── Phase 1: collect sitter IDs, closest wards first ───────────────
        for city_id, ward_name, ward_dist in ward_order:
            print(f"[ward] {ward_name} ({ward_dist:.1f} km, city_id={city_id})")
            pairs = collect_sitter_ids_from_listing(
                page, city_id, args.max_pages, args.verbose
            )
            for sid, tagline in pairs:
                if sid not in seen_ids or (tagline and not seen_ids[sid]):
                    seen_ids[sid] = tagline
                ward_membership.setdefault(sid, set()).add(ward_name)
            print(f"  -> {len(pairs)} entries, {len(seen_ids)} unique total")
            time.sleep(1)

        # ── Phase 2: scrape profiles selectively ───────────────────────────
        # - Known sitters >5km: skip entirely (distance won't change, won't email)
        # - Known sitters <=5km: scrape for fresh availability only
        # - New sitters: scrape to geocode, then store if <=5km
        all_sids = list(seen_ids.keys())
        new_sids      = [s for s in all_sids if not is_known(conn, s)]
        nearby_known  = [s for s in all_sids if is_known(conn, s) and
                         conn.execute("SELECT distance_km FROM sitters WHERE id=?",
                                      (s,)).fetchone()[0] is not None and
                         conn.execute("SELECT distance_km FROM sitters WHERE id=?",
                                      (s,)).fetchone()[0] <= 5.0]
        to_scrape = new_sids + nearby_known
        print(f"\n[profiles] scraping {len(to_scrape)} profiles "
              f"({len(new_sids)} new, {len(nearby_known)} known within 5km, "
              f"{len(all_sids) - len(to_scrape)} far sitters skipped)")

        notify_candidates: list[dict] = []

        for i, sid in enumerate(to_scrape):
            print(f"  [profile {i + 1}/{len(to_scrape)}] sitter {sid}", flush=True)
            if args.verbose:
                print(f"  [{i + 1}/{len(to_scrape)}] sitter {sid}")

            profile = scrape_profile(page, sid, args.verbose)
            if not profile:
                continue

            noted_hours = parse_time_range(profile["self_intro"])

            if is_known(conn, sid):
                # ── Known sitter: refresh availability + last_seen only ──
                row = conn.execute(
                    "SELECT tagline, distance_km, profile_url FROM sitters WHERE id=?",
                    (sid,)
                ).fetchone()
                tagline, dist, profile_url = row
                now = dt.datetime.now().isoformat()
                conn.execute(
                    "UPDATE sitters SET last_seen=? WHERE id=?", (now, sid)
                )
                for date_str, status in profile["availability"].items():
                    insert_availability(conn, sid, date_str, status, noted_hours)
                conn.commit()
            else:
                # ── New sitter: full ward filter + geocode ───────────────
                tagline = profile["tagline"] or seen_ids.get(sid, "")
                location_text = profile["location_text"]
                service_area = profile["service_area"]
                self_intro = profile["self_intro"]

                ward_ok, ward_reason = matches_23_wards(
                    location_text, service_area, self_intro
                )
                listing_wards = ward_membership.get(sid, set())
                if not ward_ok and listing_wards & WARD_NAMES:
                    ward_ok = True
                    ward_reason = f"listed under: {', '.join(listing_wards & WARD_NAMES)}"

                if not ward_ok:
                    if args.verbose:
                        print(f"    skip (no 23-ward match) loc={location_text!r}")
                    continue
                if args.verbose:
                    print(f"    ward match: {ward_reason}")

                station = parse_station(self_intro) or parse_station(location_text)

                coords = None
                # Priority 1: station explicitly mentioned in profile text
                if station:
                    coords = geocode_gsi(f"東京都 {station}駅", conn, args.verbose)
                # Priority 2: nearest ward the sitter serves (from city_id
                # listing). Distance reflects where they'll travel to, not
                # where they live — a 国立市 sitter who serves 中央区 is
                # correctly placed ~2 km from Harumi.
                if not coords and listing_wards:
                    best_ward_coords, best_ward_dist = None, float("inf")
                    for w in listing_wards:
                        wc = geocode_gsi(f"東京都{w}", conn, args.verbose)
                        if wc:
                            d = haversine(HARUMI_LAT, HARUMI_LON, wc[0], wc[1])
                            if d < best_ward_dist:
                                best_ward_dist = d
                                best_ward_coords = wc
                    if best_ward_coords:
                        coords = best_ward_coords
                        if args.verbose:
                            print(f"    geocoded via nearest served ward "
                                  f"({best_ward_dist:.1f} km)")
                # Priority 3: home location as last resort
                if not coords and location_text:
                    coords = geocode_gsi(location_text, conn, args.verbose)

                lat = lon = dist = None
                if coords:
                    lat, lon = coords
                    dist = haversine(HARUMI_LAT, HARUMI_LON, lat, lon)
                    if args.verbose:
                        print(f"    distance: {dist:.2f} km")

                profile_url = profile["profile_url"]
                upsert_sitter(conn, sid, tagline, location_text, service_area,
                              station, lat, lon, dist, profile_url, self_intro)
                for date_str, status in profile["availability"].items():
                    insert_availability(conn, sid, date_str, status, noted_hours)
                conn.commit()

            # ── Notification candidate? ──────────────────────────────────
            dist = conn.execute(
                "SELECT distance_km FROM sitters WHERE id=?", (sid,)
            ).fetchone()[0]
            profile_url = profile["profile_url"]

            has_avail = any(
                v in ("available", "partial")
                for v in profile["availability"].values()
            )
            is_new = is_newly_seen(conn, sid)

            # Check whether any available dates are NEW since yesterday.
            # A known sitter whose calendar opens up should trigger a notification.
            prev_avail_dates = set(
                row[0] for row in conn.execute(
                    """SELECT DISTINCT date FROM availability
                       WHERE sitter_id=? AND status IN ('available','partial')
                         AND scraped_at < date('now')""",
                    (sid,)
                ).fetchall()
            )
            newly_available = any(
                d not in prev_avail_dates
                for d, v in profile["availability"].items()
                if v in ("available", "partial")
            )

            # Notify when: sitter within 5km AND has availability AND
            # (first time seen  OR  calendar newly opened up  OR  --test)
            if dist is not None and dist <= 5.0 and has_avail and (is_new or newly_available or args.test):
                notify_candidates.append({
                    "tagline": conn.execute(
                        "SELECT tagline FROM sitters WHERE id=?", (sid,)
                    ).fetchone()[0],
                    "distance_km": dist,
                    "availability": profile["availability"],
                    "noted_hours": noted_hours,
                    "profile_url": profile_url,
                })

            time.sleep(1.5)

        browser.close()

    # Sort by distance
    notify_candidates.sort(key=lambda s: s["distance_km"])
    print(f"\n[done] {len(notify_candidates)} sitter(s) to notify about")

    # ── Phase 3: send email ──────────────────────────────────────────────────
    if args.dry_run:
        if notify_candidates:
            print("\n[dry-run] Would email about:")
            print(build_email_plain(notify_candidates, today))
        else:
            print("[dry-run] No sitters to notify about")
    elif notify_candidates:
        print(f"[email] sending to {gmail_address} …")
        send_gmail(gmail_address, gmail_app_password,
                   notify_candidates, today, args.verbose)
    else:
        print("[email] nothing new – no email sent")

    conn.close()
    print("[end]")


if __name__ == "__main__":
    main()
