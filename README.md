# SmartSitter.jp Tokyo Sitter Scraper

Scrapes babysitter listings from [smartsitter.jp](https://smartsitter.jp) for Tokyo's 23 special wards, filters by proximity to Harumi (Chuo-ku), and sends a Gmail email for new or newly available sitters within 5 km.

## Setup

### 1. Install dependencies

```bash
python -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate
pip install -r requirements.txt
playwright install chromium
```

### 2. Generate a Gmail App Password

Gmail App Passwords let the scraper authenticate without using your main account password and without enabling "less secure app access".

1. Go to your Google Account → **Security**
2. Under "How you sign in to Google", enable **2-Step Verification** (required)
3. Back on the Security page, click **App passwords** (or search "App passwords" in My Account)
4. Select app: **Mail** / Select device: **Other (custom name)** → type "SmartSitter Scraper"
5. Click **Generate** – copy the 16-character password shown

> The App Password is shown only once. Save it somewhere safe.

### 3. Set environment variables

```bash
# Linux / macOS (add to ~/.bashrc or ~/.zshrc for persistence)
export GMAIL_ADDRESS="you@gmail.com"
export GMAIL_APP_PASSWORD="abcd efgh ijkl mnop"

# Windows PowerShell (current session)
$env:GMAIL_ADDRESS="you@gmail.com"
$env:GMAIL_APP_PASSWORD="abcd efgh ijkl mnop"

# Windows – persist across sessions (System Properties → Environment Variables)
# Or add to your venv activation script.
```

Never hardcode credentials in the script.

### 4. Run

```bash
# Full run: scrape + send email
python scraper.py

# Scrape only (no email)
python scraper.py --dry-run

# Verbose output (ward matching, geocoding, distances)
python scraper.py --dry-run --verbose

# Force send email for all sitters within 5 km (ignores newness check)
python scraper.py --test

# Limit listing pages per ward (much faster for testing)
python scraper.py --dry-run --max-pages 2 --verbose
```

## CLI flags

| Flag | Description |
|------|-------------|
| `--dry-run` | Scrape only, skip email |
| `--verbose` | Print ward matching, geocoding, and distance details |
| `--test` | Force send email ignoring the newness/availability check |
| `--max-pages N` | Limit listing pages per ward (default: unlimited) |
| `--db PATH` | Custom SQLite database path (default: `sitters.db` next to script) |

## Cron – daily at 6 PM JST (UTC+9 = 09:00 UTC)

```bash
# Open crontab
crontab -e

# Add this line (adjust paths):
0 9 * * * cd /path/to/Myclaud && /path/to/venv/bin/python scraper.py >> /var/log/smartsitter.log 2>&1
```

Make sure `GMAIL_ADDRESS` and `GMAIL_APP_PASSWORD` are available to the cron environment:

```bash
# Option A – prepend vars in the crontab line:
0 9 * * * GMAIL_ADDRESS=you@gmail.com GMAIL_APP_PASSWORD="xxxx xxxx xxxx xxxx" \
  cd /path/to/Myclaud && /path/to/venv/bin/python scraper.py >> /var/log/smartsitter.log 2>&1

# Option B – source a file that exports them:
0 14 * * * . /home/you/.smartsitter_env && cd /path/to/Myclaud && \
  /path/to/venv/bin/python scraper.py >> /var/log/smartsitter.log 2>&1
```

### Windows Task Scheduler

Create a new task:
- **Trigger**: Daily at 18:00
- **Action – Program**: `C:\path\to\venv\Scripts\python.exe`
- **Action – Arguments**: `C:\Users\rimha\Desktop\Myclaud\scraper.py`
- **Start in**: `C:\Users\rimha\Desktop\Myclaud`
- Add `GMAIL_ADDRESS` and `GMAIL_APP_PASSWORD` under the task's environment variables

## Email format

Each email lists sitters sorted by distance (closest first). For each sitter:

- **Name** (tagline / profile headline) as a clickable link
- Distance from Harumi, Chuo-ku in km
- Per-day availability table for the next 14 days – only available (○) and partially available (△) days are shown, with parsed time ranges where mentioned in the sitter's profile text
- Direct profile URL

## Data notes

- **Names**: The site does not display real names. Sitter taglines / profile headlines are used instead.
- **Nearest station**: Not a structured field. The scraper uses regex to extract station names from self-introduction text (e.g. "月島駅より徒歩5分"). Falls back to ward-center coordinates when no station is found.
- **Time ranges**: The calendar shows available/partial/unavailable per day only – no structured time slots. The scraper parses time patterns (e.g. "9時〜17時") from free-text introductions as a best-effort approximation.
- **Geocoding**: Uses the 国土地理院 (GSI) Geocoding API. Results are cached in SQLite to minimise repeat calls.
- **Distance reference**: Harumi, Chuo-ku, Tokyo (35.6544, 139.7855).

## Database

`sitters.db` is created next to the script. Tables:

| Table | Contents |
|-------|---------|
| `sitters` | Profile data: tagline, location, station, coordinates, distance, profile URL |
| `availability` | Per-day status (available / partial / unavailable) + parsed hours, per scrape run |
| `geocode_cache` | Cached GSI geocoding results |
