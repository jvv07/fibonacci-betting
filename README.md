# fibonacci-betting

A Streamlit dashboard for tracking and managing football draw bets using the Fibonacci staking strategy. Connects to Supabase for persistence, API-Football for live data, and deploys free on Streamlit Community Cloud with daily automation via GitHub Actions.

---

## Quickstart (local)

```bash
git clone https://github.com/YOUR_USERNAME/fibonacci-betting.git
cd fibonacci-betting
pip install -r requirements.txt
cp .env.example .env          # fill in your credentials (see below)
streamlit run app.py
```

---

## Environment Variables

| Variable | Where to get it |
|---|---|
| `SUPABASE_URL` | Supabase → Project Settings → API → Project URL |
| `SUPABASE_KEY` | Supabase → Project Settings → API → `anon` public key |
| `API_FOOTBALL_KEY` | dashboard.api-football.com → My Account → API Key |
| `ALERT_EMAIL` | Your Gmail address |
| `EMAIL_PASSWORD` | Gmail App Password (not your login password — see note) |

> **Gmail note:** Go to myaccount.google.com → Security → 2-Step Verification → App Passwords. Generate one for "Mail / Other". Use that 16-character string as `EMAIL_PASSWORD`.

---

## Supabase SQL Setup

Paste the following into **Supabase → SQL Editor → New Query** and click Run:

```sql
-- 1. Settings (single row)
create table if not exists settings (
  id               serial primary key,
  base_stake       float  not null default 10,
  min_odds         float  not null default 2.88,
  max_fib_step     int    not null default 7,
  bankroll         float  not null default 0,
  commission_pct   float  not null default 0,
  updated_at       timestamptz default now()
);

-- Insert the default settings row
insert into settings (base_stake, min_odds, max_fib_step, bankroll, commission_pct)
values (10, 2.88, 7, 500, 0)
on conflict do nothing;

-- 2. Leagues
create table if not exists leagues (
  id                  serial primary key,
  league_key          text unique not null,
  league_name         text,
  country             text,
  api_id              int,
  draw_rate_current   float default 0,
  draw_rate_season    float default 0,
  score               float default 0,
  is_active           boolean default false,
  last_scanned        timestamptz
);

-- 3. Fixtures
create table if not exists fixtures (
  id            serial primary key,
  fixture_id    int unique not null,
  league_key    text,
  home_team     text,
  away_team     text,
  kickoff_utc   timestamptz,
  draw_odds     float,
  h2h_draw_rate float,
  fetched_at    timestamptz
);

-- 4. Fibonacci sequences (one row per league)
create table if not exists fibonacci_sequences (
  id               serial primary key,
  league_key       text unique not null,
  current_step     int    not null default 1,
  cumulative_loss  float  not null default 0,
  series_start     timestamptz default now(),
  last_updated     timestamptz default now()
);

-- 5. Bets
create table if not exists bets (
  id            serial primary key,
  fixture_id    int,
  league_key    text,
  home_team     text,
  away_team     text,
  kickoff_utc   timestamptz,
  fib_step      int,
  stake         float,
  odds          float,
  result        text default 'PENDING',
  gross_return  float default 0,
  net_pnl       float default 0,
  created_at    timestamptz default now()
);
```

---

## Deployment Checklist

Follow these steps **in order**.

### 1 — Push to GitHub

```bash
cd /Users/Joshua/Desktop/fibonacci-betting

# Initialise git and push
git init
git add .
git commit -m "Initial commit: fibonacci-betting scaffold and full implementation"

# Create the repo on GitHub (requires GitHub CLI — install with: brew install gh)
gh auth login
gh repo create fibonacci-betting --public --source=. --remote=origin --push
```

If you prefer the browser:
1. Go to github.com → New repository → name it `fibonacci-betting` → Create
2. Run the commands printed by GitHub under "…or push an existing repository"

---

### 2 — Set up Supabase

1. Go to **app.supabase.com** → New Project (free tier is fine).
2. Note your **Project URL** and **anon key** from Project Settings → API.
3. Open **SQL Editor → New Query**, paste the SQL block above, click **Run**.
4. Verify: open **Table Editor** — you should see 5 tables, and `settings` should have 1 row.

---

### 3 — Deploy on Streamlit Community Cloud

1. Go to **share.streamlit.io** → Sign in with GitHub.
2. Click **New app**.
3. Repository: `YOUR_USERNAME/fibonacci-betting`
4. Branch: `main`
5. Main file path: `app.py`
6. Click **Advanced settings** → **Secrets** tab.
7. Paste the following (fill in your real values):

```toml
SUPABASE_URL      = "https://xxxx.supabase.co"
SUPABASE_KEY      = "eyJ..."
API_FOOTBALL_KEY  = "your_key_here"
ALERT_EMAIL       = "you@gmail.com"
EMAIL_PASSWORD    = "abcd efgh ijkl mnop"
```

8. Click **Deploy!** — the app will be live at `https://YOUR_USERNAME-fibonacci-betting-app-XXXX.streamlit.app`.

---

### 4 — Add GitHub Actions Secrets

1. Open your GitHub repo → **Settings** → **Secrets and variables** → **Actions**.
2. Click **New repository secret** for each of the five variables:

| Secret name | Value |
|---|---|
| `SUPABASE_URL` | your Supabase project URL |
| `SUPABASE_KEY` | your Supabase anon key |
| `API_FOOTBALL_KEY` | your API-Football key |
| `ALERT_EMAIL` | your Gmail address |
| `EMAIL_PASSWORD` | your Gmail App Password |

---

### 5 — Manually trigger the GitHub Actions workflow

1. Go to your repo on GitHub → **Actions** tab.
2. Click **Daily Data Refresh** in the left panel.
3. Click **Run workflow** → **Run workflow** (green button).
4. Watch the logs — you should see timestamped output matching `scripts/daily_refresh.py`.

This is the best way to verify everything works before waiting for the 07:00 UTC cron.

---

### 6 — End-to-end verification checklist

- [ ] Supabase SQL Editor ran without errors and all 5 tables exist
- [ ] `settings` table has exactly 1 row
- [ ] Local `streamlit run app.py` loads without errors (⚙️ Settings page shows saved values)
- [ ] GitHub Actions manual run completes without errors
- [ ] `fixtures` table in Supabase is populated after the Actions run
- [ ] Email alert arrives (or check GitHub Actions logs for "sent successfully")
- [ ] Streamlit Community Cloud app loads and shows the dashboard
- [ ] Toggle a league active in the 🏆 League Scanner page and verify DB updates

---

## Project Structure

```
fibonacci-betting/
├── app.py                          Streamlit dashboard (6 pages)
├── requirements.txt
├── .env.example
├── .gitignore
├── README.md
├── api_calls.json                  Auto-created; tracks daily API quota usage
├── .streamlit/
│   └── config.toml                 Dark theme + wide layout
├── src/
│   ├── __init__.py
│   ├── db.py                       Supabase CRUD layer
│   ├── fibonacci_engine.py         Staking ladder + simulation
│   ├── data_fetcher.py             API-Football HTTP client
│   ├── league_scanner.py           Draw-rate ranking + DB sync
│   └── notifications.py           Gmail alert sender
├── scripts/
│   └── daily_refresh.py            Morning job (runs in GitHub Actions)
└── .github/workflows/
    └── daily_refresh.yml           Cron: 07:00 UTC daily
```

---

## How the Fibonacci system works

| Step | Multiplier | Stake (£10 base) |
|------|-----------|-----------------|
| 1    | ×1        | £10             |
| 2    | ×1        | £10             |
| 3    | ×2        | £20             |
| 4    | ×3        | £30             |
| 5    | ×5        | £50             |
| 6    | ×8        | £80             |
| 7    | ×13       | £130  ← default stop-loss |

A **WIN** (draw result) recoups the series loss and resets to step 1.
A **LOSS** advances one step. Reaching the stop-loss step resets without further escalation.

**This is a gambling system. Past draw rates do not guarantee future results. Never bet more than you can afford to lose.**
