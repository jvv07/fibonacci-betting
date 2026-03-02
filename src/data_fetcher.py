"""
data_fetcher.py — Multi-source football data client.
v3: The Odds API (upcoming fixtures + draw odds), OpenLigaDB (German leagues),
    football-data.co.uk (historical results + Bet365 odds), API-Football (legacy).

Base URL : https://v3.football.api-sports.io
Auth     : x-apisports-key header (API_FOOTBALL_KEY env var)
Daily cap: 100 calls/day on free tier — this module refuses at 95 to leave headroom.
Call count is persisted in api_calls.json at the project root and resets at midnight UTC.
"""

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()

BASE_URL = "https://v3.football.api-sports.io"
DAILY_LIMIT = 95
_CALLS_FILE = Path(__file__).parent.parent / "api_calls.json"


# ---------------------------------------------------------------------------
# Rate-limit helpers
# ---------------------------------------------------------------------------

def _load_counter() -> dict:
    """Load today's call counter, resetting it if the date has changed."""
    today = datetime.now(timezone.utc).date().isoformat()
    if _CALLS_FILE.exists():
        try:
            data = json.loads(_CALLS_FILE.read_text())
            if data.get("date") == today:
                return data
        except (json.JSONDecodeError, OSError):
            pass
    return {"date": today, "count": 0}


def _save_counter(data: dict) -> None:
    try:
        _CALLS_FILE.write_text(json.dumps(data))
    except OSError as e:
        print(f"[data_fetcher] Could not write api_calls.json: {e}")


def _increment() -> int:
    """Increment the counter and return the new value."""
    data = _load_counter()
    data["count"] += 1
    _save_counter(data)
    return data["count"]


def get_api_calls_today() -> int:
    """Return today's API call count (safe to call anytime)."""
    return _load_counter().get("count", 0)


def _within_limit() -> bool:
    count = _load_counter().get("count", 0)
    if count >= DAILY_LIMIT:
        print(
            f"[data_fetcher] Daily API limit reached ({count}/{DAILY_LIMIT}). "
            "Skipping call to protect quota."
        )
        return False
    return True


# ---------------------------------------------------------------------------
# Core HTTP helper
# ---------------------------------------------------------------------------

def _get(endpoint: str, params: dict | None = None) -> dict | None:
    """
    Make a GET request to the API-Football endpoint.
    Returns the parsed JSON dict or None on any error / rate-limit breach.
    """
    if not _within_limit():
        return None
    try:
        url = f"{BASE_URL}/{endpoint}"
        headers = {"x-apisports-key": os.environ["API_FOOTBALL_KEY"]}
        resp = requests.get(url, headers=headers, params=params or {}, timeout=20)
        resp.raise_for_status()
        _increment()
        return resp.json()
    except KeyError:
        print("[data_fetcher] API_FOOTBALL_KEY env var not set.")
        return None
    except requests.RequestException as e:
        print(f"[data_fetcher] GET /{endpoint} error: {e}")
        return None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def fetch_upcoming_fixtures(league_ids: list[int], days_ahead: int = 3) -> list[dict]:
    """
    Fetch not-started fixtures for *league_ids* within the next *days_ahead* days.

    Returns a list of dicts:
        fixture_id, league_key, home_team, away_team, kickoff_utc, draw_odds, h2h_draw_rate
    """
    results: list[dict] = []
    today = datetime.now(timezone.utc).date()
    end_date = (today + timedelta(days=days_ahead)).isoformat()
    today_str = today.isoformat()

    for league_id in league_ids:
        data = _get(
            "fixtures",
            {"league": league_id, "from": today_str, "to": end_date, "status": "NS"},
        )
        if not data or not data.get("response"):
            continue

        for item in data["response"]:
            try:
                fix = item["fixture"]
                teams = item["teams"]
                results.append(
                    {
                        "fixture_id": fix["id"],
                        "league_key": f"league_{league_id}",
                        "home_team": teams["home"]["name"],
                        "away_team": teams["away"]["name"],
                        "kickoff_utc": fix["date"],
                        "draw_odds": None,       # populated separately
                        "h2h_draw_rate": None,   # populated separately
                    }
                )
            except (KeyError, TypeError):
                continue

    return results


def fetch_draw_odds(fixture_id: int) -> float | None:
    """
    Fetch the Bet365 draw odds for a fixture.
    Uses bookmaker=8 (Bet365) and bet=1 (Match Winner).
    Returns the draw value as a float, or None if unavailable.
    """
    data = _get("odds", {"fixture": fixture_id, "bookmaker": 8, "bet": 1})
    if not data or not data.get("response"):
        return None
    try:
        for item in data["response"]:
            for bookmaker in item.get("bookmakers", []):
                for bet in bookmaker.get("bets", []):
                    if bet.get("id") == 1:
                        for val in bet.get("values", []):
                            if val.get("value") == "Draw":
                                return float(val["odd"])
    except (IndexError, KeyError, TypeError, ValueError):
        pass
    return None


def fetch_results(fixture_ids: list[int]) -> list[dict]:
    """
    Fetch final results for a list of fixture IDs.
    Processes up to 20 IDs per API call using the `ids` parameter.

    Returns a list of dicts:
        fixture_id, result ('WIN' if draw else 'LOSS'), home_goals, away_goals
    """
    results: list[dict] = []
    if not fixture_ids:
        return results

    chunk_size = 20
    for i in range(0, len(fixture_ids), chunk_size):
        chunk = fixture_ids[i : i + chunk_size]
        ids_param = "-".join(str(fid) for fid in chunk)
        data = _get("fixtures", {"ids": ids_param})
        if not data or not data.get("response"):
            continue

        for item in data["response"]:
            try:
                fix = item["fixture"]
                goals = item["goals"]
                status = fix["status"]["short"]
                # Only process fully finished matches
                if status not in ("FT", "AET", "PEN"):
                    continue
                home_g = goals.get("home") or 0
                away_g = goals.get("away") or 0
                results.append(
                    {
                        "fixture_id": fix["id"],
                        "result": "WIN" if home_g == away_g else "LOSS",
                        "home_goals": home_g,
                        "away_goals": away_g,
                    }
                )
            except (KeyError, TypeError):
                continue

    return results


def get_league_draw_rate(league_id: int, season: int) -> float:
    """
    Calculate the season draw rate for a league.
    Calls /fixtures?league=id&season=year&status=FT and counts draws.
    Returns a fraction (0.0–1.0), e.g. 0.28 for 28 % draws.
    """
    data = _get("fixtures", {"league": league_id, "season": season, "status": "FT"})
    if not data or not data.get("response"):
        return 0.0

    fixtures = data["response"]
    total = len(fixtures)
    if total == 0:
        return 0.0

    draws = 0
    for item in fixtures:
        try:
            goals = item["goals"]
            if (goals.get("home") or -1) == (goals.get("away") or -2):
                draws += 1
        except (KeyError, TypeError):
            continue

    return round(draws / total, 4)


def fetch_historical_fixtures(league_id: int, season: int) -> list[dict]:
    """
    Fetch all finished fixtures for a league/season for backtesting.

    Returns a list of dicts:
        fixture_id, home_team, away_team, home_goals, away_goals, kickoff_utc

    Note: tries without status filter first (more compatible), then filters
    for finished matches in Python.
    """
    # Try without status filter — more compatible with free-tier plans
    data = _get("fixtures", {"league": league_id, "season": season})
    if not data:
        return []

    # Surface any API-level errors (e.g. suspended account)
    if data.get("errors"):
        print(f"[data_fetcher] fetch_historical_fixtures API error: {data['errors']}")
        return []

    response = data.get("response", [])
    if not response:
        # Fallback: try with explicit status filter
        data2 = _get("fixtures", {"league": league_id, "season": season, "status": "FT"})
        if data2 and not data2.get("errors"):
            response = data2.get("response", [])

    results: list[dict] = []
    for item in response:
        try:
            fix = item["fixture"]
            teams = item["teams"]
            goals = item["goals"]
            status = fix.get("status", {}).get("short", "")
            # Only include fully finished matches
            if status not in ("FT", "AET", "PEN"):
                continue
            results.append(
                {
                    "fixture_id": fix["id"],
                    "home_team": teams["home"]["name"],
                    "away_team": teams["away"]["name"],
                    "home_goals": goals.get("home") or 0,
                    "away_goals": goals.get("away") or 0,
                    "kickoff_utc": fix.get("date", ""),
                }
            )
        except (KeyError, TypeError):
            continue
    return results


# ---------------------------------------------------------------------------
# Football-data.co.uk — free public historical data (no API key required)
# ---------------------------------------------------------------------------

# Mapping from display label → football-data.co.uk file code
FDCO_LEAGUES: dict[str, str] = {
    "England — Premier League":   "E0",
    "England — Championship":     "E1",
    "England — League One":       "E2",
    "England — League Two":       "E3",
    "Germany — Bundesliga":       "D1",
    "Germany — 2. Bundesliga":    "D2",
    "Italy — Serie A":            "I1",
    "Italy — Serie B":            "I2",
    "Spain — La Liga":            "SP1",
    "Spain — Segunda División":   "SP2",
    "France — Ligue 1":           "F1",
    "France — Ligue 2":           "F2",
    "Netherlands — Eredivisie":   "N1",
    "Belgium — First Division A": "B1",
    "Portugal — Primeira Liga":   "P1",
    "Turkey — Süper Lig":         "T1",
    "Greece — Super League":      "G1",
    "Scotland — Premiership":     "SC0",
    "Scotland — Championship":    "SC1",
}

_FDCO_BASE = "https://www.football-data.co.uk/mmz4281"


def _fdco_season_code(season: int) -> str:
    """Convert start-year integer to football-data.co.uk folder code.

    e.g. 2024 → '2425'  (season runs 2024-25)
         2020 → '2021'
    """
    return f"{str(season)[2:]}{str(season + 1)[2:]}"


def fetch_historical_from_fdco(league_code: str, season: int) -> list[dict]:
    """
    Download historical match data from football-data.co.uk.
    No API key required — completely free public source.

    Args:
        league_code: FDCO code, e.g. 'E1' for Championship, 'I1' for Serie A.
        season     : Start year of the season, e.g. 2024 for 2024-25.

    Returns:
        List of dicts with keys: home_team, away_team, home_goals, away_goals,
        draw_odds (real Bet365 historical odds), kickoff_utc.
    """
    import csv as csv_mod
    import io as io_mod

    season_str = _fdco_season_code(season)
    url = f"{_FDCO_BASE}/{season_str}/{league_code}.csv"

    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"[data_fetcher] fetch_historical_from_fdco: HTTP error for {url}: {e}")
        return []

    # Strip BOM if present and parse
    text = resp.content.decode("utf-8-sig")
    reader = csv_mod.DictReader(io_mod.StringIO(text))

    results: list[dict] = []
    for row in reader:
        try:
            # Goals — both old (HG/AG) and new (FTHG/FTAG) column names
            home_g_raw = row.get("FTHG") or row.get("HG") or ""
            away_g_raw = row.get("FTAG") or row.get("AG") or ""
            if not home_g_raw.strip() or not away_g_raw.strip():
                continue  # skip rows without result (postponed / future)

            home_g = int(float(home_g_raw))
            away_g = int(float(away_g_raw))

            # Draw odds — prefer Bet365, fall back through common bookmakers
            draw_odds = 3.0  # safe default
            for col in ("B365D", "BWD", "IWD", "WHD", "VCD", "PSC", "MaxD", "AvgD"):
                val = (row.get(col) or "").strip()
                if val:
                    try:
                        draw_odds = float(val)
                        break
                    except ValueError:
                        continue

            results.append(
                {
                    "home_team":   row.get("HomeTeam", ""),
                    "away_team":   row.get("AwayTeam", ""),
                    "home_goals":  home_g,
                    "away_goals":  away_g,
                    "draw_odds":   draw_odds,
                    "kickoff_utc": row.get("Date", ""),
                }
            )
        except (ValueError, TypeError, KeyError):
            continue

    return results


def check_api_suspended() -> tuple[bool, str]:
    """
    Quick check whether the API-Football account is active.
    Returns (is_suspended, error_message).
    Does NOT count towards the daily API quota.
    """
    try:
        key = os.environ.get("API_FOOTBALL_KEY", "")
        if not key:
            return True, "API_FOOTBALL_KEY environment variable is not set."
        resp = requests.get(
            f"{BASE_URL}/status",
            headers={"x-apisports-key": key},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        errors = data.get("errors")
        if errors:
            msg = list(errors.values())[0] if isinstance(errors, dict) else str(errors)
            return True, msg
        return False, ""
    except Exception as e:
        return True, str(e)


# ---------------------------------------------------------------------------
# Cross-source league mappings
# Centralised here so data_fetcher stays the single source of truth;
# league_scanner + daily_refresh import these dicts instead of duplicating them.
# ---------------------------------------------------------------------------

# Odds API sport key → internal league_key (matches the bets/sequences DB tables)
ODDS_SPORT_KEY_TO_LEAGUE_KEY: dict[str, str] = {
    "soccer_epl":                    "league_39",
    "soccer_efl_champ":              "league_40",
    "soccer_germany_bundesliga":     "league_78",
    "soccer_germany_bundesliga2":    "league_79",
    "soccer_italy_serie_a":          "league_135",
    "soccer_italy_serie_b":          "league_136",
    "soccer_spain_la_liga":          "league_140",
    "soccer_spain_segunda_division": "league_141",
    "soccer_france_ligue_1":         "league_61",
    "soccer_france_ligue_2":         "league_66",
    "soccer_netherlands_eredivisie": "league_88",
    "soccer_belgium_first_div":      "league_144",
    "soccer_portugal_primeira_liga": "league_94",
    "soccer_turkey_super_lig":       "league_203",
    "soccer_greece_super_league":    "league_197",
    "soccer_scotland_prem":          "league_179",
}

# Reverse: internal league_key → Odds API sport key
LEAGUE_KEY_TO_ODDS_SPORT_KEY: dict[str, str] = {
    v: k for k, v in ODDS_SPORT_KEY_TO_LEAGUE_KEY.items()
}

# Internal league_key → football-data.co.uk code (for FDCO draw-rate calculation)
LEAGUE_KEY_TO_FDCO_CODE: dict[str, str] = {
    "league_39":  "E0",   # Premier League
    "league_40":  "E1",   # Championship
    "league_41":  "E2",   # League One
    "league_42":  "E3",   # League Two
    "league_78":  "D1",   # Bundesliga
    "league_79":  "D2",   # 2. Bundesliga
    "league_135": "I1",   # Serie A
    "league_136": "I2",   # Serie B
    "league_140": "SP1",  # La Liga
    "league_141": "SP2",  # Segunda División
    "league_61":  "F1",   # Ligue 1
    "league_66":  "F2",   # Ligue 2
    "league_88":  "N1",   # Eredivisie
    "league_144": "B1",   # Belgian Pro League
    "league_94":  "P1",   # Primeira Liga
    "league_203": "T1",   # Süper Lig
    "league_197": "G1",   # Super League Greece
    "league_179": "SC0",  # Scottish Premiership
}

# Internal league_key → OpenLigaDB league shortcut (German leagues only)
LEAGUE_KEY_TO_OPENLIGADB: dict[str, str] = {
    "league_78": "bl1",   # Bundesliga
    "league_79": "bl2",   # 2. Bundesliga
    "league_80": "bl3",   # 3. Liga
}


# ---------------------------------------------------------------------------
# The Odds API (v4) — upcoming fixtures + real-time draw odds (free tier: 500/month)
# ---------------------------------------------------------------------------

_ODDS_API_BASE = "https://api.the-odds-api.com/v4"
_odds_credits_remaining: int | None = None
_odds_credits_used: int | None = None


def _odds_get(path: str, params: dict) -> dict | list | None:
    """GET from The Odds API. Updates the module-level credit counters from headers."""
    global _odds_credits_remaining, _odds_credits_used
    key = os.environ.get("ODDS_API_KEY", "")
    if not key:
        print("[data_fetcher] ODDS_API_KEY env var not set.")
        return None
    try:
        resp = requests.get(
            f"{_ODDS_API_BASE}/{path}",
            params={"apiKey": key, **params},
            timeout=20,
        )
        try:
            rem = resp.headers.get("x-requests-remaining")
            used = resp.headers.get("x-requests-used")
            if rem is not None:
                _odds_credits_remaining = int(rem)
            if used is not None:
                _odds_credits_used = int(used)
        except (ValueError, TypeError):
            pass
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as e:
        print(f"[data_fetcher] Odds API /{path} error: {e}")
        return None


def get_odds_api_credits() -> tuple[int | None, int | None]:
    """Return (remaining, used) credits cached from the last Odds API response."""
    return _odds_credits_remaining, _odds_credits_used


def get_odds_api_status() -> tuple[bool, str, int | None]:
    """
    Lightweight status check using the /sports endpoint (costs 0 credits).
    Returns (is_ok, message, credits_remaining).
    """
    key = os.environ.get("ODDS_API_KEY", "")
    if not key:
        return False, "ODDS_API_KEY environment variable is not set.", None
    try:
        resp = requests.get(
            f"{_ODDS_API_BASE}/sports",
            params={"apiKey": key, "all": "false"},
            timeout=10,
        )
        try:
            global _odds_credits_remaining
            rem = resp.headers.get("x-requests-remaining")
            if rem is not None:
                _odds_credits_remaining = int(rem)
        except (ValueError, TypeError):
            pass
        if resp.status_code == 401:
            return False, "Invalid API key — check ODDS_API_KEY in secrets.", None
        if resp.status_code == 422:
            return False, resp.json().get("message", "API error (422)"), None
        resp.raise_for_status()
        return True, "OK", _odds_credits_remaining
    except Exception as e:
        return False, str(e), None


def fetch_odds_api_fixtures(
    sport_keys: list[str],
    days_ahead: int = 3,
) -> list[dict]:
    """
    Fetch upcoming fixtures with Bet365 draw odds from The Odds API.
    One call per sport_key returns ALL current fixtures + odds for that league.

    Returns a list of dicts matching the same schema as fetch_upcoming_fixtures:
        fixture_id, league_key, home_team, away_team, kickoff_utc, draw_odds, h2h_draw_rate
    """
    import hashlib

    results: list[dict] = []
    now_utc = datetime.now(timezone.utc)
    cutoff = now_utc + timedelta(days=days_ahead)
    cutoff_iso = cutoff.strftime("%Y-%m-%dT%H:%M:%SZ")
    now_iso = now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

    for sport_key in sport_keys:
        data = _odds_get(
            f"sports/{sport_key}/odds",
            {
                "regions": "uk",
                "markets": "h2h",
                "oddsFormat": "decimal",
                "dateFormat": "iso",
                "bookmakers": "bet365",
                "commenceTimeFrom": now_iso,
                "commenceTimeTo": cutoff_iso,
            },
        )
        if not data or not isinstance(data, list):
            continue

        league_key = ODDS_SPORT_KEY_TO_LEAGUE_KEY.get(sport_key, f"league_{sport_key}")

        for event in data:
            try:
                home = event["home_team"]
                away = event["away_team"]
                commence = event["commence_time"]

                # Stable integer fixture_id from home + away + date
                hash_key = f"{home}:{away}:{commence[:10]}"
                fixture_id = int(hashlib.md5(hash_key.encode()).hexdigest()[:8], 16)

                # Extract draw price (prefer Bet365, accept any bookmaker)
                draw_odds: float | None = None
                for bm in event.get("bookmakers", []):
                    for market in bm.get("markets", []):
                        if market.get("key") == "h2h":
                            for outcome in market.get("outcomes", []):
                                if outcome.get("name") == "Draw":
                                    try:
                                        draw_odds = float(outcome["price"])
                                    except (ValueError, TypeError):
                                        pass
                                    break
                    if draw_odds is not None:
                        break

                results.append({
                    "fixture_id":    fixture_id,
                    "league_key":    league_key,
                    "home_team":     home,
                    "away_team":     away,
                    "kickoff_utc":   commence,
                    "draw_odds":     draw_odds,
                    "h2h_draw_rate": None,
                })
            except (KeyError, TypeError, ValueError):
                continue

    return results


def fetch_odds_api_scores(
    overdue_bets: list[dict],
    sport_key_map: dict[str, str],
) -> dict:
    """
    Fetch completed scores from The Odds API and match against overdue bets.

    Args:
        overdue_bets: pending bets from the DB (need home_team, away_team, league_key)
        sport_key_map: dict mapping league_key → odds_api_sport_key

    Returns:
        dict mapping fixture_id → {fixture_id, result ('WIN'|'LOSS'), home_goals, away_goals}
    """
    results: dict = {}

    # Group bets by sport_key
    by_sport: dict[str, list] = {}
    for bet in overdue_bets:
        lk = bet.get("league_key", "")
        sk = sport_key_map.get(lk)
        if sk:
            by_sport.setdefault(sk, []).append(bet)
        else:
            print(f"[data_fetcher] No Odds API sport key for league {lk} — skipping result check.")

    for sport_key, bets in by_sport.items():
        data = _odds_get(
            f"sports/{sport_key}/scores",
            {"daysFrom": "3", "dateFormat": "iso"},
        )
        if not data or not isinstance(data, list):
            continue

        # Build lookup: "HomeTeam:AwayTeam" → score info
        score_lookup: dict[str, dict] = {}
        for game in data:
            if not game.get("completed"):
                continue
            home = game.get("home_team", "")
            away = game.get("away_team", "")
            raw_scores = game.get("scores") or []
            try:
                scores_dict = {s["name"]: int(s.get("score") or 0) for s in raw_scores}
                home_g = scores_dict.get(home, 0)
                away_g = scores_dict.get(away, 0)
                score_lookup[f"{home}:{away}"] = {
                    "home_goals": home_g,
                    "away_goals": away_g,
                    "result": "WIN" if home_g == away_g else "LOSS",
                }
            except (KeyError, TypeError, ValueError):
                continue

        for bet in bets:
            key = f"{bet.get('home_team', '')}:{bet.get('away_team', '')}"
            if key in score_lookup:
                entry = {**score_lookup[key], "fixture_id": bet["fixture_id"]}
                results[bet["fixture_id"]] = entry

    return results


# ---------------------------------------------------------------------------
# FDCO-based draw-rate calculation (no API key, more accurate than API calls)
# ---------------------------------------------------------------------------

def get_league_draw_rate_from_fdco(fdco_code: str, season: int) -> float:
    """
    Calculate a league's draw rate from football-data.co.uk historical data.
    Returns fraction 0.0–1.0. Falls back to 0.0 if the league/season isn't available.
    """
    matches = fetch_historical_from_fdco(fdco_code, season)
    if not matches:
        return 0.0
    draws = sum(1 for m in matches if m["home_goals"] == m["away_goals"])
    return round(draws / len(matches), 4)


# ---------------------------------------------------------------------------
# OpenLigaDB — German leagues, completely free, no API key required
# ---------------------------------------------------------------------------

_OPENLIGADB_BASE = "https://api.openligadb.de"

# Human-readable labels for the backtester UI
OPENLIGADB_LEAGUES: dict[str, str] = {
    "Germany — Bundesliga":   "bl1",
    "Germany — 2. Bundesliga": "bl2",
    "Germany — 3. Liga":      "bl3",
}


def fetch_openligadb_historical(league_shortcut: str, season: int) -> list[dict]:
    """
    Fetch all finished matches for a German league season from OpenLigaDB.
    No API key required.

    Args:
        league_shortcut : 'bl1' (Bundesliga), 'bl2' (2. Bundesliga), 'bl3' (3. Liga)
        season          : start year, e.g. 2024 for the 2024/25 season

    Returns:
        List of dicts with keys: home_team, away_team, home_goals, away_goals,
        draw_odds (always 3.0 — OpenLigaDB has no odds data), kickoff_utc.
    """
    url = f"{_OPENLIGADB_BASE}/getmatchdata/{league_shortcut}/{season}"
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except requests.RequestException as e:
        print(f"[data_fetcher] OpenLigaDB {league_shortcut}/{season} error: {e}")
        return []

    results: list[dict] = []
    for match in data:
        try:
            if not match.get("matchIsFinished"):
                continue

            results_list = match.get("matchResults", [])
            # resultTypeID=2 is the final (90-min) result
            final = next(
                (r for r in results_list if r.get("resultTypeID") == 2),
                results_list[-1] if results_list else None,
            )
            if not final:
                continue

            home_g = int(final.get("pointsTeam1") or 0)
            away_g = int(final.get("pointsTeam2") or 0)
            home_name = (match.get("team1") or {}).get("teamName", "")
            away_name = (match.get("team2") or {}).get("teamName", "")
            kickoff = match.get("matchDateTime", "")

            results.append({
                "home_team":   home_name,
                "away_team":   away_name,
                "home_goals":  home_g,
                "away_goals":  away_g,
                "draw_odds":   3.0,   # OpenLigaDB has no odds — use neutral default
                "kickoff_utc": kickoff,
            })
        except (KeyError, TypeError, ValueError, IndexError):
            continue

    return results
