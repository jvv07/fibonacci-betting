"""
data_fetcher.py — API-Football v3 HTTP client with daily rate-limit tracking.

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
    """
    data = _get("fixtures", {"league": league_id, "season": season, "status": "FT"})
    if not data or not data.get("response"):
        return []

    results: list[dict] = []
    for item in data["response"]:
        try:
            fix = item["fixture"]
            teams = item["teams"]
            goals = item["goals"]
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
