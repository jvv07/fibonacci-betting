"""
db.py — Supabase database layer using the PostgREST REST API directly.

Uses the `requests` library (HTTP/1.1) instead of supabase-py's httpx stack.
This avoids HTTP/2 StreamReset errors and bytes-header issues that appear in
GitHub Actions with recent versions of supabase-py / postgrest-py.

All public functions match the original interface so the rest of the codebase
is unchanged.
"""

import os
from datetime import datetime, timedelta, timezone

import requests
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Lazy-initialised connection config
# ---------------------------------------------------------------------------

_base_url: str | None = None
_headers: dict | None = None


def _init() -> None:
    """Set up the REST base URL and auth headers from env vars (once)."""
    global _base_url, _headers
    if _base_url is None:
        supabase_url = os.environ["SUPABASE_URL"].rstrip("/")
        key = os.environ["SUPABASE_KEY"]
        _base_url = f"{supabase_url}/rest/v1"
        _headers = {
            "apikey": key,
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }


def ping() -> bool:
    """Return True if the Supabase REST API is reachable with the current credentials."""
    try:
        _init()
        r = requests.get(
            f"{_base_url}/settings",
            headers={**_headers, "Prefer": "count=none"},
            params={"limit": "1"},
            timeout=10,
        )
        return r.status_code < 500
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Internal HTTP helpers
# ---------------------------------------------------------------------------

def _get(table: str, params: list[tuple] | dict | None = None) -> list:
    """GET all matching rows from *table*."""
    _init()
    r = requests.get(f"{_base_url}/{table}", headers=_headers, params=params or {}, timeout=30)
    r.raise_for_status()
    data = r.json()
    return data if isinstance(data, list) else []


def _post(table: str, data, extra_prefer: str = "") -> list:
    """INSERT a row (or rows) into *table*, returning the saved rows."""
    _init()
    prefer = "return=representation"
    if extra_prefer:
        prefer = f"{extra_prefer},{prefer}"
    headers = {**_headers, "Prefer": prefer}
    r = requests.post(f"{_base_url}/{table}", headers=headers, json=data, timeout=30)
    r.raise_for_status()
    result = r.json()
    return result if isinstance(result, list) else ([result] if result else [])


def _patch(table: str, data: dict, filter_params: dict) -> list:
    """UPDATE rows in *table* matching *filter_params*."""
    _init()
    headers = {**_headers, "Prefer": "return=representation"}
    r = requests.patch(
        f"{_base_url}/{table}", headers=headers, json=data, params=filter_params, timeout=30
    )
    r.raise_for_status()
    result = r.json()
    return result if isinstance(result, list) else ([result] if result else [])


def _upsert(table: str, data, on_conflict: str) -> list:
    """
    INSERT or UPDATE (upsert) using PostgREST's merge-duplicates resolution.
    *on_conflict* is the unique column name(s) that determine the conflict.
    """
    _init()
    headers = {
        **_headers,
        "Prefer": "resolution=merge-duplicates,return=representation",
    }
    r = requests.post(
        f"{_base_url}/{table}",
        headers=headers,
        json=data,
        params={"on_conflict": on_conflict},
        timeout=30,
    )
    r.raise_for_status()
    result = r.json()
    return result if isinstance(result, list) else ([result] if result else [])


# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------

def get_settings() -> dict | None:
    """Return the single settings row, or None on error."""
    try:
        rows = _get("settings", {"limit": "1"})
        return rows[0] if rows else None
    except Exception as e:
        print(f"[db] get_settings error: {e}")
        return None


def update_settings(**kwargs) -> bool:
    """Update arbitrary columns on the settings row (id = 1)."""
    try:
        kwargs["updated_at"] = datetime.now(timezone.utc).isoformat()
        _patch("settings", kwargs, {"id": "eq.1"})
        return True
    except Exception as e:
        print(f"[db] update_settings error: {e}")
        return False


# ---------------------------------------------------------------------------
# Bets
# ---------------------------------------------------------------------------

def save_bet(bet_dict: dict) -> dict | None:
    """Insert a new bet row; returns the saved row or None on error."""
    try:
        bet_dict.setdefault("created_at", datetime.now(timezone.utc).isoformat())
        bet_dict.setdefault("result", "PENDING")
        rows = _post("bets", bet_dict)
        return rows[0] if rows else None
    except Exception as e:
        print(f"[db] save_bet error: {e}")
        return None


def update_bet_result(bet_id: int, result: str, gross_return: float, net_pnl: float) -> bool:
    """Record WIN or LOSS outcome on an existing bet."""
    try:
        _patch(
            "bets",
            {"result": result, "gross_return": gross_return, "net_pnl": net_pnl},
            {"id": f"eq.{bet_id}"},
        )
        return True
    except Exception as e:
        print(f"[db] update_bet_result error: {e}")
        return False


def get_pending_bets() -> list[dict]:
    """Return all bets with result = 'PENDING'."""
    try:
        return _get("bets", {"result": "eq.PENDING"})
    except Exception as e:
        print(f"[db] get_pending_bets error: {e}")
        return []


def get_bet_history(days: int = 90) -> list[dict]:
    """Return all bets created in the last *days* days, newest first."""
    try:
        since = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        return _get(
            "bets",
            [("created_at", f"gte.{since}"), ("order", "created_at.desc")],
        )
    except Exception as e:
        print(f"[db] get_bet_history error: {e}")
        return []


# ---------------------------------------------------------------------------
# Fibonacci sequences
# ---------------------------------------------------------------------------

def get_active_sequences() -> list[dict]:
    """Return all rows from fibonacci_sequences."""
    try:
        return _get("fibonacci_sequences")
    except Exception as e:
        print(f"[db] get_active_sequences error: {e}")
        return []


def update_sequence(league_key: str, step: int, cumulative_loss: float) -> bool:
    """Upsert the sequence state for a league."""
    try:
        _upsert(
            "fibonacci_sequences",
            {
                "league_key": league_key,
                "current_step": step,
                "cumulative_loss": cumulative_loss,
                "last_updated": datetime.now(timezone.utc).isoformat(),
            },
            on_conflict="league_key",
        )
        return True
    except Exception as e:
        print(f"[db] update_sequence error: {e}")
        return False


def reset_sequence(league_key: str) -> bool:
    """Reset a league's sequence back to step 1 with zero cumulative loss."""
    try:
        _upsert(
            "fibonacci_sequences",
            {
                "league_key": league_key,
                "current_step": 1,
                "cumulative_loss": 0.0,
                "series_start": datetime.now(timezone.utc).isoformat(),
                "last_updated": datetime.now(timezone.utc).isoformat(),
            },
            on_conflict="league_key",
        )
        return True
    except Exception as e:
        print(f"[db] reset_sequence error: {e}")
        return False


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def get_fixtures_today() -> list[dict]:
    """Return all fixtures whose kickoff falls on today (UTC)."""
    try:
        today = datetime.now(timezone.utc).date().isoformat()
        tomorrow = (datetime.now(timezone.utc).date() + timedelta(days=1)).isoformat()
        # Use list-of-tuples so requests sends both kickoff_utc params
        return _get(
            "fixtures",
            [("kickoff_utc", f"gte.{today}"), ("kickoff_utc", f"lt.{tomorrow}")],
        )
    except Exception as e:
        print(f"[db] get_fixtures_today error: {e}")
        return []


def upsert_fixtures(fixture_list: list[dict]) -> bool:
    """Insert or update fixture rows (conflict on fixture_id)."""
    try:
        now = datetime.now(timezone.utc).isoformat()
        for f in fixture_list:
            f["fetched_at"] = now
        _upsert("fixtures", fixture_list, on_conflict="fixture_id")
        return True
    except Exception as e:
        print(f"[db] upsert_fixtures error: {e}")
        return False


# ---------------------------------------------------------------------------
# Leagues
# ---------------------------------------------------------------------------

def get_leagues() -> list[dict]:
    """Return all league rows ordered by seasonal draw rate descending."""
    try:
        return _get("leagues", {"order": "draw_rate_season.desc"})
    except Exception as e:
        print(f"[db] get_leagues error: {e}")
        return []


def upsert_league(league_dict: dict) -> bool:
    """Insert or update a league row (conflict on league_key)."""
    try:
        _upsert("leagues", league_dict, on_conflict="league_key")
        return True
    except Exception as e:
        print(f"[db] upsert_league error: {e}")
        return False


# ---------------------------------------------------------------------------
# Portfolio stats
# ---------------------------------------------------------------------------

def get_portfolio_stats() -> dict:
    """
    Calculate aggregate portfolio metrics from the bets table.

    Returns:
        dict with keys: total_staked, total_return, net_pnl, roi,
                        win_rate, draw_rate, total_bets, total_wins
    """
    _empty = {
        "total_staked": 0.0, "total_return": 0.0, "net_pnl": 0.0,
        "roi": 0.0, "win_rate": 0.0, "draw_rate": 0.0,
        "total_bets": 0, "total_wins": 0,
    }
    try:
        bets = _get("bets", {"select": "stake,gross_return,net_pnl,result"})

        total_staked = sum(float(b["stake"]) for b in bets if b.get("stake") is not None)
        total_return = sum(
            float(b["gross_return"]) for b in bets if b.get("gross_return") is not None
        )
        net_pnl = sum(float(b["net_pnl"]) for b in bets if b.get("net_pnl") is not None)

        settled = [b for b in bets if b.get("result") in ("WIN", "LOSS")]
        wins = [b for b in settled if b["result"] == "WIN"]

        win_rate = (len(wins) / len(settled) * 100) if settled else 0.0
        roi = (net_pnl / total_staked * 100) if total_staked else 0.0

        return {
            "total_staked": round(total_staked, 2),
            "total_return": round(total_return, 2),
            "net_pnl": round(net_pnl, 2),
            "roi": round(roi, 2),
            "win_rate": round(win_rate, 2),
            "draw_rate": round(win_rate, 2),
            "total_bets": len(settled),
            "total_wins": len(wins),
        }
    except Exception as e:
        print(f"[db] get_portfolio_stats error: {e}")
        return _empty
