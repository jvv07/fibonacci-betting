"""
db.py — Supabase database layer for fibonacci-betting.

Handles all reads and writes for five tables:
  settings, leagues, fixtures, fibonacci_sequences, bets

SQL to create tables is in the project README / deployment checklist.
"""

import os
from datetime import datetime, timedelta, timezone

import httpx
from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv()

# ---------------------------------------------------------------------------
# Client singleton
# ---------------------------------------------------------------------------

_client: Client | None = None


def _force_http1(client: Client) -> None:
    """
    Replace the postgrest httpx session with one that uses HTTP/1.1 only.

    Supabase's free tier (and GitHub Actions' network) can trigger HTTP/2
    StreamReset (RST_STREAM PROTOCOL_ERROR) on every connection.  Forcing
    HTTP/1.1 on the postgrest transport eliminates those errors entirely.

    Headers are rebuilt from env vars rather than copied from the old session
    to avoid httpx rejecting bytes-typed header values.
    """
    try:
        key = os.environ["SUPABASE_KEY"]
        base_url = str(client.postgrest.session.base_url)
        client.postgrest.session = httpx.Client(
            base_url=base_url,
            headers={
                "apikey": key,
                "Authorization": f"Bearer {key}",
                "Accept": "application/json",
                "Content-Type": "application/json",
            },
            http2=False,
            timeout=30.0,
        )
    except Exception as e:
        print(f"[db] HTTP/1.1 patch skipped ({e}) — using default transport.")


def get_client() -> Client:
    """Return a cached Supabase client configured to use HTTP/1.1."""
    global _client
    if _client is None:
        url = os.environ["SUPABASE_URL"]
        key = os.environ["SUPABASE_KEY"]
        _client = create_client(url, key)
        _force_http1(_client)
    return _client


def _fresh_client() -> Client:
    """Force a brand-new client instance (used after unrecoverable errors)."""
    global _client
    url = os.environ["SUPABASE_URL"]
    key = os.environ["SUPABASE_KEY"]
    _client = create_client(url, key)
    _force_http1(_client)
    return _client


def _execute(fn):
    """
    Run a Supabase query, retrying once with a fresh client on any
    connection-level error (belt-and-suspenders on top of the HTTP/1.1 fix).
    """
    try:
        return fn(get_client())
    except Exception as e:
        err = str(e).lower()
        if any(k in err for k in ("streamreset", "stream", "connect", "timeout")):
            print(f"[db] Connection error — retrying with fresh client. ({e})")
            try:
                return fn(_fresh_client())
            except Exception as e2:
                print(f"[db] Retry also failed: {e2}")
                return None
        raise


# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------

def get_settings() -> dict | None:
    """Return the single settings row, or None on error."""
    try:
        res = get_client().table("settings").select("*").limit(1).execute()
        return res.data[0] if res.data else None
    except Exception as e:
        print(f"[db] get_settings error: {e}")
        return None


def update_settings(**kwargs) -> bool:
    """Update arbitrary columns on the settings row (id=1)."""
    try:
        kwargs["updated_at"] = datetime.now(timezone.utc).isoformat()
        get_client().table("settings").update(kwargs).eq("id", 1).execute()
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
        res = get_client().table("bets").insert(bet_dict).execute()
        return res.data[0] if res.data else None
    except Exception as e:
        print(f"[db] save_bet error: {e}")
        return None


def update_bet_result(bet_id: int, result: str, gross_return: float, net_pnl: float) -> bool:
    """Record WIN or LOSS outcome on an existing bet."""
    try:
        get_client().table("bets").update(
            {"result": result, "gross_return": gross_return, "net_pnl": net_pnl}
        ).eq("id", bet_id).execute()
        return True
    except Exception as e:
        print(f"[db] update_bet_result error: {e}")
        return False


def get_pending_bets() -> list[dict]:
    """Return all bets with result='PENDING'."""
    try:
        res = get_client().table("bets").select("*").eq("result", "PENDING").execute()
        return res.data or []
    except Exception as e:
        print(f"[db] get_pending_bets error: {e}")
        return []


def get_bet_history(days: int = 90) -> list[dict]:
    """Return all bets created in the last *days* days, newest first."""
    try:
        since = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        res = (
            get_client()
            .table("bets")
            .select("*")
            .gte("created_at", since)
            .order("created_at", desc=True)
            .execute()
        )
        return res.data or []
    except Exception as e:
        print(f"[db] get_bet_history error: {e}")
        return []


# ---------------------------------------------------------------------------
# Fibonacci sequences
# ---------------------------------------------------------------------------

def get_active_sequences() -> list[dict]:
    """Return all rows from fibonacci_sequences."""
    try:
        res = get_client().table("fibonacci_sequences").select("*").execute()
        return res.data or []
    except Exception as e:
        print(f"[db] get_active_sequences error: {e}")
        return []


def update_sequence(league_key: str, step: int, cumulative_loss: float) -> bool:
    """Upsert the sequence state for a league."""
    try:
        get_client().table("fibonacci_sequences").upsert(
            {
                "league_key": league_key,
                "current_step": step,
                "cumulative_loss": cumulative_loss,
                "last_updated": datetime.now(timezone.utc).isoformat(),
            },
            on_conflict="league_key",
        ).execute()
        return True
    except Exception as e:
        print(f"[db] update_sequence error: {e}")
        return False


def reset_sequence(league_key: str) -> bool:
    """Reset a league's sequence back to step 1 with zero cumulative loss."""
    try:
        get_client().table("fibonacci_sequences").upsert(
            {
                "league_key": league_key,
                "current_step": 1,
                "cumulative_loss": 0.0,
                "series_start": datetime.now(timezone.utc).isoformat(),
                "last_updated": datetime.now(timezone.utc).isoformat(),
            },
            on_conflict="league_key",
        ).execute()
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
        res = _execute(
            lambda c: c.table("fixtures")
            .select("*")
            .gte("kickoff_utc", today)
            .lt("kickoff_utc", tomorrow)
            .execute()
        )
        return (res.data if res else None) or []
    except Exception as e:
        print(f"[db] get_fixtures_today error: {e}")
        return []


def upsert_fixtures(fixture_list: list[dict]) -> bool:
    """Insert or update fixture rows (conflict on fixture_id)."""
    try:
        now = datetime.now(timezone.utc).isoformat()
        for f in fixture_list:
            f["fetched_at"] = now
        get_client().table("fixtures").upsert(fixture_list, on_conflict="fixture_id").execute()
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
        res = _execute(
            lambda c: c.table("leagues")
            .select("*")
            .order("draw_rate_season", desc=True)
            .execute()
        )
        return (res.data if res else None) or []
    except Exception as e:
        print(f"[db] get_leagues error: {e}")
        return []


def upsert_league(league_dict: dict) -> bool:
    """Insert or update a league row (conflict on league_key)."""
    try:
        get_client().table("leagues").upsert(
            league_dict, on_conflict="league_key"
        ).execute()
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
    try:
        res = _execute(
            lambda c: c.table("bets")
            .select("stake, gross_return, net_pnl, result")
            .execute()
        )
        bets = res.data or []

        total_staked = sum(float(b["stake"]) for b in bets if b.get("stake") is not None)
        total_return = sum(
            float(b["gross_return"]) for b in bets if b.get("gross_return") is not None
        )
        net_pnl = sum(
            float(b["net_pnl"]) for b in bets if b.get("net_pnl") is not None
        )

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
            "draw_rate": round(win_rate, 2),  # wins == draws in this system
            "total_bets": len(settled),
            "total_wins": len(wins),
        }
    except Exception as e:
        print(f"[db] get_portfolio_stats error: {e}")
        return {
            "total_staked": 0.0,
            "total_return": 0.0,
            "net_pnl": 0.0,
            "roi": 0.0,
            "win_rate": 0.0,
            "draw_rate": 0.0,
            "total_bets": 0,
            "total_wins": 0,
        }
