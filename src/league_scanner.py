"""
league_scanner.py — Identifies and ranks football leagues by historical draw rate.

Seed league list covers 13 European / South American leagues known for
above-average draw frequencies.  The scorer applies bonuses/penalties:
  +5  if draw_rate > 30 %
  −10 if draw_rate < 25 %

Only re-scans if the most recent scan is older than 7 days (API quota protection).
"""

from datetime import datetime, timedelta, timezone

from src import db, data_fetcher

# Season to use when fetching draw-rate stats
CURRENT_SEASON = 2024

# ---------------------------------------------------------------------------
# Seed league catalogue
# Note: Greece Super League 2 uses API id 207 (98 is Turkey 1. Lig).
# ---------------------------------------------------------------------------
SEED_LEAGUES: list[dict] = [
    {"api_id": 271, "league_key": "league_271", "league_name": "Premier League",    "country": "Israel"},
    {"api_id": 272, "league_key": "league_272", "league_name": "Liga Leumit",       "country": "Israel"},
    {"api_id": 273, "league_key": "league_273", "league_name": "Liga Bet",          "country": "Israel"},
    {"api_id": 135, "league_key": "league_135", "league_name": "Serie B",           "country": "Italy"},
    {"api_id": 66,  "league_key": "league_66",  "league_name": "Ligue 2",           "country": "France"},
    {"api_id": 61,  "league_key": "league_61",  "league_name": "National",          "country": "France"},
    {"api_id": 98,  "league_key": "league_98",  "league_name": "1. Lig",            "country": "Turkey"},
    {"api_id": 128, "league_key": "league_128", "league_name": "Primera Nacional",  "country": "Argentina"},
    {"api_id": 78,  "league_key": "league_78",  "league_name": "3. Liga",           "country": "Germany"},
    {"api_id": 40,  "league_key": "league_40",  "league_name": "Championship",      "country": "England"},
    {"api_id": 72,  "league_key": "league_72",  "league_name": "Serie B",           "country": "Brazil"},
    {"api_id": 283, "league_key": "league_283", "league_name": "Liga I",            "country": "Romania"},
    {"api_id": 207, "league_key": "league_207", "league_name": "Super League 2",    "country": "Greece"},
]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _should_rescan() -> bool:
    """Return True if we've never scanned, or the last scan is > 7 days old."""
    leagues = db.get_leagues()
    if not leagues:
        return True

    timestamps = [l.get("last_scanned") for l in leagues if l.get("last_scanned")]
    if not timestamps:
        return True

    last = max(
        datetime.fromisoformat(ts.replace("Z", "+00:00")) for ts in timestamps
    )
    return (datetime.now(timezone.utc) - last) > timedelta(days=7)


def _score(draw_rate: float) -> float:
    """Convert a draw_rate fraction to a ranking score."""
    score = draw_rate * 100
    if draw_rate > 0.30:
        score += 5
    if draw_rate < 0.25:
        score -= 10
    return round(score, 2)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def scan_best_leagues(top_n: int = 10) -> list[dict]:
    """
    Fetch draw rates for all seed leagues and return the top *top_n* by score.

    Makes one API call per league — ensure you have enough quota before calling.
    """
    scored: list[dict] = []
    now_iso = datetime.now(timezone.utc).isoformat()

    for league in SEED_LEAGUES:
        rate = data_fetcher.get_league_draw_rate(league["api_id"], CURRENT_SEASON)
        scored.append(
            {
                **league,
                "draw_rate_season": rate,
                "draw_rate_current": rate,
                "score": _score(rate),
                "last_scanned": now_iso,
                "is_active": False,
            }
        )

    scored.sort(key=lambda x: x["score"], reverse=True)
    return scored[:top_n]


def update_league_draw_rates() -> list[dict]:
    """
    Rescan draw rates (if due), upsert to DB, and mark the top 6 as active.
    Returns the full updated league list.
    """
    if not _should_rescan():
        print("[league_scanner] Last scan < 7 days ago — skipping rescan.")
        return db.get_leagues()

    print("[league_scanner] Starting league rescan…")
    results = scan_best_leagues(top_n=len(SEED_LEAGUES))

    for idx, league in enumerate(results):
        league["is_active"] = idx < 6  # top 6 get active flag
        db.upsert_league(league)

    print(f"[league_scanner] Upserted {len(results)} leagues. Top 6 marked active.")
    return results


def get_active_league_ids() -> list[int]:
    """Return the api_id values for all leagues where is_active=True."""
    return [l["api_id"] for l in db.get_leagues() if l.get("is_active", False)]
