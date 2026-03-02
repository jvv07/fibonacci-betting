"""
league_scanner.py — League catalogue + draw-rate rankings.

v3: uses football-data.co.uk (via data_fetcher.get_league_draw_rate_from_fdco)
for draw-rate calculation instead of API-Football. This means the scanner works
even when the API-Football account is suspended and costs zero API quota.

The Odds API sport keys and FDCO codes are stored in SEED_LEAGUES so the daily
refresh script can look them up by league_key without touching the DB schema.
"""

from datetime import datetime, timedelta, timezone

from src import db, data_fetcher

# Season to use when calculating draw rates from historical data
CURRENT_SEASON = 2024

# ---------------------------------------------------------------------------
# Seed league catalogue
# Fields:
#   api_id         — API-Football ID (legacy; no longer used for live calls)
#   league_key     — internal key stored in DB (bets, sequences tables)
#   league_name    — display name
#   country        — country for display
#   odds_api_key   — The Odds API sport key (None if not covered)
#   fdco_code      — football-data.co.uk file code (None if not covered)
#   openligadb_key — OpenLigaDB shortcut (German leagues only)
# ---------------------------------------------------------------------------
SEED_LEAGUES: list[dict] = [
    # English leagues — Odds API + FDCO
    {"api_id": 39,  "league_key": "league_39",  "league_name": "Premier League",   "country": "England",     "odds_api_key": "soccer_epl",                    "fdco_code": "E0"},
    {"api_id": 40,  "league_key": "league_40",  "league_name": "Championship",     "country": "England",     "odds_api_key": "soccer_efl_champ",              "fdco_code": "E1"},
    {"api_id": 41,  "league_key": "league_41",  "league_name": "League One",       "country": "England",     "odds_api_key": None,                            "fdco_code": "E2"},
    {"api_id": 42,  "league_key": "league_42",  "league_name": "League Two",       "country": "England",     "odds_api_key": None,                            "fdco_code": "E3"},
    # German leagues — Odds API + FDCO + OpenLigaDB
    {"api_id": 78,  "league_key": "league_78",  "league_name": "Bundesliga",       "country": "Germany",     "odds_api_key": "soccer_germany_bundesliga",     "fdco_code": "D1",  "openligadb_key": "bl1"},
    {"api_id": 79,  "league_key": "league_79",  "league_name": "2. Bundesliga",    "country": "Germany",     "odds_api_key": "soccer_germany_bundesliga2",    "fdco_code": "D2",  "openligadb_key": "bl2"},
    {"api_id": 80,  "league_key": "league_80",  "league_name": "3. Liga",          "country": "Germany",     "odds_api_key": None,                            "fdco_code": None,  "openligadb_key": "bl3"},
    # Italian leagues — Odds API + FDCO
    {"api_id": 135, "league_key": "league_135", "league_name": "Serie A",          "country": "Italy",       "odds_api_key": "soccer_italy_serie_a",          "fdco_code": "I1"},
    {"api_id": 136, "league_key": "league_136", "league_name": "Serie B",          "country": "Italy",       "odds_api_key": "soccer_italy_serie_b",          "fdco_code": "I2"},
    # Spanish leagues — Odds API + FDCO
    {"api_id": 140, "league_key": "league_140", "league_name": "La Liga",          "country": "Spain",       "odds_api_key": "soccer_spain_la_liga",          "fdco_code": "SP1"},
    {"api_id": 141, "league_key": "league_141", "league_name": "Segunda División", "country": "Spain",       "odds_api_key": "soccer_spain_segunda_division", "fdco_code": "SP2"},
    # French leagues — Odds API + FDCO
    {"api_id": 61,  "league_key": "league_61",  "league_name": "Ligue 1",          "country": "France",      "odds_api_key": "soccer_france_ligue_1",         "fdco_code": "F1"},
    {"api_id": 66,  "league_key": "league_66",  "league_name": "Ligue 2",          "country": "France",      "odds_api_key": "soccer_france_ligue_2",         "fdco_code": "F2"},
    # Other major European — Odds API + FDCO
    {"api_id": 88,  "league_key": "league_88",  "league_name": "Eredivisie",       "country": "Netherlands", "odds_api_key": "soccer_netherlands_eredivisie", "fdco_code": "N1"},
    {"api_id": 144, "league_key": "league_144", "league_name": "Pro League",       "country": "Belgium",     "odds_api_key": "soccer_belgium_first_div",      "fdco_code": "B1"},
    {"api_id": 94,  "league_key": "league_94",  "league_name": "Primeira Liga",    "country": "Portugal",    "odds_api_key": "soccer_portugal_primeira_liga", "fdco_code": "P1"},
    {"api_id": 203, "league_key": "league_203", "league_name": "Süper Lig",        "country": "Turkey",      "odds_api_key": "soccer_turkey_super_lig",       "fdco_code": "T1"},
    {"api_id": 197, "league_key": "league_197", "league_name": "Super League",     "country": "Greece",      "odds_api_key": "soccer_greece_super_league",    "fdco_code": "G1"},
    {"api_id": 179, "league_key": "league_179", "league_name": "Premiership",      "country": "Scotland",    "odds_api_key": "soccer_scotland_prem",          "fdco_code": "SC0"},
]

# O(1) lookup by league_key
_LEAGUE_META: dict[str, dict] = {l["league_key"]: l for l in SEED_LEAGUES}


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


def _get_draw_rate(league: dict) -> float:
    """
    Get draw rate for a league.
    Priority: FDCO (free, full-season) → API-Football (legacy, uses quota) → 0.27 default.
    """
    fdco_code = league.get("fdco_code")
    if fdco_code:
        rate = data_fetcher.get_league_draw_rate_from_fdco(fdco_code, CURRENT_SEASON)
        if rate > 0:
            return rate
        # Try previous season if current season not yet complete
        rate = data_fetcher.get_league_draw_rate_from_fdco(fdco_code, CURRENT_SEASON - 1)
        if rate > 0:
            return rate

    # Fallback: API-Football (may be suspended — errors return 0.0)
    try:
        rate = data_fetcher.get_league_draw_rate(league["api_id"], CURRENT_SEASON)
        if rate > 0:
            return rate
    except Exception:
        pass

    # Final default — European average
    return 0.27


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def scan_best_leagues(top_n: int = 10) -> list[dict]:
    """
    Calculate draw rates for all seed leagues and return the top *top_n* by score.
    Uses FDCO historical data — no API-Football quota consumed.
    """
    scored: list[dict] = []
    now_iso = datetime.now(timezone.utc).isoformat()

    for league in SEED_LEAGUES:
        rate = _get_draw_rate(league)
        scored.append(
            {
                "api_id":           league["api_id"],
                "league_key":       league["league_key"],
                "league_name":      league["league_name"],
                "country":          league["country"],
                "draw_rate_season": rate,
                "draw_rate_current": rate,
                "score":            _score(rate),
                "last_scanned":     now_iso,
                "is_active":        False,
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

    print("[league_scanner] Starting league rescan (using FDCO historical data)…")
    results = scan_best_leagues(top_n=len(SEED_LEAGUES))

    for idx, league in enumerate(results):
        league["is_active"] = idx < 6  # top 6 by draw rate get active flag
        db.upsert_league(league)

    print(f"[league_scanner] Upserted {len(results)} leagues. Top 6 marked active.")
    return results


def get_active_league_ids() -> list[int]:
    """Return the api_id values for all leagues where is_active=True."""
    return [l["api_id"] for l in db.get_leagues() if l.get("is_active", False)]


def get_active_sport_keys() -> dict[str, str]:
    """
    Return a dict of league_key → Odds API sport_key for all active leagues
    that have Odds API coverage.

    Falls back to SEED_LEAGUES metadata if the DB row doesn't carry odds_api_key.
    """
    result: dict[str, str] = {}
    for league in db.get_leagues():
        if not league.get("is_active"):
            continue
        lk = league.get("league_key", "")
        # Use the mapping from data_fetcher (single source of truth)
        sk = data_fetcher.LEAGUE_KEY_TO_ODDS_SPORT_KEY.get(lk)
        if sk:
            result[lk] = sk
    return result
