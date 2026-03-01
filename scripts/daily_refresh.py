"""
scripts/daily_refresh.py — Morning refresh job for fibonacci-betting.

Triggered by GitHub Actions at 07:00 UTC every day, or manually via
workflow_dispatch.  Can also be run locally:
    python scripts/daily_refresh.py

Steps performed:
  1. Load active league IDs from the database.
  2. Fetch upcoming fixtures + draw odds → upsert to Supabase.
  3. Resolve results for any PENDING bets older than 2 hours.
  4. Re-scan league draw rates if last scan > 7 days ago.
  5. Build today's qualifying bet list and send the daily alert email.
  6. Print a timestamped summary.
"""

import sys
from pathlib import Path

# Ensure the project root is importable when called as a script
sys.path.insert(0, str(Path(__file__).parent.parent))

from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv

load_dotenv()

from src import data_fetcher, db, fibonacci_engine, league_scanner, notifications


# ---------------------------------------------------------------------------
# Logging helper
# ---------------------------------------------------------------------------

def log(msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"[{ts}] {msg}")


# ---------------------------------------------------------------------------
# Main refresh routine
# ---------------------------------------------------------------------------

def run() -> dict:
    log("=" * 60)
    log("Fibonacci Betting — Daily Refresh Started")
    log("=" * 60)

    # ------------------------------------------------------------------
    # Step 1 — Active league IDs
    # ------------------------------------------------------------------
    log("Step 1: Loading active league IDs…")
    active_ids = league_scanner.get_active_league_ids()

    if not active_ids:
        log("WARNING: No active leagues in DB. Falling back to seed league list.")
        active_ids = [l["api_id"] for l in league_scanner.SEED_LEAGUES[:6]]

    log(f"  Active league IDs: {active_ids}")

    # ------------------------------------------------------------------
    # Step 2 — Upcoming fixtures + draw odds
    # ------------------------------------------------------------------
    log("Step 2: Fetching upcoming fixtures (next 3 days)…")
    fixtures = data_fetcher.fetch_upcoming_fixtures(active_ids, days_ahead=3)
    log(f"  Fetched {len(fixtures)} fixtures from API-Football.")

    # Enrich each fixture with draw odds (1 API call each — costs quota)
    enriched = 0
    for fix in fixtures:
        odds = data_fetcher.fetch_draw_odds(fix["fixture_id"])
        if odds is not None:
            fix["draw_odds"] = odds
            enriched += 1

    log(f"  Enriched {enriched}/{len(fixtures)} fixtures with draw odds.")

    if fixtures:
        ok = db.upsert_fixtures(fixtures)
        log(f"  Upserted fixtures to DB: {'OK' if ok else 'FAILED'}")

    # ------------------------------------------------------------------
    # Step 3 — Resolve pending bets
    # ------------------------------------------------------------------
    log("Step 3: Checking PENDING bets for results…")
    pending = db.get_pending_bets()
    cutoff = datetime.now(timezone.utc) - timedelta(hours=2)

    # Only bets whose kickoff was at least 2 hours ago (match likely finished)
    overdue = [
        b
        for b in pending
        if b.get("kickoff_utc")
        and datetime.fromisoformat(
            b["kickoff_utc"].replace("Z", "+00:00")
        ) < cutoff
    ]
    log(f"  {len(overdue)} PENDING bet(s) ready for result check.")

    processed = 0
    if overdue:
        fixture_ids = [b["fixture_id"] for b in overdue]
        results_map = {
            r["fixture_id"]: r for r in data_fetcher.fetch_results(fixture_ids)
        }

        for bet in overdue:
            fid = bet["fixture_id"]
            if fid not in results_map:
                log(f"  No result yet for fixture {fid} — will retry tomorrow.")
                continue

            res = results_map[fid]
            outcome = fibonacci_engine.process_result(
                bet_id=bet["id"],
                league_key=bet["league_key"],
                result=res["result"],
                stake=float(bet.get("stake", 0)),
                odds=float(bet.get("odds", 2.88)),
            )
            log(f"  Bet {bet['id']} ({bet.get('home_team')} vs {bet.get('away_team')}): {outcome['message']}")
            processed += 1

    log(f"  Processed {processed} bet result(s).")

    # ------------------------------------------------------------------
    # Step 4 — League draw-rate rescan (if due)
    # ------------------------------------------------------------------
    log("Step 4: Checking if league rescan is needed…")
    leagues = league_scanner.update_league_draw_rates()
    log(f"  League catalogue up to date ({len(leagues)} leagues tracked).")

    # ------------------------------------------------------------------
    # Step 5 — Build today's qualifying bets
    # ------------------------------------------------------------------
    log("Step 5: Building today's qualifying bet list…")
    today_fixtures = db.get_fixtures_today()
    leagues_lookup = {l["league_key"]: l for l in db.get_leagues()}

    qualifying: list[dict] = []
    for fix in today_fixtures:
        odds = fix.get("draw_odds")
        league_key = fix.get("league_key", "")

        if not odds or not fibonacci_engine.is_bet_qualified(odds, league_key):
            continue

        stake = fibonacci_engine.get_required_stake(league_key)
        sequences = db.get_active_sequences()
        seq_map = {s["league_key"]: s for s in sequences}
        fib_step = seq_map.get(league_key, {}).get("current_step", 1)
        league_info = leagues_lookup.get(league_key, {})

        qualifying.append(
            {
                **fix,
                "stake": stake,
                "fib_step": fib_step,
                "league_name": league_info.get("league_name", league_key),
            }
        )

        # Auto-save a PENDING bet record so the dashboard can track it
        existing_pending = [
            b for b in db.get_pending_bets() if b.get("fixture_id") == fix["fixture_id"]
        ]
        if not existing_pending:
            db.save_bet(
                {
                    "fixture_id": fix["fixture_id"],
                    "league_key": league_key,
                    "home_team": fix.get("home_team"),
                    "away_team": fix.get("away_team"),
                    "kickoff_utc": fix.get("kickoff_utc"),
                    "fib_step": fib_step,
                    "stake": stake,
                    "odds": odds,
                    "result": "PENDING",
                }
            )

    log(f"  {len(qualifying)} qualifying bet(s) found for today.")

    # ------------------------------------------------------------------
    # Step 6 — Send email alert
    # ------------------------------------------------------------------
    log("Step 6: Sending daily email alert…")
    notifications.send_daily_alert(qualifying)

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    api_calls = data_fetcher.get_api_calls_today()
    stats = db.get_portfolio_stats()

    log("=" * 60)
    log("Daily Refresh Complete")
    log(f"  Fixtures fetched   : {len(fixtures)}")
    log(f"  Bets processed     : {processed}")
    log(f"  Qualifying today   : {len(qualifying)}")
    log(f"  API calls used     : {api_calls}/100")
    log(f"  Portfolio Net P&L  : £{stats['net_pnl']:.2f}")
    log(f"  Portfolio ROI      : {stats['roi']:.1f}%")
    log(f"  Win Rate           : {stats['win_rate']:.1f}%")
    log("=" * 60)

    return {
        "fixtures_fetched": len(fixtures),
        "bets_processed": processed,
        "qualifying_today": len(qualifying),
        "api_calls": api_calls,
    }


if __name__ == "__main__":
    summary = run()
    print(f"\nFinal summary: {summary}")
