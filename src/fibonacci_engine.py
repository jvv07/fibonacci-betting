"""
fibonacci_engine.py — Fibonacci staking ladder logic for fibonacci-betting.

The Fibonacci sequence used is [1,1,2,3,5,8,13,21,34,55] (steps 1–10).
On a LOSS  → advance one step (stake increases).
On a WIN   → reset to step 1 (profit covers the series loss).
Stop-loss  → if next step would exceed max_fib_step, log and reset.
"""

from src import db

# 10-level Fibonacci multipliers (1-indexed: step 1 → index 0)
FIBONACCI = [1, 1, 2, 3, 5, 8, 13, 21, 34, 55]


def _settings() -> dict:
    """Return settings from DB, with safe defaults if the table is empty."""
    s = db.get_settings()
    if not s:
        return {
            "base_stake": 10.0,
            "min_odds": 2.88,
            "max_fib_step": 7,
            "commission_pct": 0.0,
        }
    return s


def _seq_map() -> dict[str, dict]:
    """Return a dict of league_key → sequence row."""
    return {s["league_key"]: s for s in db.get_active_sequences()}


# ---------------------------------------------------------------------------
# Core public functions
# ---------------------------------------------------------------------------

def get_required_stake(league_key: str) -> float:
    """
    Return the stake to place for the next bet in *league_key*'s sequence.
    stake = base_stake × fib[current_step - 1]
    """
    s = _settings()
    base = float(s.get("base_stake", 10.0))
    step = _seq_map().get(league_key, {}).get("current_step", 1)
    idx = min(step - 1, len(FIBONACCI) - 1)
    return round(base * FIBONACCI[idx], 2)


def process_result(
    bet_id: int,
    league_key: str,
    result: str,
    stake: float,
    odds: float,
) -> dict:
    """
    Process a settled bet, update the database, and advance / reset the sequence.

    Args:
        bet_id    : ID of the bet row in the bets table.
        league_key: e.g. 'league_135'.
        result    : 'WIN' or 'LOSS'.
        stake     : Amount staked (£).
        odds      : Decimal odds at which the bet was placed.

    Returns:
        dict with keys: result, net_pnl, new_step, message
    """
    s = _settings()
    commission_rate = float(s.get("commission_pct", 0.0)) / 100.0
    max_step = int(s.get("max_fib_step", 7))

    seq = _seq_map().get(league_key, {"current_step": 1, "cumulative_loss": 0.0})
    current_step = int(seq.get("current_step", 1))
    cumulative_loss = float(seq.get("cumulative_loss", 0.0))

    if result == "WIN":
        gross_return = round(stake * odds, 2)
        commission = round(gross_return * commission_rate, 2)
        net_pnl = round(gross_return - commission - stake, 2)

        db.update_bet_result(bet_id, "WIN", gross_return, net_pnl)
        db.reset_sequence(league_key)

        return {
            "result": "WIN",
            "net_pnl": net_pnl,
            "new_step": 1,
            "message": (
                f"WIN! Gross return £{gross_return:.2f}"
                + (f" (−£{commission:.2f} commission)" if commission > 0 else "")
                + f". Net P&L: £{net_pnl:+.2f}. Sequence reset to step 1."
            ),
        }

    # LOSS ----------------------------------------------------------------
    gross_return = 0.0
    net_pnl = round(-stake, 2)
    new_cumulative_loss = round(cumulative_loss + stake, 2)

    db.update_bet_result(bet_id, "LOSS", gross_return, net_pnl)

    new_step = current_step + 1

    if new_step > max_step:
        # Stop-loss triggered — reset and warn
        print(
            f"[fibonacci_engine] STOP-LOSS for {league_key}: "
            f"step {new_step} > max {max_step}. Series loss £{new_cumulative_loss:.2f}. Resetting."
        )
        db.reset_sequence(league_key)
        return {
            "result": "LOSS",
            "net_pnl": net_pnl,
            "new_step": 1,
            "message": (
                f"LOSS. Stop-loss triggered — step {current_step} was the final allowed step. "
                f"Total series loss: £{new_cumulative_loss:.2f}. Sequence reset to step 1."
            ),
        }

    db.update_sequence(league_key, new_step, new_cumulative_loss)
    return {
        "result": "LOSS",
        "net_pnl": net_pnl,
        "new_step": new_step,
        "message": (
            f"LOSS. Moving to step {new_step} "
            f"(next stake: £{get_required_stake(league_key):.2f}). "
            f"Cumulative series exposure: £{new_cumulative_loss:.2f}."
        ),
    }


def is_bet_qualified(draw_odds: float | None, league_key: str) -> bool:
    """
    Return True if this fixture passes the qualification criteria:
      • draw_odds >= min_odds from settings
      • current Fibonacci step <= max_fib_step
    """
    if draw_odds is None:
        return False

    s = _settings()
    min_odds = float(s.get("min_odds", 2.88))
    max_step = int(s.get("max_fib_step", 7))

    if draw_odds < min_odds:
        return False

    current_step = _seq_map().get(league_key, {}).get("current_step", 1)
    return current_step <= max_step


def get_portfolio_summary() -> dict:
    """
    Return a combined dict of portfolio stats and active Fibonacci sequences.
    Suitable for the dashboard page.
    """
    return {
        "stats": db.get_portfolio_stats(),
        "sequences": db.get_active_sequences(),
    }


def simulate_season(
    matches: list[dict],
    base_stake: float,
    min_odds: float,
    max_step: int,
) -> dict:
    """
    Run a full Fibonacci simulation over a list of historical matches.

    Each match dict should contain:
        home_goals (int), away_goals (int), draw_odds (float)

    Returns:
        dict with keys: total_bets, wins, losses, draw_rate, total_staked,
                        net_pnl, roi, max_drawdown, longest_loss_streak, pnl_series
    """
    step = 1
    total_staked = 0.0
    net_pnl = 0.0
    wins = losses = bets_placed = 0
    pnl_series: list[float] = []
    peak_pnl = 0.0
    max_drawdown = 0.0
    current_streak = longest_streak = 0

    for match in matches:
        odds = float(match.get("draw_odds") or 0.0)
        home_g = int(match.get("home_goals") or 0)
        away_g = int(match.get("away_goals") or 0)

        if odds < min_odds:
            continue

        if step > max_step:
            # Stop-loss — reset and skip this match
            step = 1
            continue

        idx = min(step - 1, len(FIBONACCI) - 1)
        stake = round(base_stake * FIBONACCI[idx], 2)
        total_staked += stake
        bets_placed += 1

        is_draw = home_g == away_g

        if is_draw:
            pnl = round(stake * odds - stake, 2)
            net_pnl += pnl
            wins += 1
            step = 1
            current_streak = 0
        else:
            pnl = -stake
            net_pnl += pnl
            losses += 1
            step += 1
            current_streak += 1
            if current_streak > longest_streak:
                longest_streak = current_streak

        pnl_series.append(round(net_pnl, 2))

        # Track drawdown
        if net_pnl > peak_pnl:
            peak_pnl = net_pnl
        drawdown = peak_pnl - net_pnl
        if drawdown > max_drawdown:
            max_drawdown = drawdown

    # Compute draw rate only across matches with qualifying odds
    eligible = [m for m in matches if float(m.get("draw_odds") or 0) >= min_odds]
    qualified_draws = sum(
        1
        for m in eligible
        if int(m.get("home_goals") or 0) == int(m.get("away_goals") or 0)
    )
    draw_rate = (qualified_draws / len(eligible) * 100) if eligible else 0.0
    roi = (net_pnl / total_staked * 100) if total_staked else 0.0

    return {
        "total_bets": bets_placed,
        "wins": wins,
        "losses": losses,
        "draw_rate": round(draw_rate, 1),
        "total_staked": round(total_staked, 2),
        "net_pnl": round(net_pnl, 2),
        "roi": round(roi, 2),
        "max_drawdown": round(max_drawdown, 2),
        "longest_loss_streak": longest_streak,
        "pnl_series": pnl_series,
    }
