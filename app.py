"""
app.py — Streamlit dashboard for fibonacci-betting.

Six pages via sidebar navigation:
  🎯 Today's Bets   — qualifying fixtures + log results
  📊 Dashboard      — portfolio metrics, P&L chart, sequence ladder
  📋 Bet History    — filterable log of all settled bets + CSV export
  🔬 Backtester     — API-Football or CSV-driven season simulation
  🏆 League Scanner — draw-rate rankings + manual activate/deactivate
  ⚙️  Settings       — base stake, odds threshold, Fibonacci cap, bankroll
"""

import io
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from dotenv import load_dotenv

# Make project root importable when streamlit is run from any cwd
sys.path.insert(0, str(Path(__file__).parent))
load_dotenv()

from src import data_fetcher, db, fibonacci_engine, league_scanner

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Fibonacci Betting",
    page_icon="🎯",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Shared CSS
# ---------------------------------------------------------------------------
st.markdown(
    """
    <style>
    /* Tighten metric cards */
    [data-testid="metric-container"] {
        background: #1c1f26;
        border: 1px solid #2d3140;
        border-radius: 8px;
        padding: 12px 16px;
    }
    /* Step-progress bar colour */
    .stProgress > div > div { background-color: #00c853; }
    /* Sidebar radio label size */
    [data-testid="stSidebar"] .stRadio label { font-size: 0.95rem; }
    </style>
    """,
    unsafe_allow_html=True,
)

# ---------------------------------------------------------------------------
# Sidebar navigation
# ---------------------------------------------------------------------------
with st.sidebar:
    st.markdown("## 🎰 Fibonacci Betting")
    st.markdown("---")
    page = st.radio(
        "Navigation",
        [
            "🎯 Today's Bets",
            "📊 Dashboard",
            "📋 Bet History",
            "🔬 Backtester",
            "🏆 League Scanner",
            "⚙️ Settings",
        ],
        label_visibility="collapsed",
    )
    st.markdown("---")
    api_calls = data_fetcher.get_api_calls_today()
    st.caption(f"API calls today: **{api_calls}/100**")
    st.progress(min(api_calls / 100, 1.0))


# ---------------------------------------------------------------------------
# Helper — safe DB connection indicator
# ---------------------------------------------------------------------------
def _db_connected() -> bool:
    return db.ping()


# ===========================================================================
# PAGE 1 — Today's Bets
# ===========================================================================

def page_today_bets():
    st.title("🎯 Today's Bets")

    col_refresh, col_status = st.columns([1, 3])
    with col_refresh:
        if st.button("🔄 Refresh", use_container_width=True):
            st.rerun()

    if not _db_connected():
        st.error("Database not connected — check SUPABASE_URL and SUPABASE_KEY in your .env file.")
        return

    # Load data ---------------------------------------------------------------
    all_fixtures = db.get_fixtures_today()
    settings = db.get_settings() or {}
    sequences = db.get_active_sequences()
    seq_map = {s["league_key"]: s for s in sequences}
    leagues_data = {l["league_key"]: l for l in db.get_leagues()}

    # Build qualifying list ---------------------------------------------------
    qualifying_fixtures: list[dict] = []
    display_rows: list[dict] = []

    for fix in all_fixtures:
        odds = fix.get("draw_odds")
        league_key = fix.get("league_key", "")
        if not odds or not fibonacci_engine.is_bet_qualified(odds, league_key):
            continue

        step = seq_map.get(league_key, {}).get("current_step", 1)
        stake = fibonacci_engine.get_required_stake(league_key)
        h2h = float(fix.get("h2h_draw_rate") or 0)
        league_name = leagues_data.get(league_key, {}).get("league_name", league_key)
        kickoff = (fix.get("kickoff_utc") or "")[:16].replace("T", " ")
        confidence = "🟢" if h2h > 0.30 else "🟡" if h2h > 0.25 else "🔴"

        qualifying_fixtures.append(
            {**fix, "_step": step, "_stake": stake, "_league_name": league_name}
        )
        display_rows.append(
            {
                "Match": f"{fix['home_team']} vs {fix['away_team']}",
                "League": league_name,
                "Kickoff (UTC)": kickoff,
                "Draw Odds": odds,
                "Fib Step": step,
                "Stake (£)": f"£{stake:.2f}",
                "H2H Draw%": f"{h2h*100:.1f}%" if h2h else "—",
                "Conf": confidence,
            }
        )

    # Summary -----------------------------------------------------------------
    if not qualifying_fixtures:
        st.info(
            "No qualifying bets today. "
            "Run the daily refresh script or wait for the 07:00 UTC automation."
        )
        with col_status:
            st.caption(f"🕐 {datetime.now(timezone.utc).strftime('%H:%M UTC')} — No fixtures match the current criteria.")
        return

    with col_status:
        st.success(f"**{len(qualifying_fixtures)}** qualifying bet(s) identified today")

    # Fixtures table ----------------------------------------------------------
    df = pd.DataFrame(display_rows)

    def _colour_odds(val):
        if isinstance(val, float):
            if val >= 3.0:
                return "color: #00ff88; font-weight: bold"
            if val >= 2.88:
                return "color: #ffc107; font-weight: bold"
        return ""

    raw_odds_col = [float(r["Draw Odds"]) for r in display_rows]
    df_display = df.copy()
    df_display["Draw Odds"] = raw_odds_col

    styled = df_display.style.applymap(_colour_odds, subset=["Draw Odds"]).format(
        {"Draw Odds": "{:.2f}"}
    )
    st.dataframe(styled, use_container_width=True, hide_index=True)

    # Total exposure today
    total_exposure = sum(f["_stake"] for f in qualifying_fixtures)
    st.metric("Total Stake Today", f"£{total_exposure:.2f}")

    st.markdown("---")

    # Log Result expander -----------------------------------------------------
    with st.expander("📝 Log a Result"):
        st.markdown("Record the outcome of a bet once the match has finished.")

        labels = [
            f"{f['home_team']} vs {f['away_team']}  |  "
            f"Step {f['_step']}  |  £{f['_stake']:.2f}  |  "
            f"@ {f.get('draw_odds', '?')}"
            for f in qualifying_fixtures
        ]
        sel_idx = st.selectbox("Select fixture", range(len(labels)), format_func=lambda i: labels[i])
        sel_fix = qualifying_fixtures[sel_idx]

        c1, c2 = st.columns([1, 2])
        with c1:
            result = st.radio("Outcome", ["WIN", "LOSS"], horizontal=True)
        with c2:
            st.markdown(
                f"**Stake:** £{sel_fix['_stake']:.2f}  &nbsp;&nbsp;  "
                f"**Odds:** {sel_fix.get('draw_odds', '?')}  &nbsp;&nbsp;  "
                f"**Step:** {sel_fix['_step']}"
            )

        if st.button("✅ Submit Result", type="primary"):
            # Find or create the pending bet record
            pending = db.get_pending_bets()
            existing = next(
                (b for b in pending if b.get("fixture_id") == sel_fix.get("fixture_id")),
                None,
            )

            if existing:
                bet_id = existing["id"]
            else:
                saved = db.save_bet(
                    {
                        "fixture_id": sel_fix.get("fixture_id"),
                        "league_key": sel_fix.get("league_key"),
                        "home_team": sel_fix.get("home_team"),
                        "away_team": sel_fix.get("away_team"),
                        "kickoff_utc": sel_fix.get("kickoff_utc"),
                        "fib_step": sel_fix["_step"],
                        "stake": sel_fix["_stake"],
                        "odds": sel_fix.get("draw_odds"),
                        "result": "PENDING",
                    }
                )
                bet_id = saved["id"] if saved else None

            if bet_id:
                outcome = fibonacci_engine.process_result(
                    bet_id=bet_id,
                    league_key=sel_fix.get("league_key", ""),
                    result=result,
                    stake=float(sel_fix["_stake"]),
                    odds=float(sel_fix.get("draw_odds") or 2.88),
                )
                if result == "WIN":
                    st.success(f"✅ {outcome['message']}")
                    st.balloons()
                else:
                    st.warning(f"📉 {outcome['message']}")
                st.rerun()
            else:
                st.error("Could not save bet record — check database connection.")


# ===========================================================================
# PAGE 2 — Dashboard
# ===========================================================================

def page_dashboard():
    st.title("📊 Dashboard")

    if not _db_connected():
        st.error("Database not connected.")
        return

    summary = fibonacci_engine.get_portfolio_summary()
    stats = summary["stats"]
    sequences = summary["sequences"]
    settings = db.get_settings() or {}
    leagues_data = {l["league_key"]: l for l in db.get_leagues()}

    # Metric cards ------------------------------------------------------------
    bankroll = float(settings.get("bankroll") or 0)
    net_pnl = stats["net_pnl"]
    pnl_delta_colour = "normal" if net_pnl >= 0 else "inverse"

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Bankroll", f"£{bankroll:,.2f}")
    c2.metric(
        "Net P&L",
        f"£{net_pnl:+,.2f}",
        delta=f"£{net_pnl:+.2f}",
        delta_color=pnl_delta_colour,
    )
    c3.metric("Win Rate", f"{stats['win_rate']:.1f}%", f"{stats['total_wins']}/{stats['total_bets']} bets")
    c4.metric("ROI", f"{stats['roi']:+.2f}%")

    st.markdown("---")

    # P&L line chart ----------------------------------------------------------
    history = db.get_bet_history(days=180)
    if history:
        history_sorted = sorted(
            [b for b in history if b.get("result") in ("WIN", "LOSS") and b.get("net_pnl") is not None],
            key=lambda b: b.get("created_at", ""),
        )
        if history_sorted:
            pnl_cumsum = 0.0
            chart_data = []
            for b in history_sorted:
                pnl_cumsum += float(b["net_pnl"])
                chart_data.append(
                    {
                        "Date": b.get("created_at", "")[:10],
                        "Cumulative P&L (£)": round(pnl_cumsum, 2),
                        "Match": f"{b.get('home_team')} vs {b.get('away_team')}",
                        "Result": b.get("result"),
                    }
                )
            df_chart = pd.DataFrame(chart_data)
            fig = px.line(
                df_chart,
                x="Date",
                y="Cumulative P&L (£)",
                title="Cumulative P&L Over Time",
                hover_data=["Match", "Result"],
                color_discrete_sequence=["#00c853"],
            )
            fig.update_layout(
                plot_bgcolor="#0e1117",
                paper_bgcolor="#0e1117",
                font_color="#fafafa",
                xaxis=dict(gridcolor="#2d3140"),
                yaxis=dict(gridcolor="#2d3140", zeroline=True, zerolinecolor="#555"),
                hovermode="x unified",
            )
            fig.add_hline(y=0, line_dash="dash", line_color="#555")
            st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No settled bets yet — P&L chart will appear once results are logged.")

    st.markdown("---")

    # Active Fibonacci sequences table ----------------------------------------
    st.subheader("Active Fibonacci Sequences")

    if not sequences:
        st.info("No active sequences. Add leagues and place bets to see the ladder here.")
        return

    max_step = int(settings.get("max_fib_step", 7))
    base_stake = float(settings.get("base_stake", 10.0))
    FIBONACCI = [1, 1, 2, 3, 5, 8, 13, 21, 34, 55]

    for seq in sequences:
        lk = seq.get("league_key", "")
        league_name = leagues_data.get(lk, {}).get("league_name", lk)
        step = int(seq.get("current_step", 1))
        cum_loss = float(seq.get("cumulative_loss", 0))
        idx = min(step - 1, len(FIBONACCI) - 1)
        next_stake = round(base_stake * FIBONACCI[idx], 2)
        progress = step / max_step

        if step <= 3:
            status = "✅ Safe"
            badge_colour = "#00c853"
        elif step <= 5:
            status = "⚠️ Caution"
            badge_colour = "#ffc107"
        else:
            status = "🚨 High Risk"
            badge_colour = "#ff4444"

        with st.container():
            col_name, col_step, col_bar, col_stake, col_exp, col_status = st.columns(
                [2, 1, 2, 1.5, 1.5, 1.5]
            )
            col_name.markdown(f"**{league_name}**")
            col_step.markdown(f"Step **{step}** / {max_step}")
            with col_bar:
                st.progress(min(progress, 1.0))
            col_stake.markdown(f"Next: **£{next_stake:.2f}**")
            col_exp.markdown(f"Exposure: £{cum_loss:.2f}")
            col_status.markdown(
                f"<span style='color:{badge_colour}'>{status}</span>",
                unsafe_allow_html=True,
            )


# ===========================================================================
# PAGE 3 — Bet History
# ===========================================================================

def page_bet_history():
    st.title("📋 Bet History")

    if not _db_connected():
        st.error("Database not connected.")
        return

    # Filters -----------------------------------------------------------------
    fc1, fc2, fc3 = st.columns([2, 2, 2])
    with fc1:
        days_back = st.slider("Show last N days", min_value=7, max_value=365, value=90, step=7)
    with fc2:
        all_leagues = db.get_leagues()
        league_options = {l["league_key"]: l.get("league_name", l["league_key"]) for l in all_leagues}
        selected_leagues = st.multiselect(
            "Filter by league",
            options=list(league_options.keys()),
            format_func=lambda k: league_options[k],
            default=[],
        )
    with fc3:
        result_filter = st.multiselect(
            "Filter by result",
            options=["WIN", "LOSS", "PENDING"],
            default=["WIN", "LOSS", "PENDING"],
        )

    bets = db.get_bet_history(days=days_back)

    if selected_leagues:
        bets = [b for b in bets if b.get("league_key") in selected_leagues]
    if result_filter:
        bets = [b for b in bets if b.get("result") in result_filter]

    if not bets:
        st.info("No bets found for the selected filters.")
        return

    # Build display DataFrame -------------------------------------------------
    rows = []
    for b in bets:
        rows.append(
            {
                "Date": (b.get("created_at") or "")[:10],
                "Match": f"{b.get('home_team', '?')} vs {b.get('away_team', '?')}",
                "League": league_options.get(b.get("league_key", ""), b.get("league_key", "")),
                "Step": b.get("fib_step", ""),
                "Stake (£)": float(b.get("stake") or 0),
                "Odds": float(b.get("odds") or 0),
                "Result": b.get("result", "PENDING"),
                "Gross Return (£)": float(b.get("gross_return") or 0),
                "Net P&L (£)": float(b.get("net_pnl") or 0),
            }
        )

    df = pd.DataFrame(rows)

    # Colour rows by result ---------------------------------------------------
    def _row_style(row):
        result = row.get("Result", "")
        if result == "WIN":
            return ["background-color: #0a3d1a"] * len(row)
        if result == "LOSS":
            return ["background-color: #3d0a0a"] * len(row)
        return [""] * len(row)

    styled = df.style.apply(_row_style, axis=1).format(
        {"Stake (£)": "£{:.2f}", "Gross Return (£)": "£{:.2f}", "Net P&L (£)": "£{:+.2f}", "Odds": "{:.2f}"}
    )
    st.dataframe(styled, use_container_width=True, hide_index=True)

    # Totals row --------------------------------------------------------------
    settled = df[df["Result"].isin(["WIN", "LOSS"])]
    if not settled.empty:
        st.markdown("---")
        t1, t2, t3, t4 = st.columns(4)
        t1.metric("Total Bets", len(settled))
        t2.metric("Total Staked", f"£{settled['Stake (£)'].sum():.2f}")
        t3.metric("Net P&L", f"£{settled['Net P&L (£)'].sum():+.2f}")
        wins = len(settled[settled["Result"] == "WIN"])
        t4.metric("Win Rate", f"{wins/len(settled)*100:.1f}%")

    # CSV download ------------------------------------------------------------
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    st.download_button(
        label="⬇️ Download CSV",
        data=csv_bytes,
        file_name=f"fibonacci_bets_{datetime.now(timezone.utc).strftime('%Y%m%d')}.csv",
        mime="text/csv",
    )


# ===========================================================================
# PAGE 4 — Backtester
# ===========================================================================

def page_backtester():
    st.title("🔬 Backtester")

    settings = db.get_settings() or {}
    default_base = float(settings.get("base_stake", 10.0))
    default_min_odds = float(settings.get("min_odds", 2.88))
    default_max_step = int(settings.get("max_fib_step", 7))

    tab_api, tab_csv = st.tabs(["📡 API-Football Backtest", "📂 CSV Upload"])

    # ------------------------------------------------------------------
    # Tab 1 — API-Football
    # ------------------------------------------------------------------
    with tab_api:
        st.markdown("Fetch a full historical season from API-Football and run the Fibonacci simulation.")

        all_leagues = db.get_leagues()
        if not all_leagues:
            all_leagues = [
                {"league_key": f"league_{l['api_id']}", "league_name": l["league_name"], "api_id": l["api_id"]}
                for l in league_scanner.SEED_LEAGUES
            ]

        league_options = {l["league_key"]: f"{l.get('league_name', l['league_key'])} ({l.get('country', '')})" for l in all_leagues}

        ac1, ac2 = st.columns(2)
        with ac1:
            sel_league_key = st.selectbox(
                "League",
                options=list(league_options.keys()),
                format_func=lambda k: league_options[k],
            )
        with ac2:
            season = st.selectbox("Season", options=[2024, 2023, 2022, 2021, 2020], index=0)

        bc1, bc2, bc3 = st.columns(3)
        with bc1:
            bt_base = st.number_input("Base Stake (£)", value=default_base, min_value=1.0, step=1.0)
        with bc2:
            bt_min_odds = st.number_input("Min Odds", value=default_min_odds, min_value=1.5, max_value=5.0, step=0.01, format="%.2f")
        with bc3:
            bt_max_step = st.slider("Max Fib Step", min_value=3, max_value=10, value=default_max_step)

        bt_default_odds = st.number_input(
            "Default draw odds (used when historical odds unavailable)",
            value=3.0, min_value=1.5, max_value=6.0, step=0.05, format="%.2f",
        )

        run_btn = st.button("▶️ Run Backtest", type="primary")

        if run_btn or "bt_result" in st.session_state:
            if run_btn:
                # Identify api_id for the selected league
                api_id = next(
                    (l["api_id"] for l in all_leagues if l["league_key"] == sel_league_key), None
                )
                if not api_id:
                    st.error("Could not determine API ID for this league.")
                    st.stop()

                with st.spinner(f"Fetching {season} season data from API-Football… (uses API quota)"):
                    raw_fixtures = data_fetcher.fetch_historical_fixtures(api_id, season)

                if not raw_fixtures:
                    st.error("No historical data returned. Check your API key and quota.")
                    st.stop()

                matches = [
                    {
                        "home_goals": f["home_goals"],
                        "away_goals": f["away_goals"],
                        "draw_odds": bt_default_odds,
                        "home_team": f["home_team"],
                        "away_team": f["away_team"],
                        "kickoff_utc": f.get("kickoff_utc", ""),
                    }
                    for f in raw_fixtures
                ]

                result = fibonacci_engine.simulate_season(matches, bt_base, bt_min_odds, bt_max_step)
                result["_matches"] = matches
                result["_label"] = f"{league_options[sel_league_key]} — {season}"
                st.session_state["bt_result"] = result

            # Display results -------------------------------------------------
            r = st.session_state.get("bt_result", {})
            if not r:
                st.stop()

            st.success(f"Simulation complete: **{r['_label']}**")

            m1, m2, m3, m4, m5 = st.columns(5)
            m1.metric("Total Bets", r["total_bets"])
            m2.metric("Wins (draws)", r["wins"])
            m3.metric("Draw Rate", f"{r['draw_rate']:.1f}%")
            m4.metric("Net P&L", f"£{r['net_pnl']:+.2f}")
            m5.metric("ROI", f"{r['roi']:+.2f}%")

            m6, m7, m8 = st.columns(3)
            m6.metric("Total Staked", f"£{r['total_staked']:.2f}")
            m7.metric("Max Drawdown", f"£{r['max_drawdown']:.2f}")
            m8.metric("Longest Loss Streak", r["longest_loss_streak"])

            # P&L chart
            if r.get("pnl_series"):
                df_pnl = pd.DataFrame(
                    {"Bet #": range(1, len(r["pnl_series"]) + 1), "Cumulative P&L (£)": r["pnl_series"]}
                )
                fig = px.line(
                    df_pnl,
                    x="Bet #",
                    y="Cumulative P&L (£)",
                    title=f"Backtest P&L — {r['_label']}",
                    color_discrete_sequence=["#00c853"],
                )
                fig.update_layout(
                    plot_bgcolor="#0e1117",
                    paper_bgcolor="#0e1117",
                    font_color="#fafafa",
                    xaxis=dict(gridcolor="#2d3140"),
                    yaxis=dict(gridcolor="#2d3140", zeroline=True, zerolinecolor="#555"),
                )
                fig.add_hline(y=0, line_dash="dash", line_color="#555")
                st.plotly_chart(fig, use_container_width=True)

            # Match-level breakdown table
            if r.get("_matches"):
                with st.expander("📋 Match breakdown"):
                    df_matches = pd.DataFrame(r["_matches"])
                    if "home_goals" in df_matches.columns:
                        df_matches["draw"] = df_matches["home_goals"] == df_matches["away_goals"]
                    st.dataframe(df_matches, use_container_width=True, hide_index=True)

    # ------------------------------------------------------------------
    # Tab 2 — CSV Upload
    # ------------------------------------------------------------------
    with tab_csv:
        st.markdown(
            "Upload a CSV with columns: `date`, `home_team`, `away_team`, "
            "`home_goals`, `away_goals`, `draw_odds`"
        )

        uploaded = st.file_uploader("Choose a CSV file", type=["csv"])

        cc1, cc2, cc3 = st.columns(3)
        with cc1:
            csv_base = st.number_input("Base Stake (£)", value=default_base, min_value=1.0, step=1.0, key="csv_base")
        with cc2:
            csv_min_odds = st.number_input("Min Odds", value=default_min_odds, min_value=1.5, max_value=5.0, step=0.01, format="%.2f", key="csv_min")
        with cc3:
            csv_max_step = st.slider("Max Fib Step", min_value=3, max_value=10, value=default_max_step, key="csv_step")

        if uploaded and st.button("▶️ Run CSV Simulation", type="primary"):
            try:
                df_csv = pd.read_csv(uploaded)
                required = {"home_goals", "away_goals", "draw_odds"}
                missing = required - set(df_csv.columns)
                if missing:
                    st.error(f"CSV missing required columns: {missing}")
                    st.stop()

                matches = df_csv[["home_goals", "away_goals", "draw_odds"]].to_dict("records")
                csv_result = fibonacci_engine.simulate_season(matches, csv_base, csv_min_odds, csv_max_step)

                st.success(f"Simulation over {len(matches)} matches complete.")

                mc1, mc2, mc3, mc4 = st.columns(4)
                mc1.metric("Net P&L", f"£{csv_result['net_pnl']:+.2f}")
                mc2.metric("ROI", f"{csv_result['roi']:+.2f}%")
                mc3.metric("Draw Rate", f"{csv_result['draw_rate']:.1f}%")
                mc4.metric("Max Drawdown", f"£{csv_result['max_drawdown']:.2f}")

                if csv_result.get("pnl_series"):
                    df_pnl = pd.DataFrame(
                        {
                            "Bet #": range(1, len(csv_result["pnl_series"]) + 1),
                            "Cumulative P&L (£)": csv_result["pnl_series"],
                        }
                    )
                    fig = px.line(
                        df_pnl,
                        x="Bet #",
                        y="Cumulative P&L (£)",
                        title="CSV Backtest — Cumulative P&L",
                        color_discrete_sequence=["#00c853"],
                    )
                    fig.update_layout(
                        plot_bgcolor="#0e1117",
                        paper_bgcolor="#0e1117",
                        font_color="#fafafa",
                    )
                    st.plotly_chart(fig, use_container_width=True)

            except Exception as e:
                st.error(f"Error processing CSV: {e}")


# ===========================================================================
# PAGE 5 — League Scanner
# ===========================================================================

def page_league_scanner():
    st.title("🏆 League Scanner")

    if not _db_connected():
        st.error("Database not connected.")
        return

    leagues = db.get_leagues()

    # Header actions ----------------------------------------------------------
    sc1, sc2, sc3 = st.columns([2, 1, 2])
    with sc1:
        if st.button("🔍 Re-Scan Now (costs API quota)", type="primary"):
            with st.spinner("Scanning league draw rates — this may take a minute…"):
                leagues = league_scanner.update_league_draw_rates()
            st.success(f"Scan complete. {len(leagues)} leagues updated.")
            st.rerun()

    last_scanned = max(
        (l.get("last_scanned") or "" for l in leagues), default=""
    )
    if last_scanned:
        with sc3:
            st.caption(f"Last scan: {last_scanned[:16].replace('T', ' ')} UTC")

    if not leagues:
        st.info("No leagues scanned yet. Click Re-Scan to populate.")
        return

    # Leagues table -----------------------------------------------------------
    st.markdown("---")
    st.subheader("League Rankings")

    for league in leagues:
        lk = league.get("league_key", "")
        is_active = bool(league.get("is_active", False))
        draw_pct = round(float(league.get("draw_rate_season") or 0) * 100, 1)
        score = float(league.get("score") or 0) if league.get("score") else (draw_pct)

        rec_icon = "⭐" if draw_pct >= 28 else ""
        active_badge = "🟢 Active" if is_active else "⚫ Inactive"

        row_c1, row_c2, row_c3, row_c4, row_c5, row_c6 = st.columns(
            [2.5, 1.5, 1.2, 1.2, 1.5, 1.5]
        )
        row_c1.markdown(f"**{league.get('league_name', lk)}** {rec_icon}")
        row_c2.markdown(league.get("country", ""))
        row_c3.markdown(f"**{draw_pct:.1f}%** draws")
        row_c4.markdown(f"Score: **{score:.1f}**")
        row_c5.markdown(active_badge)

        with row_c6:
            new_active = st.toggle(
                "Activate",
                value=is_active,
                key=f"toggle_{lk}",
                label_visibility="collapsed",
            )
            if new_active != is_active:
                db.upsert_league({**league, "is_active": new_active})
                st.rerun()

    # Seed leagues not yet in DB ----------------------------------------------
    existing_keys = {l["league_key"] for l in leagues}
    missing = [l for l in league_scanner.SEED_LEAGUES if l["league_key"] not in existing_keys]
    if missing:
        with st.expander(f"ℹ️ {len(missing)} seed leagues not yet scanned"):
            st.table(pd.DataFrame(missing)[["league_name", "country", "api_id"]])


# ===========================================================================
# PAGE 6 — Settings
# ===========================================================================

def page_settings():
    st.title("⚙️ Settings")

    if not _db_connected():
        st.error("Database not connected.")
        return

    settings = db.get_settings() or {}

    with st.form("settings_form"):
        st.subheader("Staking Parameters")

        sc1, sc2 = st.columns(2)
        with sc1:
            base_stake = st.number_input(
                "Base Stake (£) — step 1 stake",
                value=float(settings.get("base_stake") or 10.0),
                min_value=1.0,
                step=1.0,
            )
            bankroll = st.number_input(
                "Bankroll (£)",
                value=float(settings.get("bankroll") or 0.0),
                min_value=0.0,
                step=10.0,
            )
            commission_pct = st.number_input(
                "Commission / Exchange fee (%)",
                value=float(settings.get("commission_pct") or 0.0),
                min_value=0.0,
                max_value=10.0,
                step=0.1,
                format="%.1f",
            )

        with sc2:
            min_odds = st.slider(
                "Minimum draw odds",
                min_value=2.50,
                max_value=3.50,
                value=float(settings.get("min_odds") or 2.88),
                step=0.01,
                format="%.2f",
            )
            max_fib_step = st.slider(
                "Max Fibonacci step (stop-loss)",
                min_value=3,
                max_value=10,
                value=int(settings.get("max_fib_step") or 7),
            )

        st.markdown("---")

        # Preview the stake ladder
        FIBONACCI = [1, 1, 2, 3, 5, 8, 13, 21, 34, 55]
        preview = [
            {
                "Step": i + 1,
                "Multiplier": FIBONACCI[i],
                "Stake (£)": round(base_stake * FIBONACCI[i], 2),
                "Status": "✅" if (i + 1) <= 3 else "⚠️" if (i + 1) <= 5 else "🚨",
            }
            for i in range(max_fib_step)
        ]
        st.markdown("**Fibonacci stake ladder preview:**")
        st.dataframe(pd.DataFrame(preview), hide_index=True, use_container_width=False)

        submitted = st.form_submit_button("💾 Save Settings", type="primary")
        if submitted:
            ok = db.update_settings(
                base_stake=base_stake,
                min_odds=min_odds,
                max_fib_step=max_fib_step,
                bankroll=bankroll,
                commission_pct=commission_pct,
            )
            if ok:
                st.success("Settings saved.")
            else:
                st.error("Failed to save settings — check DB connection.")

    # API usage ---------------------------------------------------------------
    st.markdown("---")
    st.subheader("API Usage")

    api_calls = data_fetcher.get_api_calls_today()
    pct = min(api_calls / 100, 1.0)

    col_a, col_b = st.columns([3, 1])
    with col_a:
        colour = "#ff4444" if pct > 0.9 else "#ffc107" if pct > 0.7 else "#00c853"
        st.markdown(
            f"<div style='margin-bottom:4px'>API calls today: "
            f"<strong style='color:{colour}'>{api_calls}/100</strong></div>",
            unsafe_allow_html=True,
        )
        st.progress(pct)
    with col_b:
        st.metric("Remaining", 100 - api_calls)

    st.caption(
        "The API-Football free tier allows 100 calls/day. "
        "The app refuses calls at 95 to leave a safety buffer. "
        "Counter resets at midnight UTC."
    )


# ===========================================================================
# Router
# ===========================================================================

if page == "🎯 Today's Bets":
    page_today_bets()
elif page == "📊 Dashboard":
    page_dashboard()
elif page == "📋 Bet History":
    page_bet_history()
elif page == "🔬 Backtester":
    page_backtester()
elif page == "🏆 League Scanner":
    page_league_scanner()
elif page == "⚙️ Settings":
    page_settings()
