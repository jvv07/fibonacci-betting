"""
app.py — Fibonacci Betting dashboard v2.

Linear-inspired dark UI. Six pages via sidebar navigation:
  Today      — qualifying fixtures, value scores, exposure warning, log results
  Dashboard  — portfolio metrics, P&L chart, monthly heatmap, sequences, risk analytics
  History    — filterable bet log, CSV + Excel download
  Backtester — API-Football or CSV-driven season simulation
  Leagues    — draw-rate rankings, activate/deactivate
  Settings   — staking parameters, ruin probability preview
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

sys.path.insert(0, str(Path(__file__).parent))
load_dotenv()

from src import data_fetcher, db, fibonacci_engine, league_scanner

# ---------------------------------------------------------------------------
# FDCO league map — defined here so the backtester never depends on the
# data_fetcher module attribute (avoids stale-.pyc AttributeErrors on Cloud).
# ---------------------------------------------------------------------------
_FDCO_LEAGUES: dict[str, str] = {
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

# ---------------------------------------------------------------------------
# Graceful streamlit-extras import
# ---------------------------------------------------------------------------
try:
    from streamlit_extras.metric_cards import style_metric_cards
    from streamlit_extras.colored_header import colored_header
    _HAS_EXTRAS = True
except ImportError:
    _HAS_EXTRAS = False

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Fibonacci",
    page_icon="💵",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Linear-style CSS
# ---------------------------------------------------------------------------
st.markdown(
    """
    <style>
    /* Global */
    [data-testid="stAppViewContainer"] { background-color: #0d0f12; }
    [data-testid="stSidebar"] {
        background-color: #111318;
        border-right: 1px solid #1a1e2a;
    }
    /* Metric cards */
    [data-testid="metric-container"] {
        background: #16191f;
        border: 1px solid #1a1e2a;
        border-left: 3px solid #00c853;
        border-radius: 7px;
        padding: 14px 18px;
    }
    [data-testid="stMetricLabel"] {
        font-size: 0.73rem !important;
        letter-spacing: 0.09em;
        text-transform: uppercase;
        color: #6b7280 !important;
    }
    [data-testid="stMetricValue"] {
        font-size: 1.55rem !important;
        font-weight: 700;
        color: #e2e5eb !important;
    }
    [data-testid="stMetricDelta"] { font-size: 0.82rem !important; }
    /* Progress */
    .stProgress > div > div { background-color: #00c853; }
    /* Primary buttons */
    .stButton > button[kind="primary"] {
        background-color: #00c853;
        color: #0d0f12;
        border: none;
        font-weight: 700;
        letter-spacing: 0.04em;
        border-radius: 6px;
    }
    .stButton > button[kind="primary"]:hover { background-color: #00a844; }
    /* Sidebar nav */
    [data-testid="stSidebar"] .stRadio label {
        font-size: 0.86rem;
        letter-spacing: 0.04em;
        color: #6b7280;
        padding: 2px 0;
    }
    /* Dividers */
    hr { border-color: #1a1e2a !important; }
    /* Dataframe */
    [data-testid="stDataFrame"] {
        border: 1px solid #1a1e2a;
        border-radius: 6px;
    }
    /* Expander */
    .streamlit-expanderHeader { font-size: 0.85rem; color: #6b7280 !important; }
    /* Tabs */
    .stTabs [data-baseweb="tab"] {
        font-size: 0.85rem;
        letter-spacing: 0.04em;
        color: #6b7280;
    }
    .stTabs [aria-selected="true"] { color: #e2e5eb !important; }
    </style>
    """,
    unsafe_allow_html=True,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _db_connected() -> bool:
    return db.ping()


def _section(label: str, description: str = "") -> None:
    """Colored section header (streamlit-extras or plain fallback)."""
    if _HAS_EXTRAS:
        try:
            colored_header(label=label, description=description, color_name="green-70")
            return
        except Exception:
            pass
    st.markdown(f"**{label}**")
    if description:
        st.caption(description)
    st.markdown("<hr style='margin:4px 0 12px 0;'>", unsafe_allow_html=True)


def _cards() -> None:
    """Apply streamlit-extras metric card polish (no-op if not installed)."""
    if _HAS_EXTRAS:
        try:
            style_metric_cards(
                background_color="#16191f",
                border_left_color="#00c853",
                border_color="#1a1e2a",
                box_shadow=False,
            )
        except Exception:
            pass


def _ruin_prob(draw_rate: float, max_step: int) -> float:
    """Geometric probability of max_step consecutive non-draws (stop-loss per series)."""
    if draw_rate <= 0:
        return 100.0
    if draw_rate >= 1:
        return 0.0
    return round((1 - draw_rate) ** max_step * 100, 2)


def _runway(bankroll: float, base_stake: float, max_step: int) -> float:
    """Number of full worst-case series the bankroll can absorb."""
    FIBONACCI = [1, 1, 2, 3, 5, 8, 13, 21, 34, 55]
    worst = sum(base_stake * FIBONACCI[i] for i in range(min(max_step, len(FIBONACCI))))
    return round(bankroll / worst, 1) if worst > 0 else float("inf")


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------
with st.sidebar:
    st.markdown(
        "<div style='padding:14px 0 6px 0;'>"
        "<span style='font-size:1.3rem; font-weight:800; letter-spacing:-0.03em; color:#e2e5eb;'>"
        "💵 FIBONACCI</span><br>"
        "<span style='font-size:0.68rem; color:#374151; letter-spacing:0.12em; text-transform:uppercase;'>"
        "Betting System</span></div>",
        unsafe_allow_html=True,
    )
    st.markdown("<hr style='margin:6px 0 10px;'>", unsafe_allow_html=True)

    page = st.radio(
        "nav",
        ["Today", "Dashboard", "History", "Backtester", "Leagues", "Settings"],
        label_visibility="collapsed",
    )

    st.markdown("<hr style='margin:10px 0 8px;'>", unsafe_allow_html=True)
    _api = data_fetcher.get_api_calls_today()
    _pct = min(_api / 100, 1.0)
    _c = "#ef4444" if _pct > 0.9 else "#f59e0b" if _pct > 0.7 else "#00c853"
    st.markdown(
        f"<div style='font-size:0.73rem; color:#4b5563; margin-bottom:3px;'>"
        f"API · <span style='color:{_c};'>{_api}/100</span></div>",
        unsafe_allow_html=True,
    )
    st.progress(_pct)


# ===========================================================================
# PAGE — TODAY
# ===========================================================================

def page_today():
    _section("Today's Bets", "Qualifying fixtures for the Fibonacci ladder")

    if not _db_connected():
        st.error("Database not connected — check SUPABASE_URL / SUPABASE_KEY.")
        return

    col_r, col_s = st.columns([1, 6])
    with col_r:
        if st.button("Refresh", type="primary", use_container_width=True):
            st.rerun()

    fixtures = db.get_fixtures_today()
    settings = db.get_settings() or {}
    seq_map = {s["league_key"]: s for s in db.get_active_sequences()}
    leagues_map = {l["league_key"]: l for l in db.get_leagues()}

    qualifying: list[dict] = []
    rows: list[dict] = []

    for fix in fixtures:
        odds = fix.get("draw_odds")
        lk = fix.get("league_key", "")
        if not odds or not fibonacci_engine.is_bet_qualified(odds, lk):
            continue

        step = seq_map.get(lk, {}).get("current_step", 1)
        stake = fibonacci_engine.get_required_stake(lk)
        h2h = float(fix.get("h2h_draw_rate") or 0)
        season_rate = float(leagues_map.get(lk, {}).get("draw_rate_season") or 0)
        league_name = leagues_map.get(lk, {}).get("league_name", lk)
        kickoff = (fix.get("kickoff_utc") or "")[:16].replace("T", " ")

        # Value Score: draw_odds × season_draw_rate (>1.0 = positive EV)
        value_score = round(float(odds) * season_rate, 3) if season_rate else None

        qualifying.append({**fix, "_step": step, "_stake": stake, "_league": league_name})
        rows.append({
            "Match": f"{fix['home_team']} vs {fix['away_team']}",
            "League": league_name,
            "Kickoff (UTC)": kickoff,
            "Odds": float(odds),
            "Step": step,
            "Stake (£)": stake,
            "H2H Draw%": f"{h2h * 100:.1f}%" if h2h else "—",
            "Value Score": value_score,
        })

    with col_s:
        if qualifying:
            st.success(f"{len(qualifying)} qualifying bet(s) identified today")
        else:
            st.info(
                "No qualifying bets today. "
                "Run the daily refresh or wait for the 07:00 UTC automation."
            )

    if not qualifying:
        return

    # Simultaneous exposure warning
    total_exposure = sum(f["_stake"] for f in qualifying)
    bankroll = float(settings.get("bankroll") or 0)
    if bankroll > 0 and total_exposure / bankroll > 0.20:
        pct_str = f"{total_exposure / bankroll * 100:.1f}%"
        st.warning(
            f"Simultaneous exposure warning: £{total_exposure:.2f} across "
            f"{len(qualifying)} bet(s) = {pct_str} of bankroll."
        )

    # Table
    df = pd.DataFrame(rows)

    def _colour_odds(val):
        if isinstance(val, float):
            if val >= 3.0:
                return "color:#00c853; font-weight:700"
            if val >= 2.88:
                return "color:#f59e0b; font-weight:700"
        return ""

    def _colour_value(val):
        if isinstance(val, float):
            if val >= 1.0:
                return "color:#00c853; font-weight:700"
            if val >= 0.85:
                return "color:#f59e0b"
        return "color:#6b7280"

    styled = (
        df.style
        .applymap(_colour_odds, subset=["Odds"])
        .applymap(_colour_value, subset=["Value Score"])
        .format({
            "Odds": "{:.2f}",
            "Stake (£)": "£{:.2f}",
            "Value Score": lambda v: f"{v:.3f}" if isinstance(v, float) else "—",
        })
    )
    st.dataframe(styled, use_container_width=True, hide_index=True)

    st.caption(
        "Value Score = draw odds × season draw rate. "
        "Above 1.00 indicates positive expected value."
    )

    # Summary metrics
    c1, c2, c3 = st.columns(3)
    c1.metric("Total Stake Today", f"£{total_exposure:.2f}")
    c2.metric("Qualifying Fixtures", len(qualifying))
    if bankroll:
        c3.metric("Exposure / Bankroll", f"{total_exposure / bankroll * 100:.1f}%")
    _cards()

    st.markdown("---")

    # Log Result
    with st.expander("Log a Result"):
        st.caption("Record the outcome once the match has finished.")
        labels = [
            f"{f['home_team']} vs {f['away_team']}  ·  "
            f"Step {f['_step']}  ·  £{f['_stake']:.2f}  @  {f.get('draw_odds', '?')}"
            for f in qualifying
        ]
        sel_idx = st.selectbox(
            "Fixture", range(len(labels)), format_func=lambda i: labels[i]
        )
        sel = qualifying[sel_idx]

        c1, c2 = st.columns([1, 2])
        with c1:
            result = st.radio("Outcome", ["WIN", "LOSS"], horizontal=True)
        with c2:
            st.markdown(
                f"Stake **£{sel['_stake']:.2f}** &nbsp;·&nbsp; "
                f"Odds **{sel.get('draw_odds', '?')}** &nbsp;·&nbsp; "
                f"Step **{sel['_step']}**"
            )

        if st.button("Submit Result", type="primary"):
            pending = db.get_pending_bets()
            existing = next(
                (b for b in pending if b.get("fixture_id") == sel.get("fixture_id")), None
            )
            if existing:
                bet_id = existing["id"]
            else:
                saved = db.save_bet({
                    "fixture_id": sel.get("fixture_id"),
                    "league_key": sel.get("league_key"),
                    "home_team": sel.get("home_team"),
                    "away_team": sel.get("away_team"),
                    "kickoff_utc": sel.get("kickoff_utc"),
                    "fib_step": sel["_step"],
                    "stake": sel["_stake"],
                    "odds": sel.get("draw_odds"),
                    "result": "PENDING",
                })
                bet_id = saved["id"] if saved else None

            if bet_id:
                outcome = fibonacci_engine.process_result(
                    bet_id=bet_id,
                    league_key=sel.get("league_key", ""),
                    result=result,
                    stake=float(sel["_stake"]),
                    odds=float(sel.get("draw_odds") or 2.88),
                )
                if result == "WIN":
                    st.success(outcome["message"])
                    st.balloons()
                else:
                    st.warning(outcome["message"])
                st.rerun()
            else:
                st.error("Could not save bet — check DB connection.")


# ===========================================================================
# PAGE — DASHBOARD
# ===========================================================================

def page_dashboard():
    _section("Dashboard", "Portfolio performance and Fibonacci sequences")

    if not _db_connected():
        st.error("Database not connected.")
        return

    summary = fibonacci_engine.get_portfolio_summary()
    stats = summary["stats"]
    sequences = summary["sequences"]
    settings = db.get_settings() or {}
    leagues_map = {l["league_key"]: l for l in db.get_leagues()}

    bankroll = float(settings.get("bankroll") or 0)
    base_stake = float(settings.get("base_stake", 10.0))
    max_step = int(settings.get("max_fib_step", 7))
    net_pnl = stats["net_pnl"]

    # Metrics
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Bankroll", f"£{bankroll:,.2f}")
    c2.metric(
        "Net P&L",
        f"£{net_pnl:+,.2f}",
        delta=f"£{net_pnl:+.2f}",
        delta_color="normal" if net_pnl >= 0 else "inverse",
    )
    c3.metric(
        "Win Rate",
        f"{stats['win_rate']:.1f}%",
        f"{stats['total_wins']}/{stats['total_bets']}",
    )
    c4.metric("ROI", f"{stats['roi']:+.2f}%")
    if bankroll:
        rw = _runway(bankroll, base_stake, max_step)
        c5.metric("Runway", f"{rw:.1f} series", help="Worst-case series before bankroll depleted")
    else:
        c5.metric("Runway", "—")
    _cards()

    # P&L chart
    st.markdown("---")
    history = db.get_bet_history(days=180)
    settled = sorted(
        [b for b in history if b.get("result") in ("WIN", "LOSS") and b.get("net_pnl") is not None],
        key=lambda b: b.get("created_at", ""),
    )

    if settled:
        cumsum = 0.0
        chart_pts = []
        for b in settled:
            cumsum += float(b["net_pnl"])
            chart_pts.append({
                "Date": b["created_at"][:10],
                "P&L": round(cumsum, 2),
                "Match": f"{b.get('home_team')} vs {b.get('away_team')}",
                "Result": b.get("result"),
            })
        df_chart = pd.DataFrame(chart_pts)

        fig = px.area(
            df_chart, x="Date", y="P&L",
            hover_data=["Match", "Result"],
            color_discrete_sequence=["#00c853"],
        )
        fig.update_traces(fillcolor="rgba(0,200,83,0.07)", line_color="#00c853", line_width=2)
        fig.update_layout(
            plot_bgcolor="#0d0f12", paper_bgcolor="#0d0f12",
            font_color="#9ca3af",
            xaxis=dict(gridcolor="#1a1e2a", title=""),
            yaxis=dict(gridcolor="#1a1e2a", zeroline=True, zerolinecolor="#2d3748", title="Cumulative P&L (£)"),
            hovermode="x unified",
            margin=dict(l=0, r=0, t=30, b=0),
            title="Cumulative P&L",
            title_font=dict(color="#e2e5eb", size=13),
        )
        fig.add_hline(y=0, line_dash="dot", line_color="#374151", line_width=1)
        st.plotly_chart(fig, use_container_width=True)

        # Monthly P&L heatmap
        st.markdown("---")
        _section("Monthly P&L Heatmap", "Daily profit/loss calendar view")
        try:
            df_daily = (
                pd.DataFrame([{
                    "date": pd.to_datetime(b["created_at"][:10]),
                    "pnl": float(b["net_pnl"]),
                } for b in settled])
                .groupby("date")["pnl"].sum()
                .reset_index()
            )
            df_daily["month"] = df_daily["date"].dt.to_period("M").astype(str)
            df_daily["day"] = df_daily["date"].dt.day

            pivot = df_daily.pivot_table(
                index="month", columns="day", values="pnl", aggfunc="sum"
            )
            # newest month first
            pivot = pivot.loc[sorted(pivot.index, reverse=True)]

            fig_heat = go.Figure(data=go.Heatmap(
                z=pivot.values,
                x=[str(d) for d in pivot.columns],
                y=list(pivot.index),
                colorscale=[
                    [0.0, "#7f1d1d"],
                    [0.45, "#1a1e2a"],
                    [1.0, "#064e3b"],
                ],
                zmid=0,
                text=[
                    [f"£{v:+.2f}" if not pd.isna(v) else "" for v in row]
                    for row in pivot.values
                ],
                texttemplate="%{text}",
                showscale=True,
                xgap=2,
                ygap=2,
                colorbar=dict(tickfont=dict(color="#6b7280"), outlinewidth=0),
            ))
            fig_heat.update_layout(
                plot_bgcolor="#0d0f12", paper_bgcolor="#0d0f12",
                font_color="#9ca3af",
                xaxis=dict(title="Day of Month", gridcolor="#1a1e2a"),
                yaxis=dict(title="", gridcolor="#1a1e2a"),
                margin=dict(l=0, r=0, t=10, b=0),
                height=max(200, len(pivot) * 52 + 60),
            )
            st.plotly_chart(fig_heat, use_container_width=True)
        except Exception as e:
            st.caption(f"Heatmap unavailable: {e}")

    else:
        st.info("No settled bets yet — charts will appear once results are logged.")

    # Fibonacci sequences
    st.markdown("---")
    _section("Active Sequences", "Current ladder state per league")

    if not sequences:
        st.info("No active sequences — place bets to see the ladder here.")
        return

    FIBONACCI = [1, 1, 2, 3, 5, 8, 13, 21, 34, 55]
    # Column header
    h = st.columns([2.5, 1, 2.5, 1.5, 1.5, 1.5])
    for col, lbl in zip(h, ["League", "Step", "Progress", "Next Stake", "Exposure", "Risk"]):
        col.markdown(
            f"<span style='font-size:0.72rem; color:#4b5563; text-transform:uppercase; "
            f"letter-spacing:0.08em;'>{lbl}</span>",
            unsafe_allow_html=True,
        )

    for seq in sequences:
        lk = seq.get("league_key", "")
        name = leagues_map.get(lk, {}).get("league_name", lk)
        step = int(seq.get("current_step", 1))
        cum_loss = float(seq.get("cumulative_loss", 0))
        idx = min(step - 1, len(FIBONACCI) - 1)
        next_stake = round(base_stake * FIBONACCI[idx], 2)

        if step <= 3:
            risk_txt, risk_col = "SAFE", "#00c853"
        elif step <= 5:
            risk_txt, risk_col = "CAUTION", "#f59e0b"
        else:
            risk_txt, risk_col = "HIGH RISK", "#ef4444"

        row = st.columns([2.5, 1, 2.5, 1.5, 1.5, 1.5])
        row[0].markdown(f"**{name}**")
        row[1].markdown(f"`{step}/{max_step}`")
        with row[2]:
            st.progress(min(step / max_step, 1.0))
        row[3].markdown(f"£{next_stake:.2f}")
        row[4].markdown(f"−£{cum_loss:.2f}")
        row[5].markdown(
            f"<span style='color:{risk_col}; font-size:0.78rem; font-weight:700; "
            f"letter-spacing:0.05em;'>{risk_txt}</span>",
            unsafe_allow_html=True,
        )

    # Risk analytics
    st.markdown("---")
    _section("Risk Analytics", "Series ruin probability and bankroll runway")

    active_rates = [
        float(l.get("draw_rate_season") or 0)
        for l in leagues_map.values()
        if l.get("is_active")
    ]
    avg_rate = sum(active_rates) / len(active_rates) if active_rates else 0.27

    ra1, ra2, ra3 = st.columns(3)
    ra1.metric(
        "Avg Draw Rate (active leagues)",
        f"{avg_rate * 100:.1f}%",
        help="Mean season draw rate across active leagues",
    )
    ra2.metric(
        "Series Ruin Probability",
        f"{_ruin_prob(avg_rate, max_step):.2f}%",
        help=f"Chance of {max_step} consecutive losses (stop-loss) before a draw win",
    )
    if bankroll:
        ra3.metric(
            "Bankroll Runway",
            f"{_runway(bankroll, base_stake, max_step):.1f} series",
            help="Max-loss series bankroll can absorb before depletion",
        )
    _cards()


# ===========================================================================
# PAGE — HISTORY
# ===========================================================================

def page_history():
    _section("Bet History", "Complete log of all placed bets")

    if not _db_connected():
        st.error("Database not connected.")
        return

    fc1, fc2, fc3 = st.columns([2, 2, 2])
    with fc1:
        days_back = st.slider("Days", min_value=7, max_value=365, value=90, step=7)
    with fc2:
        all_leagues = db.get_leagues()
        league_opts = {l["league_key"]: l.get("league_name", l["league_key"]) for l in all_leagues}
        sel_leagues = st.multiselect(
            "League",
            options=list(league_opts.keys()),
            format_func=lambda k: league_opts[k],
        )
    with fc3:
        result_filter = st.multiselect(
            "Result",
            options=["WIN", "LOSS", "PENDING"],
            default=["WIN", "LOSS", "PENDING"],
        )

    bets = db.get_bet_history(days=days_back)
    if sel_leagues:
        bets = [b for b in bets if b.get("league_key") in sel_leagues]
    if result_filter:
        bets = [b for b in bets if b.get("result") in result_filter]

    if not bets:
        st.info("No bets found for the selected filters.")
        return

    rows = [
        {
            "Date": (b.get("created_at") or "")[:10],
            "Match": f"{b.get('home_team', '?')} vs {b.get('away_team', '?')}",
            "League": league_opts.get(b.get("league_key", ""), b.get("league_key", "")),
            "Step": b.get("fib_step", ""),
            "Stake": float(b.get("stake") or 0),
            "Odds": float(b.get("odds") or 0),
            "Result": b.get("result", "PENDING"),
            "Return": float(b.get("gross_return") or 0),
            "P&L": float(b.get("net_pnl") or 0),
        }
        for b in bets
    ]
    df = pd.DataFrame(rows)

    def _row_colour(row):
        r = row.get("Result", "")
        if r == "WIN":
            return ["background-color:#052e12; color:#86efac"] * len(row)
        if r == "LOSS":
            return ["background-color:#2d0a0a; color:#fca5a5"] * len(row)
        return ["color:#6b7280"] * len(row)

    styled = df.style.apply(_row_colour, axis=1).format(
        {"Stake": "£{:.2f}", "Return": "£{:.2f}", "P&L": "£{:+.2f}", "Odds": "{:.2f}"}
    )
    st.dataframe(styled, use_container_width=True, hide_index=True)

    settled_df = df[df["Result"].isin(["WIN", "LOSS"])]
    if not settled_df.empty:
        st.markdown("---")
        wins = int((settled_df["Result"] == "WIN").sum())
        t1, t2, t3, t4 = st.columns(4)
        t1.metric("Settled Bets", len(settled_df))
        t2.metric("Total Staked", f"£{settled_df['Stake'].sum():.2f}")
        t3.metric("Net P&L", f"£{settled_df['P&L'].sum():+.2f}")
        t4.metric("Win Rate", f"{wins / len(settled_df) * 100:.1f}%")
        _cards()

    st.markdown("---")

    # Downloads
    dl1, dl2 = st.columns(2)

    # CSV
    with dl1:
        st.download_button(
            "Download CSV",
            data=df.to_csv(index=False).encode("utf-8"),
            file_name=f"fibonacci_bets_{datetime.now(timezone.utc).strftime('%Y%m%d')}.csv",
            mime="text/csv",
            use_container_width=True,
        )

    # Excel (multi-sheet)
    with dl2:
        try:
            buf = io.BytesIO()
            with pd.ExcelWriter(buf, engine="openpyxl") as writer:
                df.to_excel(writer, sheet_name="Bet History", index=False)

                if not settled_df.empty:
                    monthly = (
                        settled_df.assign(
                            Month=pd.to_datetime(settled_df["Date"]).dt.to_period("M").astype(str)
                        )
                        .groupby("Month")
                        .agg(
                            Bets=("Result", "count"),
                            Wins=("Result", lambda x: (x == "WIN").sum()),
                            Staked=("Stake", "sum"),
                            PnL=("P&L", "sum"),
                        )
                        .reset_index()
                    )
                    monthly["Win Rate %"] = (monthly["Wins"] / monthly["Bets"] * 100).round(1)
                    monthly.to_excel(writer, sheet_name="Monthly Summary", index=False)

                if not df.empty:
                    league_summary = pd.DataFrame([
                        {
                            "League": lg,
                            "Bets": len(g),
                            "Wins": int((g["Result"] == "WIN").sum()),
                            "Staked": round(g["Stake"].sum(), 2),
                            "Net P&L": round(g["P&L"].sum(), 2),
                            "Win Rate %": round((g["Result"] == "WIN").sum() / len(g) * 100, 1),
                        }
                        for lg, g in df.groupby("League")
                    ])
                    league_summary.to_excel(writer, sheet_name="By League", index=False)

                seqs = db.get_active_sequences()
                if seqs:
                    pd.DataFrame(seqs).to_excel(writer, sheet_name="Sequences", index=False)

            buf.seek(0)
            st.download_button(
                "Download Excel",
                data=buf.read(),
                file_name=f"fibonacci_bets_{datetime.now(timezone.utc).strftime('%Y%m%d')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )
        except Exception as e:
            st.caption(f"Excel export unavailable: {e}")


# ===========================================================================
# PAGE — BACKTESTER
# ===========================================================================

def _render_sim_results(r: dict, label: str) -> None:
    """Shared result display used by all three backtester tabs."""
    st.success(f"Simulation complete: **{label}**")

    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("Matches", r["total_bets"])
    m2.metric("Wins (Draws)", r["wins"])
    m3.metric("Draw Rate", f"{r['draw_rate']:.1f}%")
    m4.metric("Net P&L", f"£{r['net_pnl']:+.2f}")
    m5.metric("ROI", f"{r['roi']:+.2f}%")

    m6, m7, m8 = st.columns(3)
    m6.metric("Total Staked", f"£{r['total_staked']:.2f}")
    m7.metric("Max Drawdown", f"£{r['max_drawdown']:.2f}")
    m8.metric("Longest Loss Streak", r["longest_loss_streak"])
    _cards()

    if r.get("pnl_series"):
        fig = px.area(
            pd.DataFrame({
                "Bet #": range(1, len(r["pnl_series"]) + 1),
                "P&L (£)": r["pnl_series"],
            }),
            x="Bet #", y="P&L (£)",
            color_discrete_sequence=["#00c853"],
        )
        fig.update_traces(fillcolor="rgba(0,200,83,0.07)", line_color="#00c853", line_width=2)
        fig.update_layout(
            plot_bgcolor="#0d0f12", paper_bgcolor="#0d0f12", font_color="#9ca3af",
            xaxis=dict(gridcolor="#1a1e2a"),
            yaxis=dict(gridcolor="#1a1e2a", zeroline=True, zerolinecolor="#374151"),
            title=label,
            title_font=dict(color="#e2e5eb", size=13),
            margin=dict(l=0, r=0, t=35, b=0),
        )
        fig.add_hline(y=0, line_dash="dot", line_color="#374151")
        st.plotly_chart(fig, use_container_width=True)

    if r.get("_matches"):
        with st.expander("Match breakdown"):
            df_m = pd.DataFrame(r["_matches"])
            if "home_goals" in df_m.columns and "away_goals" in df_m.columns:
                df_m["Draw"] = df_m["home_goals"] == df_m["away_goals"]
            st.dataframe(df_m, use_container_width=True, hide_index=True)


def page_backtester():
    _section("Backtester", "Simulate the Fibonacci system over historical data")

    settings = db.get_settings() or {}
    default_base = float(settings.get("base_stake", 10.0))
    default_min_odds = float(settings.get("min_odds", 2.88))
    default_max_step = int(settings.get("max_fib_step", 7))

    tab_fdco, tab_api, tab_csv = st.tabs(
        ["Free Data (19 leagues)", "API-Football", "CSV Upload"]
    )

    # =========================================================
    # TAB 1 — football-data.co.uk  (no API key, always works)
    # =========================================================
    with tab_fdco:
        st.caption(
            "Real historical match results + Bet365 draw odds from "
            "[football-data.co.uk](https://www.football-data.co.uk) — "
            "**no API key required**, completely free."
        )

        fdco_names = list(_FDCO_LEAGUES.keys())

        fc1, fc2 = st.columns(2)
        with fc1:
            fdco_league = st.selectbox("League", fdco_names, key="fdco_league")
        with fc2:
            fdco_season = st.selectbox(
                "Season (start year)", [2024, 2023, 2022, 2021, 2020], key="fdco_season"
            )

        fb1, fb2, fb3 = st.columns(3)
        with fb1:
            fdco_base = st.number_input(
                "Base Stake (£)", value=default_base, min_value=1.0, step=1.0, key="fdco_base"
            )
        with fb2:
            fdco_min_odds = st.number_input(
                "Min Odds", value=default_min_odds, min_value=1.5, max_value=5.0,
                step=0.01, format="%.2f", key="fdco_min",
            )
        with fb3:
            fdco_max_step = st.slider(
                "Max Step", min_value=3, max_value=10, value=default_max_step, key="fdco_step"
            )

        if st.button("Run Backtest", type="primary", key="fdco_run"):
            league_code = _FDCO_LEAGUES[fdco_league]
            season_label = f"{fdco_season}/{str(fdco_season + 1)[2:]}"

            _fdco_fn = getattr(data_fetcher, "fetch_historical_from_fdco", None)
            if _fdco_fn is None:
                st.error(
                    "fetch_historical_from_fdco not available — the app may need a restart. "
                    "On Streamlit Cloud, click **Manage app → Reboot app** in the bottom-right."
                )
                st.stop()

            with st.spinner(f"Downloading {fdco_league} {season_label}…"):
                matches = _fdco_fn(league_code, fdco_season)

            if not matches:
                st.error(
                    f"No data for **{fdco_league} {season_label}**. "
                    "The season may not be available yet, or try a different season."
                )
                st.stop()

            result = fibonacci_engine.simulate_season(
                matches, fdco_base, fdco_min_odds, fdco_max_step
            )
            result["_matches"] = matches
            st.session_state["fdco_result"] = (result, f"{fdco_league} {season_label}")

        if "fdco_result" in st.session_state:
            r, lbl = st.session_state["fdco_result"]
            _render_sim_results(r, lbl)

    # =========================================================
    # TAB 2 — API-Football
    # =========================================================
    with tab_api:
        # Check API status once per session (doesn't use quota)
        if "api_status" not in st.session_state:
            _check_fn = getattr(data_fetcher, "check_api_suspended", None)
            if _check_fn:
                is_susp, susp_msg = _check_fn()
            else:
                is_susp, susp_msg = True, "API check unavailable — app may need a restart."
            st.session_state["api_status"] = (is_susp, susp_msg)

        is_suspended, susp_msg = st.session_state["api_status"]

        if is_suspended:
            st.error(
                f"**API-Football account issue:** {susp_msg}\n\n"
                "Visit [dashboard.api-football.com](https://dashboard.api-football.com) "
                "to check your account. In the meantime, use the **Free Data** tab — "
                "it works without any API key and includes real Bet365 odds."
            )
            if st.button("Re-check API status", key="recheck_api"):
                del st.session_state["api_status"]
                st.rerun()
        else:
            st.caption("Fetch a full historical season from API-Football. Uses API quota.")

            all_leagues = db.get_leagues()
            if not all_leagues:
                all_leagues = [
                    {
                        "league_key": f"league_{l['api_id']}",
                        "league_name": l["league_name"],
                        "api_id": l["api_id"],
                        "country": l.get("country", ""),
                    }
                    for l in league_scanner.SEED_LEAGUES
                ]
            league_opts = {
                l["league_key"]: f"{l.get('league_name', l['league_key'])} ({l.get('country', '')})"
                for l in all_leagues
            }

            ac1, ac2 = st.columns(2)
            with ac1:
                sel_key = st.selectbox(
                    "League", options=list(league_opts.keys()),
                    format_func=lambda k: league_opts[k], key="api_league",
                )
            with ac2:
                api_season = st.selectbox("Season", [2024, 2023, 2022, 2021, 2020], key="api_season")

            bc1, bc2, bc3 = st.columns(3)
            with bc1:
                bt_base = st.number_input(
                    "Base Stake (£)", value=default_base, min_value=1.0, step=1.0, key="api_base"
                )
            with bc2:
                bt_min_odds = st.number_input(
                    "Min Odds", value=default_min_odds, min_value=1.5, max_value=5.0,
                    step=0.01, format="%.2f", key="api_min",
                )
            with bc3:
                bt_max_step = st.slider(
                    "Max Step", min_value=3, max_value=10, value=default_max_step, key="api_step"
                )

            bt_default_odds = st.number_input(
                "Default draw odds (when historical odds unavailable)",
                value=3.0, min_value=1.5, max_value=6.0, step=0.05, format="%.2f",
            )

            if st.button("Run API Backtest", type="primary", key="api_run"):
                api_id = next(
                    (l["api_id"] for l in all_leagues if l["league_key"] == sel_key), None
                )
                if not api_id:
                    st.error("Could not determine API ID for this league.")
                    st.stop()

                with st.spinner("Fetching data from API-Football…"):
                    raw = data_fetcher.fetch_historical_fixtures(api_id, api_season)

                if not raw:
                    st.error(
                        "No data returned. Check your account at "
                        "[dashboard.api-football.com](https://dashboard.api-football.com) "
                        "or use the Free Data tab."
                    )
                    st.stop()

                matches = [
                    {
                        "home_goals": f["home_goals"],
                        "away_goals": f["away_goals"],
                        "draw_odds": bt_default_odds,
                        "home_team": f["home_team"],
                        "away_team": f["away_team"],
                    }
                    for f in raw
                ]
                result = fibonacci_engine.simulate_season(
                    matches, bt_base, bt_min_odds, bt_max_step
                )
                result["_matches"] = matches
                st.session_state["api_bt_result"] = (result, f"{league_opts[sel_key]} — {api_season}")

            if "api_bt_result" in st.session_state:
                r, lbl = st.session_state["api_bt_result"]
                _render_sim_results(r, lbl)

    # =========================================================
    # TAB 3 — CSV Upload
    # =========================================================
    with tab_csv:
        st.caption(
            "Upload your own CSV. Required columns: "
            "`home_goals`, `away_goals`, `draw_odds`. "
            "Download free CSVs from "
            "[football-data.co.uk/data.php](https://www.football-data.co.uk/data.php) "
            "for any league not in the Free Data tab."
        )
        uploaded = st.file_uploader("Choose CSV", type=["csv"])

        cc1, cc2, cc3 = st.columns(3)
        with cc1:
            csv_base = st.number_input(
                "Base Stake (£)", value=default_base, min_value=1.0, step=1.0, key="csv_base"
            )
        with cc2:
            csv_min = st.number_input(
                "Min Odds", value=default_min_odds, min_value=1.5, max_value=5.0,
                step=0.01, format="%.2f", key="csv_min",
            )
        with cc3:
            csv_step = st.slider(
                "Max Step", min_value=3, max_value=10, value=default_max_step, key="csv_step"
            )

        if uploaded and st.button("Run CSV Simulation", type="primary", key="csv_run"):
            try:
                df_csv = pd.read_csv(uploaded)
                missing_cols = {"home_goals", "away_goals", "draw_odds"} - set(df_csv.columns)
                if missing_cols:
                    st.error(f"Missing required columns: {missing_cols}")
                    st.stop()

                matches = df_csv[["home_goals", "away_goals", "draw_odds"]].to_dict("records")
                res = fibonacci_engine.simulate_season(matches, csv_base, csv_min, csv_step)
                res["_matches"] = df_csv.to_dict("records")
                st.session_state["csv_result"] = (res, f"CSV — {uploaded.name}")

            except Exception as e:
                st.error(f"Error: {e}")

        if "csv_result" in st.session_state:
            r, lbl = st.session_state["csv_result"]
            _render_sim_results(r, lbl)


# ===========================================================================
# PAGE — LEAGUES
# ===========================================================================

def page_leagues():
    _section("League Scanner", "Draw-rate rankings and activation")

    if not _db_connected():
        st.error("Database not connected.")
        return

    leagues = db.get_leagues()

    sc1, sc2 = st.columns([2, 4])
    with sc1:
        if st.button("Re-Scan Now  (costs API quota)", type="primary"):
            with st.spinner("Scanning league draw rates…"):
                leagues = league_scanner.update_league_draw_rates()
            st.success(f"Scan complete — {len(leagues)} leagues updated.")
            st.rerun()

    last_scan = max((l.get("last_scanned") or "" for l in leagues), default="")
    if last_scan:
        with sc2:
            st.caption(f"Last scan: {last_scan[:16].replace('T', ' ')} UTC")

    if not leagues:
        st.info("No leagues yet — click Re-Scan to populate.")
        return

    st.markdown("---")

    # Table header
    hdr = st.columns([2.5, 1.5, 1.3, 1.2, 1.5, 1.2])
    for col, lbl in zip(hdr, ["League", "Country", "Draw Rate", "Score", "Status", "Active"]):
        col.markdown(
            f"<span style='font-size:0.71rem; color:#4b5563; text-transform:uppercase; "
            f"letter-spacing:0.09em;'>{lbl}</span>",
            unsafe_allow_html=True,
        )

    for league in leagues:
        lk = league.get("league_key", "")
        is_active = bool(league.get("is_active", False))
        draw_pct = round(float(league.get("draw_rate_season") or 0) * 100, 1)
        score = float(league.get("score") or draw_pct)

        row = st.columns([2.5, 1.5, 1.3, 1.2, 1.5, 1.2])

        name_html = f"<b>{league.get('league_name', lk)}</b>"
        if draw_pct >= 28:
            name_html += " <span style='color:#f59e0b; font-size:0.72rem; font-weight:700;'>TOP</span>"
        row[0].markdown(name_html, unsafe_allow_html=True)

        row[1].markdown(
            f"<span style='color:#6b7280; font-size:0.88rem;'>{league.get('country', '')}</span>",
            unsafe_allow_html=True,
        )
        row[2].markdown(f"**{draw_pct:.1f}%**")
        row[3].markdown(f"{score:.1f}")

        s_col = "#00c853" if is_active else "#374151"
        s_txt = "ACTIVE" if is_active else "OFF"
        row[4].markdown(
            f"<span style='color:{s_col}; font-size:0.77rem; font-weight:700; "
            f"letter-spacing:0.06em;'>{s_txt}</span>",
            unsafe_allow_html=True,
        )

        with row[5]:
            new_active = st.toggle(
                "Toggle",
                value=is_active,
                key=f"tgl_{lk}",
                label_visibility="collapsed",
            )
            if new_active != is_active:
                db.upsert_league({**league, "is_active": new_active})
                st.rerun()

    existing_keys = {l["league_key"] for l in leagues}
    missing = [l for l in league_scanner.SEED_LEAGUES if l["league_key"] not in existing_keys]
    if missing:
        with st.expander(f"{len(missing)} seed leagues not yet scanned"):
            st.dataframe(
                pd.DataFrame(missing)[["league_name", "country", "api_id"]],
                use_container_width=True,
                hide_index=True,
            )


# ===========================================================================
# PAGE — SETTINGS
# ===========================================================================

def page_settings():
    _section("Settings", "Staking parameters and system configuration")

    if not _db_connected():
        st.error("Database not connected.")
        return

    settings = db.get_settings() or {}

    with st.form("settings_form"):
        sc1, sc2 = st.columns(2)
        with sc1:
            base_stake = st.number_input(
                "Base Stake (£) — step 1 amount",
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

        FIBONACCI = [1, 1, 2, 3, 5, 8, 13, 21, 34, 55]
        preview = [
            {
                "Step": i + 1,
                "Multiplier": f"×{FIBONACCI[i]}",
                "Stake (£)": f"£{base_stake * FIBONACCI[i]:.2f}",
                "Risk": "SAFE" if i < 3 else "CAUTION" if i < 5 else "HIGH",
            }
            for i in range(max_fib_step)
        ]
        st.caption("Fibonacci stake ladder preview:")
        st.dataframe(pd.DataFrame(preview), hide_index=True, use_container_width=False)

        submitted = st.form_submit_button("Save Settings", type="primary")
        if submitted:
            ok = db.update_settings(
                base_stake=base_stake,
                min_odds=min_odds,
                max_fib_step=max_fib_step,
                bankroll=bankroll,
                commission_pct=commission_pct,
            )
            st.success("Settings saved.") if ok else st.error("Failed to save — check DB connection.")

    # Ruin probability info (outside form so it updates with current DB values)
    st.markdown("---")
    try:
        active_leagues = [l for l in db.get_leagues() if l.get("is_active")]
        active_rates = [float(l.get("draw_rate_season") or 0) for l in active_leagues]
        avg_rate = sum(active_rates) / len(active_rates) if active_rates else 0.27
        cur_max_step = int(settings.get("max_fib_step") or 7)
        p = _ruin_prob(avg_rate, cur_max_step)
        st.info(
            f"At **{avg_rate * 100:.1f}%** avg draw rate across "
            f"{len(active_leagues)} active league(s) and max step **{cur_max_step}**: "
            f"series ruin probability = **{p:.2f}%**  "
            f"(i.e. 1 in {round(100/p) if p > 0 else 'never'} series ends in stop-loss)"
        )
    except Exception:
        pass

    # API usage
    st.markdown("---")
    _section("API Usage", "")
    api_calls = data_fetcher.get_api_calls_today()
    pct = min(api_calls / 100, 1.0)
    _c = "#ef4444" if pct > 0.9 else "#f59e0b" if pct > 0.7 else "#00c853"

    ca, cb = st.columns([3, 1])
    with ca:
        st.markdown(
            f"<div style='margin-bottom:4px; font-size:0.9rem;'>"
            f"Calls today: <strong style='color:{_c};'>{api_calls}/100</strong></div>",
            unsafe_allow_html=True,
        )
        st.progress(pct)
    with cb:
        st.metric("Remaining", 100 - api_calls)
        _cards()

    st.caption(
        "Free tier: 100 calls/day. App blocks at 95. Counter resets at midnight UTC."
    )


# ===========================================================================
# Router
# ===========================================================================

if page == "Today":
    page_today()
elif page == "Dashboard":
    page_dashboard()
elif page == "History":
    page_history()
elif page == "Backtester":
    page_backtester()
elif page == "Leagues":
    page_leagues()
elif page == "Settings":
    page_settings()
