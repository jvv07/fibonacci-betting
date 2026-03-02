"""
app.py — Fibonacci Betting dashboard v3.

Linear-inspired dark UI. Six pages via sidebar navigation:
  Today      — qualifying fixtures, value scores, exposure warning, log results
  Dashboard  — portfolio metrics, P&L chart, monthly heatmap, sequences, risk analytics
  History    — filterable bet log, CSV + Excel download
  Backtester — FDCO / OpenLigaDB / API-Football / CSV backtesting
  Leagues    — draw-rate rankings (FDCO-powered), activate/deactivate
  Settings   — staking parameters, ruin probability preview, API usage
"""

import io
import math
import sys
from collections import defaultdict
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


@st.cache_data(ttl=7200, show_spinner=False)
def _fetch_fdco_cached(league_code: str, season: int) -> list[dict]:
    """Cached FDCO download — avoids re-fetching the same CSV within 2 hours."""
    fn = getattr(data_fetcher, "fetch_historical_from_fdco", None)
    return fn(league_code, season) if fn else []


@st.cache_data(ttl=7200, show_spinner=False)
def _fetch_ol_cached(shortcut: str, season: int) -> list[dict]:
    """Cached OpenLigaDB download."""
    fn = getattr(data_fetcher, "fetch_openligadb_historical", None)
    return fn(shortcut, season) if fn else []


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
    # Odds API credits — check once per session (free /sports call, 0 credits)
    if "odds_api_credits" not in st.session_state:
        _ok, _msg, _rem = data_fetcher.get_odds_api_status()
        st.session_state["odds_api_credits"] = (_ok, _rem)
    _odds_ok, _odds_rem = st.session_state["odds_api_credits"]
    if _odds_ok and _odds_rem is not None:
        _opct = max(0.0, min(_odds_rem / 500, 1.0))
        _oc = "#ef4444" if _opct < 0.1 else "#f59e0b" if _opct < 0.3 else "#00c853"
        st.markdown(
            f"<div style='font-size:0.73rem; color:#4b5563; margin-bottom:3px;'>"
            f"Odds API · <span style='color:{_oc};'>{_odds_rem}/500 credits</span></div>",
            unsafe_allow_html=True,
        )
        st.progress(_opct)
    else:
        st.markdown(
            "<div style='font-size:0.73rem; color:#ef4444;'>Odds API · no key</div>",
            unsafe_allow_html=True,
        )


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
        .map(_colour_odds, subset=["Odds"])
        .map(_colour_value, subset=["Value Score"])
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

def _render_sim_results(r: dict, label: str, min_odds: float = 2.88, max_step: int = 7) -> None:
    """Shared result display used by all backtester tabs."""
    st.success(f"Simulation complete: **{label}**")

    # ── Core metrics ──────────────────────────────────────────────────────────
    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("Matches Bet", r["total_bets"])
    m2.metric("Wins (Draws)", r["wins"])
    m3.metric("Draw Rate", f"{r['draw_rate']:.1f}%")
    m4.metric("Net P&L", f"£{r['net_pnl']:+.2f}")
    m5.metric("ROI", f"{r['roi']:+.2f}%")
    m6, m7, m8 = st.columns(3)
    m6.metric("Total Staked", f"£{r['total_staked']:.2f}")
    m7.metric("Max Drawdown", f"£{r['max_drawdown']:.2f}")
    m8.metric("Longest Loss Streak", r["longest_loss_streak"])
    _cards()

    # Shared Plotly dark style
    _D   = dict(plot_bgcolor="#0d0f12", paper_bgcolor="#0d0f12",
                font_color="#9ca3af", margin=dict(l=0, r=0, t=38, b=0))
    _G   = dict(gridcolor="#1a1e2a", zeroline=False)
    _GZ  = dict(gridcolor="#1a1e2a", zeroline=True, zerolinecolor="#374151")
    _LEG = dict(bgcolor="rgba(0,0,0,0)", font=dict(color="#9ca3af"), bordercolor="#1a1e2a")
    _TF  = dict(color="#e2e5eb", size=13)

    pnl_series = r.get("pnl_series", [])
    bet_log    = r.get("bet_log", [])
    series_log = r.get("series_log", [])

    # ── Chart 1: Equity curve + bankroll simulation + drawdown shading ────────
    if pnl_series:
        _key = "".join(c if c.isalnum() else "_" for c in label)[:40]
        bankroll_start = st.number_input(
            "Starting bankroll (£) for simulation overlay",
            value=1000.0, min_value=100.0, step=100.0, key=f"br_{_key}",
        )
        xs = list(range(1, len(pnl_series) + 1))
        bankroll_curve = [bankroll_start + p for p in pnl_series]

        # Drawdown series
        peak, max_dd_val, max_dd_idx = 0.0, 0.0, 0
        for i, p in enumerate(pnl_series):
            if p > peak:
                peak = p
            dd = peak - p
            if dd > max_dd_val:
                max_dd_val, max_dd_idx = dd, i

        fig1 = go.Figure()
        # Green above-zero fill
        fig1.add_trace(go.Scatter(
            x=xs, y=pnl_series, mode="lines",
            line=dict(color="#00c853", width=2),
            fill="tozeroy", fillcolor="rgba(0,200,83,0.08)",
            name="Cumulative P&L",
        ))
        # Red below-zero fill
        fig1.add_trace(go.Scatter(
            x=xs, y=[min(p, 0.0) for p in pnl_series], mode="lines",
            line=dict(width=0), fill="tozeroy",
            fillcolor="rgba(239,68,68,0.20)", showlegend=False, hoverinfo="skip",
        ))
        # Bankroll line (right axis)
        fig1.add_trace(go.Scatter(
            x=xs, y=bankroll_curve, mode="lines",
            line=dict(color="#f59e0b", width=1.5, dash="dot"),
            name=f"Bankroll (£{bankroll_start:.0f})", yaxis="y2",
        ))
        # Max drawdown marker
        if max_dd_val > 0:
            fig1.add_trace(go.Scatter(
                x=[max_dd_idx + 1], y=[pnl_series[max_dd_idx]],
                mode="markers+text",
                marker=dict(color="#ef4444", size=10, symbol="x"),
                text=[f"  Max DD −£{max_dd_val:.2f}"],
                textfont=dict(color="#ef4444", size=11),
                textposition="middle right", name="Max Drawdown",
            ))
        fig1.add_hline(y=0, line_dash="dot", line_color="#374151", line_width=1)
        fig1.update_layout(
            **_D, title=label, title_font=_TF, hovermode="x unified",
            xaxis=dict(**_G, title="Bet #"),
            yaxis=dict(**_GZ, title="Cumulative P&L (£)"),
            yaxis2=dict(title="Bankroll (£)", overlaying="y", side="right",
                        showgrid=False, tickfont=dict(color="#f59e0b")),
            legend=_LEG,
        )
        st.plotly_chart(fig1, use_container_width=True)

    # ── Charts 2 & 3 ──────────────────────────────────────────────────────────
    if series_log or bet_log:
        c2, c3 = st.columns(2)

        # Chart 2 — Series depth histogram
        with c2:
            if series_log:
                win_cnt  = defaultdict(int)
                loss_cnt = defaultdict(int)
                for s in series_log:
                    (loss_cnt if s["stop_loss"] else win_cnt)[s["depth"]] += 1
                all_d = sorted(set(win_cnt) | set(loss_cnt))
                fig2 = go.Figure()
                fig2.add_trace(go.Bar(x=all_d, y=[win_cnt[d] for d in all_d],
                                      name="Win (draw)", marker_color="#00c853", opacity=0.85))
                if any(loss_cnt.values()):
                    fig2.add_trace(go.Bar(x=all_d, y=[loss_cnt[d] for d in all_d],
                                          name="Stop-loss", marker_color="#ef4444", opacity=0.85))
                total_s = len(series_log)
                pct_1  = round(win_cnt[1] / total_s * 100, 1) if total_s else 0
                fig2.update_layout(
                    **_D, barmode="stack", legend=_LEG,
                    title=f"Series Depth  ({pct_1}% win at step 1)", title_font=_TF,
                    xaxis=dict(**_G, title="Step Series Ended At", tickmode="linear", dtick=1),
                    yaxis=dict(**_G, title="# Series"),
                )
                st.plotly_chart(fig2, use_container_width=True)
                st.caption(
                    f"{total_s} total series · "
                    f"{sum(win_cnt.values())} wins · "
                    f"{sum(loss_cnt.values())} stop-losses"
                )

        # Chart 3 — P&L contribution by Fibonacci step
        with c3:
            if bet_log:
                step_pnl = defaultdict(float)
                step_cnt = defaultdict(int)
                for b in bet_log:
                    step_pnl[b["step"]] += b["pnl"]
                    step_cnt[b["step"]] += 1
                steps_     = sorted(step_pnl)
                pnl_vals   = [round(step_pnl[s], 2) for s in steps_]
                bar_colors = ["#00c853" if v >= 0 else "#ef4444" for v in pnl_vals]
                ylabels    = [f"Step {s}  (n={step_cnt[s]})" for s in steps_]
                fig3 = go.Figure(go.Bar(
                    x=pnl_vals, y=ylabels, orientation="h",
                    marker_color=bar_colors, opacity=0.85,
                    text=[f"£{v:+.2f}" for v in pnl_vals],
                    textposition="outside", textfont=dict(color="#9ca3af", size=11),
                ))
                fig3.add_vline(x=0, line_color="#374151", line_width=1)
                fig3.update_layout(
                    **_D, title="Net P&L Contribution by Step", title_font=_TF,
                    xaxis=dict(**_GZ, title="Net P&L (£)"),
                    yaxis=dict(gridcolor="#1a1e2a"),
                    height=max(260, len(steps_) * 54 + 80),
                )
                st.plotly_chart(fig3, use_container_width=True)
                # Insight callout
                first_neg = next((s for s in steps_ if step_pnl[s] < 0), None)
                if first_neg:
                    st.caption(f"Step {first_neg}+ bets are net-negative in aggregate — "
                               "each recovery win must overcome compounding losses.")
                else:
                    st.caption("All steps are net-positive — the win rate comfortably covers losses.")

    # ── Charts 4 & 5 ──────────────────────────────────────────────────────────
    if bet_log:
        c4, c5 = st.columns(2)

        # Chart 4 — Odds calibration scatter
        with c4:
            bucket_data: dict = defaultdict(list)
            for b in bet_log:
                bk = round(math.floor(b["odds"] * 10) / 10, 1)
                bucket_data[bk].append(1 if b["is_draw"] else 0)
            bks = sorted(bucket_data)
            if len(bks) >= 2:
                imp  = [round(1.0 / bk, 4) for bk in bks]
                act  = [round(sum(v) / len(v), 4) for v in [bucket_data[bk] for bk in bks]]
                ns   = [len(bucket_data[bk]) for bk in bks]
                max_n = max(ns)
                szs  = [8 + 22 * (n / max_n) for n in ns]
                xlbls = [f"{bk:.1f}–{bk + 0.09:.2f}" for bk in bks]
                _maxp = max(max(imp), max(act)) * 1.15
                fig4 = go.Figure()
                fig4.add_trace(go.Scatter(
                    x=[0, _maxp], y=[0, _maxp], mode="lines",
                    line=dict(color="#374151", dash="dash", width=1),
                    name="Perfect calibration",
                ))
                fig4.add_trace(go.Scatter(
                    x=imp, y=act, mode="markers",
                    marker=dict(size=szs, color="#f59e0b", opacity=0.85,
                                line=dict(color="#1a1e2a", width=1)),
                    text=[f"Odds {lbl}<br>Implied: {ip:.1%}<br>Actual: {af:.1%}<br>n={n}"
                          for lbl, ip, af, n in zip(xlbls, imp, act, ns)],
                    hovertemplate="%{text}<extra></extra>",
                    name="Observed draw freq",
                ))
                fig4.update_layout(
                    **_D, title="Odds Calibration", title_font=_TF, legend=_LEG,
                    xaxis=dict(**_G, title="Implied Draw Prob (1/odds)", tickformat=".0%"),
                    yaxis=dict(**_G, title="Observed Draw Frequency", tickformat=".0%"),
                )
                st.plotly_chart(fig4, use_container_width=True)
                # Check for value
                above = sum(1 for a, i in zip(act, imp) if a > i)
                below = len(act) - above
                st.caption(
                    f"Dots above the line = bookmaker underpriced the draw risk. "
                    f"{above} buckets above · {below} below."
                )

        # Chart 5 — Rolling draw rate
        with c5:
            WINDOW = min(20, max(5, len(bet_log) // 8))
            is_draws = [1 if b["is_draw"] else 0 for b in bet_log]
            bet_nums = [b["bet_num"] for b in bet_log]
            rolling  = [
                sum(is_draws[max(0, i - WINDOW + 1): i + 1]) /
                len(is_draws[max(0, i - WINDOW + 1): i + 1])
                for i in range(len(is_draws))
            ]
            overall  = sum(is_draws) / len(is_draws) if is_draws else 0
            fig5 = go.Figure()
            fig5.add_trace(go.Scatter(
                x=bet_nums, y=rolling, mode="lines",
                line=dict(color="#00c853", width=2),
                fill="tozeroy", fillcolor="rgba(0,200,83,0.07)",
                name=f"{WINDOW}-bet rolling draw rate",
            ))
            fig5.add_hline(
                y=overall, line_dash="dot", line_color="#f59e0b", line_width=1.5,
                annotation_text=f"Season avg {overall:.1%}",
                annotation_font_color="#f59e0b", annotation_position="bottom right",
            )
            fig5.update_layout(
                **_D, title=f"Rolling Draw Rate ({WINDOW}-match window)", title_font=_TF,
                xaxis=dict(**_G, title="Bet #"),
                yaxis=dict(**_G, title="Draw Rate", tickformat=".0%",
                           range=[0, min(1.0, overall * 3 + 0.05)]),
                legend=_LEG,
            )
            st.plotly_chart(fig5, use_container_width=True)
            # Streak variance insight
            if is_draws:
                chunks = []
                cur = 0
                for d in is_draws:
                    if d == 0:
                        cur += 1
                    else:
                        if cur:
                            chunks.append(cur)
                        cur = 0
                if chunks:
                    avg_streak = sum(chunks) / len(chunks)
                    st.caption(
                        f"Average losing run: {avg_streak:.1f} bets · "
                        f"Longest: {max(chunks)} · Series that reached step 4+: "
                        f"{sum(1 for c in chunks if c >= 3)}"
                    )

    # ── Chart 6 — Break-even odds per step (mathematical) ────────────────────
    FIBS   = fibonacci_engine.FIBONACCI
    steps6 = list(range(1, max_step + 1))
    be_odds = [round(sum(FIBS[:n]) / FIBS[n - 1], 4) for n in steps6]
    be_colors6 = ["#00c853" if min_odds >= be else "#ef4444" for be in be_odds]
    fig6 = go.Figure(go.Bar(
        x=[f"Step {s}" for s in steps6], y=be_odds,
        marker_color=be_colors6, opacity=0.85,
        text=[f"{be:.2f}" for be in be_odds],
        textposition="outside", textfont=dict(color="#9ca3af", size=11),
        name="Break-even odds",
    ))
    fig6.add_hline(
        y=min_odds, line_dash="dash", line_color="#f59e0b", line_width=2,
        annotation_text=f"Your min odds: {min_odds:.2f}",
        annotation_font_color="#f59e0b", annotation_position="top left",
    )
    fig6.update_layout(
        **_D,
        title="Break-even Odds by Step  (green = your filter covers recovery cost)",
        title_font=_TF,
        xaxis=dict(**_G, title="Fibonacci Step"),
        yaxis=dict(**_G, title="Min Odds to Recover Full Series Cost", rangemode="tozero"),
        legend=_LEG,
    )
    st.plotly_chart(fig6, use_container_width=True)
    st.caption(
        "The Fibonacci property: recovery odds converge to φ² ≈ 2.618. "
        f"At min odds {min_odds:.2f}, every step is {'fully covered ✓' if min_odds >= max(be_odds) else f'covered up to step {next((i+1 for i,b in enumerate(be_odds) if b > min_odds), max_step)}'}."
    )

    # ── Match breakdown expander ──────────────────────────────────────────────
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

    tab_fdco, tab_openliga, tab_api, tab_csv, tab_analysis = st.tabs(
        ["FDCO (19 leagues)", "OpenLigaDB (German)", "API-Football", "CSV Upload", "Analysis"]
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
            else:
                with st.spinner(f"Downloading {fdco_league} {season_label}…"):
                    matches = _fdco_fn(league_code, fdco_season)
                if not matches:
                    st.error(
                        f"No data for **{fdco_league} {season_label}**. "
                        "The season may not be available yet, or try a different season."
                    )
                else:
                    result = fibonacci_engine.simulate_season(
                        matches, fdco_base, fdco_min_odds, fdco_max_step
                    )
                    result["_matches"] = matches
                    st.session_state["fdco_result"] = (result, f"{fdco_league} {season_label}", fdco_min_odds, fdco_max_step)

        if "fdco_result" in st.session_state:
            r, lbl, _mo, _ms = st.session_state["fdco_result"]
            _render_sim_results(r, lbl, _mo, _ms)

    # =========================================================
    # TAB 2 — OpenLigaDB (German leagues, no API key)
    # =========================================================
    with tab_openliga:
        st.caption(
            "Historical Bundesliga / 2. Bundesliga / 3. Liga data from "
            "[openligadb.de](https://www.openligadb.de) — "
            "**no API key required**, completely free. "
            "Note: draw odds are not available from this source (a neutral 3.00 default is used)."
        )

        _openliga_leagues = getattr(data_fetcher, "OPENLIGADB_LEAGUES", {
            "Germany — Bundesliga":    "bl1",
            "Germany — 2. Bundesliga": "bl2",
            "Germany — 3. Liga":       "bl3",
        })

        ol1, ol2 = st.columns(2)
        with ol1:
            ol_league_name = st.selectbox(
                "League", list(_openliga_leagues.keys()), key="ol_league"
            )
        with ol2:
            ol_season = st.selectbox(
                "Season (start year)", [2024, 2023, 2022, 2021, 2020], key="ol_season"
            )

        olb1, olb2, olb3 = st.columns(3)
        with olb1:
            ol_base = st.number_input(
                "Base Stake (£)", value=default_base, min_value=1.0, step=1.0, key="ol_base"
            )
        with olb2:
            ol_min_odds = st.number_input(
                "Min Odds", value=default_min_odds, min_value=1.5, max_value=5.0,
                step=0.01, format="%.2f", key="ol_min",
            )
        with olb3:
            ol_max_step = st.slider(
                "Max Step", min_value=3, max_value=10, value=default_max_step, key="ol_step"
            )

        if st.button("Run OpenLigaDB Backtest", type="primary", key="ol_run"):
            ol_code = _openliga_leagues[ol_league_name]
            ol_label = f"{ol_league_name} {ol_season}/{str(ol_season + 1)[2:]}"
            _ol_fn = getattr(data_fetcher, "fetch_openligadb_historical", None)
            if _ol_fn is None:
                st.error("fetch_openligadb_historical unavailable — restart the app.")
            else:
                with st.spinner(f"Fetching {ol_label} from OpenLigaDB…"):
                    ol_matches = _ol_fn(ol_code, ol_season)
                if not ol_matches:
                    st.error(
                        f"No data returned for **{ol_label}**. "
                        "Try a different season or check your internet connection."
                    )
                else:
                    ol_result = fibonacci_engine.simulate_season(
                        ol_matches, ol_base, ol_min_odds, ol_max_step
                    )
                    ol_result["_matches"] = ol_matches
                    st.session_state["ol_result"] = (ol_result, ol_label, ol_min_odds, ol_max_step)

        if "ol_result" in st.session_state:
            r, lbl, _mo, _ms = st.session_state["ol_result"]
            st.info(
                "Draw odds from OpenLigaDB are unavailable — a neutral 3.00 was used. "
                "For real Bet365 odds, use the **FDCO** tab (D1/D2 are available there too)."
            )
            _render_sim_results(r, lbl, _mo, _ms)

    # =========================================================
    # TAB 3 — API-Football (legacy)
    # =========================================================
    with tab_api:
        # Cache the status check — avoids a live HTTP call on every page render
        if "apifb_status" not in st.session_state:
            _check_fn = getattr(data_fetcher, "check_api_suspended", None)
            if _check_fn:
                st.session_state["apifb_status"] = _check_fn()
            else:
                st.session_state["apifb_status"] = (True, "check_api_suspended unavailable — restart the app.")
        is_suspended, susp_msg = st.session_state["apifb_status"]

        if is_suspended:
            st.error(
                f"**API-Football returned an error:** `{susp_msg}`\n\n"
                "**Most common cause:** your API key is stale or invalidated. "
                "Even if your *account* is active, the key itself can expire after plan "
                "changes or account reinstatements.\n\n"
                "**To fix:**\n"
                "1. Log in to [dashboard.api-football.com](https://dashboard.api-football.com)\n"
                "2. Go to **My Account → API Key** and copy the current active key\n"
                "3. On **Streamlit Cloud** → your app → **Settings → Secrets** — "
                "update `API_FOOTBALL_KEY = \"<new key>\"`\n"
                "4. On **GitHub** → your repo → **Settings → Secrets → Actions** — "
                "update the `API_FOOTBALL_KEY` secret\n"
                "5. Also update the `.env` file locally\n\n"
                "In the meantime, the **Free Data** tab works without any API key "
                "and includes real Bet365 historical odds for 19 leagues."
            )
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
                else:
                    with st.spinner("Fetching data from API-Football…"):
                        raw = data_fetcher.fetch_historical_fixtures(api_id, api_season)
                    if not raw:
                        st.error(
                            "No data returned. Check your account at "
                            "[dashboard.api-football.com](https://dashboard.api-football.com) "
                            "or use the FDCO tab instead."
                        )
                    else:
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
                        st.session_state["api_bt_result"] = (result, f"{league_opts[sel_key]} — {api_season}", bt_min_odds, bt_max_step)

            if "api_bt_result" in st.session_state:
                r, lbl, _mo, _ms = st.session_state["api_bt_result"]
                _render_sim_results(r, lbl, _mo, _ms)

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
                else:
                    matches = df_csv[["home_goals", "away_goals", "draw_odds"]].to_dict("records")
                    res = fibonacci_engine.simulate_season(matches, csv_base, csv_min, csv_step)
                    res["_matches"] = df_csv.to_dict("records")
                    st.session_state["csv_result"] = (res, f"CSV — {uploaded.name}", csv_min, csv_step)
            except Exception as e:
                st.error(f"Error: {e}")

        if "csv_result" in st.session_state:
            r, lbl, _mo, _ms = st.session_state["csv_result"]
            _render_sim_results(r, lbl, _mo, _ms)

    # =========================================================
    # TAB 5 — Data Analysis
    # =========================================================
    with tab_analysis:
        st.caption(
            "Cross-league and cross-season analytics powered by "
            "[football-data.co.uk](https://www.football-data.co.uk) historical data. "
            "All simulations run client-side — no API quota used."
        )

        analysis_mode = st.radio(
            "Analysis mode",
            ["Multi-League Comparison", "Season Trend", "Parameter Sensitivity"],
            horizontal=True,
            key="analysis_mode",
        )

        # Shared dark theme helpers (local to this tab)
        _AD   = dict(plot_bgcolor="#0d0f12", paper_bgcolor="#0d0f12",
                     font_color="#9ca3af", margin=dict(l=0, r=0, t=40, b=0))
        _AG   = dict(gridcolor="#1a1e2a", zeroline=False)
        _ALEG = dict(bgcolor="rgba(0,0,0,0)", font=dict(color="#9ca3af"), bordercolor="#1a1e2a")
        _ATF  = dict(color="#e2e5eb", size=13)

        # ── Mode 1: Multi-League Comparison ───────────────────────────────────
        if analysis_mode == "Multi-League Comparison":
            st.markdown(
                "**Compare up to 19 FDCO leagues in a single season — "
                "see which leagues suit the Fibonacci system best.**"
            )

            mc1, mc2 = st.columns([3, 1])
            with mc1:
                ml_leagues = st.multiselect(
                    "Leagues to compare",
                    options=list(_FDCO_LEAGUES.keys()),
                    default=list(_FDCO_LEAGUES.keys())[:6],
                    key="ml_leagues",
                )
            with mc2:
                ml_season = st.selectbox(
                    "Season", [2024, 2023, 2022, 2021, 2020], key="ml_season"
                )

            mp1, mp2, mp3 = st.columns(3)
            with mp1:
                ml_base = st.number_input(
                    "Base Stake (£)", value=default_base, min_value=1.0, step=1.0, key="ml_base"
                )
            with mp2:
                ml_min_odds = st.number_input(
                    "Min Odds", value=default_min_odds, min_value=1.5, max_value=5.0,
                    step=0.01, format="%.2f", key="ml_min",
                )
            with mp3:
                ml_max_step = st.slider(
                    "Max Step", min_value=3, max_value=10, value=default_max_step, key="ml_step"
                )

            if st.button("Run Comparison", type="primary", key="ml_run"):
                if not ml_leagues:
                    st.warning("Select at least one league.")
                else:
                    _fdco_fn = getattr(data_fetcher, "fetch_historical_from_fdco", None)
                    if _fdco_fn is None:
                        st.error("fetch_historical_from_fdco unavailable — restart the app.")
                    else:
                        ml_rows = []
                        prog = st.progress(0.0)
                        for i, league_name in enumerate(ml_leagues):
                            code = _FDCO_LEAGUES[league_name]
                            matches = _fdco_fn(code, ml_season)
                            if matches:
                                res = fibonacci_engine.simulate_season(
                                    matches, ml_base, ml_min_odds, ml_max_step
                                )
                                ml_rows.append({
                                    "league": league_name.split(" — ", 1)[-1],
                                    "full_name": league_name,
                                    "roi": res["roi"],
                                    "draw_rate": res["draw_rate"],
                                    "net_pnl": res["net_pnl"],
                                    "max_drawdown": res["max_drawdown"],
                                    "total_bets": res["total_bets"],
                                    "wins": res["wins"],
                                    "losses": res["losses"],
                                    "longest_streak": res["longest_loss_streak"],
                                })
                            prog.progress((i + 1) / len(ml_leagues))
                        prog.empty()
                        season_str = f"{ml_season}/{str(ml_season + 1)[2:]}"
                        st.session_state["ml_results"] = (ml_rows, season_str)

            if "ml_results" in st.session_state:
                ml_rows, season_str = st.session_state["ml_results"]
                if not ml_rows:
                    st.warning("No data returned for any selected league.")
                else:
                    df_ml = pd.DataFrame(ml_rows).sort_values("roi", ascending=False)
                    st.markdown(f"#### {season_str} — {len(df_ml)} leagues")
                    st.dataframe(
                        df_ml[["league", "total_bets", "draw_rate", "roi", "net_pnl",
                               "max_drawdown", "longest_streak"]].rename(columns={
                            "league": "League", "total_bets": "Bets",
                            "draw_rate": "Draw %", "roi": "ROI %",
                            "net_pnl": "Net P&L (£)", "max_drawdown": "Max DD (£)",
                            "longest_streak": "Longest Loss",
                        }).reset_index(drop=True),
                        use_container_width=True, hide_index=True,
                    )

                    r1, r2 = st.columns(2)

                    with r1:
                        fig_roi = go.Figure(go.Bar(
                            x=df_ml["league"],
                            y=df_ml["roi"],
                            marker_color=["#00c853" if v >= 0 else "#ef4444" for v in df_ml["roi"]],
                            text=[f"{v:+.1f}%" for v in df_ml["roi"]],
                            textposition="outside",
                            textfont=dict(color="#9ca3af", size=10),
                            opacity=0.85,
                        ))
                        fig_roi.add_hline(y=0, line_dash="dot", line_color="#374151")
                        fig_roi.update_layout(
                            **_AD, title=f"ROI % by League  ({season_str})", title_font=_ATF,
                            xaxis=dict(**_AG, tickangle=-40),
                            yaxis=dict(**_AG, title="ROI %"),
                        )
                        st.plotly_chart(fig_roi, use_container_width=True)

                    with r2:
                        max_dd_abs = max(abs(v) for v in df_ml["max_drawdown"]) + 1e-6
                        fig_scatter = go.Figure(go.Scatter(
                            x=df_ml["draw_rate"],
                            y=df_ml["roi"],
                            mode="markers+text",
                            marker=dict(
                                size=[8 + 20 * (abs(v) / max_dd_abs) for v in df_ml["max_drawdown"]],
                                color=["#00c853" if v >= 0 else "#ef4444" for v in df_ml["roi"]],
                                opacity=0.85,
                                line=dict(color="#1a1e2a", width=1),
                            ),
                            text=df_ml["league"],
                            textposition="top center",
                            textfont=dict(color="#9ca3af", size=9),
                            hovertemplate=(
                                "<b>%{text}</b><br>"
                                "Draw Rate: %{x:.1f}%<br>"
                                "ROI: %{y:+.1f}%<extra></extra>"
                            ),
                        ))
                        fig_scatter.add_vline(
                            x=df_ml["draw_rate"].mean(), line_dash="dot",
                            line_color="#374151", line_width=1,
                        )
                        fig_scatter.add_hline(y=0, line_dash="dot", line_color="#374151", line_width=1)
                        fig_scatter.update_layout(
                            **_AD,
                            title="Draw Rate vs ROI  (bubble size = max drawdown)",
                            title_font=_ATF,
                            xaxis=dict(**_AG, title="Draw Rate (%)"),
                            yaxis=dict(**_AG, title="ROI %"),
                        )
                        st.plotly_chart(fig_scatter, use_container_width=True)

                    df_sorted_pnl = df_ml.sort_values("net_pnl", ascending=True)
                    fig_pnl = go.Figure(go.Bar(
                        x=df_sorted_pnl["net_pnl"],
                        y=df_sorted_pnl["league"],
                        orientation="h",
                        marker_color=["#00c853" if v >= 0 else "#ef4444"
                                      for v in df_sorted_pnl["net_pnl"]],
                        text=[f"£{v:+.2f}" for v in df_sorted_pnl["net_pnl"]],
                        textposition="outside",
                        textfont=dict(color="#9ca3af", size=10),
                        opacity=0.85,
                    ))
                    fig_pnl.add_vline(x=0, line_color="#374151")
                    fig_pnl.update_layout(
                        **_AD,
                        title=f"Net P&L by League  ({season_str})", title_font=_ATF,
                        xaxis=dict(**_AG, title="Net P&L (£)"),
                        yaxis=dict(gridcolor="#1a1e2a"),
                        height=max(300, len(df_sorted_pnl) * 38 + 80),
                    )
                    st.plotly_chart(fig_pnl, use_container_width=True)

        # ── Mode 2: Season Trend ──────────────────────────────────────────────
        elif analysis_mode == "Season Trend":
            st.markdown(
                "**Track how the Fibonacci system's performance evolves across "
                "multiple seasons for a single league.**"
            )

            st1c, st2c = st.columns([2, 2])
            with st1c:
                trend_league = st.selectbox(
                    "League", list(_FDCO_LEAGUES.keys()), key="trend_league"
                )
            with st2c:
                trend_seasons = st.multiselect(
                    "Seasons (start years)",
                    [2024, 2023, 2022, 2021, 2020, 2019, 2018],
                    default=[2020, 2021, 2022, 2023, 2024],
                    key="trend_seasons",
                )

            tp1, tp2, tp3 = st.columns(3)
            with tp1:
                trend_base = st.number_input(
                    "Base Stake (£)", value=default_base, min_value=1.0, step=1.0, key="trend_base"
                )
            with tp2:
                trend_min_odds = st.number_input(
                    "Min Odds", value=default_min_odds, min_value=1.5, max_value=5.0,
                    step=0.01, format="%.2f", key="trend_min",
                )
            with tp3:
                trend_max_step = st.slider(
                    "Max Step", min_value=3, max_value=10, value=default_max_step, key="trend_step"
                )

            if st.button("Run Season Trend", type="primary", key="trend_run"):
                if not trend_seasons:
                    st.warning("Select at least one season.")
                else:
                    _fdco_fn = getattr(data_fetcher, "fetch_historical_from_fdco", None)
                    if _fdco_fn is None:
                        st.error("fetch_historical_from_fdco unavailable — restart the app.")
                    else:
                        trend_rows = []
                        code = _FDCO_LEAGUES[trend_league]
                        prog2 = st.progress(0.0)
                        for i, yr in enumerate(sorted(trend_seasons)):
                            matches = _fdco_fn(code, yr)
                            if matches:
                                res = fibonacci_engine.simulate_season(
                                    matches, trend_base, trend_min_odds, trend_max_step
                                )
                                trend_rows.append({
                                    "season": f"{yr}/{str(yr + 1)[2:]}",
                                    "year": yr,
                                    "roi": res["roi"],
                                    "draw_rate": res["draw_rate"],
                                    "net_pnl": res["net_pnl"],
                                    "max_drawdown": res["max_drawdown"],
                                    "total_bets": res["total_bets"],
                                    "longest_streak": res["longest_loss_streak"],
                                })
                            prog2.progress((i + 1) / len(trend_seasons))
                        prog2.empty()
                        st.session_state["trend_results"] = (trend_rows, trend_league)

            if "trend_results" in st.session_state:
                trend_rows, _tl = st.session_state["trend_results"]
                if not trend_rows:
                    st.warning("No data found for the selected seasons.")
                else:
                    df_tr = pd.DataFrame(trend_rows).sort_values("year")
                    st.markdown(f"#### {_tl} — season-by-season")
                    st.dataframe(
                        df_tr.drop("year", axis=1).rename(columns={
                            "season": "Season", "roi": "ROI %", "draw_rate": "Draw %",
                            "net_pnl": "Net P&L (£)", "max_drawdown": "Max DD (£)",
                            "total_bets": "Bets", "longest_streak": "Longest Loss",
                        }).reset_index(drop=True),
                        use_container_width=True, hide_index=True,
                    )

                    ta1, ta2 = st.columns(2)
                    with ta1:
                        fig_tr_roi = go.Figure(go.Scatter(
                            x=df_tr["season"], y=df_tr["roi"],
                            mode="lines+markers",
                            line=dict(color="#00c853", width=2.5),
                            marker=dict(size=8, color="#00c853"),
                            fill="tozeroy", fillcolor="rgba(0,200,83,0.07)",
                            name="ROI %",
                        ))
                        fig_tr_roi.add_hline(y=0, line_dash="dot", line_color="#374151")
                        fig_tr_roi.update_layout(
                            **_AD, title="ROI % by Season", title_font=_ATF,
                            xaxis=dict(**_AG, title="Season"),
                            yaxis=dict(**_AG, title="ROI %"),
                        )
                        st.plotly_chart(fig_tr_roi, use_container_width=True)

                    with ta2:
                        fig_tr_dr = go.Figure(go.Scatter(
                            x=df_tr["season"], y=df_tr["draw_rate"],
                            mode="lines+markers",
                            line=dict(color="#f59e0b", width=2.5),
                            marker=dict(size=8, color="#f59e0b"),
                            name="Draw Rate %",
                        ))
                        avg_dr = df_tr["draw_rate"].mean()
                        fig_tr_dr.add_hline(
                            y=avg_dr, line_dash="dot", line_color="#4b5563",
                            annotation_text=f"Avg {avg_dr:.1f}%",
                            annotation_font_color="#4b5563",
                        )
                        fig_tr_dr.update_layout(
                            **_AD, title="Draw Rate % by Season", title_font=_ATF,
                            xaxis=dict(**_AG, title="Season"),
                            yaxis=dict(**_AG, title="Draw Rate %"),
                        )
                        st.plotly_chart(fig_tr_dr, use_container_width=True)

                    fig_tr_pnl = go.Figure()
                    fig_tr_pnl.add_trace(go.Bar(
                        x=df_tr["season"], y=df_tr["net_pnl"],
                        name="Net P&L",
                        marker_color=["#00c853" if v >= 0 else "#ef4444"
                                      for v in df_tr["net_pnl"]],
                        opacity=0.85,
                        text=[f"£{v:+.0f}" for v in df_tr["net_pnl"]],
                        textposition="outside",
                    ))
                    fig_tr_pnl.add_trace(go.Scatter(
                        x=df_tr["season"],
                        y=[-v for v in df_tr["max_drawdown"]],
                        mode="lines+markers",
                        line=dict(color="#ef4444", dash="dot", width=2),
                        marker=dict(size=7),
                        name="−Max Drawdown",
                    ))
                    fig_tr_pnl.update_layout(
                        **_AD, barmode="group",
                        title="Net P&L and Max Drawdown by Season", title_font=_ATF,
                        xaxis=dict(**_AG, title="Season"),
                        yaxis=dict(**_AG, title="£"),
                        legend=_ALEG,
                    )
                    st.plotly_chart(fig_tr_pnl, use_container_width=True)

        # ── Mode 3: Parameter Sensitivity ────────────────────────────────────
        elif analysis_mode == "Parameter Sensitivity":
            st.markdown(
                "**Grid-search `min_odds` × `max_step` combinations and visualise "
                "which settings maximise your chosen metric.**"
            )

            ps1, ps2 = st.columns(2)
            with ps1:
                sens_league = st.selectbox(
                    "League", list(_FDCO_LEAGUES.keys()), key="sens_league"
                )
            with ps2:
                sens_season = st.selectbox(
                    "Season", [2024, 2023, 2022, 2021, 2020], key="sens_season"
                )

            sp1, sp2 = st.columns(2)
            with sp1:
                sens_base = st.number_input(
                    "Base Stake (£)", value=default_base, min_value=1.0, step=1.0, key="sens_base"
                )
            with sp2:
                sens_metric = st.selectbox(
                    "Optimise for",
                    ["ROI %", "Net P&L (£)", "Max Drawdown (£)", "Draw Rate %"],
                    key="sens_metric",
                )

            odds_range = st.select_slider(
                "Min Odds range",
                options=[round(x * 0.1, 1) for x in range(18, 41)],
                value=(2.0, 3.5),
                key="sens_odds_range",
            )
            step_range = st.select_slider(
                "Max Step range",
                options=list(range(3, 11)),
                value=(4, 8),
                key="sens_step_range",
            )

            if st.button("Run Sensitivity Analysis", type="primary", key="sens_run"):
                _fdco_fn = getattr(data_fetcher, "fetch_historical_from_fdco", None)
                if _fdco_fn is None:
                    st.error("fetch_historical_from_fdco unavailable — restart the app.")
                else:
                    sens_code = _FDCO_LEAGUES[sens_league]
                    with st.spinner(f"Downloading {sens_league} {sens_season}…"):
                        sens_matches = _fdco_fn(sens_code, sens_season)
                    if not sens_matches:
                        st.error("No data returned for this league/season.")
                    else:
                        odds_vals = [
                            round(x * 0.1, 1)
                            for x in range(
                                int(odds_range[0] * 10),
                                int(odds_range[1] * 10) + 1,
                                2,  # step by 0.2 to keep grid size manageable
                            )
                        ]
                        step_vals = list(range(step_range[0], step_range[1] + 1))
                        total_runs = len(odds_vals) * len(step_vals)
                        prog3 = st.progress(0.0)
                        metric_key = {
                            "ROI %": "roi",
                            "Net P&L (£)": "net_pnl",
                            "Max Drawdown (£)": "max_drawdown",
                            "Draw Rate %": "draw_rate",
                        }[sens_metric]

                        grid: list[list[float]] = []
                        run_count = 0
                        for step_val in step_vals:
                            row = []
                            for odds_val in odds_vals:
                                res = fibonacci_engine.simulate_season(
                                    sens_matches, sens_base, odds_val, step_val
                                )
                                val = res[metric_key]
                                # Flip drawdown so higher cell = better (less drawdown)
                                if metric_key == "max_drawdown":
                                    val = -val
                                row.append(round(val, 2))
                                run_count += 1
                                prog3.progress(run_count / total_runs)
                            grid.append(row)
                        prog3.empty()

                        st.session_state["sens_result"] = {
                            "grid": grid,
                            "odds_vals": odds_vals,
                            "step_vals": step_vals,
                            "metric": sens_metric,
                            "metric_key": metric_key,
                            "label": f"{sens_league} {sens_season}/{str(sens_season + 1)[2:]}",
                        }

            if "sens_result" in st.session_state:
                sr = st.session_state["sens_result"]
                grid      = sr["grid"]
                odds_vals = sr["odds_vals"]
                step_vals = sr["step_vals"]
                metric_lbl = sr["metric"]
                metric_key = sr["metric_key"]
                slbl      = sr["label"]

                x_labels      = [f"{o:.1f}" for o in odds_vals]
                y_labels      = [f"Step {s}" for s in step_vals]
                # Reverse rows so lowest step sits at the bottom of the heatmap
                grid_disp    = list(reversed(grid))
                y_labels_disp = list(reversed(y_labels))

                fig_heat = go.Figure(go.Heatmap(
                    z=grid_disp,
                    x=x_labels,
                    y=y_labels_disp,
                    colorscale="RdYlGn",
                    text=[
                        [f"{v:+.1f}" if metric_key != "max_drawdown" else f"−£{abs(v):.0f}"
                         for v in row]
                        for row in grid_disp
                    ],
                    texttemplate="%{text}",
                    textfont=dict(size=10, color="white"),
                    hovertemplate=(
                        "Min Odds: %{x}<br>Step: %{y}<br>"
                        + metric_lbl + ": %{z}<extra></extra>"
                    ),
                    colorbar=dict(title=metric_lbl, tickfont=dict(color="#9ca3af")),
                ))
                fig_heat.update_layout(
                    **_AD,
                    title=f"{metric_lbl} — Parameter Sensitivity  ({slbl})", title_font=_ATF,
                    xaxis=dict(title="Min Odds Filter", **_AG),
                    yaxis=dict(title="Max Fibonacci Step", **_AG),
                    height=max(350, len(step_vals) * 55 + 120),
                )
                st.plotly_chart(fig_heat, use_container_width=True)

                # Best cell callout
                flat = [
                    (grid[si][oi], step_vals[si], odds_vals[oi])
                    for si in range(len(step_vals))
                    for oi in range(len(odds_vals))
                ]
                best_val, best_step, best_odds = max(flat, key=lambda x: x[0])
                if metric_key == "max_drawdown":
                    st.success(
                        f"Lowest drawdown: **−£{abs(best_val):.2f}** at "
                        f"min odds **{best_odds:.1f}** / max step **{best_step}**"
                    )
                else:
                    unit = "%" if "%" in metric_lbl else "£"
                    st.success(
                        f"Best **{metric_lbl}**: **{best_val:+.2f}{unit}** at "
                        f"min odds **{best_odds:.1f}** / max step **{best_step}**"
                    )
                st.caption(
                    f"Grid: {len(odds_vals)} odds values × {len(step_vals)} step values = "
                    f"{len(odds_vals) * len(step_vals)} simulations run."
                )


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
        if st.button("Re-Scan Now  (uses FDCO — free)", type="primary"):
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

    # The Odds API (primary live source)
    if st.button("Check Odds API Status", key="check_odds_status"):
        del st.session_state["odds_api_credits"]
    _odds_ok2, _odds_rem2 = st.session_state.get("odds_api_credits", (None, None))
    if _odds_ok2 is None:
        _odds_ok2, _, _odds_rem2 = data_fetcher.get_odds_api_status()
        st.session_state["odds_api_credits"] = (_odds_ok2, _odds_rem2)

    ca, cb = st.columns([3, 1])
    with ca:
        if _odds_ok2 and _odds_rem2 is not None:
            _opct2 = max(0.0, min(_odds_rem2 / 500, 1.0))
            _oc2 = "#ef4444" if _opct2 < 0.1 else "#f59e0b" if _opct2 < 0.3 else "#00c853"
            st.markdown(
                f"<div style='margin-bottom:4px; font-size:0.9rem;'>"
                f"The Odds API: <strong style='color:{_oc2};'>{_odds_rem2} credits remaining</strong>"
                f" / 500 per month</div>",
                unsafe_allow_html=True,
            )
            st.progress(_opct2)
        else:
            st.error("Odds API key not set or invalid. Add ODDS_API_KEY to Streamlit secrets.")
    with cb:
        if _odds_ok2 and _odds_rem2 is not None:
            st.metric("Credits Left", _odds_rem2)
        _cards()

    st.caption(
        "The Odds API free tier: 500 credits/month. One call per league per day = ~16 leagues/day. "
        "Draw rates + historical data use FDCO (football-data.co.uk) — completely free, no key needed."
    )

    # Legacy API-Football usage counter (kept for reference)
    with st.expander("API-Football legacy counter"):
        api_calls = data_fetcher.get_api_calls_today()
        st.caption(f"Recorded calls today: {api_calls}/100 (account currently suspended)")


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
