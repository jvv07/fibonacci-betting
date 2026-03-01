"""
notifications.py — Email alert sender for fibonacci-betting.

Sends plain-text SMTP emails via Gmail (port 465 / SSL).
Requires ALERT_EMAIL and EMAIL_PASSWORD environment variables.
Set dry_run=True in any call to print the email to stdout instead of sending.
"""

import os
import smtplib
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from dotenv import load_dotenv

load_dotenv()


def send_daily_alert(qualifying_bets: list[dict], dry_run: bool = False) -> bool:
    """
    Send a daily summary email listing today's qualifying bets.

    Args:
        qualifying_bets: List of bet dicts, each containing at minimum:
            home_team, away_team, league_name, kickoff_utc,
            draw_odds, fib_step, stake
        dry_run: If True, print the email body to stdout instead of sending.

    Returns:
        True on success, False on failure.
    """
    if not qualifying_bets:
        print("[notifications] No qualifying bets today — skipping alert.")
        return True

    alert_email = os.environ.get("ALERT_EMAIL", "")
    email_password = os.environ.get("EMAIL_PASSWORD", "")

    if not alert_email or not email_password:
        print("[notifications] ALERT_EMAIL or EMAIL_PASSWORD not configured. Skipping.")
        return False

    today_str = datetime.now(timezone.utc).strftime("%A %-d %B %Y")
    subject = f"🎯 Fibonacci Betting — {len(qualifying_bets)} Bet(s) Today | {today_str}"

    # ------------------------------------------------------------------
    # Build plain-text body
    # ------------------------------------------------------------------
    lines = [
        "=" * 62,
        f"  FIBONACCI BETTING DAILY ALERT",
        f"  {today_str}",
        "=" * 62,
        "",
        f"  {len(qualifying_bets)} qualifying bet(s) identified today:",
        "",
    ]

    total_stake = 0.0
    for i, bet in enumerate(qualifying_bets, 1):
        home = bet.get("home_team", "?")
        away = bet.get("away_team", "?")
        league = bet.get("league_name", bet.get("league_key", "?"))
        kickoff = (bet.get("kickoff_utc") or "?")[:16].replace("T", " ") + " UTC"
        odds = bet.get("draw_odds", "?")
        step = bet.get("fib_step", 1)
        stake = float(bet.get("stake", 0))
        total_stake += stake

        lines += [
            f"  Bet {i}: {home} vs {away}",
            f"    League  : {league}",
            f"    Kickoff : {kickoff}",
            f"    Odds    : {odds}",
            f"    Step    : {step}  (Fibonacci ladder)",
            f"    Stake   : £{stake:.2f}",
            "",
        ]

    lines += [
        "-" * 62,
        f"  Total stake today  : £{total_stake:.2f}",
        "",
        "  Bet responsibly. Never stake more than you can afford to lose.",
        "=" * 62,
        "",
        "  fibonacci-betting — automated draw betting system",
    ]

    body = "\n".join(lines)

    if dry_run:
        print("[notifications] DRY RUN — email content:\n")
        print(body)
        return True

    # ------------------------------------------------------------------
    # Send via Gmail SMTP SSL
    # ------------------------------------------------------------------
    try:
        msg = MIMEMultipart()
        msg["From"] = alert_email
        msg["To"] = alert_email
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "plain"))

        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(alert_email, email_password)
            server.send_message(msg)

        print(f"[notifications] Alert sent successfully to {alert_email}")
        return True

    except smtplib.SMTPAuthenticationError:
        print(
            "[notifications] SMTP authentication failed. "
            "For Gmail, use an App Password (not your account password). "
            "Enable 2FA then generate one at myaccount.google.com/apppasswords."
        )
        return False
    except Exception as e:
        print(f"[notifications] Failed to send email: {e}")
        return False
