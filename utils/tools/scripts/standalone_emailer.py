# mm/utils/tools/scripts/standalone_emailer.py

# stdlib imports
import importlib
import os
import smtplib
import ssl
import uuid
from email.message import EmailMessage
from email.utils import formataddr

# third-party imports
from dotenv import load_dotenv

# local application imports
import mm.config.marcus as marcus

# load env for this process
load_dotenv("mm/data/secrets/.env")


def send_email(subject: str, status: str, title: str, message: str) -> str:
    import importlib
    import os
    import smtplib
    import ssl
    import uuid
    from email.message import EmailMessage
    from email.utils import formataddr
    from datetime import datetime
    from zoneinfo import ZoneInfo
    import mm.config.marcus as marcus

    importlib.reload(marcus)
    if not bool(getattr(marcus, "ALERT_EMAIL_ENABLED", False)):
        return "disabled"
    if str(getattr(marcus, "ALERT_EMAIL_ENCRYPT", "SSL")).upper() != "SSL":
        return "Simple Mail Transfer Protocol not established. No conn."

    host = getattr(marcus, "ALERT_EMAIL_SMTP_SERVER", None)
    port = getattr(marcus, "ALERT_EMAIL_SMTP_PORT", None)
    recipient = getattr(marcus, "ALERT_EMAIL_RECIPIENT", None)

    USERCODE = "AND"  # hardcode per file

    # ---- Edit Sender Info (per file) ----
    user = os.getenv(f"{USERCODE}_USR")
    pwd = os.getenv(f"{USERCODE}_PWD")
    sender_email = user
    sender_name = os.getenv(f"{USERCODE}_NAME")
    # -------------------------------------

    # status color map
    STATUS_COLORS = {
        "SYSTEM NOMINAL": "#2e7d32",
        "PROCESS DOWN": "#c0392b",
        "MULTIPLE DOWN": "#c0392b",
        "SENSITIVE": "#BE644C",
    }
    status_text = str(status).upper()
    status_color = STATUS_COLORS.get(status_text, "#BE644C")

    msg = EmailMessage()
    domain = sender_email.split("@")[1] if "@" in sender_email else "hodlcorp.io"
    msg_id = f"<{uuid.uuid4()}@{domain}>"
    msg["Message-ID"] = msg_id
    msg["From"] = formataddr((sender_name, sender_email))
    msg["To"] = recipient
    msg["Subject"] = subject
    msg["X-Priority"] = "1"
    msg["X-MSMail-Priority"] = "High"
    msg["Importance"] = "High"

    # footer fields
    now_tz = datetime.now(ZoneInfo("America/Toronto"))
    sent_str = now_tz.strftime("%Y-%m-%d %H:%M:%S America/Toronto")
    epoch_ms = int(now_tz.timestamp() * 1000)
    mid_clean = msg_id.strip("<>").split("@", 1)[0]

    # full HTML body (single block)
    html_body = f"""
<div style="font-family: monospace;">
  <table role="presentation" width="100%" height="20px" cellpadding="8px" cellspacing="0" border="0">
    <!-- Top Banner -->
    <tr style="font-family: Georgia, 'Times New Roman', Times, serif;font-size:20px;font-weight:600;background-color:#333;">
      <td align="left" style="color:#EFEFEF;letter-spacing:12px;">INTCOMM</td>
      <td align="right" style="color:{status_color};letter-spacing:4px;">{status_text}</td>
    </tr>

    <!-- Message Title -->
    <tr width="100%" cellpadding="6px" style="font-family: Tahoma, Geneva, sans-serif;text-align:left;font-size:14px;font-weight:600;color:#333;">
      <td colspan="2">
        {title}
      </td>
    </tr>

    <!-- Message Content -->
    <tr width="100%" cellpadding="6px" style="font-family: Tahoma, Geneva, sans-serif;text-align:left;font-size:11px;font-weight:400;line-height:1.5;color:#333;">
      <td colspan="2">
        {message}
      </td>
    </tr>

    <!-- UNUSED SPACER ROW -->
    <tr width="100%" height="25px"><td colspan="2">&nbsp;</td></tr>
  </table>

  <!-- Footer -->
  <table role="presentation" width="400px" height="20px" cellpadding="4" cellspacing="0" border="0" style="font-family: Tahoma, Geneva, sans-serif;">
    <!-- DOCINT -->
    <tr style="background-color:#333;">
      <td colspan="2" style="color:#efefef;font-size:12px;font-weight:600;">DOCINT</td>
    </tr>

    <tr style="background-color:#E9E9E5;">
      <td width="30px" style="color:#333;font-size:10px;font-weight:600;">SENT</td>

      <td width="10px" style="color:#333;font-size:10px;font-weight:600;">&rarr;</td>
      <td style="color:#333;font-size:11px;font-weight:400;">{sent_str}</td>
    </tr>

    <tr style="background-color:#F2F2F0;">
      <td width="30px" style="color:#333;font-size:10px;font-weight:600;">EPOCH</td>
      <td width="10px" style="color:#333;font-size:10px;font-weight:600;">&rarr;</td>
      <td style="color:#333;font-size:11px;font-weight:400;">{epoch_ms} (ms since 1970/01/01 0:00 UTC)</td>
    </tr>

    <tr style="background-color:#E9E9E5;">
      <td width="30px" style="color:#333;font-size:10px;font-weight:600;">m.ID</td>
      <td width="10px" style="color:#333;font-size:10px;font-weight:600;">&rarr;</td>
      <td style="color:#333;font-size:11px;font-weight:400;">{mid_clean}</td>
    </tr>
  </table>
</div>
"""

    msg.add_alternative(html_body, subtype="html")

    ctx = ssl.create_default_context()
    with smtplib.SMTP_SSL(host, port, context=ctx, timeout=10) as s:
        if user and pwd:
            s.login(user, pwd)
        s.send_message(msg)

    return msg_id


# ONE TEST: fire on load
print("Sending email...")
mid = send_email(
    subject="Kinetic Automated Relay Interface Node",
    status="SENSITIVE",  # e.g., SYSTEM NOMINAL / PROCESS DOWN / MULTIPLE DOWN / SENSITIVE
    title="Some urgent message about something",
    message=(
        "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod "
        "tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, "
        "quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. "
        "Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu "
        "fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa "
        "qui officia deserunt mollit anim id est laborum."
    ),
)
print("Send successful. Confirmation ->", mid)

