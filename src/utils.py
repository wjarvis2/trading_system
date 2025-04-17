

import os
import smtplib
from email.mime.text import MIMEText
from dotenv import load_dotenv

load_dotenv()  # make sure .env variables are available

SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT   = 587
GMAIL_USER  = os.getenv("GMAIL_USER")
GMAIL_PASS  = os.getenv("GMAIL_APP_PASSWORD")


def send_email(subject: str, body: str, to_email: str) -> None:
    """
    Send a plain‑text email via Gmail SMTP.
    """
    if not (GMAIL_USER and GMAIL_PASS):
        raise RuntimeError("GMAIL_USER or GMAIL_APP_PASSWORD not set.")

    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = GMAIL_USER
    msg["To"]   = to_email

    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(GMAIL_USER, GMAIL_PASS)
            server.sendmail(GMAIL_USER, [to_email], msg.as_string())
        print("✓ email sent")
    except Exception as exc:
        print(f"✗ email failed: {exc}")
