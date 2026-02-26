"""
CDReader Chapter Checker & Auto-Claimer
=======================================
Polls the CDReader translator API for available chapters across all books,
auto-claims them, and sends a Telegram notification.

Setup:
  pip install requests

Environment variables (required for GitHub Actions):
  CDREADER_EMAIL      - your login email
  CDREADER_PASSWORD   - your login password
  TELEGRAM_BOT_TOKEN  - from @BotFather on Telegram
  TELEGRAM_CHAT_ID    - your personal chat ID (from @userinfobot)
"""

import requests
import os
import json
import sys
from datetime import datetime

# ── Config ────────────────────────────────────────────────────────────────────
BASE_URL = "https://translatorserverwebapi-de.cdreader.com/api"

ACCOUNT_NAME  = os.environ.get("CDREADER_EMAIL",    "YOUR_EMAIL_HERE")
ACCOUNT_PWD   = os.environ.get("CDREADER_PASSWORD", "YOUR_PASSWORD_HERE")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT  = os.environ.get("TELEGRAM_CHAT_ID",   "")

DRY_RUN = os.environ.get("DRY_RUN", "false").lower() == "true"  # set to true to check without claiming

HEADERS = {
    "accept": "application/json, text/plain, */*",
    "accept-language": "de,de-DE;q=0.9,en;q=0.8",
    "area": "DE",
    "origin": "https://trans.cdreader.com",
    "referer": "https://trans.cdreader.com/",
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/145.0.0.0 Safari/537.36 Edg/145.0.0.0"
    ),
}

# ── Helpers ───────────────────────────────────────────────────────────────────

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def auth_headers(token):
    return {**HEADERS, "authorization": f"Bearer {token}"}


def send_telegram(message):
    """Send a Telegram message. Silently skips if credentials not configured."""
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT:
        log("⚠️  Telegram not configured — skipping notification.")
        return
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT, "text": message, "parse_mode": "HTML"},
            timeout=10,
        )
        if resp.status_code == 200:
            log("📨 Telegram notification sent.")
        else:
            log(f"⚠️  Telegram error: {resp.text}")
    except Exception as e:
        log(f"⚠️  Telegram exception: {e}")


# ── API calls ─────────────────────────────────────────────────────────────────

def login():
    log("🔐 Logging in...")
    resp = requests.post(
        f"{BASE_URL}/User/UserLogin",
        headers={**HEADERS, "content-type": "application/json;charset=UTF-8"},
        json={"accountName": ACCOUNT_NAME, "accountPwd": ACCOUNT_PWD, "checked": False},
        timeout=15,
    )
    resp.raise_for_status()
    body = resp.json()
    # Try common token locations in the response
    token = (
        body.get("data", {}).get("token")
        or body.get("data", {}).get("accessToken")
        or body.get("token")
    )
    if not token:
        log(f"❌ Login failed. Response: {json.dumps(body, indent=2)}")
        sys.exit(1)
    log("✅ Logged in successfully.")
    return token


def get_books(token):
    log("📚 Fetching book library...")
    resp = requests.post(
        f"{BASE_URL}/ObjectBook/AuthorObjectBookList",
        headers={**auth_headers(token), "content-type": "application/json;charset=UTF-8"},
        json={
            "PageIndex": 1, "PageSize": 100,
            "fromLanguage": "", "fromBookName": "", "toBookName": "",
            "translationStatus": None, "roleTypeStatus": None,
        },
        timeout=15,
    )
    resp.raise_for_status()
    body = resp.json()
    # Navigate common response shapes
    data = body.get("data", {})
    books = data.get("list") or data.get("items") or (data if isinstance(data, list) else [])
    log(f"   Found {len(books)} book(s).")
    return books


def get_available_chapters(token, book_id):
    """receiveType=2 returns chapters available for claiming."""
    resp = requests.get(
        f"{BASE_URL}/ObjectChapter/Receive?bookId={book_id}&receiveType=2",
        headers=auth_headers(token),
        timeout=15,
    )
    resp.raise_for_status()
    body = resp.json()
    data = body.get("data", [])
    return data if isinstance(data, list) else []


def claim_chapter(token, chapter_id):
    resp = requests.get(
        f"{BASE_URL}/ObjectChapter/ForeignReceive?chapter={chapter_id}",
        headers=auth_headers(token),
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()


# ── Main logic ────────────────────────────────────────────────────────────────

def run():
    token = login()
    books = get_books(token)

    if not books:
        log("⚠️  No books found in library. Check API response structure.")
        return

    claimed_chapters = []
    errors = []

    for book in books:
        # Try multiple possible field names for book ID and name
        book_id   = book.get("bookId") or book.get("objectBookId") or book.get("id")
        book_name = book.get("bookName") or book.get("name") or book.get("toBookName") or f"Book #{book_id}"

        if not book_id:
            log(f"⚠️  Could not find book ID in: {list(book.keys())}")
            continue

        log(f"📖 Checking: {book_name} (ID: {book_id})")
        chapters = get_available_chapters(token, book_id)

        if not chapters:
            log(f"   No available chapters.")
            continue

        log(f"   🎯 {len(chapters)} chapter(s) available!")

        for ch in chapters:
            chapter_id   = ch.get("chapterId") or ch.get("objectChapterId") or ch.get("id")
            chapter_name = ch.get("chapterName") or ch.get("name") or f"Chapter #{chapter_id}"

            if DRY_RUN:
                log(f"   [DRY RUN] Would claim: {chapter_name}")
                claimed_chapters.append((book_name, chapter_name, "dry-run"))
                continue

            try:
                result = claim_chapter(token, chapter_id)
                success = (
                    result.get("code") == 0
                    or result.get("success") is True
                    or result.get("status") == 200
                    or result.get("msg", "").lower() in ("success", "ok", "")
                )
                if success:
                    log(f"   ✅ Claimed: {chapter_name}")
                    claimed_chapters.append((book_name, chapter_name, "claimed"))
                else:
                    log(f"   ⚠️  Claim returned: {result}")
                    claimed_chapters.append((book_name, chapter_name, f"response: {result}"))
            except Exception as e:
                log(f"   ❌ Error claiming {chapter_name}: {e}")
                errors.append(f"{book_name} / {chapter_name}: {e}")

    # ── Notification ──────────────────────────────────────────────────────────
    if claimed_chapters:
        lines = [f"📚 <b>CDReader: Chapters Available!</b>\n"]
        for book_name, chapter_name, status in claimed_chapters:
            lines.append(f"• <b>{book_name}</b>: {chapter_name} ({status})")
        if DRY_RUN:
            lines.append("\n<i>(DRY RUN — nothing was claimed)</i>")
        send_telegram("\n".join(lines))
    else:
        log("✅ Check complete — no available chapters found this run.")

    if errors:
        log(f"⚠️  Errors: {errors}")


if __name__ == "__main__":
    run()
