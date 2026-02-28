"""
CDReader Chapter Checker & Auto-Claimer
"""

import requests
import os
import json
import sys
from datetime import datetime

BASE_URL = "https://translatorserverwebapi-de.cdreader.com/api"

ACCOUNT_NAME   = os.environ.get("CDREADER_EMAIL",    "YOUR_EMAIL_HERE")
ACCOUNT_PWD    = os.environ.get("CDREADER_PASSWORD", "YOUR_PASSWORD_HERE")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT  = os.environ.get("TELEGRAM_CHAT_ID",   "")
DRY_RUN = os.environ.get("DRY_RUN", "false").lower() == "true"

HEADERS = {
    "accept": "application/json, text/plain, */*",
    "accept-language": "de,de-DE;q=0.9,en;q=0.8",
    "area": "DE",
    "origin": "https://trans.cdreader.com",
    "referer": "https://trans.cdreader.com/",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36 Edg/145.0.0.0",
}

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

def auth_headers(token):
    return {**HEADERS, "authorization": f"Bearer {token}"}

def send_telegram(message):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT:
        log("Telegram not configured - skipping notification.")
        return
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT, "text": message, "parse_mode": "HTML"},
            timeout=10,
        )
        if resp.status_code == 200:
            log("Telegram notification sent.")
        else:
            log(f"Telegram error: {resp.text}")
    except Exception as e:
        log(f"Telegram exception: {e}")

def login():
    log("Logging in...")
    resp = requests.post(
        f"{BASE_URL}/User/UserLogin",
        headers={**HEADERS, "content-type": "application/json;charset=UTF-8"},
        json={"accountName": ACCOUNT_NAME, "accountPwd": ACCOUNT_PWD, "checked": False},
        timeout=15,
    )
    resp.raise_for_status()
    body = resp.json()
    token = (
        body.get("data", {}).get("token")
        or body.get("data", {}).get("accessToken")
        or body.get("token")
    )
    if not token:
        log(f"Login failed. Response: {json.dumps(body, indent=2)}")
        sys.exit(1)
    log("Logged in successfully.")
    return token

def get_books(token):
    log("Fetching full book list (all pages)...")
    all_books = []
    page = 1
    page_size = 100

    while True:
        resp = requests.post(
            f"{BASE_URL}/ObjectBook/AuthorObjectBookList",
            headers={**auth_headers(token), "content-type": "application/json;charset=UTF-8"},
            json={
                "PageIndex": page, "PageSize": page_size,
                "fromLanguage": "", "fromBookName": "", "toBookName": "",
                "translationStatus": None, "roleTypeStatus": None,
            },
            timeout=15,
        )
        resp.raise_for_status()
        body = resp.json()
        data = body.get("data", {})

        if page == 1:
            log(f"Response keys: {list(body.keys())}")
            if isinstance(data, dict):
                log(f"data keys: {list(data.keys())}")

        # API stores books under 'dtolist'
        if isinstance(data, dict):
            books = (
                data.get("dtolist")
                or data.get("list")
                or data.get("items")
                or data.get("records")
                or []
            )
        elif isinstance(data, list):
            books = data
        else:
            books = []

        if not books:
            break

        all_books.extend(books)
        log(f"Page {page}: {len(books)} book(s).")

        if len(books) < page_size:
            break

        page += 1

    log(f"Total books: {len(all_books)}")
    return all_books

def get_available_chapters(token, book_id):
    resp = requests.get(
        f"{BASE_URL}/ObjectChapter/Receive?bookId={book_id}&receiveType=2",
        headers=auth_headers(token),
        timeout=15,
    )
    resp.raise_for_status()
    body = resp.json()
    data = body.get("data", {})
    # API returns data as dict with "dto" key containing the chapter list
    if isinstance(data, dict):
        chapters = data.get("dto") or data.get("list") or data.get("dtolist") or []
    elif isinstance(data, list):
        chapters = data
    else:
        chapters = []
    log(f"    Chapter API response: code={body.get('code')}, chapters found={len(chapters)}")
    return chapters

def claim_chapter(token, chapter_id):
    resp = requests.get(
        f"{BASE_URL}/ObjectChapter/ForeignReceive?chapter={chapter_id}",
        headers=auth_headers(token),
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()

def run():
    token = login()
    books = get_books(token)

    if not books:
        log("No books found in library.")
        return

    claimed_chapters = []
    errors = []

    for book in books:
        book_id   = book.get("id") or book.get("objectBookId") or book.get("bookId")
        book_name = (
            book.get("toBookName")
            or book.get("bookName")
            or book.get("name")
            or f"Book #{book_id}"
        )

        if not book_id:
            log(f"Could not find book ID in: {list(book.keys())}")
            continue

        log(f"Checking: {book_name} (ID: {book_id})")
        chapters = get_available_chapters(token, book_id)

        if not chapters:
            log(f"  No available chapters.")
            continue

        log(f"  {len(chapters)} chapter(s) available!")

        for ch in chapters:
            chapter_id   = ch.get("id") or ch.get("chapterId") or ch.get("objectChapterId")
            chapter_name = ch.get("chapterName") or ch.get("name") or f"Chapter #{chapter_id}"

            if DRY_RUN:
                log(f"  [DRY RUN] Would claim: {chapter_name}")
                claimed_chapters.append((book_name, chapter_name, "dry-run"))
                continue

            try:
                result = claim_chapter(token, chapter_id)
                success = (
                    result.get("code") == 0
                    or result.get("success") is True
                    or result.get("status") == 200
                )
                if success:
                    log(f"  Claimed: {chapter_name}")
                    claimed_chapters.append((book_name, chapter_name, "claimed"))
                else:
                    log(f"  Claim response: {result}")
                    claimed_chapters.append((book_name, chapter_name, f"response: {result}"))
            except Exception as e:
                log(f"  Error claiming {chapter_name}: {e}")
                errors.append(f"{book_name} / {chapter_name}: {e}")

    if claimed_chapters:
        lines = ["<b>CDReader: Chapters Claimed!</b>\n"]
        for book_name, chapter_name, status in claimed_chapters:
            lines.append(f"* <b>{book_name}</b>: {chapter_name} ({status})")
        if DRY_RUN:
            lines.append("\n<i>(DRY RUN - nothing was actually claimed)</i>")
        send_telegram("\n".join(lines))
    else:
        log("Check complete - no available chapters found this run.")

    if errors:
        log(f"Errors: {errors}")

if __name__ == "__main__":
    run()
