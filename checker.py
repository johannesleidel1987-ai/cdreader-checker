"""
CDReader Complete Pipeline
Claim → Fetch rows → Fetch glossary → Rephrase with Gemini → Verify → Submit → Finish
"""

import requests
import os
import json
import sys
import time
from datetime import datetime

# ─── Config ──────────────────────────────────────────────────────────────────
BASE_URL   = "https://translatorserverwebapi-de.cdreader.com/api"
GEMINI_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-pro:generateContent"

ACCOUNT_NAME   = os.environ.get("CDREADER_EMAIL",    "")
ACCOUNT_PWD    = os.environ.get("CDREADER_PASSWORD", "")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT  = os.environ.get("TELEGRAM_CHAT_ID",   "")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY",     "")
DRY_RUN        = os.environ.get("DRY_RUN", "false").lower() == "true"

HEADERS = {
    "accept": "application/json, text/plain, */*",
    "accept-language": "de,de-DE;q=0.9,en;q=0.8",
    "area": "DE",
    "origin": "https://trans.cdreader.com",
    "referer": "https://trans.cdreader.com/",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36 Edg/145.0.0.0",
}

WORD_CORRECTION_DEFAULT = json.dumps({"StatusCode": 0, "SpellErrors": [], "GrammaticalErrors": []})

# ─── Rephrasing prompt (universal rules) ─────────────────────────────────────
BASE_PROMPT = """ROLE
You are an experienced German content writer and expert editor. Your task is to rephrase each row in the provided data into polished, natural, and professional German.

OUTPUT FORMAT (CRITICAL)
Return ONLY a valid JSON array — no markdown, no preamble, no explanation.
Each object must have exactly:
  "sort": original sort number (integer, unchanged)
  "content": rephrased German text

Example: [{"sort": 0, "content": "rephrased line"}, {"sort": 1, "content": "..."}]

CAPITALIZATION & FORMATTING
- All-caps lines → rephrase in ALL CAPS
- Lines starting with "Kapitel" → capitalize first letter of each word
- Lines with only punctuation or single words → retain exactly as-is
- Standard lines → standard German capitalization

LINGUISTIC GUIDELINES
- Preserve approximate word count per line — avoid excessive shortening
- Natural, conversational German; use synonyms to avoid repetition
- Maintain character action beats
- Consider surrounding lines for narrative flow
- Dashes (—): never translate literally; restructure using conjunctions or relative clauses

PRONOUN PROTOCOL (CRITICAL)
- "du": only for family, romantic partners, close long-term friends
- "Sie": default for all other relationships (colleagues, strangers, boss/subordinate)
- Never switch pronouns between the same two characters within a chapter

DIALOGUE & HONORIFICS
- German quotation marks only: „ to open, " to close
- Add comma after closing " when followed by an accompanying sentence on the next line
- Never use English quotation marks (" or ')
- "Mr." → "Herr", "Mrs."/"Miss"/"Ms." → "Frau"

UNIVERSAL GLOSSARY
Company: Briggs Group→Briggs-Gruppe; Star Wish Investments→Star Wish-Investitionen; Evans Entertainment→Evans Entertainment; Aurora Apparel Company→Aurora-Bekleidungsunternehmen; Radiant Jewels→Radiant Jewels; Yaroslav Technology→Yaroslav-Technologie; Newcrest Pharmaceuticals→NeuÄra-Pharma; North Investments→Nord-Investment; Vivian Floral Design→Vivian-Blumendesign; TurboVortex Club→Turbowirbel-Club; Summit Capital→Gipfelkapital-Konzern
Family: Williams family→Familie Williams; Holdens→Familie Holden
Locations: Blossom Estate→Blossom-Anwesen; Regal Grove→Royal-Anwesen; Presidency Estate→Präsidialanwesen; Hillside Villa→Wolkenruh-Landhaus; Stone Village→Steindorf; Cloud Sea Project→Wolkenmeer-Projekt; Faywind Village→Faywind-Dorf; Clearwater Village→Kristallquell-Dorf; Regal Diner→Goldflor-Restaurant; Rosewood Hills→Rosenschlossburg; Shaw Mansion→Herrenhaus Shaw; Crownspire Villa→Kronenspitz-Villa; Curtis Mansion→Curtis-Herrenhaus; underground market→Schwarzmarkt; Briskvale High→Frischtalschule
Medical: Crobert Hospital→Krankenhaus in Crobert; Kretol University→Universität Kretol; Faywald Hospital→Frieden-Krankenhaus; Wraith Physician→Wraith-Ärztin; Phantom Healer→Phantomheilerin; Raynesse Hospital→Rainstein-Klinik
Terms: Black Dragon Syndicate→Syndikat des Schwarzen Drachen; Black Hawk Alliance→Schwarzer-Hawk-Allianz; CEO→Geschäftsführer; Skybreaker→Himmelsschneider; Darknight→Nachtphantom; Blackdragon→Schwarzer Drache; Blackwing→Schwarzflügel; Shadow→Schatten; Askelpius→Asklepios; Violet→Violett; Snowball→Schneeball; Heavenly Melody→Himmlische Melodie
Characters: Mr. Moss→Herr Moos; Ms. Braxton→Fräulein Braxton; Miss Briggs→Fräulein Briggs; Kiley→Lena; Jennie→Jenny; Steve→Stefan; Garry→Gerhard; Ethan→Elias; Monica→Monika; Gabby→Gabi; Claire→Klara
Currency: Dollar→Euro

FINAL SELF-CHECK (do before responding)
1. Output has EXACTLY the same number of objects as input rows?
2. German quotation marks used with mandatory comma rule?
3. du/Sie consistent per relationship?
4. All glossary terms applied?
5. No literal dash translations?
6. Response is pure JSON with zero extra text?"""


# ─── Helpers ──────────────────────────────────────────────────────────────────
def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

def auth_headers(token):
    return {**HEADERS, "authorization": f"Bearer {token}"}

def send_telegram(message):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT:
        log("Telegram not configured — skipping.")
        return
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT, "text": message, "parse_mode": "HTML"},
            timeout=10,
        )
        if resp.status_code == 200:
            log("Telegram sent.")
        else:
            log(f"Telegram error: {resp.text}")
    except Exception as e:
        log(f"Telegram exception: {e}")


# ─── Auth ─────────────────────────────────────────────────────────────────────
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
        log(f"Login failed: {body}")
        sys.exit(1)
    log("Logged in.")
    return token


# ─── Phase 1: Claim ───────────────────────────────────────────────────────────
def get_books(token):
    log("Fetching book list...")
    all_books, page = [], 1
    while True:
        resp = requests.post(
            f"{BASE_URL}/ObjectBook/AuthorObjectBookList",
            headers={**auth_headers(token), "content-type": "application/json;charset=UTF-8"},
            json={"PageIndex": page, "PageSize": 100,
                  "fromLanguage": "", "fromBookName": "", "toBookName": "",
                  "translationStatus": None, "roleTypeStatus": None},
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json().get("data", {})
        books = (
            data.get("dtolist") or data.get("list") or
            data.get("items") or data.get("records") or
            (data if isinstance(data, list) else [])
        )
        if not books:
            break
        all_books.extend(books)
        log(f"  Page {page}: {len(books)} book(s).")
        if len(books) < 100:
            break
        page += 1
    log(f"Total books: {len(all_books)}")
    return all_books

def get_available_chapters(token, book_id):
    resp = requests.get(
        f"{BASE_URL}/ObjectChapter/Receive?bookId={book_id}&receiveType=2",
        headers=auth_headers(token), timeout=15,
    )
    resp.raise_for_status()
    body = resp.json()
    data = body.get("data", {})
    if isinstance(data, dict):
        chapters = data.get("dto") or data.get("list") or data.get("dtolist") or []
    elif isinstance(data, list):
        chapters = data
    else:
        chapters = []
    log(f"    Chapter API: code={body.get('code')}, available={len(chapters)}")
    return chapters

def claim_chapter(token, chapter_id):
    resp = requests.get(
        f"{BASE_URL}/ObjectChapter/ForeignReceive?chapter={chapter_id}",
        headers=auth_headers(token), timeout=15,
    )
    resp.raise_for_status()
    return resp.json()


# ─── Phase 2: Find the processing chapter ID ─────────────────────────────────
def find_chapter_processing_id(token, book, claimed_chapter_name):
    """
    After claiming, find the internal chapterId used by Start/CatChapterList/Submit/Finish.
    Searches AuthorChapterList for the claimed chapter by name.
    """
    book_id_for_list = (
        book.get("bookId") or book.get("objectBookId") or book.get("id")
    )
    log(f"  Searching AuthorChapterList for '{claimed_chapter_name}' (bookId={book_id_for_list})...")

    page = 1
    while True:
        resp = requests.post(
            f"{BASE_URL}/ObjectChapter/AuthorChapterList",
            headers={**auth_headers(token), "content-type": "application/json;charset=UTF-8"},
            json={"PageIndex": page, "PageSize": 100,
                  "chapterType": "", "chapterName": "",
                  "bookId": book_id_for_list,
                  "contentCode": "", "translationType": None,
                  "cnValue": "", "orderFile": ""},
            timeout=15,
        )
        resp.raise_for_status()
        body = resp.json()
        data = body.get("data", {})
        chapters = (
            data.get("dtolist") or data.get("list") or
            data.get("items") or (data if isinstance(data, list) else [])
        )

        if page == 1:
            log(f"  AuthorChapterList response keys: {list(body.keys())}")
            if chapters:
                log(f"  First chapter fields: {list(chapters[0].keys())}")

        for ch in chapters:
            name = ch.get("chapterName") or ch.get("name") or ""
            # Match by name (exact or partial)
            if claimed_chapter_name.lower() in name.lower() or name.lower() in claimed_chapter_name.lower():
                proc_id = ch.get("id") or ch.get("chapterId") or ch.get("objectChapterId")
                log(f"  Found match: '{name}' → processing ID: {proc_id}")
                log(f"  Full chapter fields: {ch}")
                return proc_id, ch

        if not chapters or len(chapters) < 100:
            break
        page += 1

    log(f"  ⚠️ Could not find chapter '{claimed_chapter_name}' in AuthorChapterList")
    return None, None


# ─── Phase 3: Fetch data ──────────────────────────────────────────────────────
def start_chapter(token, chapter_id):
    log(f"  Starting chapter {chapter_id}...")
    resp = requests.get(
        f"{BASE_URL}/ObjectCatChapter/StartChapter?chapterId={chapter_id}&ScheduleStatusType=2",
        headers=auth_headers(token), timeout=15,
    )
    resp.raise_for_status()
    result = resp.json()
    log(f"  Start response: {result}")
    return result

def get_chapter_rows(token, chapter_id):
    log(f"  Fetching rows for chapter {chapter_id}...")
    resp = requests.get(
        f"{BASE_URL}/ObjectCatChapter/CatChapterList?flowType=2&chapterId={chapter_id}&ToLanguage=412&FromLanguage=0",
        headers=auth_headers(token), timeout=15,
    )
    resp.raise_for_status()
    body = resp.json()
    data = body.get("data", {})
    if isinstance(data, dict):
        rows = data.get("dto") or data.get("list") or data.get("dtolist") or []
    elif isinstance(data, list):
        rows = data
    else:
        rows = []

    if rows:
        log(f"  Row fields available: {list(rows[0].keys())}")

    log(f"  Fetched {len(rows)} rows.")
    return rows

def get_glossary(token, object_book_id):
    log(f"  Fetching glossary for book {object_book_id}...")
    all_terms, page = [], 1
    while True:
        resp = requests.post(
            f"{BASE_URL}/ObjectDictionary/DictionaryList",
            headers={**auth_headers(token), "content-type": "application/json;charset=UTF-8"},
            json={"PageIndex": page, "PageSize": 100,
                  "objectBookId": str(object_book_id), "orderByFile": ""},
            timeout=15,
        )
        resp.raise_for_status()
        body = resp.json()
        data = body.get("data", {})
        terms = (
            data.get("dtolist") or data.get("list") or
            data.get("items") or (data if isinstance(data, list) else [])
        )

        if page == 1 and terms:
            log(f"  Glossary term fields: {list(terms[0].keys())}")

        if not terms:
            break
        all_terms.extend(terms)
        log(f"  Glossary page {page}: {len(terms)} terms.")
        if len(terms) < 100:
            break
        page += 1

    log(f"  Total glossary terms: {len(all_terms)}")
    return all_terms

def format_glossary_for_prompt(glossary_terms):
    """Convert glossary API response into readable text for the prompt."""
    if not glossary_terms:
        return "(No book-specific glossary terms)"
    lines = []
    for t in glossary_terms:
        src = t.get("fromContent") or t.get("sourceWord") or t.get("word") or t.get("from") or ""
        tgt = t.get("toContent") or t.get("targetWord") or t.get("translation") or t.get("to") or ""
        if src and tgt:
            lines.append(f"{src} → {tgt}")
    return "\n".join(lines) if lines else "(No book-specific glossary terms)"


# ─── Phase 4: Rephrase with Gemini ───────────────────────────────────────────
def rephrase_with_gemini(rows, glossary_terms, book_name):
    if not GEMINI_API_KEY:
        log("❌ GEMINI_API_KEY not set.")
        return None

    glossary_text = format_glossary_for_prompt(glossary_terms)

    # Build input data: only sort + original (English) + content (German pre-translation)
    input_data = [
        {"sort": r.get("sort", i), "original": r.get("original", ""), "content": r.get("content", "")}
        for i, r in enumerate(rows)
    ]

    prompt = f"""{BASE_PROMPT}

BOOK-SPECIFIC GLOSSARY FOR "{book_name}" (apply these in addition to universal glossary above):
{glossary_text}

ROWS TO REPHRASE ({len(input_data)} rows):
For each row, "original" is the English source and "content" is the German pre-translation to rephrase.
{json.dumps(input_data, ensure_ascii=False)}"""

    log(f"  Sending {len(rows)} rows to Gemini...")

    try:
        resp = requests.post(
            f"{GEMINI_URL}?key={GEMINI_API_KEY}",
            json={
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {
                    "temperature": 0.3,
                    "maxOutputTokens": 32768,
                },
            },
            timeout=300,
        )
        resp.raise_for_status()
        body = resp.json()

        # Extract text from Gemini response
        text = (
            body.get("candidates", [{}])[0]
            .get("content", {})
            .get("parts", [{}])[0]
            .get("text", "")
        )

        if not text:
            log(f"❌ Empty Gemini response: {body}")
            return None

        # Strip any markdown code fences
        text = text.strip()
        if text.startswith("```"):
            text = text.split("\n", 1)[-1]
            text = text.rsplit("```", 1)[0].strip()

        rephrased = json.loads(text)
        log(f"  Gemini returned {len(rephrased)} rows.")
        return rephrased

    except json.JSONDecodeError as e:
        log(f"❌ Gemini JSON parse error: {e}")
        log(f"   Raw response: {text[:500]}")
        return None
    except Exception as e:
        log(f"❌ Gemini error: {e}")
        return None


# ─── Phase 5: Verify ──────────────────────────────────────────────────────────
def verify_output(original_rows, rephrased_rows):
    issues = []

    # Check 1: row count
    if len(rephrased_rows) != len(original_rows):
        issues.append(
            f"Row count mismatch: input={len(original_rows)}, output={len(rephrased_rows)}"
        )

    # Check 2: all sort numbers present
    input_sorts  = {r.get("sort", i) for i, r in enumerate(original_rows)}
    output_sorts = {r.get("sort") for r in rephrased_rows}
    missing = input_sorts - output_sorts
    if missing:
        issues.append(f"Missing sort numbers: {sorted(missing)}")

    # Check 3: no empty rows
    empty = [r.get("sort") for r in rephrased_rows if not r.get("content", "").strip()]
    if empty:
        issues.append(f"Empty content in rows: {empty}")

    # Check 4: no obviously untouched rows (content identical to input)
    unchanged = []
    orig_by_sort = {r.get("sort", i): r.get("content", "") for i, r in enumerate(original_rows)}
    for r in rephrased_rows:
        s = r.get("sort")
        if r.get("content") == orig_by_sort.get(s) and r.get("content", "").strip():
            unchanged.append(s)
    if len(unchanged) > len(original_rows) * 0.3:
        issues.append(
            f"Warning: {len(unchanged)} rows appear unchanged from input "
            f"({len(unchanged)/len(original_rows)*100:.0f}%)"
        )

    # Check 5: sample check for English quotation marks (should be German)
    english_quotes = [
        r.get("sort") for r in rephrased_rows
        if '"' in r.get("content", "") or "'" in r.get("content", "")
    ]
    if len(english_quotes) > 5:
        issues.append(
            f"Warning: {len(english_quotes)} rows may contain English quotation marks"
        )

    return issues


# ─── Phase 6: Submit & Finish ─────────────────────────────────────────────────
def submit_chapter(token, chapter_id, rephrased_rows, original_rows):
    log(f"  Submitting {len(rephrased_rows)} rows to chapter {chapter_id}...")

    # Build original lookup for fields we need to preserve
    orig_by_sort = {r.get("sort", i): r for i, r in enumerate(original_rows)}

    payload = []
    for r in rephrased_rows:
        sort = r.get("sort", 0)
        orig = orig_by_sort.get(sort, {})
        content = r.get("content", "")
        payload.append({
            "sort": sort,
            "original": orig.get("original", ""),
            "content": content,
            "wordCorrection": WORD_CORRECTION_DEFAULT,
            "wordCorrectionData": "",
            "contentShowData": content,   # platform may regenerate display version
        })

    resp = requests.put(
        f"{BASE_URL}/ObjectCatChapter/CreateExeclAsync?chapterId={chapter_id}&status=1",
        headers={**auth_headers(token), "content-type": "application/json;charset=UTF-8"},
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        timeout=60,
    )
    resp.raise_for_status()
    result = resp.json()
    log(f"  Submit response: {result}")
    return result

def finish_chapter(token, chapter_id):
    log(f"  Finishing chapter {chapter_id}...")
    resp = requests.get(
        f"{BASE_URL}/ObjectCatChapter/UpdateForeign?id={chapter_id}&score=0",
        headers=auth_headers(token), timeout=15,
    )
    resp.raise_for_status()
    result = resp.json()
    log(f"  Finish response: {result}")
    return result


# ─── Phase 0: Find already active chapter ────────────────────────────────────
def find_active_chapter(token, books):
    """
    Use TaskCenter/AuthorTaskCenterList to find currently active/claimed chapter.
    Returns (book, chapter_name, proc_id) or None.
    """
    log("  Checking Task Center for active chapter...")

    try:
        resp = requests.post(
            f"{BASE_URL}/TaskCenter/AuthorTaskCenterList",
            headers={**auth_headers(token), "content-type": "application/json;charset=UTF-8"},
            json={"PageIndex": 1, "PageSize": 10,
                  "status": "", "optUsers": "",
                  "taskType": [], "taskTitle": ""},
            timeout=15,
        )
        resp.raise_for_status()
        body = resp.json()
        data = body.get("data", {})
        tasks = (
            data.get("dtolist") or data.get("list") or
            data.get("items") or (data if isinstance(data, list) else [])
        )

        log(f"  Task Center: {len(tasks)} task(s) found")
        if tasks:
            log(f"  Task fields: {list(tasks[0].keys())}")
            for t in tasks:
                log(f"  Task: {t}")

        for task in tasks:
            # Extract chapter ID — this is the proc_id used by CatChapterList/Submit/Finish
            proc_id = (
                task.get("chapterId") or task.get("objectChapterId") or
                task.get("id") or task.get("taskId")
            )
            ch_name = (
                task.get("chapterName") or task.get("taskTitle") or
                task.get("title") or f"Chapter #{proc_id}"
            )
            book_name = (
                task.get("bookName") or task.get("toBookName") or
                task.get("objectBookName") or ""
            )
            book_id = (
                task.get("objectBookId") or task.get("bookId") or
                task.get("objectBook", {}).get("id") if isinstance(task.get("objectBook"), dict) else None
            )

            log(f"  Active task: '{ch_name}' proc_id={proc_id} book='{book_name}'")

            # Find the matching book object from our books list
            matched_book = None
            for b in books:
                b_name = b.get("toBookName") or b.get("bookName") or b.get("name") or ""
                b_id = b.get("id") or b.get("objectBookId") or b.get("bookId")
                if (book_id and str(b_id) == str(book_id)) or (book_name and book_name.lower() in b_name.lower()):
                    matched_book = b
                    break

            if not matched_book and books:
                # Fallback: use first book if we can't match
                log(f"  Could not match book '{book_name}' — using task data directly")
                # Build a minimal book dict from task data
                matched_book = {
                    "id": book_id,
                    "objectBookId": book_id,
                    "bookId": book_id,
                    "toBookName": book_name,
                    "bookName": book_name,
                }

            if proc_id:
                return matched_book, ch_name, proc_id

    except Exception as e:
        log(f"  Task Center error: {e}")

    log("  No active chapter found in Task Center.")
    return None


# ─── Main ─────────────────────────────────────────────────────────────────────
def run():
    token = login()
    books = get_books(token)

    if not books:
        log("No books found.")
        send_telegram("✅ CDReader check ran — no chapters available right now.")
        return

    claimed_chapters = []
    errors = []

    # ── Phase 0: Check for already active/claimed chapter ──
    log("Checking for already active chapter across all books...")
    active = find_active_chapter(token, books)
    if active:
        active_book, active_ch_name, active_proc_id = active
        log(f"Found active chapter: {active_ch_name} (proc_id={active_proc_id})")
        claimed_chapters.append((active_book, active_ch_name, None, "already-claimed"))
    else:
        # ── Phase 1: Claim ──
        for book in books:
            if claimed_chapters:
                break

            book_id   = book.get("id") or book.get("objectBookId") or book.get("bookId")
            book_name = (
                book.get("toBookName") or book.get("bookName") or
                book.get("name") or f"Book #{book_id}"
            )

            if not book_id:
                log(f"Could not find book ID in: {list(book.keys())}")
                continue

            log(f"Checking: {book_name} (ID: {book_id})")
            chapters = get_available_chapters(token, book_id)

            if not chapters:
                log("  No available chapters.")
                continue

            log(f"  {len(chapters)} chapter(s) available!")

            for ch in chapters:
                ch_id   = ch.get("id") or ch.get("chapterId") or ch.get("objectChapterId")
                ch_name = ch.get("chapterName") or ch.get("name") or f"Chapter #{ch_id}"

                if DRY_RUN:
                    log(f"  [DRY RUN] Would claim: {ch_name}")
                    claimed_chapters.append((book, ch_name, ch_id, "dry-run"))
                    break

                result = claim_chapter(token, ch_id)
                success = (
                    result.get("status") is True
                    or result.get("message") == "SaveSuccess"
                    or result.get("code") == "311"
                    or result.get("code") == 0
                )
                no_chapter = result.get("message") in ("NoChapterNumber", "submithint")

                if success:
                    log(f"  ✅ Claimed: {ch_name}")
                    claimed_chapters.append((book, ch_name, ch_id, "claimed"))
                    break
                elif no_chapter:
                    log(f"  ⏭  Not claimable right now: {ch_name}")
                else:
                    log(f"  ⚠️  Unexpected claim response: {result}")

    if not claimed_chapters:
        log("No chapters claimed this run.")
        send_telegram("✅ CDReader check ran — no chapters available right now.")
        return

    # ── Phase 2-6: Process each claimed chapter ──
    book, ch_name, ch_id, status = claimed_chapters[0]
    book_id   = book.get("id") or book.get("objectBookId") or book.get("bookId")
    book_name = book.get("toBookName") or book.get("bookName") or book.get("name") or ""

    if status == "dry-run":
        send_telegram(f"[DRY RUN] Would process: <b>{book_name}</b>: {ch_name}")
        return

    log(f"\n── Processing: {book_name} / {ch_name} ──")

    # Resolve processing ID
    if status == "already-claimed":
        # Re-run active chapter detection on this specific book to get proc_id
        active = find_active_chapter(token, [book])
        if active:
            _, ch_name, proc_id = active
            log(f"  Active chapter proc_id resolved: {proc_id}")
        else:
            # Fallback: search by name
            proc_id, _ = find_chapter_processing_id(token, book, ch_name)
        if not proc_id:
            msg = f"⚠️ Could not resolve processing ID for active chapter {ch_name}. Manual action required."
            send_telegram(msg)
            return
    else:
        # Freshly claimed — find internal processing ID by name
        proc_id, _ = find_chapter_processing_id(token, book, ch_name)
        if not proc_id:
            msg = (
                f"⚠️ <b>CDReader:</b> Claimed <b>{ch_name}</b> from {book_name} "
                f"but could not find processing ID.\nManual action required."
            )
            send_telegram(msg)
            log("Could not find processing chapter ID — stopping.")
            return

    # Start chapter (unlock for editing)
    start_chapter(token, proc_id)
    time.sleep(2)

    # Fetch rows
    rows = get_chapter_rows(token, proc_id)
    if not rows:
        msg = f"⚠️ <b>CDReader:</b> No rows fetched for {ch_name}. Manual action required."
        send_telegram(msg)
        return

    # Fetch glossary
    glossary = get_glossary(token, book_id)

    # Rephrase with Gemini
    log(f"  Rephrasing {len(rows)} rows with Gemini...")
    rephrased = rephrase_with_gemini(rows, glossary, book_name)

    if not rephrased:
        msg = (
            f"❌ <b>CDReader:</b> Gemini rephrasing failed for {ch_name}.\n"
            f"Manual action required."
        )
        send_telegram(msg)
        return

    # Verify output
    log("  Verifying output...")
    issues = verify_output(rows, rephrased)

    if issues:
        issue_text = "\n".join(f"• {i}" for i in issues)
        msg = (
            f"⚠️ <b>CDReader: Review needed</b>\n\n"
            f"Book: {book_name}\nChapter: {ch_name}\n\n"
            f"Verification issues:\n{issue_text}\n\n"
            f"Please review and submit manually."
        )
        send_telegram(msg)
        log(f"Verification failed — {len(issues)} issue(s) found. Stopping for human review.")
        for i in issues:
            log(f"  Issue: {i}")
        return

    log(f"  ✅ Verification passed.")

    # Submit
    if DRY_RUN:
        log("  [DRY RUN] Skipping submit and finish.")
        send_telegram(f"[DRY RUN] Rephrasing verified OK for <b>{ch_name}</b>")
        return

    submit_result = submit_chapter(token, proc_id, rephrased, rows)
    submit_ok = (
        submit_result.get("status") is True
        or submit_result.get("message") in ("SaveSuccess", "OperSuccess")
        or submit_result.get("code") in ("311", "315", 0)
    )

    if not submit_ok:
        msg = (
            f"❌ <b>CDReader: Submit failed</b>\n"
            f"Chapter: {ch_name}\nResponse: {submit_result}"
        )
        send_telegram(msg)
        return

    time.sleep(2)

    # Finish
    finish_result = finish_chapter(token, proc_id)

    # Notify success
    send_telegram(
        f"✅ <b>CDReader: Chapter complete!</b>\n\n"
        f"📖 {book_name}\n"
        f"📄 {ch_name}\n\n"
        f"Rephrased, submitted and finished automatically."
    )
    log("✅ Pipeline complete.")


if __name__ == "__main__":
    run()
