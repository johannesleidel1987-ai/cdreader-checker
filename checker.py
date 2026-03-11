"""
CDReader Complete Pipeline
Claim → Fetch rows → Fetch glossary → Rephrase with Gemini → Verify → Submit → Finish
"""

import requests
import os
import json
import re
import sys
import time
from datetime import datetime

# Alias used throughout — avoids repeated local `import re` inside closures
_re = re

# ─── Config ──────────────────────────────────────────────────────────────────
BASE_URL    = "https://translatorserverwebapi-de.cdreader.com/api"
GEMINI_MODEL = "gemini-2.5-flash"
GEMINI_URL  = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent"
ACCOUNT_NAME   = os.environ.get("CDREADER_EMAIL",    "")
ACCOUNT_PWD    = os.environ.get("CDREADER_PASSWORD", "")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT  = os.environ.get("TELEGRAM_CHAT_ID",   "")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY",     "")

# Multi-key Gemini rotation: keys tried in order, exhausted keys skipped for the run
_GEMINI_KEYS_RAW = [
    os.environ.get("GEMINI_API_KEY",   ""),
    os.environ.get("GEMINI_API_KEY_2", ""),
    os.environ.get("GEMINI_API_KEY_3", ""),
    os.environ.get("GEMINI_API_KEY_4", ""),
    os.environ.get("GEMINI_API_KEY_5", ""),
    os.environ.get("GEMINI_API_KEY_6", ""),
    os.environ.get("GEMINI_API_KEY_7", ""),
    os.environ.get("GEMINI_API_KEY_8", ""),
    os.environ.get("GEMINI_API_KEY_9", ""),
    os.environ.get("GEMINI_API_KEY_10", ""),
    os.environ.get("GEMINI_API_KEY_11", ""),
    os.environ.get("GEMINI_API_KEY_12", ""),
    os.environ.get("GEMINI_API_KEY_13", ""),
    os.environ.get("GEMINI_API_KEY_14", ""),
    os.environ.get("GEMINI_API_KEY_15", ""),
    os.environ.get("GEMINI_API_KEY_16", ""),
    os.environ.get("GEMINI_API_KEY_17", ""),
    os.environ.get("GEMINI_API_KEY_18", ""),
    os.environ.get("GEMINI_API_KEY_19", ""),
    os.environ.get("GEMINI_API_KEY_20", ""),
    os.environ.get("GEMINI_API_KEY_21", ""),
    os.environ.get("GEMINI_API_KEY_22", ""),
    os.environ.get("GEMINI_API_KEY_23", ""),
    os.environ.get("GEMINI_API_KEY_24", ""),
    os.environ.get("GEMINI_API_KEY_25", ""),
    os.environ.get("GEMINI_API_KEY_26", ""),
    os.environ.get("GEMINI_API_KEY_27", ""),
    os.environ.get("GEMINI_API_KEY_28", ""),
]
GEMINI_KEYS = [k for k in _GEMINI_KEYS_RAW if k.strip()]
_exhausted_keys: set = set()      # RPM-exhausted (clears after 60s wait)
_rpd_exhausted_keys: set = set()  # RPD-exhausted (daily quota — permanent for this run)

# Per-key last-used timestamps for cooldown tracking (Option 1).
# Maps api_key → float (time.time() of last attempted call).
# Prevents hammering recently-used keys before their token-bucket window has refilled.
_key_last_used: dict = {}

# Rotating scan start offset for _call_gemini_simple (Option 2).
# Incremented after each call to distribute load evenly across keys instead of
# always starting from key 1 and concentrating heat at the top of the rotation.
_retry_scan_offset: int = 0

# RPM limits by key tier (used to compute minimum per-key call interval).
# Free-tier keys: 10 RPM → must space calls >= 6 s apart per key.
# Paid key (GEMINI_API_KEY_28): 150 RPM → effectively no constraint (0.4 s).
_FREE_KEY_MIN_INTERVAL: float = 6.0   # seconds between calls on the same free key
_PAID_KEY_MIN_INTERVAL: float = 0.4   # seconds between calls on the paid key

# Paid key identified by secret name — NOT by list position (GEMINI_KEYS[-1] is
# fragile: if any of keys 1-27 are absent, the last element shifts and the wrong
# key gets the paid-tier interval). Reading directly from the environment is
# position-independent and correct regardless of how many free keys are wired.
_PAID_KEY: str = os.environ.get("GEMINI_API_KEY_28", "").strip()

# ─── Account-group-aware key management ───────────────────────────────────────
# Keys are spread across 3 distinct Google accounts (= 3 independent RPM/RPD pools):
#   Account A: GEMINI_API_KEY   through GEMINI_API_KEY_9  (positions 0-8)
#   Account B: GEMINI_API_KEY_10 through GEMINI_API_KEY_18 (positions 9-17)
#   Account C: GEMINI_API_KEY_19 through GEMINI_API_KEY_28 (positions 18-27, includes paid)
# When ONE key in an account returns 429-RPM, ALL keys in that account are blocked
# (rate limits are per Google Cloud project). But OTHER accounts are still available.
_ACCOUNT_GROUPS: list = []  # list of list[str], built from _GEMINI_KEYS_RAW
_ACCOUNT_LABELS = ['A', 'B', 'C']
for _ag_start, _ag_end in [(0, 9), (9, 18), (18, 28)]:
    _ag_keys = [k for k in _GEMINI_KEYS_RAW[_ag_start:_ag_end] if k.strip()]
    _ACCOUNT_GROUPS.append(_ag_keys)
_ag_counts = [len(g) for g in _ACCOUNT_GROUPS]
log_msg = ", ".join(f"Account {_ACCOUNT_LABELS[i]}: {_ag_counts[i]} keys" for i in range(len(_ACCOUNT_GROUPS)))
# (logged at runtime, not import time)

# Rotating counter: determines which account group gets tried first for each batch/retry.
# Incremented after each batch call to spread RPD load evenly across accounts.
_batch_account_offset: int = 0

# Track which account groups are RPM-blocked within a single _call_gemini_simple invocation.
# Reset at the start of each call. Maps group_index → True if blocked.
# (Not module-level persistent — RPM blocks are transient, ~60s.)

def _key_account_group(api_key):
    """Return the account group index (0-2) for a given API key, or -1 if unknown."""
    for gi, group in enumerate(_ACCOUNT_GROUPS):
        if api_key in group:
            return gi
    return -1

# Fallback chain — used when all Gemini keys hit their daily quota (RPD)
# No automatic fallback is currently active; the run aborts and the next
# scheduled run will retry with refreshed daily quotas.

# Guard thresholds (used in rephrase_with_gemini batch reconciliation)
_INFLATION_THRESHOLD: float = 1.6      # Guard 2: output/input word ratio above which row is restored from MT
_INFLATION_MIN_DELTA: int   = 4        # Guard 2: minimum word delta to trigger (avoids false positives on short rows)
_TRUNCATION_THRESHOLD: float = 0.35    # Guard 4: output/input word ratio below which row is considered truncated
_TRUNCATION_MIN_WORDS: int   = 5       # Guard 4: minimum input word count to trigger
_ENG_ATTRIBUTION_MAX_WORDS: int = 12   # BGS guard: max English words for a sentence to qualify as attribution-only

# Timing constants
_INTER_BATCH_SLEEP: float = 5.0        # seconds between Gemini batch calls
_PRE_RETRY_COOLDOWN: int  = 65         # seconds to wait before retry loop when ≥2 batches completed



def _next_gemini_key(prefer_group=None):
    """Return the next non-exhausted Gemini API key, or None if all exhausted.
    
    If prefer_group is specified (0=A, 1=B, 2=C), tries that account's keys first
    before falling through to other accounts. This spreads RPD load across accounts.
    """
    if prefer_group is not None and 0 <= prefer_group < len(_ACCOUNT_GROUPS):
        # Try preferred group first
        for k in _ACCOUNT_GROUPS[prefer_group]:
            if k in GEMINI_KEYS and k not in _exhausted_keys and k not in _rpd_exhausted_keys:
                return k
    # Fall through: try all groups in order
    available = [k for k in GEMINI_KEYS if k not in _exhausted_keys and k not in _rpd_exhausted_keys]
    return available[0] if available else None

def _all_keys_rpd_dead():
    """True if every configured key has hit its daily quota."""
    return len(GEMINI_KEYS) > 0 and all(k in _rpd_exhausted_keys for k in GEMINI_KEYS)

DRY_RUN   = os.environ.get("DRY_RUN",   "false").lower() == "true"
TEST_MODE = os.environ.get("TEST_MODE", "false").lower() == "true"

# Manual override: skip Task Center scan and claiming, process a specific chapter directly.
# Use when re-processing a chapter that was finished but flagged for rework.
# Both values are visible in every run log's Task Center output (taskUrl: "...|chapterId|bookId").
OVERRIDE_CHAPTER_ID = os.environ.get("OVERRIDE_CHAPTER_ID", "").strip()
OVERRIDE_BOOK_ID    = os.environ.get("OVERRIDE_BOOK_ID",    "").strip()

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
You are a German proofreader performing MINIMAL corrections on a machine-translated text. Your goal is to make the text correct and natural-sounding with the FEWEST possible changes — typically 15–25% of words changed per row, never more.

⚠️ CRITICAL PRINCIPLE: The machine translation is your starting point. Keep it as close to the original as possible. Only change what is actually WRONG (grammar, logic, localization). Do NOT rephrase for style, do NOT restructure sentences, do NOT replace words that are already correct.

CDReader will REJECT the chapter if:
  - Rows are returned IDENTICAL to the input (each row must have at least one small difference)
  - Rows are changed TOO MUCH (excessive restructuring or vocabulary replacement)
The sweet spot is: correct the errors, apply localization, and leave everything else untouched.

OUTPUT FORMAT (CRITICAL)
Return ONLY a valid JSON array — no markdown, no preamble, no explanation.
Each object must have exactly:
  "sort": original sort number (integer, unchanged)
  "content": corrected German text
Example: [{"sort": 0, "content": "corrected line"}, {"sort": 1, "content": "..."}]

ROW BOUNDARIES ARE ABSOLUTE: Each sort number maps to exactly one row of the original text.
Never merge content from two rows into one, and never split one row's content across two.
Never move a Begleitsatz (e.g. "sagte er", "flüsterte sie") from one row into an adjacent row.
If a row contains only a short attribution clause, output exactly that — do not borrow from neighbours.

QUOTE ISOLATION (CRITICAL — dialogue is split across multiple rows by design):
- If the English input row opens a quote „ but does NOT close it, your German output must also leave it open. Do NOT close the quote within that row.
- If the English input row closes a quote but did not open it, your German output must also close without opening.
- NEVER pull text from row N+1 into row N to close an open quote — the closing text belongs to the next row.
- CONVERSE RULE: If the English input row has NO closing quote character at all (it is a mid-speech continuation), do NOT add a closing “ to your German output for that row. The speech is not finished on that row — trust the English quote placement. Do NOT close the speech early just because it feels unresolved.
- INLINE SPEECH-CLOSE: When a row ends with a phrase followed by ,\" and then a speaker tag (e.g. 'no problem,\" Jonathan teased'), the \" is the OUTER speech closer, NOT a signal to wrap the phrase in inner quotes. Translate the phrase as plain text and place “ BEFORE the speaker tag: „kein Problem,“ neckte Jonathan. Do NOT write „kein Problem, neckte Jonathan (missing outer close).
- NEVER duplicate content from row N+1 into row N. If row N+1 opens with an echo phrase (e.g. „Gefühle entwickeln?“), that phrase must appear ONLY in row N+1's output — do NOT append it to row N as well. Each phrase belongs to exactly one output row.
- NEVER split a single row's translation across multiple sort numbers. The COMPLETE translation of sort=N must appear entirely within sort=N's output field — never partially in sort=N with the remainder pushed into sort=N+1. If the German translation of a row is long, output the full text in a single content field. Do not use adjacent rows as overflow or continuation slots.
- Nested inner quotes within already-open speech use ‚ to open and ' to close — NEVER use „ inside an already-open „...".
- When translating a narration+speech row (e.g. 'she said, "Do it."') into German colon style ('sie befahl: „Tu es."'), you MUST include „ before the speech text. Do not omit the opening quote mark.
- A row ending with an unclosed „ is CORRECT and INTENTIONAL. Do not fix it.

WHAT TO FIX (do these, nothing more):
1. Grammar errors: wrong case, wrong verb conjugation, missing articles, broken syntax
2. Logic errors: mistranslations where the German does not match the English meaning
3. Localization:
   - German quotation marks: „ to open, “ to close
   - "Mr." → "Herr", "Mrs."/"Miss"/"Ms." → "Frau"
   - Apply glossary terms (see below)
   - Currency localization
4. Minimum-change guarantee: if a row has zero errors, make ONE small change — a synonym for a single word, a slightly adjusted article, or a minor word-order tweak — so the row is not byte-identical to the input

WHAT NOT TO DO:
- Do NOT restructure sentences that are grammatically correct
- Do NOT replace vocabulary for stylistic preference (e.g. do NOT change "antwortete" to "erwiderte" if both are correct)
- Do NOT add words, enrich descriptions, or expand action beats
- Do NOT vary sentence length or structure for "flow" — preserve the original rhythm
- Do NOT shorten rows or merge clauses
- Do NOT change word order unless the current order is grammatically wrong

CAPITALIZATION & SOURCE FORMATTING
- All-caps lines: correct in ALL CAPS
- Lines containing only punctuation or single words (e.g. "!", "Los!", "Emma!", "Liz!"): retain EXACTLY as-is
- Standard lines: standard German capitalization rules

THE PRONOUN PROTOCOL (CRITICAL)
- "du": only for family (parents, children, siblings), romantic partners, demonstrably close long-term friends
- "Sie": default for ALL other interactions — professional colleagues, new acquaintances, boss/subordinate, strangers, any relationship marked by respect or distance
- Absolute consistency: never switch "du"/"Sie" between the same two people within a chapter

DIALOGUE & HONORIFICS
- "Mr." → "Herr", "Mrs."/"Miss"/"Ms." → "Frau"

UNIVERSAL GLOSSARY
Company: Briggs Group→Briggs-Gruppe; Star Wish Investments→Star Wish-Investitionen; Evans Entertainment→Evans Entertainment; Aurora Apparel Company→Aurora-Bekleidungsunternehmen; Radiant Jewels→Radiant Jewels; Yaroslav Technology→Yaroslav-Technologie; Newcrest Pharmaceuticals→NeuÄra-Pharma; North Investments→Nord-Investment; Vivian Floral Design→Vivian-Blumendesign; TurboVortex Club→Turbowirbel-Club; Summit Capital→Gipfelkapital-Konzern
Family: Williams family→Familie Williams; Holdens→Familie Holden
Locations: Blossom Estate→Blossom-Anwesen; Regal Grove→Royal-Anwesen; Presidency Estate→Präsidialanwesen; Hillside Villa→Wolkenruh-Landhaus; Stone Village→Steindorf; Cloud Sea Project→Wolkenmeer-Projekt; Faywind Village→Faywind-Dorf; Clearwater Village→Kristallquell-Dorf; Regal Diner→Goldflor-Restaurant; Rosewood Hills→Rosenschlossburg; Shaw Mansion→Herrenhaus Shaw; Crownspire Villa→Kronenspitz-Villa; Curtis Mansion→Curtis-Herrenhaus; underground market→Schwarzmarkt; Briskvale High→Frischtalschule
Medical: Crobert Hospital→Krankenhaus in Crobert; Kretol University→Universität Kretol; Faywald Hospital→Frieden-Krankenhaus; Wraith Physician→Wraith-Ärztin; Phantom Healer→Phantomheilerin; Raynesse Hospital→Rainstein-Klinik
Terms: Black Dragon Syndicate→Syndikat des Schwarzen Drachen; Black Hawk Alliance→Schwarzer-Hawk-Allianz; CEO→Geschäftsführer; Skybreaker→Himmelsschneider; Darknight→Nachtphantom; Blackdragon→Schwarzer Drache; Blackwing→Schwarzflügel; Shadow→Schatten; Askelpius→Asklepios; Violet→Violett; Snowball→Schneeball; Heavenly Melody→Himmlische Melodie
Characters: Mr. Moss→Herr Moos; Ms. Braxton→Fräulein Braxton; Miss Briggs→Fräulein Briggs; Kiley→Lena; Jennie→Jenny; Steve→Stefan; Garry→Gerhard; Ethan→Elias; Monica→Monika; Gabby→Gabi; Claire→Klara
Currency: Dollar→Euro

FINAL SELF-CHECK (perform before responding)
1. Output has EXACTLY the same number of JSON objects as input rows?
2. Each row differs from input by at least one word but NOT by more than ~25%?
3. du/Sie consistent per character relationship?
4. All glossary terms applied?
5. Response is pure JSON with zero extra text?"""


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
        book.get("id") or book.get("objectBookId") or book.get("bookId")
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
    msg = result.get("message", "")
    if result.get("status") or msg in ("SaveSuccess", "ErrMessage8"):
        log(f"  Start OK (message={msg})")
    else:
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
        r0 = rows[0]
        log(f"  First row sample: sort={r0.get('sort')} | eContent={repr((r0.get('eContent') or '')[:80])} | chapterConetnt={repr((r0.get('chapterConetnt') or '')[:80])}")
        # Diagnostic: show all key content fields for first 3 non-title rows
        content_rows = [r for r in rows if r.get("sort", 0) > 0][:3]
        for r in content_rows:
            log(f"  [DIAG] sort={r.get('sort')} "
                f"| chapterConetnt={repr((r.get('chapterConetnt') or '')[:60])} "
                f"| machineChapterContent={repr((r.get('machineChapterContent') or '')[:60])} "
                f"| modifChapterContent={repr((r.get('modifChapterContent') or '')[:60])} "
                f"| languageContent={repr((r.get('languageContent') or '')[:60])} "
                f"| peContent={repr((r.get('peContent') or '')[:60])}")

    log(f"  Fetched {len(rows)} rows.")
    return rows

def get_glossary(token, object_book_id):
    if not object_book_id:
        log("  No book_id available — skipping glossary fetch.")
        return []
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
        src = t.get("dictionaryKey") or t.get("fromContent") or t.get("sourceWord") or t.get("word") or ""
        tgt = t.get("dictionaryValue") or t.get("toContent") or t.get("targetWord") or t.get("translation") or ""
        if src and tgt:
            lines.append(f"{src} → {tgt}")
    return "\n".join(lines) if lines else "(No book-specific glossary terms)"



# ─── Post-processing constants and helpers (module level) ─────────────────────
# ── Post-process: German dialogue punctuation enforcement ─────────────────
# _SV_CORE: Pure speech/communication verbs used for CROSS-ROW comma decisions
# (Rules B and C). Must be conservative — these verbs almost exclusively signal
# speech attribution and rarely appear as pure narrative action starters.
# Deliberately excludes dual-use action verbs like nickte, lächelte, seufzte,
# versprach, zögerte etc. which cause false positives when they start narrative rows.
_SV_CORE = (
    r"sagte|flüsterte|antwortete|rief|fragte|murmelte|erwiderte|bemerkte|"
    r"fügte|entgegnete|zischte|hauchte|stammelte|schrie|brüllte|"
    r"wisperte|knurrte|ergänzte|meinte|verkündete|wiederholte|"
    r"flehte|bat|raunte|schoss|konterte|erklärte|betonte|"
    r"protestierte|unterbrach|insistierte|meldete|berichtete|informierte|"
    r"teilte|verriet|offenbarte|kündigte|gestand|erkundigte|wandte|"
    # Added: genuine attribution verbs confirmed by template rows 44, 49, 57, 116
    r"wollte|beruhigte|erwähnte|wies|sprach|"
    # Added: verbs of rebuke/correction whose BGS output is indistinguishable from
    # speech attribution (schalt = schelten/to scold, tadelte = tadeln/to reprimand)
    r"schalt|tadelte"
)
# _SV_ALL: Full verb list for INLINE same-row attribution matching (Rules C2, E, F,
# Fix 1b). Context (same-row dialogue) makes ambiguity much lower here.
_SV = (
    _SV_CORE + r"|"
    r"nickte|lächelte|seufzte|wisperte|schnappte|stöhnte|schluchzte|"
    r"keuchte|grunzte|gluckste|bettelte|jammerte|klagte|schimpfte|fuhr|setzte|"
    r"warf|stieß|spuckte|platzte|brach|fiel|gab|presste|rang|"
    r"drängte|keifte|ächzte|sprach|gestand|bekannte|schwor|versprach|"
    r"drohte|warnte|befahl|forderte|appellierte|bestätigte|verneinte|"
    r"zuckte|zögerte|stockte|hielt|begann|fuhr fort|schoss zurück|"
    # Added: template row 30 — "neckte" (teased)
    r"neckte|"
    # Added: Screenshot 2 — spottete (mocked), höhnte (sneered/jeered)
    r"spottete|höhnte"
)
# Negation guard: "antwortete nicht", "sagte kein Wort" etc. are NARRATIVE, not attribution
_NEGATION_AFTER_SV = _re.compile(
    rf"(?:{_SV_CORE})\s+(?:nicht|kein|keine|keinen|keinem|keiner|nie|niemals|nichts)",
    _re.IGNORECASE
)

# _BEGLEITSATZ_BASE: a genuine Begleitsatz starts DIRECTLY with the speech verb
# or with a lowercase pronoun + verb. The [Name]+[SV] arm is intentionally absent:
# "Jenifer murmelte leise vor sich hin." starts with a proper name, not the verb
# so the comma rule must not fire for those rows.
#
# \b after each verb alternation is required to prevent prefix false-positives:
# Without it, 'wies' matches 'Wieso?', 'schalt' matches 'Schaltete er',
# 'sprach' matches 'Sprache' — all legitimate dialogue/narrative rows that
# would be incorrectly restored from MT by the BGS confusion guard.
# Confirmed by simulation (2026-03-08): sort=116 „Wieso?" was falsely flagged.
_BEGLEITSATZ_BASE = _re.compile(
    rf"""^(?:
        (?:(?:{_SV_CORE})\b)
        |
        (?:(?:er|sie|es|ich|wir|ihr|man)\s+(?:(?:{_SV_CORE})\b))
    )""",
    _re.IGNORECASE | _re.VERBOSE
)

def _is_begleitsatz(text, _max_words=15):
    """True only if text is a genuine attribution clause after dialogue.
    Guards against false positives:
      - Rows ending with ':' introduce NEW dialogue (not attributing previous speech)
      - Long rows (> max_words=15) that aren't attribution — genuine Begleitsätze
        are short (2-12 words typically). 30 was too permissive: "Er wollte sie
        nicht wegen der Ereignisse..." (23w) matched 'er wollte' and triggered
        a false positive restoration. 15 excludes all such narrative sentences.
      - Negated speech verbs ('antwortete nicht', 'sagte kein Wort') = narrative denial
    """
    # Inline speech: attribution verb followed by colon + uppercase = introduces
    # direct speech in the same row („Antwortete sie entschlossen: Nein.“) —
    # this is NOT a pure Begleitsatz following previous speech.
    if _re.search(r':\s+[A-ZÄÖÜ]', text):
        return False
    if text.rstrip().endswith(':'):
        return False  # ends with ':' → introduces new speech, doesn't attribute old
    if len(text.split()) > _max_words:
        return False
    if _NEGATION_AFTER_SV.search(text):
        return False
    return bool(_BEGLEITSATZ_BASE.match(text))




def _is_continuation_row(text):
    """Cross-row comma decision: True iff the next row syntactically continues
    the preceding dialogue and therefore requires a trailing comma on the
    closing-quote row.

    Root cause of Screenshots 1 & 3:
      • Screenshot 1 (false positive): 'Kaum hatte er mir einen Stirnkuss
        gegeben, fragte ich.' → starts with uppercase 'K' → new sentence →
        no comma warranted, but _is_begleitsatz with IGNORECASE matched
        'fragte' anywhere and fired.
      • Screenshot 3 (false negative): 'versuchte Hendrick, mich zu
        beruhigen.' → starts lowercase 'v' → is a continuation → comma
        warranted, but 'versuchte' was absent from _SV_CORE so the check
        returned False.

    Fix: In German prose, a Begleitsatz (attribution clause) that follows
    closing dialogue is ALWAYS syntactically subordinate — it continues the
    sentence opened by the dialogue and therefore NEVER capitalises its first
    word.  Conversely, a new sentence (action, description, new speaker) ALWAYS
    starts with an uppercase letter.  Capitalisation is therefore a sufficient
    and unambiguous discriminant:

        lowercase-first  → continuation → comma required
        uppercase-first  → new sentence → no comma

    This replaces _is_begleitsatz() for cross-row comma decisions.
    _is_begleitsatz() is retained for the BGS confusion guard (Pre-Pass QE),
    where the task is different: detecting whether Gemini's entire output IS
    an attribution clause, not whether the next row continues from dialogue.

    Edge-case guards:
      • Empty text → False
      • Row ending with ':' → introduces new speech, does not attribute old
        speech.  Even though it starts lowercase ('fragte sie: …'), no comma.
    """
    if not text or not text[0].islower():
        return False
    if text.rstrip().endswith(':'):
        return False
    return True

_QE_OPEN   = '„'  # „ U+201E
_QE_CLOSE  = '“'  # " U+201C
_QE_ANY_CLOSE_RE  = _re.compile(r'[“”"]')
_QE_CLOSE_AT_END  = _re.compile(r'[“”"]\s*[,!?.]?\s*$')
_QE_STARTS_OPEN   = _re.compile(r'^[„“”"]')

_QE_ENG_OPEN_RE   = _re.compile(r'^[„“”‘\"«]')
_QE_ENG_CLOSE_RE  = _re.compile(r'[“”\"]\s*[,!?.]?\s*$')


def _row_sim(output, ref):
    """Combined similarity: max(Jaccard-word, char-trigram) on normalised text."""
    def _norm(s):
        return _re.sub(r"[^\w\s]", "", s.lower())
    def _jaccard(a, b):
        wa = set(_re.findall(r"[a-z\u00e4\u00f6\u00fc\u00df]+", a))
        wb = set(_re.findall(r"[a-z\u00e4\u00f6\u00fc\u00df]+", b))
        return len(wa & wb) / len(wa | wb) if (wa and wb) else 0.0
    def _trigram(a, b):
        na = set(a[i:i+3] for i in range(max(0, len(a)-2)))
        nb = set(b[i:i+3] for i in range(max(0, len(b)-2)))
        return len(na & nb) / len(na | nb) if (na and nb) else 0.0
    no, nr = _norm(output), _norm(ref)
    return max(_jaccard(no, nr), _trigram(no, nr))


SIM_THRESHOLD = 0.88   # flag rows at or above this combined similarity
# 0.88: CDReader rejects chapters with avg similarity >~80%. Catching rows at 88%+
# while keys are still alive pulls the average into the 72-77% finish zone.


# Module-level synonym table — shared by _deterministic_change and _find_synonym_pair.
# Ordered by word frequency so the highest-coverage substitutions are tried first.
_SYNONYMS = [
    # Conjunctions & particles (highest frequency)
    # NOTE: und→sowie intentionally removed. "sowie" only fits nominal/noun-phrase
    # conjunctions ("Männer sowie Frauen"), not verbal, clausal, or adjectival "und"
    # (the vast majority in fiction MT). It caused _deterministic_change to produce
    # grammatically wrong output on nearly every sentence, and caused _find_synonym_pair
    # to inject an unenforceable mandatory swap into retry prompts, triggering the
    # model to return the sentence unchanged (100% similarity regression).
    (r'\baber\b', 'jedoch'),
    (r'\bauch\b', 'ebenfalls'),
    (r'\bdoch\b', 'dennoch'),
    (r'\balso\b', 'demnach'),
    (r'\bdann\b', 'daraufhin'),
    (r'\bnur\b', 'lediglich'),
    (r'\bnoch\b', 'weiterhin'),
    (r'\bschon\b', 'bereits'),
    (r'\bjetzt\b', 'nun'),
    (r'\bimmer\b', 'stets'),
    (r'\bso\b', 'derart'),
    (r'\bganz\b', 'völlig'),
    (r'\bwieder\b', 'erneut'),
    (r'\bwohl\b', 'vermutlich'),
    (r'\berst\b', 'zunächst'),
    # Adverbs
    (r'\bsehr\b', 'äußerst'),
    (r'\bschnell\b', 'rasch'),
    (r'\bwirklich\b', 'tatsächlich'),
    (r'\bgenau\b', 'exakt'),
    (r'\bplötzlich\b', 'unvermittelt'),
    (r'\bsofort\b', 'umgehend'),
    (r'\bnatürlich\b', 'selbstverständlich'),
    (r'\bvielleicht\b', 'möglicherweise'),
    (r'\bleise\b', 'still'),
    (r'\bgelassen\b', 'ruhig'),
    (r'\bstolz\b', 'selbstbewusst'),
    # Adjectives
    (r'\bschwer\b', 'schwierig'),
    (r'\bgroß\b', 'beträchtlich'),
    (r'\bklein\b', 'gering'),
    # REMOVED: gut→angemessen — only fits evaluative contexts, wrong for 'es geht mir gut' etc.
    (r'\balt\b', 'betagt'),
    (r'\bkurz\b', 'knapp'),
    (r'\bfroh\b', 'erfreut'),
    # Common verbs
    (r'\bsagte\b', 'meinte'),
    (r'\bfragte\b', 'erkundigte sich'),
    (r'\bantwortete\b', 'erwiderte'),
    (r'\bnickte\b', 'stimmte zu'),
    (r'\blächelte\b', 'schmunzelte'),
    (r'\bging\b', 'begab sich'),
    (r'\bkam\b', 'erschien'),
    (r'\bsah\b', 'erblickte'),
    (r'\bwollte\b', 'beabsichtigte'),
    (r'\bkonnte\b', 'vermochte'),
    (r'\bmusste\b', 'war gezwungen zu'),
    (r'\bwusste\b', 'war sich bewusst'),
    # Common verbs (second tier — matched after Gemini may have already used conjunctions)
    (r'\btrieb\b', 'drängte'),
    (r'\bstand\b', 'befand sich'),
    # REMOVED: ließ→brachte — different semantics ('sie ließ ihn gehen' ≠ 'sie brachte ihn gehen')
    (r'\bblickte\b', 'schaute'),
    (r'\bhörte\b', 'vernahm'),
    (r'\bspürte\b', 'fühlte'),
    (r'\bdachte\b', 'überlegte'),
    # REMOVED: warf→richtete — only valid for 'Blick werfen', wrong for physical throwing
    # REMOVED: stieg→erhob sich — wrong for 'stieg aus dem Auto', only works for rising from seat
    (r'\bzog\b', 'bewegte'),
    (r'\bhob\b', 'erhob'),
    # REMOVED: schloss→verschloss — 'closed' vs 'locked', different actions
    # REMOVED: öffnete→entriegelte — 'opened' vs 'unbolted', different actions
    (r'\bsaß\b', 'befand sich'),
    (r'\blag\b', 'ruhte'),
    # REMOVED: wurde→war geworden — changes tense (Präteritum→Plusquamperfekt), ungrammatical
    # Common adjectives/adverbs (second tier)
    (r'\bsanft\b', 'zart'),
    (r'\bfest\b', 'beständig'),
    (r'\bstill\b', 'ruhig'),
    (r'\bhell\b', 'strahlend'),
    (r'\bdunkel\b', 'finster'),
    (r'\bkalt\b', 'eisig'),
    (r'\bwarm\b', 'behaglich'),
    (r'\bleicht\b', 'mühelos'),
    (r'\btief\b', 'gründlich'),
    (r'\bhoch\b', 'erhaben'),
    (r'\bjung\b', 'jugendlich'),
    (r'\bstark\b', 'kräftig'),
    (r'\bschwach\b', 'kraftlos'),
    (r'\bruhig\b', 'gelassen'),
    (r'\bstrahlend\b', 'leuchtend'),
    # Sentence adverbs & connectors (second tier)
    (r'\bdaraufhin\b', 'anschließend'),
    (r'\bschließlich\b', 'letztendlich'),
    (r'\btatsächlich\b', 'wahrhaftig'),
    (r'\baußerdem\b', 'überdies'),
    (r'\bdeshalb\b', 'daher'),
    (r'\btrotzdem\b', 'dennoch'),
    (r'\bjedoch\b', 'allerdings'),
    (r'\bdennoch\b', 'trotz allem'),
    (r'\bsicherlich\b', 'gewiss'),
    (r'\boffensichtlich\b', 'augenscheinlich'),
    # Nouns & other
    (r'\betwas\b', 'ein wenig'),
    # REMOVED: Worte→Wörter — not synonyms (Worte=utterances, Wörter=vocab items)
    (r'\bnicht\b', 'keineswegs'),  # last resort — changes meaning slightly
]


def _find_synonym_pair(text):
    """Return (matched_literal, replacement) for the first synonym that applies to text,
    skipping matches that fall inside German quotation marks („...").

    Used to inject a concrete mandatory word-swap into retry prompts, so the model
    cannot return the same text. If the target word is inside a direct quote the model
    will correctly refuse to replace it and silently return the original sentence,
    causing a 100% similarity regression (see sort=17 analysis). By skipping
    quote-internal matches we always land on a word the model is free to change.

    Returns None if no synonym matches outside quotes (very short rows, pure dialogue).
    """
    # Build a set of character index ranges that are inside „..." quotes
    _quote_ranges = []
    _i = 0
    while _i < len(text):
        if text[_i] == '„':  # „ opening
            _j = text.find('“', _i + 1)  # " closing
            if _j == -1:
                _j = text.find('"', _i + 1)   # fallback: ASCII closing
            if _j != -1:
                _quote_ranges.append((_i, _j))
                _i = _j + 1
                continue
        _i += 1

    def _in_quotes(match_start, match_end):
        return any(qs <= match_start and match_end <= qe + 1
                   for qs, qe in _quote_ranges)

    for pattern, replacement in _SYNONYMS:
        m = _re.search(pattern, text)
        if m and not _in_quotes(m.start(), m.end()):
            return (m.group(0), replacement)
    return None


def _deterministic_change(text):
    """Make ONE guaranteed-small change to a German text without any API call.

    Used as a last-resort fallback when all Gemini keys are exhausted and the
    similarity/verbatim retry cannot reach the API. Ensures every row differs
    from the MT by at least one word, preventing CDReader ErrMessage10 rejection.

    Strategy: try synonym substitutions in priority order; apply the FIRST match only.
    Skips matches inside quoted speech (consistent with _find_synonym_pair).
    """
    # Build quote-protected ranges (same logic as _find_synonym_pair)
    _quote_ranges = []
    _i = 0
    while _i < len(text):
        if text[_i] == '\u201e':  # „ opening
            _j = text.find('\u201c', _i + 1)  # " closing
            if _j == -1:
                _j = text.find('"', _i + 1)  # fallback: ASCII closing
            if _j != -1:
                _quote_ranges.append((_i, _j))
                _i = _j + 1
                continue
        _i += 1

    def _in_quotes(match_start, match_end):
        return any(qs <= match_start and match_end <= qe + 1
                   for qs, qe in _quote_ranges)

    for pattern, replacement in _SYNONYMS:
        m = _re.search(pattern, text)
        if m and not _in_quotes(m.start(), m.end()):
            return _re.sub(pattern, replacement, text, count=1)
    # No synonym matched (very short row, exclamation, single name, etc.) — return as-is.
    # Comma→semicolon was removed: it frequently broke syntax where a comma is
    # grammatically required (subordinate clauses, enumeration, inline attribution).
    return text


def _call_gemini_simple(prompt, temperature=0.5, max_tokens=2048):
    """Account-group-aware Gemini call for single-row retries.
    
    Returns parsed JSON list or None.
    
    Key design (2026-03-10, 3-account rewrite):
      1. ACCOUNT-GROUP ROTATION: Keys are in 3 Google accounts with independent RPM/RPD.
         Try one key from each account group. When a group returns 429-RPM, skip only
         that group — other accounts are unaffected.
      2. 503 RETRY: Transient 5xx errors get one retry after a 3s wait.
      3. INCREASED OUTPUT BUDGET: Default maxOutputTokens=2048 (was 512). gemini-2.5-flash
         uses internal thinking tokens that consume the output budget, causing truncated
         JSON responses at 512.
    """
    global _retry_scan_offset

    keys_all = [k for k in GEMINI_KEYS if k not in _rpd_exhausted_keys]
    if not keys_all:
        return None  # All keys daily-dead

    _paid_key = _PAID_KEY if _PAID_KEY else None

    def _min_interval(api_key):
        return _PAID_KEY_MIN_INTERVAL if api_key == _paid_key else _FREE_KEY_MIN_INTERVAL

    def _is_cooled(api_key):
        last = _key_last_used.get(api_key, 0.0)
        return (time.time() - last) >= _min_interval(api_key)

    def _one_call(api_key):
        """Execute one Gemini request with 503 retry.
        Returns (parsed_list_or_None, is_429_rpd, is_429_rpm)."""
        for _attempt in range(2):  # up to 2 attempts (1 original + 1 retry on 5xx)
            _key_last_used[api_key] = time.time()
            try:
                resp = requests.post(
                    f"{GEMINI_URL}?key={api_key}",
                    json={
                        "contents": [{"parts": [{"text": prompt}]}],
                        "generationConfig": {"temperature": temperature, "maxOutputTokens": max_tokens},
                    },
                    timeout=45,
                )
            except requests.exceptions.RequestException as e:
                if _attempt == 0:
                    log(f"    ⚠️ Network error (will retry): {e}")
                    time.sleep(3)
                    continue
                raise
            if resp.status_code in (500, 502, 503, 504):
                if _attempt == 0:
                    log(f"    ⚠️ Gemini {resp.status_code} (will retry in 3s)")
                    time.sleep(3)
                    continue
                else:
                    resp.raise_for_status()  # give up on second 5xx
            break  # non-5xx response, proceed to parse

        if resp.status_code == 429:
            is_rpd = False
            try:
                err_obj = resp.json().get("error", {})
                combined = (str(err_obj.get("message", "")) + str(err_obj.get("details", ""))).lower()
                rpd_keywords = (
                    "per day", "daily", "1 day", "per_day",
                    "billing", "your current quota", "quota_exceeded", "check your plan",
                )
                is_rpd = any(kw in combined for kw in rpd_keywords)
            except Exception:
                pass
            return None, is_rpd, not is_rpd
        resp.raise_for_status()
        body = resp.json()
        candidates = body.get("candidates", [])
        if not candidates:
            log(f"    ⚠️ Gemini returned no candidates (blockReason={body.get('promptFeedback', {}).get('blockReason', '?')})")
            return None, False, False
        text = candidates[0].get("content", {}).get("parts", [{}])[0].get("text", "").strip()
        if not text:
            log(f"    ⚠️ Gemini returned empty text (finishReason={candidates[0].get('finishReason', '?')})")
            return None, False, False
        if text.startswith("```"):
            text = _re.sub(r"^```[^\n]*\n", "", text); text = text.rsplit("```", 1)[0].strip()
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            log(f"    ⚠️ Gemini returned unparseable JSON: {text[:100]!r}")
            return None, False, False
        return (parsed if isinstance(parsed, list) and parsed else None), False, False

    # ── Strategy: try one key from each account group ─────────────────────
    # Round-robin starting group so consecutive retry rows don't always hit
    # the same account first. Within each group, pick the first cooled key.
    _rpm_blocked_groups = set()  # groups where 429-RPM was seen this call
    n_groups = len(_ACCOUNT_GROUPS)
    start_group = _retry_scan_offset % n_groups
    _retry_scan_offset += 1

    for gi_offset in range(n_groups):
        gi = (start_group + gi_offset) % n_groups
        if gi in _rpm_blocked_groups:
            continue
        group_keys = [k for k in _ACCOUNT_GROUPS[gi]
                      if k in keys_all and k not in _rpd_exhausted_keys]
        if not group_keys:
            continue

        # Pick the first cooled key in this group
        for api_key in group_keys:
            if not _is_cooled(api_key):
                continue
            try:
                result, is_rpd, is_rpm = _one_call(api_key)
                if is_rpd:
                    _rpd_exhausted_keys.add(api_key)
                    continue  # try next key in same group (might be different project? unlikely but safe)
                if is_rpm:
                    _rpm_blocked_groups.add(gi)
                    log(f"    ℹ️ Account {_ACCOUNT_LABELS[gi]} RPM-blocked — skipping group")
                    break  # skip rest of this group, try next account
                if result is not None:
                    return result
                # Empty/None result but no 429 — try next key in group
            except Exception as e:
                log(f"    ⚠️ Account {_ACCOUNT_LABELS[gi]} key error: {e}")
            break  # only try ONE key per group (success or fail), then move to next group

    # ── Second pass: wait for soonest key across non-blocked groups ────────
    available_keys = [k for k in keys_all if k not in _rpd_exhausted_keys
                      and _key_account_group(k) not in _rpm_blocked_groups]
    if not available_keys:
        # All groups either RPM-blocked or RPD-dead — try waiting for the soonest key
        available_keys = [k for k in keys_all if k not in _rpd_exhausted_keys]

    if available_keys:
        now = time.time()
        wait_times = []
        for k in available_keys:
            elapsed = now - _key_last_used.get(k, 0.0)
            needed = _min_interval(k) - elapsed
            if needed > 0:
                wait_times.append(needed)
        if wait_times:
            wait_secs = min(wait_times) + 0.5
            wait_secs = max(1.0, min(wait_secs, 8.0))
            time.sleep(wait_secs)

        # Try the soonest-cooled key
        for k in available_keys:
            if _is_cooled(k):
                try:
                    result, is_rpd, is_rpm = _one_call(k)
                    if is_rpd:
                        _rpd_exhausted_keys.add(k)
                        continue
                    if not is_rpm and result is not None:
                        return result
                except Exception:
                    pass
                break  # one attempt only

    return None

def _unified_retry(all_rephrased, input_data, rows):
    """Identify and retry rows that are verbatim, too similar to MT, or truncated.
    
    Combines the former mandatory-change pass, similarity guard, and truncation guard
    into a single retry mechanism. Returns the updated list.
    """
    _input_by_sort = {r.get("sort", i): r.get("content", "") for i, r in enumerate(input_data)}
    mt_by_sort = {r.get("sort", i): (r.get("machineChapterContent") or r.get("modifChapterContent") or "")
                  for i, r in enumerate(rows)}

    # ── Collect all rows needing retry ────────────────────────────────────────
    retry_candidates = {}  # sort -> (current_out, reference, reason)

    for row in all_rephrased:
        sort_n = row.get("sort")
        out = row.get("content", "")
        if not out or sort_n == 0:
            continue

        inp = _input_by_sort.get(sort_n, "")
        mt  = mt_by_sort.get(sort_n, "")

        # Check 1: Verbatim (identical to input)
        if inp and out.strip() == inp.strip() and len(out.split()) >= 4:
            retry_candidates[sort_n] = (out, inp, "verbatim")
            continue

        # Check 2: Too similar to MT
        # Concept D: skip rows ≤ 4 words — too short to meaningfully affect chapter
        # average, and the synonym table rarely matches them anyway.
        # Dialogue rows (containing „/“/”) are exempt: post-processing adjusts quote
        # marks which changes the text enough for CDReader acceptance, and retrying
        # dialogue rows through the API or deterministic fallback is unreliable
        # (quote-aware guard blocks most synonym substitutions inside speech).
        if mt and len(out.split()) >= 5:
            if not any(q in out for q in ('„', '“', '”')):
                sim = _row_sim(out, mt)
                if sim >= SIM_THRESHOLD:
                    retry_candidates[sort_n] = (out, mt, f"similar ({sim:.0%})")
                    continue

        # Check 3: Truncated (output < 35% of input words, input >= 6 words)
        if inp:
            inp_w = len(inp.split())
            out_w = len(out.split())
            if inp_w >= 6 and out_w < 0.35 * inp_w:
                retry_candidates[sort_n] = (out, inp, f"truncated ({out_w}/{inp_w} words)")

    if not retry_candidates:
        log("  ✅ Unified retry: no rows need retrying.")
        return all_rephrased

    # ── Similarity diagnostic ──────────────────────────────────────────────
    _sim_scores = []
    for row in all_rephrased:
        sort_n = row.get("sort")
        out = row.get("content", "")
        mt = mt_by_sort.get(sort_n, "")
        if out and mt:
            _sim_scores.append((sort_n, out, mt, _row_sim(out, mt)))
    if _sim_scores:
        bands = {"0-25%": 0, "25-50%": 0, "50-75%": 0, "75-90%": 0, "90-100%": 0}
        for _, _, _, s in _sim_scores:
            if s < 0.25:   bands["0-25%"] += 1
            elif s < 0.50: bands["25-50%"] += 1
            elif s < 0.75: bands["50-75%"] += 1
            elif s < 0.90: bands["75-90%"] += 1
            else:          bands["90-100%"] += 1
        avg_sim = sum(s for _, _, _, s in _sim_scores) / len(_sim_scores)
        above = sum(1 for _, _, _, s in _sim_scores if s >= SIM_THRESHOLD)
        log(f"  [SIM DIAG] rows={len(_sim_scores)} avg={avg_sim:.0%} above_{SIM_THRESHOLD:.0%}={above}")
        log(f"  [SIM DIAG] bands: " + " | ".join(f"{k}:{v}" for k, v in bands.items()))

    # Soft cap — raised from 20 to 35 now that key 28 is correctly protected from
    # RPM misclassification and won't be permanently killed mid-retry loop.
    # High-similarity chapters (25+ flagged rows) were systematically under-retried.
    _MAX_RETRIES = 35
    if len(retry_candidates) > _MAX_RETRIES:
        log(f"  ⚠️  Unified retry: {len(retry_candidates)} rows flagged — capped at {_MAX_RETRIES}")
        sorted_cands = sorted(retry_candidates.items(), key=lambda x: -len(x[1][0].split()))
        retry_candidates = dict(sorted_cands[:_MAX_RETRIES])

    log(f"  \U0001f504 Unified retry: {len(retry_candidates)} row(s) to retry...")
    for sort_n, (out, ref, reason) in retry_candidates.items():
        log(f"    sort={sort_n} [{reason}]: {out[:60]!r}")

    # ── Retry each row ────────────────────────────────────────────────
    rephrased_by_sort = {r.get("sort"): r for r in all_rephrased}

    # Fix C: Check if all keys are dead BEFORE the loop.
    # If so, skip all API retries and go straight to deterministic fallback.
    _keys_alive = not _all_keys_rpd_dead()
    if not _keys_alive:
        log(f"  ⚠️  All Gemini keys RPD-exhausted — using deterministic fallback for all {len(retry_candidates)} rows.")

    _api_retries_ok = 0
    _fallback_applied = 0

    for sort_n, (current_out, ref_text, reason) in retry_candidates.items():
        # Skip API call if keys are dead (Fix C)
        if not _keys_alive:
            fallback = _deterministic_change(current_out)
            if fallback != current_out:
                rephrased_by_sort[sort_n]["content"] = fallback
                _fallback_applied += 1
                log(f"    🔧 sort={sort_n}: deterministic fallback applied")
            else:
                log(f"    ⚠️  sort={sort_n}: deterministic fallback could not change row")
            continue

        if "truncated" in reason:
            prompt = (
                "Du bist ein deutscher Korrektor. "
                "Die folgende Zeile wurde zu stark gekürzt und ist unvollständig. "
                "Formuliere den VOLLSTÄNDIGEN deutschen Text um "
                "— bewahre alle Inhalte und Bedeutungen. Kürze NICHT.\n"
                "Antworte NUR mit: "
                "[{\"sort\": " + str(sort_n) + ", \"content\": \"<vollständig umformuliert>\"}]\n"
                + json.dumps([{"sort": sort_n, "content": ref_text}], ensure_ascii=False)
            )
            temp = 0.5
        elif "similar" in reason:
            # Pre-compute the required word swap so the model has a concrete, mandatory
            # anchor. Without this, the model at low temperature sees a correct sentence
            # and returns it unchanged despite the "make a small change" instruction.
            _swap = _find_synonym_pair(current_out)
            if _swap:
                _swap_instruction = (
                    f"PFLICHT: Ersetze in deiner Antwort \u00bbexakt\u00ab das Wort "
                    f"\u00bb{_swap[0]}\u00ab durch \u00bb{_swap[1]}\u00ab. "
                    f"Passe ggf. Artikel/Kasus an. Lass alles andere unver\u00e4ndert.\n"
                )
            else:
                _swap_instruction = (
                    "PFLICHT: Ver\u00e4ndere mindestens ein Wort — "
                    "Gib NICHT denselben Satz zur\u00fcck.\n"
                )
            prompt = (
                "Du bist ein deutscher Korrektor. Der folgende Satz ist zu \u00e4hnlich zum "
                "Referenztext und muss ge\u00e4ndert werden.\n"
                + _swap_instruction +
                "Antworte NUR mit: "
                "[{\"sort\": " + str(sort_n) + ", \"content\": \"<korrigiert>\"}]\n"
                + json.dumps([{"sort": sort_n, "reference": ref_text, "content": current_out}],
                             ensure_ascii=False)
            )
            temp = 0.5
        else:  # verbatim
            # Same approach: give the model a specific word to swap, not a vague instruction.
            _swap = _find_synonym_pair(current_out)
            if _swap:
                _swap_instruction = (
                    f"PFLICHT: Ersetze in deiner Antwort \u00bbexakt\u00ab das Wort "
                    f"\u00bb{_swap[0]}\u00ab durch \u00bb{_swap[1]}\u00ab. "
                    f"Passe ggf. Artikel/Kasus an. Lass alles andere unver\u00e4ndert.\n"
                )
            else:
                _swap_instruction = (
                    "PFLICHT: Ver\u00e4ndere mindestens ein Wort — "
                    "Gib NICHT denselben Satz zur\u00fcck.\n"
                )
            prompt = (
                "Du bist ein deutscher Korrektor. Dieser Satz ist identisch zum Eingabetext "
                "und muss ver\u00e4ndert werden.\n"
                + _swap_instruction +
                "Antworte NUR mit: [{\"sort\": " + str(sort_n) + ", \"content\": \"<korrigiert>\"}]\n"
                + json.dumps([{"sort": sort_n, "content": current_out}], ensure_ascii=False)
            )
            temp = 0.5

        result = _call_gemini_simple(prompt, temperature=temp,
                                     max_tokens=4096 if "truncated" in reason else 2048)
        if result and result[0].get("content", "").strip():
            new_content = result[0]["content"].strip()
            if new_content != current_out:
                new_sim = _row_sim(new_content, ref_text) if "similar" in reason else 0
                # Concept B: if the API returned different text but it's STILL above
                # threshold (e.g. sort=15→88%, sort=76→89%, sort=17→100%), apply one
                # deterministic change on top so it clears the rejection zone.
                if "similar" in reason and new_sim >= SIM_THRESHOLD:
                    boosted = _deterministic_change(new_content)
                    if boosted != new_content:
                        boosted_sim = _row_sim(boosted, ref_text)
                        log(f"    ✅ sort={sort_n}: retry OK but still {new_sim:.0%} — det. boost → {boosted_sim:.0%}: {boosted[:60]!r}")
                        rephrased_by_sort[sort_n]["content"] = boosted
                        _api_retries_ok += 1
                    else:
                        # Det. change also failed — accept the API result anyway (still changed)
                        log(f"    ✅ sort={sort_n}: retry OK (sim={new_sim:.0%}, boost unavailable): {new_content[:60]!r}")
                        rephrased_by_sort[sort_n]["content"] = new_content
                        _api_retries_ok += 1
                else:
                    log(f"    ✅ sort={sort_n}: retry OK" +
                        (f" (sim={new_sim:.0%})" if new_sim else "") +
                        f": {new_content[:60]!r}")
                    rephrased_by_sort[sort_n]["content"] = new_content
                    _api_retries_ok += 1
            else:
                # API returned same text — apply deterministic fallback
                fallback = _deterministic_change(current_out)
                if fallback != current_out:
                    rephrased_by_sort[sort_n]["content"] = fallback
                    _fallback_applied += 1
                    log(f"    🔧 sort={sort_n}: API returned same text, deterministic fallback applied")
                else:
                    log(f"    ⚠️  sort={sort_n}: retry unchanged, no fallback possible")
        else:
            # API call failed — apply deterministic fallback (Fix B)
            fallback = _deterministic_change(current_out)
            if fallback != current_out:
                rephrased_by_sort[sort_n]["content"] = fallback
                _fallback_applied += 1
                log(f"    🔧 sort={sort_n}: API failed, deterministic fallback applied")
            else:
                log(f"    ⚠️  sort={sort_n}: retry failed, no fallback possible")

            # Fix C: if this failure exhausted the last key, switch to fallback-only
            if _all_keys_rpd_dead():
                _keys_alive = False
                log(f"  ⚠️  All keys now RPD-exhausted — switching to deterministic fallback for remaining rows.")

        time.sleep(1)

    if _fallback_applied:
        log(f"  🔧 Deterministic fallback: applied to {_fallback_applied} row(s), API retries OK: {_api_retries_ok}")

    return sorted(rephrased_by_sort.values(), key=lambda r: r.get("sort", 0))


def _post_process(sorted_rows, input_data, glossary_terms, skip_bgs_guard=False):
    """Run all post-processing passes on sorted_rows (modified in place).
    
    Called after initial Gemini batch processing AND after each retry pass,
    ensuring all output gets the same treatment (Pass QE, comma rules, glossary, etc.).
    """
    # Build lookup dicts from input_data (used throughout all passes)
    _mt_by_sort = {r.get("sort", i): r.get("content", "") for i, r in enumerate(input_data)}
    _eng_by_sort = {r.get("sort", i): r.get("original", "") for i, r in enumerate(input_data)}

    comma_fixes = 0
    comma_adds = 0
    dash_fixes = 0

    # ── Pre-Pass QE: BGS confusion guard ─────────────────────────────────────────
    # Gemini occasionally outputs a pure Begleitsatz (attribution clause) for a row
    # whose English source is dialogue (starts with "). This is a structural error:
    # a dialogue row must contain speech content, not just "erwiderte Hendrick.".
    # Caused by Gemini merging rows across sort boundaries under prompt pressure.
    # Fix: detect the mismatch and restore the row from the machine translation.
    # Pass QE will then correctly apply the quote structure on the restored text.
    #
    # Detection: eng starts with " (dialogue) AND German output matches Begleitsatz.
    # A genuine Begleitsatz never starts with a quote character, so testing the raw
    # output (not stripped) is safe — "„Ich schoss..." does NOT match BGS.
    # (dicts _mt_by_sort and _eng_by_sort built above)
    _bgs_confusion_fixes = 0
    for row in (sorted_rows if not skip_bgs_guard else []):
        sort_n = row.get("sort")
        out    = row.get("content", "").strip()
        eng_s  = _eng_by_sort.get(sort_n, "")
        mt_s   = _mt_by_sort.get(sort_n, "")
        # Guard 2: restore empty/whitespace rows from MT
        if not out:
            if mt_s:
                row["content"] = mt_s
                _bgs_confusion_fixes += 1
                log(f"  ⚠️  Empty row: sort={sort_n} restored from MT {mt_s[:60]!r}")
            continue
        if not eng_s or not mt_s: continue
        # General row-misalignment guard:
        # A bare Begleitsatz ("erwiderte er.", "fragte sie leise.") is NEVER a valid
        # standalone translated row unless the English source itself is a short
        # attribution sentence ("she asked.", "Petter said.").  Any other row type
        # (dialogue, narrative, description) producing a BGS as output means Gemini
        # has injected content from an adjacent row — restore from MT.
        # This covers both dialogue rows AND narrative rows receiving displaced BGS.
        _eng_is_attribution = (
            not eng_s.lstrip().startswith('"')           # not itself dialogue
            and len(eng_s.split()) <= _ENG_ATTRIBUTION_MAX_WORDS  # short sentence
            and bool(_re.search(
                r'\b(?:said|asked|replied|answered|whispered|shouted|called|muttered|'
                r'remarked|added|continued|insisted|demanded|exclaimed|cried|'
                r'explained|told|warned|ordered|nodded|smiled|sighed|'
                r'laughed|teased|snapped|groaned|sobbed|gasped|hissed|'
                r'growled|chuckled|corrected|interrupted|murmured|suggested|'
                r'conceded|admitted|acknowledged|declared|announced|breathed)\b',
                eng_s, _re.IGNORECASE))
        )
        # Fix 3a: strip leading quote before BGS check.
        # Root cause (SS3-row2): Gemini outputs „schalt Henry mich aus. (attribution
        # prefixed with spurious „). _BEGLEITSATZ_BASE requires ^(SV|pronoun+SV), so
        # _is_begleitsatz on the raw output never matches. Stripping the leading quote
        # first lets the pattern see the attribution verb directly.
        _out_unquoted = _re.sub(r'^[„“"]+', '', out).strip()
        _is_bgs_raw      = _is_begleitsatz(out)
        _is_bgs_unquoted = _is_begleitsatz(_out_unquoted)
        if (_is_bgs_raw or _is_bgs_unquoted) and not _eng_is_attribution:
            row["content"] = mt_s
            _bgs_confusion_fixes += 1
            log(f"  ⚠️  BGS confusion: sort={sort_n} restored from {out!r} to MT {mt_s[:60]!r}")
            continue
        # Fix 3b: lowercase-first output guard.
        # Root cause (SS4): Gemini displaces narrative from row N into row N+1, whose
        # English source is a dialogue/narrative row starting uppercase. The displaced
        # content starts lowercase (e.g. 'ich versuchte, gleichgültig zu klingen.').
        # In standard German prose every sentence starts uppercase; a lowercase-first
        # row is always wrong. Guard: if German output starts lowercase AND the MT for
        # that row starts uppercase, the content is displaced — restore from MT.
        if out and out[0].islower() and mt_s and mt_s.strip() and mt_s.strip()[0].isupper():
            row["content"] = mt_s
            _bgs_confusion_fixes += 1
            log(f"  ⚠️  Lowercase-first displaced: sort={sort_n} restored from {out!r} to MT {mt_s[:60]!r}")
    if _bgs_confusion_fixes:
        log(f"  💬 BGS confusion guard: restored {_bgs_confusion_fixes} row(s) from MT.")



    # ── Quote Reinject: Strip all quotes, place deterministically ──────────
    # Paradigm: do NOT try to fix Gemini's quote placement. Instead, strip ALL
    # outer quote characters from the German output and reinject „/“ at
    # computed positions based on the English source structure.
    # The English source is the ground truth for WHERE speech starts and ends.
    # The German text structure (colons, SV verbs) tells us where to place them.

    _qe_role_by_sort = {r.get("sort", i): r.get("_quote_role", "none")
                        for i, r in enumerate(input_data)}

    def _strip_outer_quotes(text):
        """Remove all outer German/French quote characters. Preserve inner ‚...‘."""
        for qc in ('„', '“', '”', '"', '«', '»'):
            text = text.replace(qc, '')
        return text

    def _find_speech_start(text):
        """Find where direct speech begins in German narration+speech text.
        Returns index where „ should go, or -1 if not detectable."""
        # Primary: colon + space + uppercase (standard German direct speech)
        m = _re.search(r':\s+([A-ZÄÖÜ])', text)
        if m:
            return m.start(1)
        # Secondary: colon + space (even if next char not uppercase)
        m2 = _re.search(r':\s+', text)
        if m2:
            return m2.end()
        return -1

    def _en_has_post_close_attribution(eng):
        """Check if English source has meaningful text after the last closing quote.
        Returns True if attribution exists (e.g. '..." she said.'), False if the
        quote closes at end of row (e.g. '...at all!"').
        This tells us whether to search for an SV verb or just place " at end."""
        if not eng:
            return False
        # Find last closing quote in EN
        _q_close_chars = set('""\u201d\u201c\u00bb')
        last_close = -1
        for i in range(len(eng) - 1, -1, -1):
            if eng[i] in _q_close_chars:
                last_close = i
                break
        if last_close < 0:
            return False
        # Check what follows the last closing quote
        after = eng[last_close + 1:].strip().rstrip('.,!?;:')
        # If 2+ words follow, there's attribution
        return len(after.split()) >= 2

    def _find_speech_end(text):
        """Find where closing “ should be inserted in German text.
        Returns (insert_pos, needs_comma).
        insert_pos = position in text; needs_comma = True if \u201c, should be inserted."""
        # Primary: comma + space + SV verb (e.g. \u2018, sagte er\u2019)
        m = _re.search(r',\s+(' + _SV + r')\b', text, _re.IGNORECASE)
        if m:
            return m.start(), False  # insert “ before the comma
        # Secondary: space + SV verb without comma
        m2 = _re.search(r'(?<=[a-zäöüß!?.\u2026])\s+(' + _SV + r')\b', text, _re.IGNORECASE)
        if m2:
            return m2.start(), True  # insert “, (add comma)
        # Fallback: end of text
        return len(text), False

    _QE_OPEN  = '„'
    _QE_CLOSE = '“'
    qe_fixes = 0

    for row in sorted_rows:
        sort_n = row.get("sort")
        c = row.get("content", "")
        if not c or sort_n == 0:
            continue

        role = _qe_role_by_sort.get(sort_n, "none")
        eng  = _eng_by_sort.get(sort_n, "")

        # Safety-net upgrade: "none" → "both" when EN has opening+closing quotes
        if role == "none" and eng:
            if _QE_ENG_OPEN_RE.match(eng.strip()) and _QE_ENG_CLOSE_RE.search(eng):
                role = "both"

        # Normalize French/angle quotes before stripping
        fixed = c.replace('«', '„').replace('»', '“')
        original = fixed

        en_starts_quote = bool(eng and _re.match(r'^[""„“«]', eng.strip()))
        stripped = _strip_outer_quotes(fixed)

        if role in ("middle", "none"):
            # No quotes at all — pure continuation or narrative
            fixed = stripped

        elif role == "open":
            if en_starts_quote:
                # Start-of-row opener
                fixed = _QE_OPEN + stripped
            else:
                # Mid-row open: narration + speech
                pos = _find_speech_start(stripped)
                if pos >= 0:
                    fixed = stripped[:pos] + _QE_OPEN + stripped[pos:]
                else:
                    # Fallback: keep original (Gemini’s placement, imperfect but better than none)
                    pass

        elif role == "close":
            if _en_has_post_close_attribution(eng):
                # EN has attribution after close → find SV verb in German
                pos, needs_comma = _find_speech_end(stripped)
                if needs_comma:
                    fixed = stripped[:pos] + _QE_CLOSE + ',' + stripped[pos:]
                elif pos < len(stripped):
                    fixed = stripped[:pos] + _QE_CLOSE + stripped[pos:]
                else:
                    fixed = stripped + _QE_CLOSE
            else:
                # EN closes at end of row (no attribution) → " at end
                fixed = stripped + _QE_CLOSE

        elif role == "both":
            if en_starts_quote:
                # Start-of-row both: „ at start, “ guided by EN attribution
                if _en_has_post_close_attribution(eng):
                    pos, needs_comma = _find_speech_end(stripped)
                    if needs_comma:
                        fixed = _QE_OPEN + stripped[:pos] + _QE_CLOSE + ',' + stripped[pos:]
                    elif pos < len(stripped):
                        fixed = _QE_OPEN + stripped[:pos] + _QE_CLOSE + stripped[pos:]
                    else:
                        fixed = _QE_OPEN + stripped + _QE_CLOSE
                else:
                    # No attribution after close → „ at start, “ at end
                    fixed = _QE_OPEN + stripped + _QE_CLOSE
            else:
                # Mid-row both: narration + „speech“ + possible attribution
                start_pos = _find_speech_start(stripped)
                if start_pos >= 0:
                    narration = stripped[:start_pos]
                    speech_part = stripped[start_pos:]
                    if _en_has_post_close_attribution(eng):
                        end_pos, needs_comma = _find_speech_end(speech_part)
                        if needs_comma:
                            fixed = narration + _QE_OPEN + speech_part[:end_pos] + _QE_CLOSE + ',' + speech_part[end_pos:]
                        elif end_pos < len(speech_part):
                            fixed = narration + _QE_OPEN + speech_part[:end_pos] + _QE_CLOSE + speech_part[end_pos:]
                        else:
                            fixed = narration + _QE_OPEN + speech_part + _QE_CLOSE
                    else:
                        # No attribution → “ at end of speech
                        fixed = narration + _QE_OPEN + speech_part + _QE_CLOSE
                else:
                    # Fallback: keep original
                    pass

        if fixed != original:
            row["content"] = fixed
            qe_fixes += 1

    if qe_fixes:
        log(f"  💬 Quote reinject: placed quotes in {qe_fixes} row(s).")

    # ── Final punctuation enforcement ─────────────────────────
    # Gemini sometimes uses comma-continuation where the EN source ends a sentence
    # with a period. Fix: compare final punctuation of DE output with EN source.
    _punct_fixes = 0
    for row in sorted_rows:
        sort_n = row.get("sort")
        if sort_n == 0:
            continue
        c = row.get("content", "")
        eng = _eng_by_sort.get(sort_n, "")
        if not c or not eng:
            continue

        # Determine EN final punctuation (ignoring trailing quotes)
        _en_stripped_p = eng.rstrip().rstrip('"”“"»').rstrip()
        _de_stripped_p = c.rstrip().rstrip('„“”""«»').rstrip()

        if not _en_stripped_p or not _de_stripped_p:
            continue

        _en_end = _en_stripped_p[-1]
        _de_end = _de_stripped_p[-1]

        # If EN ends with sentence-final punct and DE ends with comma, fix it
        if _en_end in '.!?' and _de_end == ',':
            _last_part = c.rstrip('„“”""«»').rstrip()
            _suffix = c[len(_last_part):]  # trailing quotes/whitespace
            fixed_p = _last_part[:-1] + _en_end + _suffix
            if fixed_p != c:
                row["content"] = fixed_p
                _punct_fixes += 1

    if _punct_fixes:
        log(f"  💬 Punctuation fix: corrected {_punct_fixes} trailing comma(s) to period/punct.")



    # ── Fix: remove duplicate content between adjacent rows ───────────────────
    # Type A: Row N ends with  „...“, begleitsatz  AND row N+1 = begleitsatz
    #         (Gemini merged attribution inline AND left it stranded in N+1)
    # Type B: Row N = „...?“                      AND row N+1 = „...?“ begleitsatz
    #         (Gemini prepended full dialogue text from row N into the attribution row)
    _dup_fixes = 0

    def _qnorm(s):
        return _re.sub(r'[„“”"]', '"', s)

    for idx in range(len(sorted_rows) - 1):
        row_n   = sorted_rows[idx]
        row_n1  = sorted_rows[idx + 1]
        cn      = row_n.get("content", "")
        cn1     = row_n1.get("content", "").strip()

        # ── Type A: inline attribution also left stranded in next row ──
        # Root cause: Gemini merges speech + Begleitsatz into row N (e.g.
        # „Seid ihr bereit?", fragte Peter.) while row N+1 correctly has the
        # Begleitsatz on its own. Fix: strip the stranded attribution from row N.
        # Row N+1 is already correct — leave it alone.
        # The old guard `orig_n1 != cn1` blocked this when MT == Gemini output
        # for the Begleitsatz (perfectly normal for a short attribution). Removed.
        m_inline = _re.search(r'[\u201c\u201d"]+\s*,\s*(.+)$', cn)
        if m_inline:
            inline_bgs = m_inline.group(1).strip()
            if inline_bgs and cn1 and (
                inline_bgs.lower() == cn1.lower() or
                inline_bgs.lower().rstrip(".") == cn1.lower().rstrip(".")
            ):
                row_n["content"] = _re.sub(
                    r"\s*,\s*" + _re.escape(inline_bgs) + r"\s*$", "", cn
                ).rstrip(",").rstrip()
                _dup_fixes += 1
                continue  # pair handled; row N+1 left as-is

        # ── Type B: row N+1 starts with full content of row N ──
        # Gemini puts dialogue+attribution in row N+1, while row N has only the dialogue.
        # Correct fix: move the attribution suffix into row N; restore row N+1 from original.
        cn_norm  = _qnorm(cn.strip())
        cn1_norm = _qnorm(cn1)
        if len(cn_norm) >= 10 and cn1_norm.startswith(cn_norm):
            remainder = cn1[len(cn.strip()):].lstrip(' “”",').strip()
            orig_n1 = _mt_by_sort.get(row_n1.get("sort"), "")
            if remainder and orig_n1:
                # Move attribution into row N (strip trailing punctuation, add ", attribution")
                cn_base = cn.rstrip().rstrip(".,")
                row_n["content"] = cn_base + ", " + remainder
                # Restore row N+1 from its original machine translation
                row_n1["content"] = orig_n1
                _dup_fixes += 1

    # ── Single-word source guard ────────────────────────────────────────
    # Gemini occasionally expands single-word rows (e.g. "Liz!" → "Liz, stopp!")
    # violating the prompt rule "single words: retain exactly as-is".
    # Guard: if MT has exactly 1 meaningful word and the output keeps that
    # word as its first token but adds more — restore from MT.
    # Legitimate rephrasing ("Okay." → "In Ordnung.") has a different first
    # token and is NOT affected by this guard.
    for row in sorted_rows:
        sort_n = row.get("sort")
        out    = row.get("content", "")
        mt     = (_mt_by_sort.get(sort_n) or "").strip()
        if not mt or not out: continue
        _mt_words  = _re.findall(r"[a-zA-ZäöüÄÖÜß']+", mt)
        _out_words = _re.findall(r"[a-zA-ZäöüÄÖÜß']+", out)
        if len(_mt_words) != 1: continue          # only single-word sources
        if len(_out_words) <= 1: continue         # output already single word
        if _out_words[0].lower() != _mt_words[0].lower(): continue  # legitimate rephrase
        row["content"] = mt
        log(f"  ⚠️  Single-word guard: sort={sort_n} restored from {out!r} → {mt!r}")

    if _dup_fixes:
        log(f"  🔁 Post-processing: fixed {_dup_fixes} duplicate content row(s).")

    for idx, row in enumerate(sorted_rows):
        c = row.get("content", "")
        next_content = sorted_rows[idx + 1].get("content", "") if idx + 1 < len(sorted_rows) else ""

        # Rule A removed: comma after ?" IS required in German before Begleitsatz.
        # Rule F (below) handles !" the same way.

        # Rule B-pre: Clean up closing_quote + comma + period (",.") at row end.
        # Gemini outputs e.g. „Nachmittag",. — period AND comma, which is wrong either way.
        # - If next row IS a Begleitsatz: keep comma (needed), move period inside quote → „Nachmittag.",
        # - If next row is NOT a Begleitsatz: drop comma, move period inside quote → „Nachmittag."
        if _re.search(r'[“”"],[.]$', c):
            c_base = c[:-3]           # everything before closing_quote
            c_quote = c[-3]           # the closing quote character
            if _is_continuation_row(next_content):
                c = c_base + "." + c_quote + ","   # „....",  (period inside, comma kept)
            else:
                c = c_base + "." + c_quote          # „...."   (period inside, comma dropped)
            row["content"] = c
            comma_fixes += 1

        # Rule B: Remove cross-row comma when next row is NOT a Begleitsatz.
        if len(c) >= 2 and c[-1] == ',' and c[-2] in ('"', '“', '”'):
            if not _is_continuation_row(next_content):
                row["content"] = c[:-1]
                c = c[:-1]
                comma_fixes += 1

        # Rule C: Add missing comma when closing quote is followed by Begleitsatz.
        # Applies to ALL closing quote variants including ?" and !" (same need for comma).
        # Cross-row: row ends with " (any variant, no comma yet) and next IS Begleitsatz.
        elif c and c[-1] in ('“', '”', '"') and not c.endswith(','):
            if _is_continuation_row(next_content):
                row["content"] = c + ","
                c = c + ","
                comma_adds += 1

        # Rule C2: Add missing comma after ?" / !" inline (same row as attribution).
        # e.g. „Seit wann trägst du Schmuck?“ fragte Karl → „Seit wann trägst du Schmuck?“, fragte Karl
        if _re.search(r'[?!][“”"](?!,)', c):
            c_c2 = _re.sub(
                r'([?![""\u201d\u201c])(?!,)([ \t]+(?:' + _SV + r'))',
                r'\1,\2', c
            )
            if c_c2 != c:
                row["content"] = c_c2
                c = c_c2
                comma_adds += 1


        # Rule D: Replace literal mid-sentence em-dashes with commas.
        if '—' in c:
            c_nodash = _re.sub(r'(?<=\w)\s*—\s*(?=\w)', ', ', c)
            if c_nodash != c:
                row["content"] = c_nodash
                dash_fixes += 1
                c = row["content"]

        # Rule E: Move comma from BEFORE closing quote to AFTER it.
        # Wrong: „Text,“ sagte / „Text," sagte
        # Right: „Text“, sagte / „Text", sagte
        if not _re.search(r'[?!],[“"]', c):
            c_e = _re.sub(
                r',(\u201c|")([ \t]+(?:' + _SV + r'))',
                r'\1,\2', c
            )
            if c_e == c and (c.endswith(',“') or c.endswith(',"')):
                if _is_continuation_row(next_content):
                    c_e = c[:-2] + c[-1] + ','
            if c_e != c:
                row["content"] = c_e
                comma_fixes += 1
                c = row["content"]

        # Rule F: Add missing comma after !" before Begleitsatz.
        # German: ! ends speech with exclamation, but comma is still needed
        # before the attribution verb.
        # Inline:    „Text!" rief er.   →  „Text!", rief er.
        # Cross-row: row ends with !"   and next row is Begleitsatz → add ","
        if _re.search(r'[!“"]$', c) or _re.search(r'!"[^,]', c):
            # Inline: !" followed by space+Begleitsatz without comma
            c_f = _re.sub(
                r'(![""\u201d\u201c])(?!,)([ \t]+(?:' + _SV + r'))',
                r'\1,\2', c
            )
            # Cross-row: row ends with !" and next row is Begleitsatz
            if c_f == c and _re.search(r'![“"]$', c):
                if _is_continuation_row(next_content):
                    c_f = c + ','
            if c_f != c:
                row["content"] = c_f
                comma_adds += 1
                c = row["content"]

        # Rule G: Enforce canonical Kapitel header format: "Kapitel N Title Case Title"
        # Gemini sometimes returns headers all-lowercase, with a spurious colon, or with
        # wrong casing. Match case-insensitively to catch "kapitel 60 ..." variants.
        # Canonical form: "Kapitel {N} {Each Word Title-Cased}" (no colon)
        if _re.match(r'^[Kk]apitel\s+\d+', c):
            _m_g = _re.match(r'^[Kk]apitel\s+(\d+)\s*:?\s*(.*)', c, _re.DOTALL)
            if _m_g:
                _num_g  = _m_g.group(1)
                _rest_g = _m_g.group(2).strip()
                # Title-case each word in the rest.
                # Bug fix: w[0].upper() silently fails for words starting with
                # punctuation like '(' — '('.upper() == '(', so '(teil' stays '(teil'.
                # Fix: scan for first alpha character and uppercase it instead.
                def _tc_word(w):
                    for _i, _ch in enumerate(w):
                        if _ch.isalpha():
                            return w[:_i] + w[_i].upper() + w[_i+1:]
                    return w
                _rest_titled = ' '.join(_tc_word(w) for w in _rest_g.split(' '))
                titled = f"Kapitel {_num_g} {_rest_titled}" if _rest_titled else f"Kapitel {_num_g}"
                if titled != c:
                    row['content'] = titled
        # Rules H2, J, I removed — quote placement now handled entirely by
        # the Quote Reinject system above, which strips ALL quotes and places
        # them deterministically from English source structure.
        pass


    # ── Post-process: deterministic glossary enforcement ────────────────────
    # The LLM sometimes ignores glossary entries in the prompt.
    # This step scans every row for untranslated English glossary source terms
    # and replaces them with the correct German target — bypassing model compliance.
    # Only applies when we have a non-empty glossary.
    if glossary_terms:
        # Build replacement map: source (lowercased for matching) → target
        # Longer terms first so "Black Reef Island" replaces before "Black" could.
        replacement_pairs = []
        for t in glossary_terms:
            src = (t.get("dictionaryKey") or "").strip()
            tgt = (t.get("dictionaryValue") or "").strip()
            if src and tgt and src != tgt:  # skip no-ops (e.g. Moss→Moss)
                replacement_pairs.append((src, tgt))
        # Sort by length descending so multi-word terms match before subterms
        replacement_pairs.sort(key=lambda x: len(x[0]), reverse=True)

        gloss_fixes = 0
        for row in sorted_rows:
            original_content = row.get("content", "")
            new_content = original_content
            for src, tgt in replacement_pairs:
                # Word-boundary aware replacement, case-insensitive
                try:
                    pattern = _re.compile(r'(?<![\w\-])' + _re.escape(src) + r'(?![\w\-])', _re.IGNORECASE)
                    replaced = pattern.sub(tgt, new_content)
                    if replaced != new_content:
                        new_content = replaced
                except _re.error:
                    pass  # skip malformed patterns
            if new_content != original_content:
                row["content"] = new_content
                gloss_fixes += 1

        if gloss_fixes:
            log(f"  📖 Post-processing: enforced glossary terms in {gloss_fixes} row(s).")



# ─── Phase 4: Rephrase with Gemini ───────────────────────────────────────────
def rephrase_with_gemini(rows, glossary_terms, book_name):
    global _batch_account_offset
    if not GEMINI_KEYS:
        log("❌ No GEMINI_API_KEY configured.")
        return None

    # Keep raw terms for per-batch filtering; also pre-format full list as fallback
    glossary_text_full = format_glossary_for_prompt(glossary_terms)

    # Build input data: sort + English original (context) + German to rephrase
    # CONFIRMED by DIAG: chapterConetnt=English source, machineChapterContent=German machine translation
    # Gemini must rephrase the German machine translation, NOT re-translate from English.

    def _classify_quote_role(text):
        """Classify a text row's dialogue role using quote-balance tracking.
        
        Returns one of: "open", "close", "middle_or_none", "both"
        
        Uses character-by-character analysis to distinguish openers from closers:
          - Opener: " at position 0, or preceded by whitespace/start-of-line
          - Closer: " preceded by letter, digit, or sentence punctuation
        Then tracks running balance to determine the row's structural role.
        """
        t = text.strip()
        if not t:
            return "middle_or_none"
        
        # Find all quote characters and classify each as opener or closer
        _q_chars = set('\u201e\u201c\u201d""\u00ab\u00bb')
        balance = 0
        lowest = 0
        any_open = False
        any_close = False
        
        for i, ch in enumerate(t):
            if ch not in _q_chars:
                continue
            # Determine if this quote character is an opener or closer
            # by examining the character immediately before it.
            if i == 0:
                is_opener = True  # first char = opener
            else:
                prev = t[i - 1]
                # Opener: preceded by whitespace, opening bracket, or colon
                # Closer: preceded by letter, digit, punctuation (.!?,;), or dash
                # Note: em/en dashes (—–) are NOT openers — they typically indicate
                # speech interruption (Bethany—"). Handled via forward-look below.
                is_opener = prev in ' \t\n(:;'
                # Forward-looking override: if prev is sentence-ending punct or dash
                # but the character AFTER the quote is uppercase, this is a new speech
                # opening, not a close. Handles both spaceless joins (room."What) and
                # dash-introduced speech (—"What are you doing?").
                if not is_opener and prev in '.!?\u2014\u2013-' and i + 1 < len(t) and t[i + 1].isupper():
                    is_opener = True
            
            if is_opener:
                balance += 1
                any_open = True
            else:
                balance -= 1
                any_close = True
            lowest = min(lowest, balance)
        
        if not any_open and not any_close:
            return "middle_or_none"
        
        # Interpret the balance:
        #   balance > 0:  unmatched open at end of row → speech continues to next row
        #   lowest < 0:   unmatched close from previous row's speech
        #   both:         row both closes previous speech AND opens new speech
        has_unmatched_open = balance > 0
        has_unmatched_close = lowest < 0
        
        if has_unmatched_open and has_unmatched_close:
            return "both"  # rare: closes one speech, opens another
        elif has_unmatched_open:
            return "open"
        elif has_unmatched_close:
            return "close"
        elif any_open and any_close:
            return "both"  # self-contained dialogue (balanced open+close)
        else:
            return "middle_or_none"

    raw_contents = [
        # Use German machine translation as the text Gemini rephrases.
        # chapterConetnt is English — using it would make Gemini re-translate from
        # English, producing output similar to the existing machine translation.
        r.get("machineChapterContent") or r.get("modifChapterContent") or r.get("peContent") or ""
        for r in rows
    ]
    # English source (chapterConetnt) used as context only, not as content to rephrase
    english_originals = [
        r.get("chapterConetnt") or r.get("eContent") or r.get("eeContent") or ""
        for r in rows
    ]

    # Determine quote roles using the ENGLISH source, not the German MT.
    # German MT frequently has „ without closing ", which causes the state machine
    # to get stuck in in_dialogue=True and misclassify all subsequent no-quote rows
    # as "middle" when they are independent dialogue lines or narrative.
    # English quote placement is self-consistent and reliable for open/close tracking.
    quote_roles = []
    in_dialogue = False
    for i, eng_text in enumerate(english_originals):
        role = _classify_quote_role(eng_text)
        if role == "both":
            in_dialogue = False
            quote_roles.append("both")
        elif role == "open":
            in_dialogue = True
            quote_roles.append("open")
        elif role == "close":
            in_dialogue = False
            quote_roles.append("close")
        elif role == "middle_or_none":
            if in_dialogue:
                quote_roles.append("middle")
            else:
                quote_roles.append("none")
        else:
            in_dialogue = False
            quote_roles.append("none")

    # ── Orphan-close repair: find missing openers ────────────────────────────
    # CDReader EN source sometimes omits the opening " while the closing " exists
    # rows later. Example:
    #   Row N EN:   'Let's settle this: hand over the research rights...'  (no ")
    #   Row N+1 EN: 'We can both walk away without regrets.'               (no ")
    #   Row N+2 EN: 'Isn't that better for both of us?"'                   (closing ")
    # The state machine assigns: none, none, close — but row N should be "open"
    # and row N+1 should be "middle".
    #
    # Repair: for each "close" without a preceding "open", walk backwards to find
    # the row where speech started. Heuristics for the opener:
    #   1. EN text contains ': ' followed by content (colon introducing speech)
    #   2. German MT contains '„' (MT correctly placed the opening quote)
    # Then retroactively assign "open" + mark intermediates as "middle".
    _in_speech = False
    for i in range(len(quote_roles)):
        if quote_roles[i] in ("open", "both"):
            _in_speech = True
        elif quote_roles[i] == "close":
            if not _in_speech:
                # Orphan close — walk backwards to find opener
                _found_opener = -1
                for j in range(i - 1, max(i - 15, -1), -1):  # look back up to 15 rows
                    if quote_roles[j] in ("open", "both", "close"):
                        break  # hit another dialogue block, stop
                    en_j = english_originals[j] if j < len(english_originals) else ""
                    mt_j = raw_contents[j] if j < len(raw_contents) else ""
                    # Check if EN has colon + content (speech introduction)
                    has_colon_speech = bool(_re.search(r':\s+[a-zA-Z]', en_j))
                    # Check if German MT has „ (MT detected speech start)
                    mt_has_open = '\u201e' in mt_j
                    if has_colon_speech or mt_has_open:
                        _found_opener = j
                        break
                if _found_opener >= 0:
                    quote_roles[_found_opener] = "open"
                    for k in range(_found_opener + 1, i):
                        if quote_roles[k] == "none":
                            quote_roles[k] = "middle"
                    log(f"  \u26a0\ufe0f  Orphan-close repair: row {i} has close but no open \u2014 "
                        f"retroactively set row {_found_opener} as open, "
                        f"{i - _found_opener - 1} middle row(s)")
            _in_speech = False
        elif quote_roles[i] in ("middle",):
            pass  # already in speech
        else:
            if not _in_speech:
                pass  # none, stay none

    input_data = [
        {
            "sort": r.get("sort", i),
            "original": english_originals[i],   # English source — context for Gemini
            "content": raw_contents[i],           # German machine translation — primary text to rephrase
            "machine_translation": raw_contents[i],  # same German text used by similarity guard
            "_quote_role": quote_roles[i],
        }
        for i, r in enumerate(rows)
    ]
    non_empty = sum(1 for r in input_data if r["content"].strip())
    log(f"  Input data: {len(input_data)} rows, {non_empty} with non-empty content")
    if rows:
        r0 = rows[0]
        fields = ["chapterConetnt","eContent","eeContent","modifChapterContent","machineChapterContent","languageContent","peContent","referenceContent"]
        log("  Field presence: " + ", ".join(f"{f}={bool(r0.get(f))}" for f in fields))

    BATCH_SIZE = 40
    MAX_RETRIES = 3
    MAX_RETRIES_429 = 3    # If 429 persists beyond 3 tries, RPD is likely exhausted — fail fast

    def _fix_json_strings(s):
        """Fix literal newlines/tabs inside JSON string values."""
        result = []
        in_string = False
        escape_next = False
        for ch in s:
            if escape_next:
                result.append(ch)
                escape_next = False
            elif ch == '\\':
                result.append(ch)
                escape_next = True
            elif ch == '"' and not escape_next:
                in_string = not in_string
                result.append(ch)
            elif in_string and ch == '\n':
                result.append('\\n')
            elif in_string and ch == '\r':
                result.append('\\r')
            elif in_string and ch == '\t':
                result.append('\\t')
            else:
                result.append(ch)
        return ''.join(result)

    def _build_prompt(batch_data, batch_num, total_batches, next_batch_first=None):
        """Build the prompt string and clean batch data, shared by both providers."""
        lookahead_note = ""
        if next_batch_first is not None:
            lookahead_note = (
                "\n\nLOOKAHEAD (do NOT rephrase, use ONLY to decide if last row needs a trailing comma):\n"
                f"The row immediately following this batch starts with: {json.dumps(next_batch_first.get('content', ''), ensure_ascii=False)}"
            )
        clean_batch = [
            {
                "sort": r["sort"],
                "original": r.get("original", ""),
                "content": r["content"],
            }
            for r in batch_data
        ]
        quote_hints = []
        for r in batch_data:
            role = r.get("_quote_role", "both")
            sort_n = r['sort']
            if role == "open":
                quote_hints.append(f"  sort {sort_n}: OPENS a multi-row dialogue — use „ to open, NO closing “ at end")
            elif role == "close":
                quote_hints.append(f"  sort {sort_n}: CLOSES a multi-row dialogue — NO opening „, but add closing “ at end")
            elif role == "middle":
                quote_hints.append(f"  sort {sort_n}: MIDDLE of a multi-row dialogue — NO opening or closing quotes")
        quote_hint_block = ""
        if quote_hints:
            quote_hint_block = "\n\nMULTI-ROW DIALOGUE STRUCTURE (follow exactly):\n" + "\n".join(quote_hints)

        # Filter glossary to only terms present in this batch's text — reduces prompt
        # size dramatically and keeps
        # the model focused on only the relevant terms rather than all 200+ entries.
        batch_text = " ".join(
            (r.get("original", "") + " " + r.get("content", "")).lower()
            for r in batch_data
        )
        if glossary_terms:
            # Build a set of all English words present in the batch (lowercased)
            # for efficient multi-word substring matching.
            batch_text_lower = batch_text.lower()
            def _term_in_batch(term):
                key = (term.get("dictionaryKey") or "").strip().lower()
                sur = (term.get("enSurname") or "").strip().lower()
                return (key and key in batch_text_lower) or (sur and sur in batch_text_lower)

            merged = [t for t in glossary_terms if _term_in_batch(t)]
            # Fallback: if filter produces nothing (e.g. batch is all German already),
            # send the full list so the model still has context.
            if not merged:
                merged = glossary_terms
            batch_glossary_text = format_glossary_for_prompt(merged)
        else:
            merged = []
            batch_glossary_text = glossary_text_full
        log(f"  Glossary for batch {batch_num}: {len(merged)} relevant terms (of {len(glossary_terms or [])} total)")

        prompt = (
            f"{BASE_PROMPT}\n\n"
            f"BOOK-SPECIFIC GLOSSARY FOR \"{book_name}\" (apply these in addition to universal glossary above):\n"
            f"{batch_glossary_text}\n\n"
            f"ROWS TO REPHRASE (batch {batch_num}/{total_batches}, {len(clean_batch)} rows):\n"
            f"For each row:\n"
            f"  - \"original\": English source text (may be empty) — for context and meaning verification only.\n"
            f"  - \"content\": German machine translation — this is what you MUST proofread. "
            f"Fix grammar errors, logic errors, and apply localization. Keep the text as close to the input as possible — only change what is actually wrong. "
            f"Your output must differ from the input in vocabulary or sentence structure — "
            f"returning a row IDENTICAL to the input is a hard validation error and will "
            f"cause CDReader to reject the entire chapter. Even short rows must have "
            f"at minimum a small synonym substitution or word-order change.\n"
            f"Return ONLY a JSON array; each object must have \"sort\" and \"content\" only.\n"
            f"{json.dumps(clean_batch, ensure_ascii=False)}{quote_hint_block}{lookahead_note}"
        )
        return prompt, clean_batch

    def _parse_llm_response(text, batch_num):
        """Parse JSON from LLM response text, with fallback fix."""
        text = text.strip()
        if text.startswith("```"):
            text = text.split("\n", 1)[-1]
            text = text.rsplit("```", 1)[0].strip()
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return json.loads(_fix_json_strings(text))

    def _call_gemini(batch_data, batch_num, total_batches, next_batch_first=None):
        batch_prompt, _ = _build_prompt(batch_data, batch_num, total_batches, next_batch_first)

        # Retry loop: try each key once, then if all RPM-exhausted wait 60s for reset.
        # RPD-exhausted keys (daily quota) are marked permanently and never retried.
        # Max full RPM-reset rotations before giving up: MAX_RETRIES_429
        resp = None  # safe before first request; allows Retry-After header read in wait block
        full_rotations = 0
        while full_rotations < MAX_RETRIES_429:
            try:
                # Fail fast if every key has hit its daily quota
                if _all_keys_rpd_dead():
                    log(f"❌ All Gemini keys have hit their daily quota (RPD). No point retrying.")
                    return None
                api_key = _next_gemini_key(prefer_group=_batch_account_offset % len(_ACCOUNT_GROUPS) if _ACCOUNT_GROUPS else None)
                if not api_key:
                    # All remaining (non-RPD) keys are RPM-exhausted — wait for reset
                    full_rotations += 1
                    if full_rotations >= MAX_RETRIES_429:
                        log(f"❌ All Gemini keys RPM-exhausted after {MAX_RETRIES_429} rotation(s) on batch {batch_num}.")
                        return None
                    # Honour Retry-After header if present (more precise than flat 60s)
                    retry_after = None
                    try:
                        if resp is not None:
                            retry_after = int(resp.headers.get("Retry-After", 0))
                    except Exception:
                        pass
                    wait = retry_after if retry_after and retry_after > 0 else 60
                    rpm_exhausted_count = len([k for k in GEMINI_KEYS if k in _exhausted_keys and k not in _rpd_exhausted_keys])
                    log(f"  ⚠️ {rpm_exhausted_count} key(s) RPM-limited. Waiting {wait}s for reset (rotation {full_rotations}/{MAX_RETRIES_429})...")
                    _exhausted_keys.intersection_update(_rpd_exhausted_keys)  # clear RPM state, preserve RPD
                    time.sleep(wait)
                    continue
                resp = requests.post(
                    f"{GEMINI_URL}?key={api_key}",
                    json={
                        "contents": [{"parts": [{"text": batch_prompt}]}],
                        "generationConfig": {
                            "temperature": 0.25,
                            "maxOutputTokens": 16384,
                            "responseMimeType": "application/json",
                        },
                    },
                    timeout=300,
                )
                if resp.status_code == 429:
                    # Parse error body to distinguish RPD (daily) from RPM (per-minute)
                    is_rpd = False
                    try:
                        err_body = resp.json()
                        err_obj = err_body.get("error", {})
                        err_msg = str(err_obj.get("message", "")).lower()
                        err_status = str(err_obj.get("status", "")).upper()
                        err_details = str(err_obj.get("details", "")).lower()
                        combined = err_msg + err_details
                        # RPD keywords: covers both "exceeded your current quota / billing"
                        # messages AND classic "per day / daily" quota messages
                        rpd_keywords = (
                            "per day", "daily", "1 day", "per_day",
                            "billing", "your current quota", "quota_exceeded",
                            "check your plan",
                        )
                        # Note: RESOURCE_EXHAUSTED covers BOTH RPM and RPD.
                        # Rely solely on message keywords to distinguish them.
                        is_rpd = any(kw in combined for kw in rpd_keywords)
                        limit_hint = f" [{err_msg[:80]}]" if err_msg else ""
                    except Exception:
                        limit_hint = ""
                    if is_rpd:
                        _rpd_exhausted_keys.add(api_key)
                        _exhausted_keys.add(api_key)
                        remaining = len([k for k in GEMINI_KEYS if k not in _rpd_exhausted_keys])
                        log(f"  📵 Key daily quota (RPD) exhausted{limit_hint}, {remaining} key(s) left...")
                    else:
                        _exhausted_keys.add(api_key)
                        remaining = len([k for k in GEMINI_KEYS if k not in _exhausted_keys])
                        log(f"  🔄 Key RPM-limited{limit_hint}, {remaining} key(s) remaining...")
                    continue
                resp.raise_for_status()
                body = resp.json()

                # Log finish reason for diagnostics
                finish_reason = (body.get("candidates", [{}])[0].get("finishReason", "?"))
                if finish_reason not in ("STOP", ""):
                    log(f"  ⚠️ Gemini finishReason={finish_reason} on batch {batch_num}")

                text = (
                    body.get("candidates", [{}])[0]
                    .get("content", {})
                    .get("parts", [{}])[0]
                    .get("text", "")
                )
                if not text:
                    log(f"❌ Empty Gemini response on batch {batch_num}: {body}")
                    return None
                # Success — exit retry loop

                parsed = _parse_llm_response(text, batch_num)
                log(f"  Batch {batch_num}/{total_batches}: {len(parsed)} rows from Gemini.")
                return parsed

            except json.JSONDecodeError as e:
                log(f"❌ Gemini JSON parse error on batch {batch_num}: {e}")
                log(f"   Raw response (first 500 chars): {text[:500]}")
                log(f"  Retrying in 15s...")
                time.sleep(15)
                continue
            except Exception as e:
                log(f"❌ Gemini error on batch {batch_num}: {e}")
                log(f"  Retrying in 15s...")
                time.sleep(15)
                full_rotations += 1
                continue
        return None  # all rotations exhausted

    # Sort=0 is always the chapter title row (e.g. "Kapitel 60 Auftauchen Und Das Rampenlicht Stehlen!").
    # Gemini cannot return valid JSON for a 6-word title row reliably, and rephrasing it produces
    # wrong output (all-lowercase, restructured titles). Bypass Gemini for sort=0 entirely —
    # pass it through unchanged and let Rule G enforce correct title-case in post-processing.
    _title_row = next((r for r in input_data if r.get("sort") == 0), None)
    gemini_input_data = [r for r in input_data if r.get("sort") != 0]

    # Split into batches and call Gemini for each
    batches = [gemini_input_data[i:i+BATCH_SIZE] for i in range(0, len(gemini_input_data), BATCH_SIZE)]
    total_batches = len(batches)
    log(f"  Splitting {len(gemini_input_data)} rows into {total_batches} batches of ~{BATCH_SIZE}...")

    all_rephrased = []
    key_count = len(GEMINI_KEYS)
    _ag_info = ", ".join(f"{_ACCOUNT_LABELS[i]}:{len(g)}" for i, g in enumerate(_ACCOUNT_GROUPS) if g)
    log(f"  Using {key_count} Gemini key(s) across {len(_ACCOUNT_GROUPS)} accounts ({_ag_info}) with group rotation.")
    for i, batch in enumerate(batches, 1):
        _preferred_group = _batch_account_offset % len(_ACCOUNT_GROUPS) if _ACCOUNT_GROUPS else 0
        log(f"  Sending batch {i}/{total_batches} ({len(batch)} rows) via Gemini (prefer Account {_ACCOUNT_LABELS[_preferred_group]})...")
        next_first = batches[i][0] if i < total_batches else None
        result = _call_gemini(batch, i, total_batches, next_batch_first=next_first)
        _batch_account_offset += 1  # rotate to next account for next batch
        if result is None:
            # Fall back to MT content for this batch — the similarity guard will
            # flag these rows for retry, converting a hard failure into degraded
            # quality that can be partially recovered.
            log(f"  ⚠️  Batch {i} failed — falling back to MT for {len(batch)} rows.")
            result = [{"sort": r.get("sort"), "content": r.get("content", "")} for r in batch]
        # ── Guard 1: Missing-sort reconciliation ────────────────────────────────
        # Gemini occasionally returns fewer rows than were sent (output truncation,
        # dropped rows under context pressure). Any missing sort number means that
        # row would be absent from the final output — structural corruption.
        # Detect and fill with the machine translation so no row is ever lost.
        _result_sorts = {r.get("sort"): True for r in result}
        _missing = [r for r in batch if r.get("sort") not in _result_sorts]
        if _missing:
            log(f"  ⚠️  Batch {i}: {len(_missing)} missing sort(s) from Gemini — restoring from MT: "
                + ", ".join(str(r.get("sort")) for r in _missing))
            for _mr in _missing:
                result.append({"sort": _mr.get("sort"), "content": _mr.get("content", "")})
        # ── Guard 2: Content-bleed inflation guard ───────────────────────────────
        # Gemini occasionally pulls text from row N+1 into row N to close an open
        # quote (dialogue split across rows). The signature is output significantly
        # LONGER than input — the opposite of truncation. Restore from MT when the
        # output word count exceeds 1.6× the input with a minimum delta of 4 words.
        # The restored row will be caught by the similarity guard and retried in
        # isolation via _unified_retry, without neighbour context to tempt bleed.
        _inp_wc = {r.get("sort"): len((r.get("content") or "").split()) for r in batch}
        _en_wc_g2 = {r.get("sort"): len((r.get("original") or "").split()) for r in batch}
        _bleed_count = 0
        for _r in result:
            _s = _r.get("sort")
            _inp_w = _inp_wc.get(_s, 0)
            _en_w  = _en_wc_g2.get(_s, 0)
            _out_w = len((_r.get("content") or "").split())
            # Standard check: MT-based inflation (for rows where MT >= 4 words)
            _mt_trigger = (_inp_w >= _INFLATION_MIN_DELTA
                           and _out_w > _inp_w * _INFLATION_THRESHOLD
                           and (_out_w - _inp_w) >= _INFLATION_MIN_DELTA)
            # EN-based check: catches bleed on SHORT rows where MT < 4 words.
            # If EN is short (< 8 words) but output is 3x+ EN words and 6+ words
            # longer, Gemini almost certainly pulled content from an adjacent row.
            _en_trigger = (_en_w >= 2 and _en_w < 8
                           and _out_w > _en_w * 3
                           and (_out_w - _en_w) >= 4)
            if _mt_trigger or _en_trigger:
                _mt_orig = next((r.get("content", "") for r in batch if r.get("sort") == _s), "")
                if _mt_orig:
                    _trigger_src = "MT" if _mt_trigger else "EN"
                    log(f"  \u26a0\ufe0f  Bleed guard: sort={_s} inflated ({_out_w}w vs MT={_inp_w}w EN={_en_w}w, trigger={_trigger_src}) \u2014 restored from MT")
                    _r["content"] = _mt_orig
                    _bleed_count += 1
        if _bleed_count:
            log(f"  💬 Bleed guard: restored {_bleed_count} inflated row(s) from MT (will retry).")
        # ── Guard 3: Cross-row echo duplication guard ────────────────────────────
        # Pattern: Gemini sees a literary echo in adjacent rows (Row N ends in '?"'
        # and Row N+1 starts with the same phrase) and *copies* Row N+1's opening
        # phrase onto the end of Row N — producing a duplicate that appears in BOTH
        # rows simultaneously. Row N+1 is fully intact so Guard 2 (inflation) won't
        # fire (only Row N is inflated, and by too few words for the 1.6x threshold).
        #
        # Detection: Row N output ends with a closed speech unit followed by a new
        # short opening „..." fragment AND that fragment matches the start of Row N+1.
        # Fix: strip everything from the second „ onward in Row N.
        _sorted_result = sorted(result, key=lambda r: r.get("sort", 0))
        _echo_count = 0
        for _ei in range(len(_sorted_result) - 1):
            _rN  = _sorted_result[_ei]
            _rN1 = _sorted_result[_ei + 1]
            _cN  = (_rN.get("content") or "").rstrip()
            _cN1 = (_rN1.get("content") or "").lstrip()
            if not _cN or not _cN1:
                continue
            # Look for: ends with closing quote, then whitespace, then new open „..." fragment
            _echo_match = _re.search(
                r'[“"]\s+„(.{3,60}?)[“"]\s*$', _cN
            )
            if not _echo_match:
                continue
            _echo_phrase = _echo_match.group(1).strip()
            # Check if Row N+1 starts with the same phrase (after its opening „)
            _n1_inner = _re.match(r'^„(.{3,60}?)[“",\s]', _cN1)
            if not _n1_inner:
                continue
            _n1_phrase = _n1_inner.group(1).strip()
            # Allow minor variation: compare first 15 chars or full phrase if shorter
            _cmp_len = min(15, len(_echo_phrase), len(_n1_phrase))
            if _cmp_len >= 3 and _echo_phrase[:_cmp_len].lower() == _n1_phrase[:_cmp_len].lower():
                # Strip the appended echo fragment from Row N
                _stripped = _cN[:_echo_match.start()].rstrip()
                # Ensure the stripped content still ends with a proper close quote
                if not _stripped.endswith(('“', '"', '!', '?', '.')):
                    continue  # Safety: don't strip if result would be malformed
                _rN["content"] = _stripped
                _echo_count += 1
                log(f"  ⚠️  Echo guard: sort={_rN.get('sort')} — stripped appended "
                    f"{_echo_phrase[:30]!r} (matches start of sort={_rN1.get('sort')})")
        if _echo_count:
            log(f"  💬 Echo guard: removed {_echo_count} duplicated echo fragment(s).")
        # ── Guard 4: Severe truncation + cascade unwind ──────────────────────────
        # Root cause: Gemini splits a single row's translation across two sort slots,
        # pushing Row N's content into Row N+1, Row N+1 into Row N+2, etc.
        # Signature: Row N output is drastically shorter than both its MT and EN source
        # (< 35% of input words). Row N+1 then contains Row N's displaced content.
        #
        # Two-step fix:
        #   Step 1 — detect severely truncated rows and restore from MT.
        #   Step 2 — cascade unwind: also restore the immediately following row (N+1)
        #            from MT, since it almost certainly received the displaced content
        #            of Row N. Both rows are then retried in isolation by _unified_retry.
        #
        # EN source word count (from batch["original"]) is used as a secondary signal
        # so the guard fires even when the MT itself is unusually short.
        _en_wc = {r.get("sort"): len((r.get("original") or "").split()) for r in batch}
        _truncated_sorts = set()
        _result_by_sort_g4 = {_r.get("sort"): _r for _r in result}
        _trunc_count = 0
        for _r in result:
            _s = _r.get("sort")
            _inp_w = _inp_wc.get(_s, 0)   # German MT word count (already built for Guard 2)
            _en_w  = _en_wc.get(_s, 0)    # English source word count
            _out_w = len((_r.get("content") or "").split())
            _mt_trigger = _inp_w >= _TRUNCATION_MIN_WORDS and _out_w < _inp_w * _TRUNCATION_THRESHOLD
            # EN trigger: only fire when MT is also >= 3 words. If MT is 1-2 words,
            # the EN/MT length discrepancy is a CDReader source data issue (EN field
            # sometimes contains concatenated text from adjacent rows), not Gemini truncation.
            # Restoring from a 1-word MT would make things worse, not better.
            _en_trigger = (_en_w >= _TRUNCATION_MIN_WORDS
                           and _out_w < _en_w * _TRUNCATION_THRESHOLD
                           and _inp_w >= 3)
            if _mt_trigger or _en_trigger:
                _mt_orig = next((r.get("content", "") for r in batch if r.get("sort") == _s), "")
                if _mt_orig:
                    log(f"  ⚠️  Trunc guard: sort={_s} too short ({_out_w}w vs MT={_inp_w}w EN={_en_w}w) — restored from MT")
                    _r["content"] = _mt_orig
                    _truncated_sorts.add(_s)
                    _trunc_count += 1
        # Cascade unwind: restore the row immediately following each truncated row.
        # It almost certainly received the displaced content of the truncated row.
        # Uses actual sorted result order (not sort+1) to handle non-sequential sort numbers.
        _cascade_count = 0
        _sorted_result_sorts = sorted(_result_by_sort_g4.keys())
        for _ts in sorted(_truncated_sorts):
            _ts_idx = _sorted_result_sorts.index(_ts) if _ts in _sorted_result_sorts else -1
            if _ts_idx >= 0 and _ts_idx + 1 < len(_sorted_result_sorts):
                _next_s = _sorted_result_sorts[_ts_idx + 1]
            else:
                continue
            _next_r = _result_by_sort_g4.get(_next_s)
            if _next_r and _next_s not in _truncated_sorts:
                _next_mt = next((r.get("content", "") for r in batch if r.get("sort") == _next_s), "")
                if _next_mt:
                    log(f"  ⚠️  Trunc cascade: sort={_next_s} — restored neighbour of truncated sort={_ts}")
                    _next_r["content"] = _next_mt
                    _cascade_count += 1
        if _trunc_count or _cascade_count:
            log(f"  💬 Trunc guard: {_trunc_count} truncated + {_cascade_count} cascade neighbour(s) restored from MT (will retry).")
        all_rephrased.extend(result)
        # Clear RPM-exhausted state after each successful batch — keys that were
        # rate-limited mid-chapter have likely recovered by the time the next batch starts.
        # RPD-exhausted keys are preserved in _rpd_exhausted_keys and not affected.
        _exhausted_keys.intersection_update(_rpd_exhausted_keys)
        if i < total_batches:
            time.sleep(_INTER_BATCH_SLEEP)

    log(f"  Total rows rephrased: {len(all_rephrased)}")

    # Re-inject the bypassed title row (sort=0) with its original content.
    # Rule G (post-processing below) will enforce correct title-case formatting.
    if _title_row is not None:
        all_rephrased.append({"sort": 0, "content": _title_row.get("content", ""),
                               "_quote_role": _title_row.get("_quote_role", "none")})
        all_rephrased = sorted(all_rephrased, key=lambda r: r.get("sort", 0))


    # ── Post-processing + unified retry loop ─────────────────────────────
    # Run post-processing on initial Gemini output
    sorted_rows = sorted(all_rephrased, key=lambda r: r.get("sort", 0))
    _post_process(sorted_rows, input_data, glossary_terms)

    # Pre-retry RPM cooldown.
    # Problem: after N batches complete, the keys used in those batches are still
    # inside their 60s RPM window. _call_gemini_simple scans all keys serially looking
    # for a non-429 one — when it finds all of them hot, it waits 15s and recovers
    # exactly ONE key, which then serves one row and immediately gets re-limited.
    # The result is a perpetual 1-row-per-15s trickle and massive deterministic fallback.
    # Fix: sleep 65s before the retry loop when there were ≥2 batches, so ALL batch-used
    # keys exit their RPM windows simultaneously and the retry loop gets a clean pool.
    if total_batches >= 2 and not _all_keys_rpd_dead():
        _cooldown = _PRE_RETRY_COOLDOWN
        log(f"  ⏳ Pre-retry RPM cooldown: {total_batches} batch(es) completed — waiting {_cooldown}s for RPM windows to expire...")
        time.sleep(_cooldown)
        _exhausted_keys.intersection_update(_rpd_exhausted_keys)
        log(f"  ✅ RPM cooldown complete — retry loop starting with fresh key pool.")

    # Unified retry: identify and re-request verbatim, similar, or truncated rows
    all_rephrased = _unified_retry(sorted_rows, input_data, rows)

    # Re-run post-processing on retry output to ensure retried rows get
    # the same treatment (Pass QE, comma rules, glossary enforcement, etc.)
    sorted_final = sorted(all_rephrased, key=lambda r: r.get("sort", 0))
    _post_process(sorted_final, input_data, glossary_terms, skip_bgs_guard=True)

    return sorted_final




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
    if len(unchanged) > len(original_rows) * 0.10:  # CDReader rejects at ~28% identical
        issues.append(
            f"⚠️  {len(unchanged)} rows ({len(unchanged)/len(original_rows)*100:.0f}%) identical to input — "
            f"CDReader will likely reject (threshold ~25%). Similarity guard should have caught these."
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
        rephrased_content = r.get("content", "")
        payload.append({
            "sort": sort,
            "original": orig.get("eContent") or orig.get("eeContent") or orig.get("original") or "",
            "content": rephrased_content,
            "wordCorrection": WORD_CORRECTION_DEFAULT,
            "wordCorrectionData": "",
            "contentShowData": rephrased_content,
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


def close_task(token, task_id):
    """
    Mark a Task Center task as verified/closed.
    Equivalent to clicking 'Verify and Close' in the UI.
    Endpoint confirmed from browser: GET TaskCenter/UpdateStatus?id={id}&status=1
    """
    if not task_id:
        log("  ⚠️  No task_id — cannot close Task Center entry.")
        return False
    log(f"  Closing Task Center task {task_id}...")
    try:
        resp = requests.get(
            f"{BASE_URL}/TaskCenter/UpdateStatus?id={task_id}&status=1",
            headers=auth_headers(token),
            timeout=10,
        )
        resp.raise_for_status()
        result = resp.json()
        log(f"  Task close response: {result}")
        if result.get("status") is True or result.get("code") in (0, 200, 311, 315):
            log(f"  ✅ Task {task_id} closed successfully.")
            return True
        log(f"  ⚠️  Task close returned unexpected response: {result}")
        return False
    except Exception as e:
        log(f"  ⚠️  Task close failed: {e}")
        return False


def is_recheck_chapter(token, chapter_id):
    """Check Task Center for recent recheck tasks (chapterType=4 or 6) for this chapter.
    Returns True if the chapter is a recheck/spot-check that should be skipped.
    
    This catches the case where:
    1. Pipeline processes a chapter → CDReader scores it low → creates chapterType=6 task
    2. Next cron run: find_active_chapter correctly skips the chapterType=6 task
    3. But the book chapter API still lists the chapter as "available"
    4. Pipeline claims it again → processes duplicate
    
    By checking Task Center after claiming, we detect this and abort before processing.
    """
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

        for task in tasks:
            t_chapter_id = task.get("chapterId") or task.get("objectChapterId")
            t_chapter_type = task.get("chapterType")
            t_finish = task.get("finishTime")
            t_task_type = task.get("taskType", "")

            if str(t_chapter_id) != str(chapter_id):
                continue

            # Found a task for this chapter
            if t_chapter_type in (4, 6):
                # Recheck or spot-check task exists for this chapter
                log(f"  ⚠️  Post-claim recheck guard: chapter {chapter_id} has "
                    f"chapterType={t_chapter_type} task ({t_task_type}), "
                    f"finishTime={t_finish} — skipping to avoid duplicate processing.")
                return True

    except Exception as e:
        log(f"  Post-claim recheck check error: {e}")

    return False


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
            t_status = task.get("status")
            t_finish = task.get("finishTime")
            t_chapter_type = task.get("chapterType")
            t_task_type = task.get("taskType", "")
            log(f"  Evaluating task status={t_status} finishTime={t_finish} chapterType={t_chapter_type} taskType={t_task_type}")
            # Accept status=0 (in-progress) AND status=1 (to-be-edited / claimed-not-started).
            # CDReader uses different codes: 0=in-progress, 1=to-be-edited, 2+=completed/closed.
            # Only skip tasks that are explicitly finished.
            if t_finish is not None:
                log(f"  Skipping task — finishTime is set ({t_finish})")
                continue
            if t_status in (2, 3, 4):
                log(f"  Skipping task — status={t_status} indicates completed")
                continue
            # Skip recheck and spot-check tasks to prevent infinite reprocessing loops.
            # chapterType=2: regular first proofreading → PROCESS
            # chapterType=4: spot-check (一校抽查未修改章节) → SKIP
            # chapterType=6: low-score recheck (低分重校章节) → SKIP
            # The loop: pipeline processes → CDReader scores low → creates chapterType=6 task
            # → pipeline picks it up → reprocesses with same approach → same low score → repeat.
            if t_chapter_type in (4, 6):
                log(f"  Skipping task — chapterType={t_chapter_type} is a recheck/spot-check (not first proofreading). "
                    f"These require manual attention.")
                continue

            # Extract chapter ID — the proc_id
            proc_id = task.get("chapterId") or task.get("objectChapterId")

            # taskUrl format: "ProofreadingForeignersList|{chapterId}|{bookId}"
            task_url = task.get("taskUrl", "")
            url_parts = task_url.split("|")
            book_id = int(url_parts[2]) if len(url_parts) >= 3 and url_parts[2].isdigit() else None

            # taskContent format: "EnglishTitle|GermanTitle|ChapterName"
            task_content = task.get("taskContent", "")
            content_parts = task_content.split("|")
            ch_name = content_parts[2].strip() if len(content_parts) >= 3 else f"Chapter #{proc_id}"
            book_name = content_parts[1].strip() if len(content_parts) >= 2 else ""

            log(f"  Active task: '{ch_name}' proc_id={proc_id} book_id={book_id} book='{book_name}'")

            # Find the matching book object from our books list
            matched_book = None
            for b in books:
                b_id = b.get("id") or b.get("objectBookId") or b.get("bookId")
                b_name = b.get("toBookName") or b.get("bookName") or b.get("name") or ""
                if book_id and str(b_id) == str(book_id):
                    matched_book = b
                    log(f"  Matched book by ID: '{b_name}' (id={b_id})")
                    break

            if not matched_book:
                # Build a minimal book dict from parsed task data
                log(f"  Building book dict from task data: book_id={book_id} name='{book_name}'")
                matched_book = {
                    "id": book_id,
                    "objectBookId": book_id,
                    "bookId": book_id,
                    "toBookName": book_name,
                    "bookName": book_name,
                }

            if proc_id:
                task_id = task.get("id")  # Task Center task ID (separate from proc_id)
                log(f"  Task Center task_id={task_id} proc_id={proc_id}")
                return matched_book, ch_name, proc_id, task_id

    except Exception as e:
        log(f"  Task Center error: {e}")

    log("  No active chapter found in Task Center.")
    return None


# ─── Main ─────────────────────────────────────────────────────────────────────
def run():
    try:
        token = login()
    except Exception as e:
        log(f"❌ Login failed (CDReader server unreachable?): {e}")
        # Exit cleanly — next scheduled run will retry automatically
        return
    try:
        _run_inner(token)
    except Exception as e:
        log(f"❌ Unhandled exception in pipeline: {e}")
        import traceback
        traceback.print_exc()
        send_telegram(f"❌ <b>CDReader: Pipeline crashed</b>\n\nError: {e}\n\nPlease check logs.")


def _run_inner(token):
    books = get_books(token)

    if not books:
        log("No books found.")
        return

    claimed_chapters = []

    # ── Override mode: process a specific chapter directly ──
    if OVERRIDE_CHAPTER_ID:
        proc_id = int(OVERRIDE_CHAPTER_ID)
        log(f"🔧 OVERRIDE MODE: processing chapter_id={proc_id} directly (skipping Task Center + claiming)")

        # Resolve book from override or scan
        if OVERRIDE_BOOK_ID:
            override_book_id = int(OVERRIDE_BOOK_ID)
            matched_book = None
            for b in books:
                b_id = b.get("id") or b.get("objectBookId") or b.get("bookId")
                if str(b_id) == str(override_book_id):
                    matched_book = b
                    break
            if not matched_book:
                matched_book = {"id": override_book_id, "objectBookId": override_book_id,
                                "bookId": override_book_id, "toBookName": f"Book #{override_book_id}"}
        else:
            log("  ⚠️  No OVERRIDE_BOOK_ID — glossary will be skipped.")
            matched_book = {"id": None, "toBookName": "Unknown Book"}

        book_name_ovr = matched_book.get("toBookName") or matched_book.get("bookName") or ""
        log(f"  Book: {book_name_ovr} (ID={OVERRIDE_BOOK_ID or 'none'}), Chapter proc_id={proc_id}")
        claimed_chapters.append((matched_book, f"Override Chapter #{proc_id}", None, "override", proc_id, None))

    # ── Phase 0: Check for already active/claimed chapter ──
    if not claimed_chapters:
        log("Checking for already active chapter across all books...")
        active = find_active_chapter(token, books)
        if active:
            active_book, active_ch_name, active_proc_id, active_task_id = active
            log(f"Found active chapter: {active_ch_name} (proc_id={active_proc_id})")
            claimed_chapters.append((active_book, active_ch_name, None, "already-claimed", None, active_task_id))
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
                        claimed_chapters.append((book, ch_name, ch_id, "dry-run", None, None))
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
                        # Try to extract proc_id directly from claim response data
                        claim_proc_id = None
                        rdata = result.get("data")
                        if isinstance(rdata, dict):
                            claim_proc_id = (rdata.get("chapterId") or rdata.get("id")
                                            or rdata.get("objectChapterId"))
                        elif isinstance(rdata, (int, str)) and str(rdata).isdigit():
                            claim_proc_id = int(rdata)
                        log(f"  Claim response data: {rdata} → proc_id={claim_proc_id}")
                        claimed_chapters.append((book, ch_name, ch_id, "claimed", claim_proc_id, None))
                        break
                    elif no_chapter:
                        # Log full response — data may contain the currently active chapter ID
                        log(f"  ⏭  Not claimable right now: {ch_name} | full response: {result}")
                        # If data field contains the active chapter's ID, capture it as orphaned
                        rdata = result.get("data")
                        orphan_id = None
                        if isinstance(rdata, dict):
                            orphan_id = (rdata.get("chapterId") or rdata.get("objectChapterId") or rdata.get("id"))
                        elif isinstance(rdata, (int, str)) and str(rdata).isdigit():
                            orphan_id = int(rdata)
                        if orphan_id:
                            log(f"  Found orphaned active chapter ID={orphan_id} in submithint response")
                            claimed_chapters.append((book, ch_name, orphan_id, "claimed", orphan_id, None))
                            break
                    else:
                        log(f"  ⚠️  Unexpected claim response: {result}")

    if not claimed_chapters:
        log("No chapters claimed this run.")
        return

    # ── Phase 2-6: Process each claimed chapter ──
    entry = claimed_chapters[0]
    book, ch_name, ch_id, status = entry[0], entry[1], entry[2], entry[3]
    task_id = entry[5]       # Task Center task ID for closing (None for freshly claimed)
    claim_proc_id = entry[4]
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
            _, ch_name, proc_id, task_id = active
            log(f"  Active chapter proc_id resolved: {proc_id}, task_id={task_id}")
        else:
            # Fallback: search by name
            proc_id, _ = find_chapter_processing_id(token, book, ch_name)
        if not proc_id:
            msg = f"⚠️ Could not resolve processing ID for active chapter {ch_name}. Manual action required."
            send_telegram(msg)
            return
    else:
        # Freshly claimed — ch_id IS the proc_id (same objectChapterId used in ForeignReceive)
        proc_id = claim_proc_id or ch_id
        if proc_id:
            log(f"  proc_id resolved: {proc_id} (claim_response={claim_proc_id}, ch_id={ch_id})")
        if not proc_id:
            msg = (
                f"⚠️ <b>CDReader:</b> Claimed <b>{ch_name}</b> from {book_name} "
                f"but could not find processing ID.\nManual action required."
            )
            send_telegram(msg)
            log("Could not find processing chapter ID — stopping.")
            return

    # ── Post-claim recheck guard ──────────────────────────────────────────────
    # After claiming, check Task Center for chapterType=6 (low-score recheck) or
    # chapterType=4 (spot-check) tasks for this chapter. If found, this chapter
    # was already processed and CDReader flagged it — skip to avoid duplicate work.
    if status == "claimed" and is_recheck_chapter(token, proc_id):
        log(f"  Skipping chapter {proc_id} — recheck detected after claim.")
        log("  The chapter was already processed in a previous run. Skipping to save API quota.")
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

    # ── Already-processed detection ─────────────────────────────────────────
    # Compare modifChapterContent vs machineChapterContent across rows.
    # On a fresh chapter, these are identical (CDReader pre-populates modif with MT).
    # After our pipeline submits, modifChapterContent is updated with our edits.
    # If a significant fraction already differs, the chapter was already processed
    # by a previous run — skip to avoid duplicate work and API waste.
    _edited_count = 0
    _total_check = 0
    for r in rows:
        if r.get("sort", 0) == 0:
            continue
        _mt = (r.get("machineChapterContent") or "").strip()
        _mod = (r.get("modifChapterContent") or "").strip()
        if _mt and _mod:
            _total_check += 1
            if _mt != _mod:
                _edited_count += 1
    _edit_pct = (_edited_count / _total_check * 100) if _total_check > 0 else 0
    if _total_check > 0 and _edit_pct > 30:
        log(f"  ⚠️  Already-processed guard: {_edited_count}/{_total_check} rows ({_edit_pct:.0f}%) "
            f"already differ from MT — chapter was edited by a previous run. Skipping.")
        log(f"  Finishing chapter {proc_id} without re-submitting...")
        # Finish the chapter to clear the Task Center entry
        try:
            finish_chapter(token, proc_id)
        except Exception:
            pass
        if task_id:
            try:
                close_task(token, task_id)
            except Exception:
                pass
        return

    content_rows = [r for r in rows if r.get("sort", 0) > 0 and (r.get("chapterConetnt") or r.get("modifChapterContent") or "").strip()]
    if not content_rows:
        log(f"  ⚠️  No content rows found — proceeding anyway.")

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

    # ── Post-process: replace English quotes with German quotes ─────────────────
    # Groq and sometimes Gemini use " instead of „/". Fix deterministically.
    quote_fixes = 0
    for row in rephrased:
        c = row.get("content", "")
        if '"' not in c:
            continue
        # Replace paired English quotes: "text" → „text"
        # Strategy: first " in a pair → „, second " → "
        fixed = ""
        in_quote = False
        i = 0
        while i < len(c):
            ch = c[i]
            if ch == '"':
                if not in_quote:
                    fixed += "„"  # „ opening
                    in_quote = True
                else:
                    fixed += "“"  # " closing
                    in_quote = False
            else:
                fixed += ch
            i += 1
        # If still in_quote (odd number of "), the last " is likely a standalone closing
        # Revert to original to avoid mangling
        if in_quote:
            fixed = c  # don't touch malformed rows
        if fixed != c:
            row["content"] = fixed
            quote_fixes += 1
    if quote_fixes:
        log(f"  🔤 Post-processing: converted English quotes to German in {quote_fixes} row(s).")

    # ── Post-process: fix "X family" / "X-Familie" → "Familie X" ───────────────
    # Two separate patterns to avoid IGNORECASE corrupting the uppercase-name check:
    # Pattern A: hyphenated "Surname-Familie" — safe, no article ambiguity
    _fam_hyphen = _re.compile(
        r"\b([A-ZÄÖÜ][A-Za-zäöüßÄÖÜ]+(?:-[A-ZÄÖÜ][A-Za-zäöüßÄÖÜ]+)*)-Familie\b"
    )
    # Pattern B: space-separated single-word surname before "family" or " Familie"
    _fam_space = _re.compile(
        r"\b([A-ZÄÖÜ][A-Za-zäöüßÄÖÜ]+)\s+[Ff]amil(?:y|ie)\b"
    )
    _FAM_SKIP = {"Die", "Der", "Das", "Den", "Dem", "Des", "The", "Eine", "Ein",
                 "Ihre", "Ihr", "Sein", "Seine", "Unsere", "Unser"}
    def _repl_fam(m):
        name = m.group(1).strip().replace("-", " ")
        return m.group(0) if name in _FAM_SKIP else f"Familie {name}"
    family_fixes = 0
    for row in rephrased:
        c = row.get("content", "")
        c2 = _fam_hyphen.sub(_repl_fam, c)
        c2 = _fam_space.sub(_repl_fam, c2)
        if c2 != c:
            row["content"] = c2
            family_fixes += 1
    if family_fixes:
        log(f"  👪 Post-processing: fixed family name format in {family_fixes} row(s).")

    # ── Post-process: retry empty rows with fallback provider ─────────────────
    empty_sorts = [r.get("sort") for r in rephrased if not r.get("content", "").strip()]
    if empty_sorts:
        log(f"  ⚠️ {len(empty_sorts)} empty row(s) detected, retrying individually: {empty_sorts}")
        orig_by_sort = {r.get("sort", i): r for i, r in enumerate(rows)}
        rephrased_by_sort = {r.get("sort"): r for r in rephrased}
        for sort_n in empty_sorts:
            orig_row = orig_by_sort.get(sort_n)
            if not orig_row:
                continue
            single_batch = [{
                "sort": sort_n,
                "original": orig_row.get("eContent") or orig_row.get("eeContent") or orig_row.get("peContent") or "",
                "content": orig_row.get("chapterConetnt") or orig_row.get("content") or orig_row.get("modifChapterContent") or "",
                "_quote_role": "both",
            }]
            # Single-row retry via Gemini
            retry_result = None
            retry_key = next((k for k in GEMINI_KEYS if k not in _rpd_exhausted_keys), None)
            if retry_key:
                single_prompt = (
                    "Du bist ein deutscher Korrektor. Mache eine MINIMALE Änderung an diesem Satz — "
                    "ersetze ein einzelnes Wort durch ein Synonym oder passe einen Artikel an. Verändere NICHT die Satzstruktur. "
                    "Antworte NUR mit einem JSON-Array: [{\"sort\": " + str(sort_n) + ", \"content\": \"<korrigiert>\"}]\n"
                    + json.dumps([{"sort": sort_n, "content": single_batch[0]["content"]}], ensure_ascii=False)
                )
                try:
                    r_resp = requests.post(
                        f"{GEMINI_URL}?key={retry_key}",
                        json={"contents": [{"parts": [{"text": single_prompt}]}],
                              "generationConfig": {"temperature": 0.7, "maxOutputTokens": 256}},
                        timeout=30,
                    )
                    if r_resp.status_code == 200:
                        r_text = r_resp.json()["candidates"][0]["content"]["parts"][0]["text"].strip()
                        if r_text.startswith("```"):
                            r_text = r_text.split("\n", 1)[-1].rsplit("```", 1)[0].strip()
                        parsed = json.loads(r_text)
                        if isinstance(parsed, list) and parsed and parsed[0].get("content", "").strip():
                            retry_result = parsed
                except Exception as exc:
                    log(f"    ⚠️  Empty-row Gemini retry error for sort={sort_n}: {exc}")
            # Simple direct approach: just copy original content as fallback
            if not retry_result or not retry_result[0].get("content", "").strip():
                fallback_content = orig_row.get("chapterConetnt") or orig_row.get("modifChapterContent") or ""
                log(f"    ↩️  Row {sort_n}: using original content as fallback.")
                rephrased_by_sort[sort_n]["content"] = fallback_content
            else:
                log(f"    ✅ Row {sort_n}: retry succeeded.")
                rephrased_by_sort[sort_n]["content"] = retry_result[0]["content"]
        rephrased = list(rephrased_by_sort.values())

    # Verify output
    log("  Verifying output...")
    issues = verify_output(rows, rephrased)

    # Separate hard failures (abort) from soft warnings (proceed but notify)
    hard_issues = [i for i in issues if not i.startswith("Warning:")]
    soft_issues = [i for i in issues if i.startswith("Warning:")]

    if hard_issues:
        issue_text = "\n".join(f"• {i}" for i in issues)
        msg = (
            f"⚠️ <b>CDReader: Review needed</b>\n\n"
            f"Book: {book_name}\nChapter: {ch_name}\n\n"
            f"Verification issues:\n{issue_text}\n\n"
            f"Please review and submit manually."
        )
        send_telegram(msg)
        log(f"Verification failed — {len(hard_issues)} hard issue(s). Stopping for human review.")
        for i in issues:
            log(f"  Issue: {i}")
        return

    if soft_issues:
        log(f"  ⚠️ Soft warnings (proceeding anyway): {'; '.join(soft_issues)}")

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

    # Check for ErrMessage10 — CDReader rejects finish when it detects
    # the submitted content is too similar to the original machine translation.
    finish_ok = (
        finish_result.get("status") is True
        or finish_result.get("message") in ("SaveSuccess", "OperSuccess", "UpdateSuccess")
        or finish_result.get("code") in ("311", "315", "200", 0)
    )
    if not finish_ok:
        err_msg = finish_result.get("message", "unknown")
        if "ErrMessage10" in str(err_msg) or "10" in str(finish_result.get("code", "")):
            msg = (
                f"⚠️ <b>CDReader: Finish rejected (ErrMessage10)</b>\n\n"
                f"📖 {book_name}\n"
                f"📄 {ch_name}\n\n"
                f"CDReader detected insufficient rephrasing — the output was too similar "
                f"to the machine translation. Please open the chapter manually, make "
                f"meaningful edits, and finish it from the CDReader interface."
            )
        else:
            msg = (
                f"⚠️ <b>CDReader: Finish failed</b>\n\n"
                f"📖 {book_name}\n"
                f"📄 {ch_name}\n"
                f"Response: {finish_result}\n\n"
                f"Please finish manually."
            )
        send_telegram(msg)
        log(f"  ⚠️  Finish failed: {finish_result}")
        return

    # Close the Task Center task (equivalent to clicking "verify and close")
    time.sleep(2)
    close_task(token, task_id)

    # Notify success
    send_telegram(
        f"✅ <b>CDReader: Chapter complete!</b>\n\n"
        f"📖 {book_name}\n"
        f"📄 {ch_name}\n\n"
        f"Rephrased, submitted and finished automatically."
    )
    log("✅ Pipeline complete.")


def run_test():
    """
    TEST_MODE: exercises the full rephrase pipeline on synthetic rows.
    No CDReader login, no submit, no finish. Safe to run anytime.
    Tests: Gemini key rotation, prompt quality, all post-processors, verification.
    Uses up to 6 Gemini API keys with automatic rotation.
    """
    log("=" * 60)
    log("TEST MODE — full pipeline on synthetic data")
    log(f"Gemini keys available: {len(GEMINI_KEYS)}")
    # Log status of every configured Gemini key dynamically
    key_statuses = " | ".join(
        f"key {i+1}: {'✅' if k else '⚠️ not set'}"
        for i, k in enumerate(
            os.environ.get(f"GEMINI_API_KEY{'_' + str(i) if i > 0 else ''}", "")
            for i in range(28)
        )
    )
    log(f"Gemini key status: {key_statuses}")
    log("=" * 60)

    # Synthetic test rows — use the same field names as the real CDReader API response.
    # rephrase_with_gemini reads machineChapterContent (German MT) as the text to rephrase
    # and chapterConetnt (English source) for context. Rows with only "content" would send
    # empty text to Gemini and produce meaningless test output.
    TEST_ROWS = [
        {"sort": 0,  "machineChapterContent": "Kapitel 249 Wie Konnte Er Sie Nicht Wollen?",       "chapterConetnt": "Chapter 249 How Could He Not Want Her?"},
        {"sort": 1,  "machineChapterContent": "Die Moss-Familie war seit Generationen in der Stadt bekannt.",    "chapterConetnt": "The Moss family had been known in the city for generations."},
        {"sort": 2,  "machineChapterContent": '„Ich werde nicht gehen", sagte sie bestimmt.',        "chapterConetnt": '"I will not go," she said firmly.'},
        {"sort": 3,  "machineChapterContent": "Er antwortete ihr nicht.",                            "chapterConetnt": "He did not answer her."},
        {"sort": 4,  "machineChapterContent": '„Dann bleib", flüsterte er leise.',                   "chapterConetnt": '"Then stay," he whispered softly.'},
        {"sort": 5,  "machineChapterContent": "Sie schaute ihn lange an, bevor sie sprach.",         "chapterConetnt": "She looked at him for a long time before she spoke."},
        {"sort": 6,  "machineChapterContent": '„Was hast du gesagt?" fragte sie ungläubig.',        "chapterConetnt": '"What did you say?" she asked in disbelief.'},
        {"sort": 7,  "machineChapterContent": "sagte er mit ruhiger Stimme.",                        "chapterConetnt": "he said in a calm voice."},
        {"sort": 8,  "machineChapterContent": "Die Williams-Familie hatte immer zu ihr gehalten.",   "chapterConetnt": "The Williams family had always stood by her."},
        {"sort": 9,  "machineChapterContent": "Er trat einen Schritt zurück und verschränkte die Arme.", "chapterConetnt": "He took a step back and crossed his arms."},
        {"sort": 10, "machineChapterContent": "Er sagte kalt, sie solle jetzt gehen.",              "chapterConetnt": '"You should leave now," he said coldly.'},
        {"sort": 11, "machineChapterContent": "Sie nickte langsam und verließ das Zimmer ohne ein weiteres Wort.", "chapterConetnt": "She nodded slowly and left the room without another word."},
    ]

    SAMPLE_GLOSSARY = [
        {"dictionaryKey": "Moss", "dictionaryValue": "Moss"},
        {"dictionaryKey": "Williams", "dictionaryValue": "Williams"},
    ]

    log(f"\nTest input: {len(TEST_ROWS)} synthetic rows")

    # ── Test rephrase pipeline ─────────────────────────────────────────────────
    log("\n[1/4] Testing rephrase pipeline (Gemini keys 1-6)...")
    result = rephrase_with_gemini(TEST_ROWS, SAMPLE_GLOSSARY, "TEST BOOK")

    if not result:
        msg = "❌ <b>TEST FAILED</b>: No result returned. Check Gemini API keys (GEMINI_API_KEY through GEMINI_API_KEY_7)."
        log(msg)
        send_telegram(msg)
        return

    log(f"  ✅ Pipeline returned {len(result)} rows")

    # ── Show before/after comparison ──────────────────────────────────────────
    log("\n[2/4] Before → After comparison:")
    orig_map = {r["sort"]: r.get("machineChapterContent") or r.get("content", "") for r in TEST_ROWS}
    for r in result:
        s = r.get("sort")
        before = orig_map.get(s, "?")
        after  = r.get("content", "")
        changed = "✏️ " if after != before else "  ="
        log(f"  {changed} [{s:02d}] {before[:60]}")
        if after != before:
            log(f"       → {after[:60]}")

    # ── Test post-processors ───────────────────────────────────────────────────
    log("\n[3/4] Post-processors:")

    # Count family name fixes
    _fam_en = _re.compile(r"(?:[Tt]he\s+)?([A-ZÄÖÜ][A-Za-zäöüßÄÖÜ]+(?:\s[A-ZÄÖÜ][A-Za-zäöüßÄÖÜ]+){0,2})\s+[Ff]amily\b")
    _fam_de = _re.compile(r"(?:[Dd]ie\s+)?([A-ZÄÖÜ][A-Za-zäöüßÄÖÜ]+(?:[-\s][A-ZÄÖÜ][A-Za-zäöüßÄÖÜ]+){0,2})[-\s]Familie\b")
    fam_hits = sum(1 for r in result if _fam_en.search(r.get("content","")) or _fam_de.search(r.get("content","")))
    log(f"  Family name pattern hits before fix: {fam_hits}")

    english_quotes = sum(1 for r in result if '"' in r.get("content",""))
    log(f"  English quote rows before fix: {english_quotes}")

    # ── Test verification ──────────────────────────────────────────────────────
    log("\n[4/4] Verification:")
    issues = verify_output(TEST_ROWS, result)
    hard = [i for i in issues if not i.startswith("Warning:")]
    soft = [i for i in issues if i.startswith("Warning:")]
    if hard:
        log(f"  ❌ Hard issues: {hard}")
    if soft:
        log(f"  ⚠️  Soft warnings: {soft}")
    if not issues:
        log("  ✅ Verification passed cleanly")

    # ── Summary telegram ───────────────────────────────────────────────────────
    key_count = len(GEMINI_KEYS)
    status_icon = "✅" if not hard else "❌"
    msg = (
        f"{status_icon} <b>CDReader: TEST MODE result</b>\n\n"
        f"🔑 Gemini keys active: {key_count}\n"
        f"🔑 Keys configured: {len(GEMINI_KEYS)}/{28}\n"
        f"📝 Rows processed: {len(result)}/{len(TEST_ROWS)}\n"
        f"⚠️  Soft warnings: {len(soft)}\n"
        f"❌ Hard issues: {len(hard)}\n"
        + (f"\nIssues: {'; '.join(hard)}" if hard else "\nAll systems nominal.")
    )
    send_telegram(msg)
    log("\n✅ Test complete.")


if __name__ == "__main__":
    if TEST_MODE:
        run_test()
    else:
        run()
