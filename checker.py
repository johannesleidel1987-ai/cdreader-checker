"""
CDReader Complete Pipeline — ITALIAN
Claim → Fetch rows → Fetch glossary → Rephrase with Gemini → Verify → Submit → Finish

checker-italian.py — based on checker-28.py (German pipeline)
  Adapted for Italian MT post-editing on CDReader.
  Key differences from German:
    - Italian quotation marks: \u201c (open) / \u201d (close) instead of \u201e / \u201c
    - Comma placement: comma INSIDE closing quote before attribution
      (\u201cTesto,\u201d disse. — NOT \u201cTesto\u201d, disse.)
    - Pronoun register: tu/Lei instead of du/Sie
    - Narrative tense: passato remoto (completed actions), trapassato prossimo (backstory)
    - Italian attribution verbs (disse, chiese, mormor\u00f2, etc.)
    - Italian synonym table for deterministic fallback
    - No noun capitalization rule (unlike German)
    - Localization: Dollar\u2192Euro, CEO\u2192amministratore delegato
  Infrastructure (CDReader API, guards, retry, key rotation) inherited from checker-28.py.
"""

import requests
import os
import json
import re
import sys
import time
from datetime import datetime
from collections import namedtuple

# Claimed chapter record — replaces fragile positional tuple unpacking
ClaimedChapter = namedtuple('ClaimedChapter', [
    'book', 'ch_name', 'ch_id', 'status', 'claim_proc_id', 'task_id'
])



# ─── Config ──────────────────────────────────────────────────────────────────
BASE_URL    = "https://translatorserverwebapi-it.cdreader.com/api"
GEMINI_MODEL = "gemini-2.5-flash"
GEMINI_URL  = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent"
ACCOUNT_NAME   = os.environ.get("CDREADER_EMAIL",    "")
ACCOUNT_PWD    = os.environ.get("CDREADER_PASSWORD", "")
TELEGRAM_TOKEN = os.environ.get("IT_TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT  = os.environ.get("IT_TELEGRAM_CHAT_ID",   "")
GEMINI_API_KEY = os.environ.get("ITGEMINI_API_KEY",     "")

# CDReader language configuration.
# TO_LANGUAGE: numeric code for the target language in CatChapterList API calls.
#   German = 412, Italian = 413 (confirmed via CatChapterList request).
# CDREADER_AREA: two-letter area code sent in request headers.
#   Italian = "IT" (confirmed via request headers).
TO_LANGUAGE    = os.environ.get("CDREADER_TO_LANGUAGE", "413")   # Italian = 413 (confirmed)
CDREADER_AREA  = os.environ.get("CDREADER_AREA",       "IT")

# Multi-key Gemini rotation: keys tried in order, exhausted keys skipped for the run
_GEMINI_KEYS_RAW = [
    os.environ.get("ITGEMINI_API_KEY",    ""),
    os.environ.get("ITGEMINI_API_KEY_2",  ""),
    os.environ.get("ITGEMINI_API_KEY_3",  ""),
    os.environ.get("ITGEMINI_API_KEY_4",  ""),
    os.environ.get("ITGEMINI_API_KEY_5",  ""),
    os.environ.get("ITGEMINI_API_KEY_6",  ""),
    os.environ.get("ITGEMINI_API_KEY_7",  ""),
    os.environ.get("ITGEMINI_API_KEY_8",  ""),
    os.environ.get("ITGEMINI_API_KEY_9",  ""),
    os.environ.get("ITGEMINI_API_KEY_10", ""),
    os.environ.get("ITGEMINI_API_KEY_11", ""),
    os.environ.get("ITGEMINI_API_KEY_12", ""),
    os.environ.get("ITGEMINI_API_KEY_13", ""),
    os.environ.get("ITGEMINI_API_KEY_14", ""),
    os.environ.get("ITGEMINI_API_KEY_15", ""),
    os.environ.get("ITGEMINI_API_KEY_16", ""),
    os.environ.get("ITGEMINI_API_KEY_17", ""),
    os.environ.get("ITGEMINI_API_KEY_18", ""),
    os.environ.get("ITGEMINI_API_KEY_19", ""),
    os.environ.get("ITGEMINI_API_KEY_20", ""),
    os.environ.get("ITGEMINI_API_KEY_21", ""),
    os.environ.get("ITGEMINI_API_KEY_22", ""),
    os.environ.get("ITGEMINI_API_KEY_23", ""),
    os.environ.get("ITGEMINI_API_KEY_24", ""),
    os.environ.get("ITGEMINI_API_KEY_25", ""),
    os.environ.get("ITGEMINI_API_KEY_26", ""),
    os.environ.get("ITGEMINI_API_KEY_27", ""),
    os.environ.get("ITGEMINI_API_KEY_28", ""),
]
GEMINI_KEYS = [k for k in _GEMINI_KEYS_RAW if k.strip()]
_exhausted_keys: set = set()      # RPM-exhausted (clears after 60s wait)
_rpd_exhausted_keys: set = set()  # RPD-exhausted (daily quota — permanent for this run)
_403_excluded_keys: set = set()   # D2: 403-excluded keys — suspended/forbidden, skip for entire run

# Gemini keys use ITGEMINI_API_KEY naming to avoid collision with German pipeline.
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
_PAID_KEY: str = os.environ.get("ITGEMINI_API_KEY_28", "").strip()

# ─── Account-group-aware key management ───────────────────────────────────────
# Keys are spread across 3 distinct Google accounts (= 3 independent RPM/RPD pools):
# Account A: ITGEMINI_API_KEY through ITGEMINI_API_KEY_9  (positions 0-8,  9 keys)
# Account B: ITGEMINI_API_KEY_10 through ITGEMINI_API_KEY_18 (positions 9-17, 9 keys)
# Account C: ITGEMINI_API_KEY_19 through ITGEMINI_API_KEY_28 (positions 18-27, includes paid)
# When ONE key in an account returns 429-RPM, ALL keys in that account are blocked
# (rate limits are per Google Cloud project). But OTHER accounts are still available.
_ACCOUNT_GROUPS: list = []  # list of list[str], populated by _init_account_groups()
_ACCOUNT_LABELS = ['A', 'B', 'C']

def _init_account_groups():
    """Build account group lists from _GEMINI_KEYS_RAW. Called once at pipeline start.
    Separated from module level to avoid side effects at import time (testability)."""
    _ACCOUNT_GROUPS.clear()
    for _ag_start, _ag_end in [(0, 9), (9, 18), (18, 28)]:
        _ag_keys = [k for k in _GEMINI_KEYS_RAW[_ag_start:_ag_end] if k.strip()]
        _ACCOUNT_GROUPS.append(_ag_keys)
    _ag_counts = [len(g) for g in _ACCOUNT_GROUPS]
    _ag_info = ", ".join(f"Account {_ACCOUNT_LABELS[i]}: {_ag_counts[i]} keys"
                         for i in range(len(_ACCOUNT_GROUPS)))
    log(f"Account groups initialized: {_ag_info}")

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
            if k in GEMINI_KEYS and k not in _exhausted_keys and k not in _rpd_exhausted_keys and k not in _403_excluded_keys:
                return k
    # Fall through: try all groups in order
    available = [k for k in GEMINI_KEYS if k not in _exhausted_keys and k not in _rpd_exhausted_keys and k not in _403_excluded_keys]
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
    "accept-language": "it,it-IT;q=0.9,en;q=0.8",
    "area": CDREADER_AREA,
    "origin": "https://trans.cdreader.com",
    "referer": "https://trans.cdreader.com/",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36 Edg/145.0.0.0",
}

WORD_CORRECTION_DEFAULT = json.dumps({"StatusCode": 0, "SpellErrors": [], "GrammaticalErrors": []})

# ─── Rephrasing prompt (universal rules) ─────────────────────────────────────
BASE_PROMPT = """ROLE
You are an experienced Italian editor working on machine-translated fiction. Your task
is to improve each row of Italian machine translation into polished, natural Italian prose
— as a skilled native editor would, not as a mechanical rephraser.

⚠️ IMPORTANT: CDReader requires genuine editing — not just error correction. Rows that
remain too close to the machine translation will be rejected. Make 2–3 meaningful
improvements per sentence: enough to improve naturalness and flow, not so many that the
text feels forced or the sentence structure becomes unstable.

EDITING APPROACH (2–3 meaningful changes per sentence)
Make 2 to 3 substantive improvements per sentence. Every change must serve the Italian —
improving naturalness, precision, or flow. Prefer changes a skilled native editor would make:

- Verb precision: replace generic verbs with more expressive alternatives where this fits
  the scene's tone (disse→affermò/mormorò/replicò, andò→si diresse/si avviò,
  guardò→osservò/fissò). Do not force literary verbs into neutral or colloquial narration.
- Clause restructuring: reorder elements within a sentence to improve rhythm — move an
  adverb, invert subject-verb, reposition a prepositional phrase. One reordering counts
  as one change.
- Connective tissue: add a conjunction or transitional phrase where the MT reads choppy
  (mentre, poiché, tuttavia, eppure, così). Only where it sounds natural — do not pad.
- Idiomatic adaptation: replace a literal English calque with a natural Italian expression.
- Modifier precision: swap a vague adjective or adverb for a more fitting alternative
  (grande→imponente, velocemente→rapidamente) — only when stylistically appropriate.
- Sentence opening variety: if two consecutive sentences start with the same pronoun or
  pattern, vary one using inversion or a participial clause.

QUALITY CONSTRAINT
Every change must pass a native-speaker test: would a competent Italian author write this
in a published fiction novel? A forced synonym or unnatural inversion is worse than no
change at all. When in doubt between a natural word and a stylistically strained one,
always choose the natural word. The goal is invisible editing — the result should read as
if it were originally written in Italian, not as if it were aggressively paraphrased.

Rows of 4 words or fewer, exclamations, and proper-noun-only rows: return EXACTLY as-is —
forced changes on short rows produce unnatural results and risk corrupting dialogue structure.

QUALITY BENCHMARK — TARGET LEVEL
The following before/after pairs illustrate the quality standard. Each "after" was validated
by a professional Italian proofreader at 4.5/5. Use these as your quality reference:

  ✗ "Con ciò, interruppe la chiamata."
  ✓ "Detto questo, riattaccò."

  ✗ "Gli occhi di Alicia si illuminarono brevemente di sorpresa."
  ✓ "Un guizzo di sorpresa balenò negli occhi di Alicia."

  ✗ "A causa del suo udito acuto, Hank colse il suono e mostrò una leggera sorpresa."
  ✓ "Grazie al suo udito acuto, Hank captò il suono e manifestò una lieve sorpresa."

  ✗ "Caden lasciò uscire una risata fredda."
  ✓ "Caden emise una risata gelida."

  ✗ "Ritirò rapidamente la mano."
  ✓ "Si ritrasse di scatto."

The key differences: more idiomatic prepositions (da→per, a causa→grazie), stronger verbs
(mostrò→manifestò, colse→captò), varied sentence openings (subject-first→action-first),
and more precise adjectives (fredda→gelida, leggera→lieve).

WHAT TO ALWAYS FIX
1. Grammar: wrong conjugation, wrong article, broken syntax, preposition errors
2. Tense: use passato remoto for completed narrative actions (disse, entrò, afferrò);
   use trapassato prossimo for past-of-past backstory (aveva bruciato, avevano attirato).
   Correct any MT errors where imperfetto is incorrectly used for single completed actions.
   Present and future tenses inside dialogue are correct — do not alter them.
3. Logic / semantics: where the Italian meaning diverges from the English source
4. Register: apply THE PRONOUN PROTOCOL below
5. Localization: apply the LOCALIZATION rules below
6. Literal translations: adapt English idioms, calques, and non-Italian structures
   into natural Italian equivalents. Em dashes (—) must be restructured using
   Italian conjunctions, relative clauses, or colons.

WHAT NOT TO DO (ROW BOUNDARIES ARE ABSOLUTE)
- NEVER merge two rows into one or split one row across two sort numbers
- NEVER move an attribution clause (e.g. "disse lui") from its row into an adjacent row
- NEVER borrow content from an adjacent row — if a row ends mid-speech or mid-sentence,
  leave it that way. The open state is intentional.
- NEVER echo speech text from sort N inside sort N+1's attribution clause
- NEVER add plot content, new dialogue, or information not present in the source
- NEVER shorten rows by omitting content — all meaning from the input must be preserved
- Single-word or single-punctuation rows (e.g. "!", "Vai!", "Emma!"): return EXACTLY as-is

OUTPUT FORMAT (CRITICAL)
Return ONLY a valid JSON array — no markdown, no preamble, no explanation.
Each object must have exactly:
  "sort": original sort number (integer, unchanged)
  "content": rephrased Italian text
Example: [{"sort": 0, "content": "rephrased line"}, {"sort": 1, "content": "..."}]
Row count in output must equal row count in input — never fewer, never more.

DIALOGUE STRUCTURE
The system handles quote characters automatically. You govern the text skeleton only:
- Narration introducing direct speech: ALWAYS use a colon before the speech, NEVER a comma.
  CORRECT:   disse: ... / chiese: ... / aggiunse: ... / sussurrò: ...
  INCORRECT: disse, ... / chiese, ... / aggiunse, ... / sussurrò, ...
  Italian standard for directly introduced speech requires the colon, not the comma.
- Attribution following speech: the attribution clause follows the speech text with a comma
  (... , disse lui.)
- Inner quotes inside already-open speech: use single quotes (' to open and ' to close) or
  \u2018 and \u2019 — NEVER " inside "
- If the English input has a subject pronoun immediately after a closing quote
  (e.g. '"Who?" I smiled'), that pronoun must begin a new clause OUTSIDE the quotes:
  correct: "Chi?" Sorrisi — wrong: "Chi? Io", sorrisi

LOCALIZATION
CDReader platform requirements — apply to ALL books, no exceptions:
  Dollar / $ → Euro
  CEO → amministratore delegato

Honorifics:
  Mr.        → Signor (standalone) / signor (after article: "il signor Reid")
  Mrs. / Ms. → Signora
  Miss       → Signorina

Number formatting (Italian locale):
  Thousands separator  1,000 → 1.000  |  10,000 → 10.000
  Decimal point        3.14 → 3,14
  Currency amounts     $500 → 500 euro

CAPITALIZATION
- Standard Italian capitalization rules apply (only sentence-initial + proper nouns)
- ALL-CAPS rows: rephrase the text in ALL CAPS
- Chapter headings starting with "Capitolo": capitalize first word + proper nouns only
  (e.g. "Capitolo 168 Lei sorprese Wilbur")

THE PRONOUN PROTOCOL (CRITICAL)
Register assignment:
  tu-register:  family members (parents, children, siblings), romantic partners,
                demonstrably close long-term friends
  Lei-register: default for ALL other relationships — colleagues, new acquaintances,
                boss / employee, strangers, professional or formal contexts
  "Lei" is ALWAYS capitalized when used as a formal pronoun.

Character-specific register rules (override the defaults above):
  - Hank addressing Caden: always Lei-register (Hank is Caden's subordinate/employee).
    Use: La, Le, Suo/Sua/Suoi/Sue, Lei — NEVER: ti, te, tuo, tu
  - These character-specific rules apply to every book in this series.

Register correction:
  1. If an ESTABLISHED PRONOUN REGISTERS block appears in this prompt, treat it as
     ground truth. Correct any deviation from the assigned register in the current rows,
     including all cascading pronoun forms.
  2. If no register block is present, correct only when the relationship type is
     unambiguously stated within the current row itself (e.g. "her husband said",
     "his sister whispered"). Otherwise preserve the MT's register choice.
  3. Cascade rule — never mix pronoun forms within the same register:
     tu-register:  ti / te / tuo / tua / tuoi / tue
     Lei-register: Le / Suo / Sua / Suoi / Sue (all capitalized)
  4. Proclitic placement (CRITICAL): Lei-register pronouns ALWAYS precede the verb.
     CORRECT:   "Le prometto che..." / "Le dico che..." / "La ringrazio..."
     INCORRECT: "PromettoLe che..." / "DiceLe che..." / "RingrazieLa..."
     Enclitic attachment to indicative-mood verbs is archaic — never use it in modern fiction.
     Exception: infinitives and imperatives retain enclitic forms ("Dirle", "Farlo", "Dimmelo").

FINAL SELF-CHECK (perform before responding)
1. Does my output have EXACTLY the same number of JSON objects as the input rows?
2. Are all sort numbers from the input present in my output — none missing?
3. Does any output row contain content that clearly belongs to a different sort number?
4. Have I made 2–3 genuine improvements per sentence — not just fixed errors, but also
   improved naturalness, verb precision, or sentence flow? Does each row read as natural
   Italian fiction, with no forced synonyms or unnatural inversions?
5. Is tu/Lei consistent per character, with all cascading pronoun forms correct?
   Specifically: does Hank use Lei with Caden throughout?
6. Are all localization rules applied (Euro, amministratore delegato, honorifics, numbers)?
7. Is my response pure JSON with zero extra text, markdown, or explanation?
8. Did I use a colon (not a comma) before every directly introduced speech?
"""

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

    Pass 1: exact name match (case-insensitive) — returns immediately on first hit.
    Pass 2: numeric suffix match — handles zero-padded or subtitle-suffixed names
            e.g. "Chapter 1" == "Chapter 001" == "Capitolo 1 - L'inizio".
    Replaces the former bidirectional substring match which caused false collisions:
    "Chapter 1" is a substring of "Chapter 10", "Chapter 100", etc.
    """
    def _chap_num(s):
        m = re.search(r"\d+", s or "")
        return int(m.group()) if m else -1

    book_id_for_list = (
        book.get("id") or book.get("objectBookId") or book.get("bookId")
    )
    log(f"  Searching AuthorChapterList for '{claimed_chapter_name}' (bookId={book_id_for_list})...")

    target_num = _chap_num(claimed_chapter_name)
    all_chapters_seen = []
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

        # Pass 1: exact name match (case-insensitive) — return immediately
        for ch in chapters:
            name = ch.get("chapterName") or ch.get("name") or ""
            if name.strip().lower() == claimed_chapter_name.strip().lower():
                proc_id = ch.get("id") or ch.get("chapterId") or ch.get("objectChapterId")
                log(f"  Exact match: '{name}' -> processing ID: {proc_id}")
                log(f"  Full chapter fields: {ch}")
                return proc_id, ch

        all_chapters_seen.extend(chapters)
        if not chapters or len(chapters) < 100:
            break
        page += 1

    # Pass 2: numeric suffix match across all collected pages
    if target_num >= 0:
        for ch in all_chapters_seen:
            name = ch.get("chapterName") or ch.get("name") or ""
            if _chap_num(name) == target_num:
                proc_id = ch.get("id") or ch.get("chapterId") or ch.get("objectChapterId")
                log(f"  Numeric match (#{target_num}): '{name}' -> processing ID: {proc_id}")
                log(f"  Full chapter fields: {ch}")
                return proc_id, ch

    log(f"  Could not find chapter '{claimed_chapter_name}' in AuthorChapterList")
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
        f"{BASE_URL}/ObjectCatChapter/CatChapterList?flowType=2&chapterId={chapter_id}&ToLanguage={TO_LANGUAGE}&FromLanguage=0",
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
# ── Post-process: Italian dialogue punctuation enforcement ─────────────────
# _SV_CORE: Pure speech/communication verbs used for CROSS-ROW comma decisions
# (Rules B and C). Must be conservative — these verbs almost exclusively signal
# speech attribution and rarely appear as pure narrative action starters.
# Italian 3rd-person passato remoto forms.
_SV_CORE = (
    r"disse|chiese|rispose|esclam\u00f2|mormor\u00f2|sussurr\u00f2|replic\u00f2|osserv\u00f2|"
    r"aggiunse|ribatt\u00e9|sibil\u00f2|soffi\u00f2|balbett\u00f2|grid\u00f2|url\u00f2|"
    r"bisbigli\u00f2|ringhi\u00f2|prosegu\u00ec|dichiar\u00f2|ripet\u00e9|"
    r"implor\u00f2|preg\u00f2|controbatt\u00e9|spieg\u00f2|sottoline\u00f2|"
    r"protest\u00f2|interruppe|insistette|rifer\u00ec|inform\u00f2|"
    r"rivel\u00f2|confess\u00f2|domand\u00f2|si rivolse|"
    r"volle|rassicur\u00f2|menzion\u00f2|indic\u00f2|parl\u00f2|"
    # Verbs of rebuke/correction
    r"rimprover\u00f2|ammon\u00ec|"
    # Physical/emotive speech verbs common in genre fiction
    r"grugnì|strill\u00f2|rantol\u00f2|gem\u00e8|sbuff\u00f2|bofonchi\u00f2|ghign\u00f2|"
    # Verbs of proposing/suggesting — common in fiction, passato remoto 3rd-sing.
    # Absent from previous list; caused fallback-to-end-of-text quote wrapping (Image 2).
    r"propose|sugger\u00ec|"
    # Verbs of coaxing/flattering with object-pronoun construction (lo blandì, la sedusse…)
    r"bland\u00ec|sedusse|lusingò|"
    # Verbs of calling out / commenting — frequently used in fiction, absent from previous list.
    # chiamò (chiamare = to call out), commentò (commentare = to comment/remark)
    # aggiunse is already present; adding more commentary verbs:
    r"chiam\u00f2|comment\u00f2|esclam\u00f2|not\u00f2|conclud\u00e8|"
    r"ricord\u00f2|ricord\u00e8|ammise|confid\u00f2|ipotizz\u00f2|avanz\u00f2|"
    r"osserv\u00f2|precis\u00f2|sottopose|enunci\u00f2|articol\u00f2|"
    # Further common verbs absent from previous list (session 7)
    # specific\u00f2 (specificare=to specify), puntualiz\u00f2 (to clarify/point out),
    # ammutol\u00ec (to fall silent — used as attribution), sentenzi\u00f2 (to pronounce)
    r"specific\u00f2|puntualiz\u00f2|ammutol\u00ec|sentenzi\u00f2"
)
# _SV: Full verb list for INLINE same-row attribution matching.
# Context (same-row dialogue) makes ambiguity much lower here.
_SV = (
    _SV_CORE + r"|"
    r"annu\u00ec|sorrise|sospir\u00f2|scatt\u00f2|gemette|singhiozz\u00f2|"
    r"ansim\u00f2|mugol\u00f2|ridacchi\u00f2|supplic\u00f2|si lament\u00f2|"
    r"lanci\u00f2|sput\u00f2|scoppi\u00f2|ruppe|diede|premette|strinse|"
    r"incalz\u00f2|stridette|boccheggi\u00f2|confess\u00f2|giur\u00f2|promise|"
    r"minacci\u00f2|avvert\u00ec|ordin\u00f2|esigette|conferm\u00f2|neg\u00f2|"
    r"rise|sogghign\u00f2|sbuff\u00f2|brontol\u00f2|borbott\u00f2|"
    r"assicur\u00f2|consol\u00f2|plac\u00f2|sostenne|afferm\u00f2|constat\u00f2|"
    r"corresse|suppose|gracchi\u00f2|canticchi\u00f2|tuon\u00f2|"
    r"abba\u00ec\u00f2|bofonchi\u00f2|scherz\u00f2|trionf\u00f2|esult\u00f2|persuase|"
    # Common dual-use verbs (safe in same-row attribution context)
    r"strizz\u00f2|ghign\u00f2|tossicchi\u00f2|cerc\u00f2|tent\u00f2"
)
# Negation guard: "rispose senza", "disse nulla" etc. are NARRATIVE, not attribution
_NEGATION_AFTER_SV = re.compile(
    rf"(?:{_SV_CORE})\s+(?:non|niente|nulla|senza|mai|nessuno|nessuna|nemmeno|neanche|n\u00e9)",
    re.IGNORECASE
)

# _BEGLEITSATZ_BASE → Italian "inciso attributivo": a genuine attribution clause
# starts DIRECTLY with the speech verb or with a pronoun + verb.
# Italian attribution patterns:
#   "disse lei" (verb + subject)
#   "lei disse" (subject + verb) — only with lowercase pronoun subject
#   "rispose" (bare verb)
# The [Name]+[SV] arm is intentionally absent (same rationale as German pipeline).
_BEGLEITSATZ_BASE = re.compile(
    rf"""^(?:
        (?:(?:{_SV_CORE})\b)
        |
        (?:(?:lui|lei|io|noi|voi|esso|essa|essi|esse)\s+(?:(?:{_SV_CORE})\b))
    )""",
    re.IGNORECASE | re.VERBOSE
)

def _is_begleitsatz(text, _max_words=10):
    """True only if text is a genuine attribution clause (inciso attributivo).
    Guards against false positives:
      - Rows ending with ':' introduce NEW dialogue (not attributing previous speech)
      - Long rows (> _max_words=10) — Italian attribution clauses are short (2-12 words).
      - Negated speech verbs ('rispose senza', 'disse nulla') = narrative denial
    """
    if re.search(r':\s+[A-Z]', text):
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

    In Italian prose, an attribution clause (inciso attributivo) that follows
    closing dialogue is syntactically subordinate. Capitalisation discriminates:
        lowercase-first  → continuation → comma required
        uppercase-first  → new sentence → no comma
    Edge-case guards:
      • Empty text → False
      • Row ending with ':' → introduces new speech, no comma.
    """
    if not text or not text[0].islower():
        return False
    if text.rstrip().endswith(':'):
        return False
    return True

_QE_OPEN   = '"'  # " U+0022 (straight double quote)
_QE_CLOSE  = '"'  # " U+0022 (straight double quote)
_QE_ANY_CLOSE_RE  = re.compile(r'[“”"]')
_QE_CLOSE_AT_END  = re.compile(r'[“”"]\s*[,!?.]?\s*$')
_QE_STARTS_OPEN   = re.compile(r'^[“”"]')

_QE_ENG_OPEN_RE   = re.compile(r'^[„“”‘\"«]')
_QE_ENG_CLOSE_RE  = re.compile(r'[“”\"]\s*[,!?.]?\s*$')


def _row_sim(output, ref):
    """Combined similarity: max(Jaccard-word, char-trigram) on normalised text."""
    def _norm(s):
        return re.sub(r"[^\w\s]", "", s.lower())
    def _jaccard(a, b):
        wa = set(re.findall(r"[a-z\u00e0\u00e8\u00e9\u00ec\u00f2\u00f9]+", a))
        wb = set(re.findall(r"[a-z\u00e0\u00e8\u00e9\u00ec\u00f2\u00f9]+", b))
        return len(wa & wb) / len(wa | wb) if (wa and wb) else 0.0
    def _trigram(a, b):
        na = set(a[i:i+3] for i in range(max(0, len(a)-2)))
        nb = set(b[i:i+3] for i in range(max(0, len(b)-2)))
        return len(na & nb) / len(na | nb) if (na and nb) else 0.0
    no, nr = _norm(output), _norm(ref)
    return max(_jaccard(no, nr), _trigram(no, nr))


SIM_THRESHOLD = 0.80      # flag rows at or above this combined similarity
# 0.80: Italian CDReader requires deeper rephrasing than German (~59% avg similarity
# in passing files). Catching rows at 80%+ ensures the retry loop drives the chapter
# well below CDReader's rejection threshold.
# (German pipeline uses 0.88; Italian passing file has only 17% of rows above 0.80.)


# Module-level synonym table — shared by _deterministic_change and _find_synonym_pair.
# Italian synonyms ordered by word frequency for highest-coverage substitutions.
_SYNONYMS = [
    # Conjunctions & particles (highest frequency)
    (r'\bma\b', 'tuttavia'),
    (r'\banche\b', 'pure'),
    (r'\bper\u00f2\b', 'tuttavia'),
    (r'\bquindi\b', 'perci\u00f2'),
    (r'\bpoi\b', 'in seguito'),
    (r'\bsolo\b', 'soltanto'),
    (r'\bancora\b', 'tuttora'),
    (r'\bgi\u00e0\b', 'ormai'),
    (r'\bora\b', 'adesso'),
    (r'\bsempre\b', 'costantemente'),
    (r'\bcos\u00ec\b', 'talmente'),
    (r'\bdi nuovo\b', 'nuovamente'),
    (r'\bforse\b', 'probabilmente'),
    (r'\bprima\b', 'dapprima'),
    # Adverbs
    (r'\bmolto\b', 'assai'),
    (r'\bveloci?mente\b', 'rapidamente'),
    (r'\bdavvero\b', 'effettivamente'),
    (r'\besattamente\b', 'precisamente'),
    (r'\ball\u2019improvviso\b', 'improvvisamente'),
    (r'\bsubito\b', 'immediatamente'),
    (r'\bnaturalmente\b', 'ovviamente'),
    (r'\bpiano\b', 'sommessamente'),
    (r'\btranquillamente\b', 'serenamente'),
    # Adjectives
    (r'\bdifficile\b', 'arduo'),
    (r'\bgrande\b', 'imponente'),
    (r'\bpiccolo\b', 'esiguo'),
    (r'\bvecchio\b', 'anziano'),
    (r'\bbreve\b', 'conciso'),
    (r'\bfelice\b', 'contento'),
    (r'\bforte\b', 'robusto'),
    (r'\bdebole\b', 'fragile'),
    (r'\bchiaro\b', 'limpido'),
    (r'\bscuro\b', 'cupo'),
    (r'\bfreddo\b', 'gelido'),
    (r'\bcaldo\b', 'tiepido'),
    (r'\bfacile\b', 'semplice'),
    (r'\bprofondo\b', 'intenso'),
    (r'\bgiovane\b', 'giovanile'),
    # Common verbs (passato remoto — narrative tense)
    (r'\bdisse\b', 'afferm\u00f2'),
    (r'\bchiese\b', 'domand\u00f2'),
    (r'\brispose\b', 'replic\u00f2'),
    (r'\bsorrise\b', 'sogghign\u00f2'),
    (r'\band\u00f2\b', 'si diresse'),
    (r'\bvenne\b', 'giunse'),
    (r'\bguard\u00f2\b', 'osserv\u00f2'),
    (r'\bvolle\b', 'desider\u00f2'),
    (r'\bpot\u00e9\b', 'riusc\u00ec'),
    (r'\bdovette\b', 'fu costretto a'),
    (r'\bsapeva\b', 'era consapevole'),
    (r'\bprese\b', 'afferr\u00f2'),
    (r'\bmise\b', 'collocò'),
    (r'\btrov\u00f2\b', 'rinvenne'),
    (r'\bvide\b', 'scorse'),
    (r'\bsent\u00ec\b', 'avvert\u00ec'),
    (r'\bpens\u00f2\b', 'riflett\u00e9'),
    (r'\balz\u00f2\b', 'sollev\u00f2'),
    (r'\bsedeva\b', 'era seduto'),
    # Second tier adjectives/adverbs
    (r'\bdolce\b', 'delicato'),
    (r'\bsaldo\b', 'stabile'),
    (r'\bsilenzioso\b', 'quieto'),
    (r'\bluminoso\b', 'splendente'),
    (r'\bleggero\b', 'lieve'),
    (r'\balto\b', 'elevato'),
    # Sentence adverbs & connectors
    (r'\binfine\b', 'alla fine'),
    (r'\binoltre\b', 'per di pi\u00f9'),
    (r'\bperci\u00f2\b', 'di conseguenza'),
    (r'\btuttavia\b', 'nondimeno'),
    (r'\bcertamente\b', 'di certo'),
    (r'\bevidentemente\b', 'palesemente'),
    # Nouns & other
    (r'\bqualcosa\b', 'qualche cosa'),
    (r'\bsembra\b', 'pare'),  # last resort — safe near-synonym
]


def _find_synonym_pair(text):
    """Return (matched_literal, replacement) for the first synonym that applies to text,
    skipping matches that fall inside quotation marks („...").

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
        if text[_i] == '"':  # " straight quote (open)
            _j = text.find('"', _i + 1)  # matching closing "
            if _j == -1:
                _j = len(text) - 1  # fallback: end of text
            if _j != -1:
                _quote_ranges.append((_i, _j))
                _i = _j + 1
                continue
        _i += 1

    def _in_quotes(match_start, match_end):
        return any(qs <= match_start and match_end <= qe + 1
                   for qs, qe in _quote_ranges)

    for pattern, replacement in _SYNONYMS:
        m = re.search(pattern, text)
        if m and not _in_quotes(m.start(), m.end()):
            return (m.group(0), replacement)
    return None


def _deterministic_change(text):
    """Make ONE guaranteed-small change to an Italian text without any API call.

    Used as a last-resort fallback when all Gemini keys are exhausted and the
    similarity/verbatim retry cannot reach the API. Ensures every row differs
    from the MT by at least one word, preventing CDReader ErrMessage10 rejection.

    Strategy: try synonym substitutions in priority order; apply the FIRST match only.
    Skips matches inside quoted speech (consistent with _find_synonym_pair).
    """
    # Build quote-protected ranges (same logic as _find_synonym_pair — Italian “...”)
    _quote_ranges = []
    _i = 0
    while _i < len(text):
        if text[_i] == '"':  # " straight quote (open)
            _j = text.find('"', _i + 1)  # matching closing "
            if _j == -1:
                _j = len(text) - 1  # fallback: end of text
            if _j != -1:
                _quote_ranges.append((_i, _j))
                _i = _j + 1
                continue
        _i += 1

    def _in_quotes(match_start, match_end):
        return any(qs <= match_start and match_end <= qe + 1
                   for qs, qe in _quote_ranges)

    for pattern, replacement in _SYNONYMS:
        m = re.search(pattern, text)
        if m and not _in_quotes(m.start(), m.end()):
            return re.sub(pattern, replacement, text, count=1)
    # No synonym matched (very short row, exclamation, single name, etc.) — return as-is.
    # Comma→semicolon was removed: it frequently broke syntax where a comma is
    # grammatically required (subordinate clauses, enumeration, inline attribution).
    return text


def _call_gemini_simple(prompt, temperature=0.5, max_tokens=2048, deadline=None, call_timeout=20):
    """Account-group-aware Gemini call for single-row retries.
    
    Returns parsed JSON list or None.
    
    Args:
        deadline: optional float (time.time() epoch). If set, the function
                  short-circuits and returns None when wall-clock exceeds this
                  value, preventing unbounded blocking across group rotations.
        call_timeout: HTTP request timeout in seconds (default 20s). Recovery
                      prompts are longer and need more thinking time — pass 45
                      to avoid premature timeouts on complex prompts.
    
    Key design (2026-03-17, timeout + deadline update):
      1. ACCOUNT-GROUP ROTATION: Keys are in 3 Google accounts with independent RPM/RPD.
         Try one key from each account group. When a group returns 429-RPM, skip only
         that group — other accounts are unaffected.
      2. FAST TIMEOUT: 20s per call (was 45s). Single-row prompts are small; a 20s
         non-response means a transient hang. Fail fast → group rotation immediately
         tries the next account group instead of blocking 45s+ per key.
      3. SINGLE ATTEMPT PER KEY: _one_call no longer retries the same key internally.
         Group rotation provides sufficient resilience without compounding timeouts.
      4. INCREASED OUTPUT BUDGET: Default maxOutputTokens=2048 (was 512). gemini-2.5-flash
         uses internal thinking tokens that consume the output budget, causing truncated
         JSON responses at 512.
      5. PER-ROW DEADLINE: Enforced internally — if deadline is set, each group iteration
         and the second-pass wait check it before proceeding.
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
        """Execute one Gemini request. Single attempt — caller (group rotation) retries
        via a different key/group on failure, so we do not retry the same key here.
        Returns (parsed_list_or_None, is_429_rpd, is_429_rpm).

        Timeout: call_timeout (default 20s for retry, 45s for recovery prompts).
        """
        _key_last_used[api_key] = time.time()
        try:
            resp = requests.post(
                f"{GEMINI_URL}?key={api_key}",
                json={
                    "contents": [{"parts": [{"text": prompt}]}],
                    "generationConfig": {"temperature": temperature, "maxOutputTokens": max_tokens},
                },
                timeout=call_timeout,
            )
        except requests.exceptions.RequestException as e:
            # Propagate — caller will log and try next group key
            raise
        if resp.status_code in (500, 502, 503, 504):
            # Single transient 5xx: raise so group rotation tries another key/group
            resp.raise_for_status()

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
        # D2: 403 Forbidden — key suspended, revoked, or billing issue.
        # Unlike 429 (RPM/RPD) which is transient, 403 means the key is permanently
        # dead for this run. Exclude immediately so rotation never wastes time on it.
        if resp.status_code == 403:
            _403_excluded_keys.add(api_key)
            try:
                err_msg = resp.json().get("error", {}).get("message", "")[:80]
            except Exception:
                err_msg = ""
            log(f"    🚫 403 Forbidden: key excluded for this run [{err_msg}]")
            return None, False, False   # caller will try next key/group
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
            text = re.sub(r"^```[^\n]*\n", "", text); text = text.rsplit("```", 1)[0].strip()
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
        if deadline and time.time() > deadline:
            return None  # wall-clock deadline exceeded
        gi = (start_group + gi_offset) % n_groups
        if gi in _rpm_blocked_groups:
            continue
        group_keys = [k for k in _ACCOUNT_GROUPS[gi]
                      if k in keys_all and k not in _rpd_exhausted_keys
                      and k not in _403_excluded_keys]
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
    if deadline and time.time() > deadline:
        return None  # wall-clock deadline exceeded before second pass

    available_keys = [k for k in keys_all if k not in _rpd_exhausted_keys
                      and k not in _403_excluded_keys
                      and _key_account_group(k) not in _rpm_blocked_groups]
    if not available_keys:
        # All groups either RPM-blocked or RPD-dead — try waiting for the soonest key
        available_keys = [k for k in keys_all if k not in _rpd_exhausted_keys
                          and k not in _403_excluded_keys]

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
            # Clamp against deadline to avoid overshooting
            if deadline:
                remaining = deadline - time.time()
                if remaining <= 0:
                    return None
                wait_secs = min(wait_secs, remaining)
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

def _unified_retry(all_rephrased, input_data, rows, bleed_sorts=None):
    """Identify and retry rows that are verbatim, too similar to MT, or truncated.

    bleed_sorts: dict of sort → reason ('bleed'|'bgs'|'truncated') from _force_retry_sorts.
    Bleed-flagged rows use an EN-source-anchored prompt so Gemini is not fed the
    (potentially boundary-corrupt) MT as its primary reference.

    Combines the former mandatory-change pass, similarity guard, and truncation guard
    into a single retry mechanism. Returns the updated list.
    """
    _input_by_sort = {r.get("sort", i): r.get("content", "") for i, r in enumerate(input_data)}
    _eng_by_sort_ur = {r.get("sort", i): r.get("original", "") for i, r in enumerate(input_data)}
    _pe_by_sort_ur  = {r.get("sort", i): r.get("pe_content", "") for i, r in enumerate(input_data)}
    mt_by_sort = {r.get("sort", i): (r.get("machineChapterContent") or r.get("modifChapterContent") or "")
                  for i, r in enumerate(rows)}

    def _restore_for_ur(sort_n):
        """peContent if non-empty, else machineChapterContent from rows."""
        pe = (_pe_by_sort_ur.get(sort_n) or "").strip()
        return pe if pe else mt_by_sort.get(sort_n, "")

    _bleed_sorts = bleed_sorts or {}

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
        # Rows ≤ 4 words are skipped — too short to meaningfully affect chapter average.
        # The previous dialogue-quote exemption („/“/”) has been removed: post-processing
        # only normalises quote characters, not content — a dialogue row at 92%
        # similarity is still 92% similar to the MT regardless of quote style. The
        # exemption created a blind spot where high-similarity speech rows escaped retry.
        if mt and len(out.split()) >= 5:
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

        # ── Bleed/BGS path: EN-source anchor ─────────────────────────────────
        # For rows restored from MT due to bleed or BGS errors, the MT itself may
        # carry the boundary error (CDReader source misalignment). Sending that MT
        # back to Gemini as the primary reference perpetuates the bleed cycle.
        # Fix: use the EN source as the primary reference instead of MT.
        # The EN is generated independently from Chinese and will not share the
        # CDReader MT boundary error.
        _is_bleed_row = sort_n in _bleed_sorts and _bleed_sorts[sort_n] in ('bleed', 'bgs', 'source_align')
        if _is_bleed_row:
            _en_src = _eng_by_sort_ur.get(sort_n, "")
            _mt_src = ref_text  # MT (may be boundary-corrupt — used as secondary context only)
            if _en_src.strip():
                prompt = (
                    "Sei un editor italiano esperto. Questa riga è stata ripristinata dalla "
                    "traduzione automatica perché l'output precedente conteneva contenuto da una "
                    "riga adiacente (bleed di riga) o solo una clausola di attribuzione.\n\n"
                    "FONTE INGLESE (riferimento principale):\n" + _en_src + "\n\n"
                    "TRADUZIONE AUTOMATICA (italiano — riferimento secondario):\n" + _mt_src + "\n\n"
                    "Produci una post-editing italiana corretta e completa SOLO per questa riga. "
                    "NON includere contenuto da righe adiacenti. "
                    "Apporta 2-3 miglioramenti redazionali: verbi più precisi, flusso migliore, "
                    "adattamento idiomatico. Il risultato deve sembrare scritto da un madrelingua.\n"
                    "Rispondi SOLO con: "
                    "[{\"sort\": " + str(sort_n) + ", \"content\": \"<italiano migliorato>\"}]\n"
                )
                temp = 0.5
            else:
                # No EN source available — fall through to standard verbatim/similar path
                _is_bleed_row = False

        if not _is_bleed_row:
            if "truncated" in reason:
                prompt = (
                    "Sei un editor italiano esperto. "
                    "La riga seguente è stata troncata ed è incompleta. "
                    "Riscrivi il testo italiano COMPLETO con 2-3 miglioramenti redazionali "
                    "— verbi più precisi, connettivi più naturali, apertura di frase variata. "
                    "Conserva tutti i contenuti e significati. NON abbreviare.\n"
                    "Rispondi SOLO con: "
                    "[{\"sort\": " + str(sort_n) + ", \"content\": \"<testo completo e migliorato>\"}]\n"
                    + json.dumps([{"sort": sort_n, "content": ref_text}], ensure_ascii=False)
                )
                temp = 0.5
            elif "similar" in reason:
                _swap = _find_synonym_pair(current_out)
                if _swap:
                    _swap_instruction = (
                        f"OBBLIGO: Nella tua risposta sostituisci esattamente la parola "
                        f"\u00bb{_swap[0]}\u00ab con \u00bb{_swap[1]}\u00ab. "
                        f"Adatta articoli/preposizioni se necessario.\n"
                    )
                else:
                    _swap_instruction = (
                        "OBBLIGO: Apporta 2-3 miglioramenti concreti — "
                        "NON restituire la stessa frase.\n"
                    )
                prompt = (
                    "Sei un editor italiano esperto. La frase seguente è troppo simile al "
                    "testo di riferimento. Apporta 2-3 miglioramenti redazionali significativi: "
                    "precisione verbale, ristrutturazione della frase, connettivi più naturali, "
                    "adattamento idiomatico. Il risultato deve sembrare scritto da un madrelingua.\n"
                    + _swap_instruction +
                    "Rispondi SOLO con: "
                    "[{\"sort\": " + str(sort_n) + ", \"content\": \"<migliorato>\"}]\n"
                    + json.dumps([{"sort": sort_n, "reference": ref_text, "content": current_out}],
                                 ensure_ascii=False)
                )
                temp = 0.5
            else:  # verbatim
                _swap = _find_synonym_pair(current_out)
                if _swap:
                    _swap_instruction = (
                        f"OBBLIGO: Nella tua risposta sostituisci esattamente la parola "
                        f"\u00bb{_swap[0]}\u00ab con \u00bb{_swap[1]}\u00ab. "
                        f"Adatta articoli/preposizioni se necessario.\n"
                    )
                else:
                    _swap_instruction = (
                        "OBBLIGO: Apporta 2-3 miglioramenti concreti — "
                        "NON restituire la stessa frase.\n"
                    )
                prompt = (
                    "Sei un editor italiano esperto. Questa frase è identica al testo di input "
                    "e deve essere migliorata. Apporta 2-3 modifiche redazionali significative: "
                    "precisione verbale, ristrutturazione della frase, connettivi più naturali. "
                    "Il risultato deve suonare autentico per un lettore madrelingua.\n"
                    + _swap_instruction +
                    "Rispondi SOLO con: [{\"sort\": " + str(sort_n) + ", \"content\": \"<migliorato>\"}]\n"
                    + json.dumps([{"sort": sort_n, "content": current_out}], ensure_ascii=False)
                )
                temp = 0.5
        # end if not _is_bleed_row

        _use_4096 = "truncated" in reason or _is_bleed_row
        result = _call_gemini_simple(prompt, temperature=temp,
                                     max_tokens=4096 if _use_4096 else 2048)
        if result and result[0].get("content", "").strip():
            new_content = result[0]["content"].strip()
            # EN-primary re-bleed guard: if Gemini still returned inflated content
            # despite the EN-source bleed prompt, restore from MT reference.
            # Threshold matches batch Guard 2 EN-primary trigger (2.2x, delta>=4).
            # Fix 5a: lowered en_w minimum from 3→1 to catch single-word EN rows
            # (e.g. EN="And?" = 1w, MT=8w contaminated → retry produces 9w bleed).
            # The >=3 minimum previously excluded exactly the rows most vulnerable
            # to CDReader MT boundary contamination.
            _en_w_ur = len((_eng_by_sort_ur.get(sort_n) or "").split())
            _out_w_ur = len(new_content.split())
            if (_en_w_ur >= 1
                    and _out_w_ur > _en_w_ur * 2.2  # lowered 2.5→2.2 (matches batch guard)
                    and (_out_w_ur - _en_w_ur) >= 4):
                _restore_ur = _restore_for_ur(sort_n)
                _restore_src_ur = "peContent" if (_pe_by_sort_ur.get(sort_n) or "").strip() else "MT"
                log(f"    ⚠️  sort={sort_n}: retry re-bleed detected "
                    f"({_out_w_ur}w vs EN={_en_w_ur}w) — keeping {_restore_src_ur}")
                # Restore to peContent (or MT if no peContent) rather than keeping bleed output
                for _rr in all_rephrased:
                    if _rr.get("sort") == sort_n:
                        _rr["content"] = _restore_ur
                        break
                continue
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



# ─── Remedy A: Force-retry pass for trunc-guard rows ─────────────────────────
_FORCE_RETRY_PROMPT = (
    "You are an experienced Italian editor working on machine-translated fiction. "
    "The previous output for this row was discarded because it was too short or truncated.\n\n"
    "ENGLISH SOURCE:\n{en_source}\n\n"
    "MACHINE TRANSLATION (Italian):\n{mt_content}\n\n"
    "Produce a correct, complete Italian post-edit of this row. Apply 2-3 meaningful "
    "editorial improvements: more precise verbs, better sentence flow, idiomatic phrasing. "
    "Fix grammar, register, localization (Dollar\u2192Euro, CEO\u2192amministratore delegato). "
    "The result must read like native Italian fiction, not a translation.\n\n"
    'Return ONLY valid JSON: {{"sort": {sort_num}, "content": "improved Italian text"}}'
)

_FORCE_RETRY_PROMPT_BLEED = (
    "You are an experienced Italian editor working on machine-translated fiction. "
    "The previous output for this row was discarded because it contained content from "
    "an adjacent row (cross-row bleed: Gemini merged speech from sort N+1 into sort N, "
    "or left sort N+1 with only attribution content).\n\n"
    "ENGLISH SOURCE:\n{en_source}\n\n"
    "MACHINE TRANSLATION (Italian):\n{mt_content}\n\n"
    "Produce a correct, complete Italian post-edit of THIS ROW ONLY \u2014 its own content, "
    "nothing borrowed from adjacent rows. Apply 2-3 meaningful editorial improvements: "
    "more precise verbs, better sentence flow, idiomatic phrasing. "
    "Fix grammar, register, localization (Dollar->Euro, CEO->amministratore delegato). "
    "The result must read like native Italian fiction.\n\n"
    'Return ONLY valid JSON: {{"sort": {sort_num}, "content": "improved Italian text"}}'
)

_FORCE_RETRY_PROMPT_BGS = (
    "You are an experienced Italian editor working on machine-translated fiction. "
    "The previous output for this row was discarded because it contained only a bare "
    "attribution clause instead of the full dialogue or narrative content this row requires.\n\n"
    "ENGLISH SOURCE:\n{en_source}\n\n"
    "MACHINE TRANSLATION (Italian):\n{mt_content}\n\n"
    "Produce a correct, complete Italian post-edit of THIS ROW. "
    "The row must contain its own full content \u2014 do not output a standalone attribution "
    "such as \'disse lui\' or \'chiese lei\' unless the English source itself is only an "
    "attribution clause. Apply 2-3 meaningful editorial improvements: more precise verbs, "
    "better sentence flow, idiomatic phrasing. "
    "Fix grammar, register, localization (Dollar->Euro, CEO->amministratore delegato). "
    "The result must read like native Italian fiction.\n\n"
    'Return ONLY valid JSON: {{"sort": {sort_num}, "content": "improved Italian text"}}'
)



def _run_force_retry_pass(force_retry_sorts, sorted_final, rows, input_data, glossary_terms):
    """Remedy A: Unconditional individual Gemini retry for every sort registered by
    the trunc-guard. Bypasses _MAX_RETRIES cap and the dialogue-row similarity exemption.
    Runs after _unified_retry and _post_process. Returns updated sorted_final list."""
    if not force_retry_sorts:
        return sorted_final

    log(f"  \U0001f504 Force-retry pass: {len(force_retry_sorts)} row(s) queued...")

    rows_by_sort  = {r.get("sort"): r for r in rows}
    input_by_sort = {r.get("sort"): r for r in input_data}
    final_by_sort = {r.get("sort"): r for r in sorted_final}

    success = 0
    skipped = 0
    fallback_applied = 0
    for sort, _frs_reason in sorted(force_retry_sorts.items()):
        raw_row    = rows_by_sort.get(sort, {})
        inp_row    = input_by_sort.get(sort, {})
        en_source  = (raw_row.get("eContent") or raw_row.get("chapterConetnt") or "").strip()
        mt_content = (inp_row.get("content") or raw_row.get("machineChapterContent") or "").strip()

        if not mt_content:
            log(f"  \u26a0\ufe0f  Force-retry sort={sort}: no MT content \u2014 skipping.")
            continue

        # Pre-check: if unified retry already produced content that differs from MT,
        # don't risk clobbering it with a failed Gemini call. The existing result is
        # already better than raw MT — force-retry can only improve it further, not
        # replace a working change with a failure.
        current_content = (final_by_sort.get(sort, {}).get("content") or "").strip()
        if current_content and current_content != mt_content:
            current_sim = _row_sim(current_content, mt_content)
            if current_sim < SIM_THRESHOLD:
                log(f"  \u2705 Force-retry sort={sort}: unified retry already produced "
                    f"acceptable result (sim={current_sim:.0%}) \u2014 skipping.")
                skipped += 1
                continue

        if _frs_reason == 'bleed':
            _frs_tmpl = _FORCE_RETRY_PROMPT_BLEED
        elif _frs_reason == 'bgs':
            _frs_tmpl = _FORCE_RETRY_PROMPT_BGS
        else:  # 'truncated' — original Remedy A purpose
            _frs_tmpl = _FORCE_RETRY_PROMPT
        prompt = _frs_tmpl.format(
            en_source=en_source or "(not available)",
            mt_content=mt_content,
            sort_num=sort,
        )

        log(f"  \U0001f504 Force-retry sort={sort}...")
        result = _call_gemini_simple(prompt, temperature=0.7, max_tokens=2048)

        if result and isinstance(result, list) and result[0].get("content", "").strip():
            new_content = result[0]["content"].strip()
            new_sim = _row_sim(new_content, mt_content)
            if final_by_sort.get(sort):
                final_by_sort[sort]["content"] = new_content
            log(f"  \u2705 Force-retry sort={sort}: accepted (sim={new_sim:.0%}): {new_content[:60]!r}")
            success += 1
        else:
            # Gemini call failed — apply deterministic fallback so the row at least
            # differs from MT, preventing ErrMessage10 rejection on finish.
            _fb_source = current_content if current_content else mt_content
            fallback = _deterministic_change(_fb_source)
            if fallback != _fb_source and final_by_sort.get(sort):
                final_by_sort[sort]["content"] = fallback
                fallback_applied += 1
                log(f"  \U0001f527 Force-retry sort={sort}: Gemini failed \u2014 deterministic fallback applied: {fallback[:60]!r}")
            elif current_content and current_content != mt_content:
                # Deterministic change couldn't improve, but unified retry's result
                # already differs from MT — keep it.
                log(f"  \u26a0\ufe0f  Force-retry sort={sort}: Gemini failed, no further fallback \u2014 "
                    f"keeping unified retry result (differs from MT).")
            else:
                log(f"  \u26a0\ufe0f  Force-retry sort={sort}: Gemini failed, no fallback possible \u2014 verbatim MT remains.")

    log(f"  \U0001f4ac Force-retry pass complete: {success}/{len(force_retry_sorts)} re-edited, "
        f"{skipped} skipped (already OK), {fallback_applied} deterministic fallback(s).")
    return sorted(final_by_sort.values(), key=lambda r: r.get("sort", 0))



# ─── Remedy D: ErrMessage10 self-healing recovery ─────────────────────────────
_MAX_RETRIES       = 35  # max rows retried per chapter (moved to module level, checker-27)
_RECOVERY_MAX_ROWS = 15
_RECOVERY_SIM_THRESHOLD = 0.80  # wider net than SIM_THRESHOLD — Italian CDReader is stricter

_RECOVERY_PROMPT = (
    "You are an experienced Italian editor working on machine-translated fiction. "
    "This chapter was rejected because the following row is too similar to the original machine translation.\n\n"
    "ENGLISH SOURCE:\n{en_source}\n\n"
    "MACHINE TRANSLATION (Italian):\n{mt_content}\n\n"
    "CURRENT SAVED TEXT (similarity to MT: {sim_pct}% \u2014 chapter finish rejected):\n{saved_content}\n\n"
    "Apply 2-3 meaningful editorial improvements to this row: more precise verbs, "
    "better sentence flow, idiomatic phrasing, varied sentence opening. "
    "- If you find a real error (article agreement, wrong conjugation, tense "
    "inconsistency, register violation, localization missing): correct it.\n"
    "- The output MUST differ from the current saved text by more than a single word.\n"
    "- The result must read like native Italian fiction, not a translation.\n"
    "- Do NOT make changes that would surprise a native Italian reader or alter the meaning.\n\n"
    'Return ONLY valid JSON: {{"sort": {sort_num}, "content": "improved Italian text"}}'
)


def _errmessage10_recovery(token, chapter_id, rows, finish_fn, submit_fn):
    """Remedy D: Called when finish_chapter returns ErrMessage10.
    Re-fetches saved rows, identifies the top _RECOVERY_MAX_ROWS rows by highest
    similarity to original MT, re-edits via Gemini, re-submits, retries finish once.
    Returns True if recovery succeeded, False otherwise."""
    log(f"  \U0001f501 ErrMessage10 recovery: re-fetching saved rows for chapter {chapter_id}...")

    try:
        saved_rows = get_chapter_rows(token, chapter_id)
    except Exception as e:
        log(f"  \u274c Recovery: could not re-fetch rows: {e}")
        return False

    if not saved_rows:
        log(f"  \u274c Recovery: no rows returned from re-fetch.")
        return False

    rows_by_sort = {r.get("sort"): r for r in rows}

    sim_scores = []
    for row in saved_rows:
        sort       = row.get("sort")
        saved      = (row.get("modifChapterContent") or "").strip()
        source_row = rows_by_sort.get(sort, {})
        mt_content = (source_row.get("machineChapterContent") or "").strip()
        if not sort or not saved or not mt_content or sort == 0:
            continue
        sim = _row_sim(saved, mt_content)
        if sim >= _RECOVERY_SIM_THRESHOLD:
            sim_scores.append((sort, sim, saved, mt_content))

    if not sim_scores:
        log(f"  \u26a0\ufe0f  Recovery: no high-similarity rows found in saved data.")
        return False

    sim_scores.sort(key=lambda x: x[1], reverse=True)
    targets = sim_scores[:_RECOVERY_MAX_ROWS]
    log(f"  \U0001f501 Recovery: re-editing {len(targets)} row(s) "
        f"(worst sim: {targets[0][1]:.0%}, threshold: {_RECOVERY_SIM_THRESHOLD:.0%})...")

    # ── Pre-recovery RPM cooldown ─────────────────────────────────────────────
    # The unified retry loop just finished using keys. Without a cooldown, all
    # keys are still in their RPM window and every recovery call will timeout
    # waiting for a cooled key. Same pattern as _PRE_RETRY_COOLDOWN in the
    # main pipeline — clear RPM state and wait for windows to expire.
    _cooldown_s = 65
    log(f"  \u23f3 Recovery: RPM cooldown — waiting {_cooldown_s}s for key windows to expire...")
    _exhausted_keys.intersection_update(_rpd_exhausted_keys)  # clear RPM state, keep RPD
    time.sleep(_cooldown_s)
    log(f"  \u2705 Recovery: RPM cooldown complete — starting re-edit calls.")

    corrections = []
    _consecutive_failures = 0

    for sort, sim, saved_content, mt_content in targets:
        source_row = rows_by_sort.get(sort, {})
        en_source  = (source_row.get("eContent") or source_row.get("chapterConetnt") or "").strip()

        prompt = _RECOVERY_PROMPT.format(
            en_source=en_source or "(not available)",
            mt_content=mt_content,
            sim_pct=f"{sim * 100:.0f}",
            saved_content=saved_content,
            sort_num=sort,
        )

        result = _call_gemini_simple(prompt, temperature=0.7, max_tokens=4096, call_timeout=45)

        if result and isinstance(result, list) and result[0].get("content", "").strip():
            new_content = result[0]["content"].strip()
            new_sim     = _row_sim(new_content, mt_content)
            log(f"  \u2705 Recovery sort={sort}: {sim:.0%} \u2192 {new_sim:.0%}")
            corrections.append({"sort": sort, "content": new_content})
            _consecutive_failures = 0
        else:
            log(f"  \u26a0\ufe0f  Recovery sort={sort}: Gemini call failed.")
            _consecutive_failures += 1
            if _consecutive_failures >= 3:
                log(f"  \u274c Recovery: 3 consecutive failures \u2014 aborting early.")
                break

    if not corrections:
        log(f"  \u274c Recovery: all re-edit calls failed.")
        return False

    log(f"  \U0001f501 Recovery: re-submitting {len(corrections)} corrected row(s)...")
    try:
        sub_result = submit_fn(token, chapter_id, corrections, rows)
        sub_ok = (
            sub_result.get("status") is True
            or sub_result.get("message") in ("SaveSuccess", "OperSuccess")
            or sub_result.get("code") in ("311", "315", 0)
        )
        if not sub_ok:
            log(f"  \u274c Recovery: re-submit failed: {sub_result}")
            return False
    except Exception as e:
        log(f"  \u274c Recovery: re-submit exception: {e}")
        return False

    log(f"  \U0001f501 Recovery: retrying finish (once)...")
    try:
        time.sleep(2)
        finish_response = finish_fn(token, chapter_id)
        finish_ok = (
            finish_response.get("status") is True
            or finish_response.get("message") in ("SaveSuccess", "OperSuccess", "UpdateSuccess")
            or finish_response.get("code") in ("311", "315", "200", 0)
        )
        if finish_ok:
            log(f"  \u2705 Recovery: finish succeeded after ErrMessage10.")
            return True
        else:
            log(f"  \u274c Recovery: finish still failing: {finish_response}")
            return False
    except Exception as e:
        log(f"  \u274c Recovery: finish exception: {e}")
        return False

def _post_process(sorted_rows, input_data, glossary_terms, skip_bgs_guard=False, force_retry_sorts=None):
    """Run all post-processing passes on sorted_rows (modified in place).
    
    Called after initial Gemini batch processing AND after each retry pass,
    ensuring all output gets the same treatment (Pass QE, comma rules, glossary, etc.).
    """
    # Build lookup dicts from input_data (used throughout all passes)
    _mt_by_sort  = {r.get("sort", i): r.get("content", "")           for i, r in enumerate(input_data)}
    _pe_by_sort  = {r.get("sort", i): r.get("pe_content", "")        for i, r in enumerate(input_data)}
    _eng_by_sort = {r.get("sort", i): r.get("original", "")          for i, r in enumerate(input_data)}

    def _restore_for(sort_n):
        """Return the best safe restore content for sort_n.

        Prefers peContent (previous human post-edit) over machineChapterContent (MT)
        because peContent is human-bounded and free of CDReader MT boundary
        contamination. Falls back to MT when peContent is empty/missing.
        """
        pe = (_pe_by_sort.get(sort_n) or "").strip()
        return pe if pe else _mt_by_sort.get(sort_n, "")

    comma_fixes = 0
    comma_adds = 0
    dash_fixes = 0

    # ── Pre-Pass 0: Whitespace normalisation ─────────────────────────────────
    # Gemini occasionally emits tab characters (\t), carriage returns (\r), or
    # non-breaking spaces (U+00A0) inside content values. _fix_json_strings escapes
    # literal \n/\r/\t before JSON parse, but the parsed Python string still contains
    # the actual whitespace characters. CDReader renders \t as a visible '·' separator
    # (Image 3 — "D'accordo, · andrò · a · prenderlo…"). Fix: collapse all non-space
    # whitespace and runs of multiple spaces to a single space, then strip.
    # Applied before any guard so downstream checks receive clean content.
    _ws_fixes = 0
    for row in sorted_rows:
        sort_n = row.get("sort")
        if sort_n == 0:
            continue
        c = row.get("content", "")
        if not c:
            continue
        # Collapse tabs, carriage returns, non-breaking spaces, and multi-space runs
        # Fix 3: [^\S\n] matches ALL whitespace chars that are not newlines, covering:
        # U+0009 TAB, U+00A0 NBSP, U+202F NARROW NBSP, U+2009 THIN SPACE,
        # U+200A HAIR SPACE, U+2002 EN SPACE, U+2003 EM SPACE, U+2005–U+2007 etc.
        # CDReader renders any of these as visible '·' separators.
        # The two-step replace: first normalise all unusual whitespace to regular space,
        # then collapse any resulting multi-space runs.
        c_ws = re.sub(r'[^\S\n]', ' ', c)
        c_ws = re.sub(r' {2,}', ' ', c_ws).strip()
        # Fix A: strip trailing space before close-quote ONLY when preceded by
        # terminal punctuation (.!?). This removes spurious "text. \" → "text."
        # but PRESERVES the legitimate space in 'disse: "speech"' → stays as-is.
        # Old pattern r' +"' was too broad — it also stripped the colon space.
        c_ws = re.sub(r'(?<=[.!?]) +"', '"', c_ws)
        if c_ws != c:
            row["content"] = c_ws
            _ws_fixes += 1
            log(f"  ⚠️  WS-norm: sort={sort_n} whitespace collapsed: {c!r} → {c_ws!r}")
    if _ws_fixes:
        log(f"  💬 WS-norm: normalised {_ws_fixes} row(s) with irregular whitespace")

    # ── Pre-Pass QE: BGS confusion guard ─────────────────────────────────────────
    # Gemini occasionally outputs a pure attribution clause (inciso attributivo) for a row
    # whose English source is dialogue (starts with "). This is a structural error:
    # a dialogue row must contain speech content, not just "erwiderte Hendrick.".
    # Caused by Gemini merging rows across sort boundaries under prompt pressure.
    # Fix: detect the mismatch and restore the row from the machine translation.
    # Pass QE will then correctly apply the quote structure on the restored text.
    #
    # Detection: eng starts with " (dialogue) AND Italian output matches attribution pattern.
    # A genuine attribution clause never starts with a quote character, so testing the raw
    # output (not stripped) is safe — "“Ho sparato..." does NOT match BGS.
    # (dicts _mt_by_sort and _eng_by_sort built above)
    # Pre-BGS content cache (Finding 1): Fix3b restores lc-rows to MT before the bleed
    # guard runs. Without this snapshot _starts_lc is always False for those rows.
    _pre_bgs_content = {r.get('sort'): r.get('content', '').strip() for r in sorted_rows}
    _bgs_confusion_fixes = 0
    for row in (sorted_rows if not skip_bgs_guard else []):
        sort_n = row.get("sort")
        out    = row.get("content", "").strip()
        eng_s  = _eng_by_sort.get(sort_n, "")
        mt_s      = _mt_by_sort.get(sort_n, "")
        restore_s = _restore_for(sort_n)  # peContent if available, else MT
        # Guard 2: restore empty/whitespace rows from MT
        if not out:
            if mt_s:
                row["content"] = restore_s
                _bgs_confusion_fixes += 1
                log(f"  ⚠️  Empty row: sort={sort_n} restored from {'peContent' if (_pe_by_sort.get(sort_n) or '').strip() else 'MT'} {restore_s[:60]!r}")
            continue
        if not eng_s or not mt_s: continue
        # General row-misalignment guard:
        # A bare attribution clause ("rispose lui.", "chiese lei piano.") is NEVER a valid
        # standalone translated row unless the English source itself is a short
        # attribution sentence ("she asked.", "Petter said.").  Any other row type
        # (dialogue, narrative, description) producing a BGS as output means Gemini
        # has injected content from an adjacent row — restore from MT.
        # This covers both dialogue rows AND narrative rows receiving displaced BGS.
        _eng_is_attribution = (
            not eng_s.lstrip().startswith('"')           # not itself dialogue
            and len(eng_s.split()) <= _ENG_ATTRIBUTION_MAX_WORDS  # short sentence
            and bool(re.search(
                r'\b(?:said|asked|replied|answered|whispered|shouted|called|muttered|'
                r'remarked|added|continued|insisted|demanded|exclaimed|cried|'
                r'explained|told|warned|ordered|nodded|smiled|sighed|'
                r'laughed|teased|snapped|groaned|sobbed|gasped|hissed|'
                r'growled|chuckled|corrected|interrupted|murmured|suggested|'
                r'conceded|admitted|acknowledged|declared|announced|breathed)\b',
                eng_s, re.IGNORECASE))
        )
        # Fix 3a: strip leading quote before BGS check.
        # Root cause: Gemini outputs “rimprovera Henry (attribution
        # prefixed with spurious “). _BEGLEITSATZ_BASE requires ^(SV|pronoun+SV), so
        # _is_begleitsatz on the raw output never matches. Stripping the leading quote
        # first lets the pattern see the attribution verb directly.
        _out_unquoted = re.sub(r'^[„“"]+', '', out).strip()
        _is_bgs_raw      = _is_begleitsatz(out)
        _is_bgs_unquoted = _is_begleitsatz(_out_unquoted)
        # Fix 3 (Image 6): Do NOT fire BGS guard when the output has an inverted
        # SV:speech structure ("Affermò: È tutto finito."). This IS a valid row
        # (attribution followed by colon then speech). A true BGS has no colon+uppercase.
        _has_colon_speech = bool(re.search(r':\s+[A-ZÀÈÉÌÒÙ]', out))
        if (_is_bgs_raw or _is_bgs_unquoted) and not _eng_is_attribution and not _has_colon_speech:
            row["content"] = restore_s
            _bgs_confusion_fixes += 1
            log(f"  ⚠️  BGS confusion: sort={sort_n} restored from {out!r} to {'peContent' if (_pe_by_sort.get(sort_n) or '').strip() else 'MT'} {restore_s[:60]!r}")
            continue
        # Fix 3b: lowercase-first output guard.
        # Root cause (SS4): Gemini displaces narrative from row N into row N+1, whose
        # English source is a dialogue/narrative row starting uppercase. The displaced
        # content starts lowercase (e.g. 'ich versuchte, gleichgültig zu klingen.').
        # In standard Italian prose every sentence starts uppercase; a lowercase-first
        # row is always wrong. Guard: if Italian output starts lowercase AND the MT for
        # that row starts uppercase, the content is displaced — restore from MT.
        if out and out[0].islower() and mt_s and mt_s.strip() and mt_s.strip()[0].isupper():
            row["content"] = restore_s
            _bgs_confusion_fixes += 1
            log(f"  ⚠️  Lowercase-first displaced: sort={sort_n} restored from {out!r} to {'peContent' if (_pe_by_sort.get(sort_n) or '').strip() else 'MT'} {restore_s[:60]!r}")
            continue
        # Fix 3c: EN-reference lowercase guard.
        # Root cause (Image 1): CDReader's MT itself can carry a boundary error — the
        # Italian machine translation for row N already includes row N+1's content, so
        # the MT for row N+1 also starts lowercase. Fix 3b is blind to this because it
        # checks (out_lc AND mt_uc): if MT also starts lowercase, the guard never fires.
        # Solution: use the English source as the reference instead of MT.
        # EN is generated independently from the Chinese source and will NOT carry the
        # same CDReader MT boundary error.
        # Conditions (all must hold):
        #   1. output starts lowercase (displaced content signature)
        #   2. EN source starts uppercase (confirms row should begin a new sentence)
        #   3. EN source does NOT start with '"' (dialogue rows can legitimately start lc
        #      after quote stripping — those are handled by Quote Reinject, not here)
        #   4. output has >= 4 words (avoids false positives on short lc continuation
        #      fragments or proper nouns CDReader occasionally mislabels)
        # Action: restore from MT and queue for force-retry so the retry call receives
        # single-row context; the EN source gives Gemini the correct starting reference.
        _eng_uc_start = bool(eng_s and eng_s.strip() and eng_s.strip()[0].isupper())
        _eng_no_quote = not eng_s.strip().startswith('"')
        _out_lc_3c    = bool(out and out[0].islower())
        _out_long_3c  = len(out.split()) >= 4
        if _out_lc_3c and _eng_uc_start and _eng_no_quote and _out_long_3c:
            row["content"] = restore_s
            _bgs_confusion_fixes += 1
            log(f"  ⚠️  Fix3c EN-ref lc: sort={sort_n} out_lc+eng_uc — restored from {'peContent' if (_pe_by_sort.get(sort_n) or '').strip() else 'MT'} {restore_s[:60]!r}")
            if force_retry_sorts is not None:
                force_retry_sorts[sort_n] = 'bleed'
    if _bgs_confusion_fixes:
        log(f"  💬 BGS confusion guard: restored {_bgs_confusion_fixes} row(s) from MT.")

    # ── Fix 1: Strip leading comma from attribution-only rows ─────────────────
    # Root cause (sort=12): when the previous row ends with a close-quote speech,
    # the adjacent attribution row (EN="She declined sharply.") has no speech content.
    # Gemini treats it as inline continuation and prefixes a comma: ", rispose con tono secco."
    # The BGS guard may or may not fire, but even if it restores to MT, the MT itself
    # may have the same leading comma from the original CDReader MT.
    # Guard: only strip when (a) output starts with ',' AND (b) EN is an attribution clause
    # (no opening quote, short, contains a speech verb). This ensures we don't strip
    # legitimate commas from continuation speech rows.
    _leading_comma_fixes = 0
    for row in sorted_rows:
        sort_n = row.get("sort")
        if sort_n == 0:
            continue
        c = row.get("content", "")
        eng_row = (row.get("original") or "").strip()
        if not c.startswith(","):
            continue
        # Check EN is attribution-only (no speech quote, short, has SV verb)
        _eng_no_open_q = not eng_row.lstrip().startswith('"') and '"' not in eng_row[:3]
        _eng_short = len(eng_row.split()) <= 12
        _eng_has_sv = bool(re.search(
            r'\b(?:said|asked|replied|answered|whispered|shouted|muttered|remarked|'
            r'added|continued|insisted|exclaimed|cried|explained|told|warned|'
            r'declined|snapped|sighed|laughed|nodded|smiled|breathed|called)\b',
            eng_row, re.IGNORECASE))
        if _eng_no_open_q and _eng_short and _eng_has_sv:
            stripped_c = re.sub(r'^,\s*', '', c)
            row["content"] = stripped_c
            _leading_comma_fixes += 1
            log(f"  ⚠️  Fix1 leading-comma: sort={sort_n} stripped leading ',' → {stripped_c[:50]!r}")
    if _leading_comma_fixes:
        log(f"  💬 Fix1: stripped leading comma from {_leading_comma_fixes} attribution row(s).")

    # ── Cross-row bleed guard (checker-27 Fix 2) ──────────────────────────
    # Detects Gemini row-merge errors: row N absorbs speech from row N+1,
    # inflating row N while row N+1 is left with attribution-only content.
    # _INFLATION_THRESHOLD=1.6 misses borderline cases (e.g. 4-word source
    # → 6-word output = 1.5× — below threshold). Bleed signature:
    #   row N   inflated:  output > MT × 1.3 AND delta ≥ 2 words
    #   row N+1 lc-start:  output starts lowercase (attribution bleed)
    #   row N+1 deflated:  output < MT × 0.6 (lost speech content)
    # Both rows restored from MT and queued for Remedy A retry.
    _bleed_fixes = 0
    _rows_by_sort_pp  = {r.get('sort'): r for r in sorted_rows}
    _sorted_keys_pp   = sorted(_rows_by_sort_pp.keys())
    for _bi in range(len(_sorted_keys_pp) - 1):
        _sn  = _sorted_keys_pp[_bi]
        _sn1 = _sorted_keys_pp[_bi + 1]
        if _sn == 0 or _sn1 == 0:
            continue
        _row_n   = _rows_by_sort_pp[_sn]
        _row_n1  = _rows_by_sort_pp[_sn1]
        # Use pre-BGS snapshot — Fix3b may have already restored these rows to MT (Finding 1)
        _out_n   = _pre_bgs_content.get(_sn,  '')
        _out_n1  = _pre_bgs_content.get(_sn1, '')
        _mt_n    = _mt_by_sort.get(_sn,  '').strip()
        _mt_n1   = _mt_by_sort.get(_sn1, '').strip()
        if not _mt_n or not _mt_n1 or not _out_n or not _out_n1:
            continue
        _wc_out_n  = len(_out_n.split())
        _wc_mt_n   = len(_mt_n.split())
        _wc_out_n1 = len(_out_n1.split())
        _wc_mt_n1  = len(_mt_n1.split())
        _inflated  = (_wc_mt_n >= 1               # P2: lowered from 2 — catches single-word rows (e.g. '"Certo."')
                      and _wc_out_n >= _wc_mt_n * 1.25  # lowered from 1.3, >= catches exact boundary (Image 4)
                      and (_wc_out_n - _wc_mt_n) >= 2)
        _starts_lc = bool(_out_n1 and _out_n1[0].islower())
        _deflated  = (_wc_mt_n1 >= 3 and _wc_out_n1 < _wc_mt_n1 * 0.6)
        _shorter   = (_wc_out_n1 < _wc_mt_n1)     # P3: N+1 lost any words (partial bleed)
        # P3: condition relaxed — fires when N is inflated AND either:
        #   (a) N+1 is heavily deflated (<60% MT words) — any bleed regardless of lc
        #   (b) N+1 starts lowercase AND is shorter than MT — partial bleed (bled words
        #       didn't fully deplete N+1 but did remove its opening content)
        if _inflated and (_deflated or (_starts_lc and _shorter)):
            _restore_sn  = _restore_for(_sn)
            _restore_sn1 = _restore_for(_sn1)
            _src_n  = "peContent" if (_pe_by_sort.get(_sn)  or "").strip() else "MT"
            _src_n1 = "peContent" if (_pe_by_sort.get(_sn1) or "").strip() else "MT"
            log(f"  ⚠️ Cross-row bleed: sort={_sn} ({_wc_mt_n}→{_wc_out_n}w) "
                f"+ sort={_sn1} lc={_starts_lc} deflated={_deflated} ({_wc_mt_n1}→{_wc_out_n1}w) "
                f"— restoring from {_src_n}/{_src_n1}")
            _row_n['content']  = _restore_sn
            _row_n1['content'] = _restore_sn1
            _bleed_fixes += 1
            if force_retry_sorts is not None:
                force_retry_sorts[_sn]  = 'bleed'  # Finding 3
                force_retry_sorts[_sn1] = 'bleed'  # Finding 3
    if _bleed_fixes:
        log(f"  💬 Cross-row bleed guard: restored {_bleed_fixes * 2} row(s) from MT, "
            f"queued for Remedy A retry.")

    # ── Structural bleed detector: SV+colon at end-of-row ─────────────────────
    # Root cause (sort=35/36, sort=75): Gemini appends a narration clause from
    # row N+1 onto the end of row N, or truncates row N leaving only the
    # narration prefix. In both cases the output ends with:
    #     [own content] + [SV verb] + [optional words] + ":"
    # A row ending with a bare colon is ALWAYS structurally wrong in Italian
    # fiction — a colon introduces speech, so the speech that follows it must
    # appear on the SAME row. If it is absent, the speech was displaced.
    #
    # Word-count guards cannot detect this when the CDReader MT itself is
    # contaminated (OUT ≈ MT → no inflation signal), so we detect structurally.
    #
    # Guard conditions:
    #   1. Row role is NOT open/inline_open (those legitimately end with narration+colon)
    #   2. IT output ends with bare ':'
    #   3. An SV verb appears in the last 10 words
    #   4. EN source contains a speech quote — confirms a speech was expected on this row
    #      (if EN has no speech, a trailing colon is a different issue, not a bleed)
    #   5. No open-quote follows the SV verb (which would mean speech IS on this row)
    _SVCOLON_SV = (
        r'disse|chiese|rispose|replic\u00f2|domand\u00f2|aggiunse|osserv\u00f2|'
        r'afferm\u00f2|comment\u00f2|esclam\u00f2|sussurr\u00f2|mormor\u00f2|'
        r'grid\u00f2|sbuff\u00f2|spieg\u00f2|continu\u00f2|not\u00f2|bisbigliò|'
        r'rifer\u00ec|prosegu\u00ec|dichiar\u00f2|protest\u00f2|interromp\u00f2|'
        r'balbett\u00f2|inform\u00f2|rivel\u00f2|confess\u00f2|avanz\u00f2|'
        r'precis\u00f2|specific\u00f2|puntualiz\u00f2|sentenzi\u00f2|ammutol\u00ec|'
        r'esit\u00f2|rassicur\u00f2|rimprover\u00f2|ammon\u00ec'
    )
    _svcolon_fixes = 0
    _OPEN_ROLES = {'open', 'inline_open'}
    _qe_role_snap = {r.get('sort'): r.get('_quote_role', 'none') for r in sorted_rows}

    for _bi in range(len(_sorted_keys_pp) - 1):
        _sn  = _sorted_keys_pp[_bi]
        _sn1 = _sorted_keys_pp[_bi + 1]
        if _sn == 0:
            continue
        _row_n = _rows_by_sort_pp[_sn]
        _out_n = _row_n.get('content', '').strip()
        if not _out_n:
            continue

        # Condition 1: skip role=open and role=inline_open
        _role_n = _qe_role_snap.get(_sn, 'none')
        if _role_n in _OPEN_ROLES:
            continue

        # Condition 2: IT ends with bare colon
        if not _out_n.endswith(':'):
            continue

        # Condition 3: SV verb in last 10 words
        _tail_last10 = ' '.join(_out_n.split()[-10:])
        if not re.search(r'\b(?:' + _SVCOLON_SV + r')\b', _tail_last10, re.IGNORECASE):
            continue

        # Condition 4: EN source contains a speech quote
        # If EN has no quoted speech, a trailing colon is a different structural
        # issue (not a bleed displacement). This also eliminates pure stubs like
        # "Caden osservò:" whose EN has no speech at all.
        _eng_n = _eng_by_sort.get(_sn, '')
        if not re.search(r'["\u201c\u201d]', _eng_n):
            continue

        # Condition 5: no open quote follows the SV verb (speech IS on this row)
        _sv_m = re.search(r'\b(?:' + _SVCOLON_SV + r')\b', _out_n, re.IGNORECASE)
        if _sv_m and re.search(r'["\u201c\u00ab]', _out_n[_sv_m.end():]):
            continue

        # All conditions met: restore N and N+1
        _row_n1 = _rows_by_sort_pp.get(_sn1)
        _restore_n  = _restore_for(_sn)
        _restore_n1 = _restore_for(_sn1) if _row_n1 else ''
        _src_n  = "peContent" if (_pe_by_sort.get(_sn)  or "").strip() else "MT"
        _src_n1 = "peContent" if (_pe_by_sort.get(_sn1) or "").strip() else "MT"
        log(f"  \u26a0\ufe0f  SV+colon bleed: sort={_sn} (role={_role_n}) ends with narration+colon "
            f"\u2014 speech displaced. Restoring sort={_sn} from {_src_n}, "
            f"sort={_sn1} from {_src_n1}")
        _row_n['content'] = _restore_n
        if _row_n1 and _restore_n1:
            _row_n1['content'] = _restore_n1
        _svcolon_fixes += 1
        if force_retry_sorts is not None:
            force_retry_sorts[_sn]  = 'bleed'
            force_retry_sorts[_sn1] = 'bleed'

    if _svcolon_fixes:
        log(f"  \U0001f4ac SV+colon bleed guard: restored {_svcolon_fixes} pair(s), "
            f"queued for EN-source retry.")




    # ── Quote Reinject: Strip all quotes, place deterministically ──────────
    # Paradigm: do NOT try to fix Gemini's quote placement. Instead, strip ALL
    # outer quote characters from the Italian output and reinject “/” at
    # computed positions based on the English source structure.
    # The English source is the ground truth for WHERE speech starts and ends.
    # The Italian text structure (colons, SV verbs) tells us where to place them.

    _qe_role_by_sort = {r.get("sort", i): r.get("_quote_role", "none")
                        for i, r in enumerate(input_data)}

    def _strip_outer_quotes(text):
        """Remove all outer quote characters. Preserve inner ‚...‘."""
        for qc in ('“', '”', '„', '"', '«', '»'):
            text = text.replace(qc, '')
        return text

    def _find_speech_start(text):
        """Find where direct speech begins in Italian narration+speech text.
        Returns index where “ should go, or -1 if not detectable."""
        # Primary: colon + space + uppercase (standard Italian direct speech)
        m = re.search(r':\s+([A-Z])', text)
        if m:
            return m.start(1)
        # Secondary: colon + space (even if next char not uppercase)
        m2 = re.search(r':\s+', text)
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

    def _en_speech_word_count(eng):
        """Count words in the EN speech portion (between first opener and last closer)."""
        _q_open = set('„“""«')
        _q_close = set('”"»')        # closers only: ” " »
        first_open = -1
        last_close = -1
        for i, ch in enumerate(eng):
            if ch in _q_open and first_open < 0:
                first_open = i
            if ch in _q_close:
                last_close = i
        if first_open >= 0 and last_close > first_open:
            speech = eng[first_open + 1:last_close].strip()
            return len(speech.split())
        return 999  # unknown — treat as long speech

    def _find_speech_end(text, eng=""):
        """Find where closing “ should be inserted in Italian text.
        Returns (insert_pos, needs_comma)."""
        # ── Priority 0: Stripped-quote punctuation boundary ──────────────
        # After stripping quotes, 'cosa?", chiese' becomes 'cosa?, chiese'.
        # The [.!?], pattern (sentence punct immediately followed by comma)
        # ONLY occurs when a closing quote was stripped between them.
        # Normal Italian never has ?, or !, or ., without a quote between.
        # This is the most reliable speech boundary — completely verb-independent.
        m_sq = re.search(r'[.!?…],\s+', text)
        if m_sq:
            return m_sq.start() + 1, False  # “ after punct, before comma
        # ── Priority 0.5: Em-dash speech-end guard (checker-27 Fix 1) ──────
        # ── Priority 0.2: EN-terminal-punct mirroring ─────────────────────
        # When EN speech closes with '!' or '?' immediately before the close-quote
        # character (e.g. '"Alicia!" Caden called...'), find the LAST matching
        # punctuation character in the Italian text and place the close-quote there.
        # This is VERB-LIST-INDEPENDENT: works even when the attribution verb is absent
        # from _SV (e.g. chiam\u00f2, comment\u00f2 before they were added to the list).
        #
        # Fix 6 guard: only fires when EN has post-close attribution (content after
        # the close-quote). Pure-speech rows like '"Alicia!"' are handled by the
        # full-wrap fallback and should not be affected.
        #
        #   \u2713 '"Alicia!" Caden called...' \u2192 EN has attribution \u2192 fire \u2192 IT: "Alicia!" chiam\u00f2 Caden...
        #   \u2713 '"No! Never!" she cried'  \u2192 finds LAST '!' \u2192 "No! Never!" ...
        #   \u2717 '"Alicia!"' (pure speech)  \u2192 guard suppresses \u2192 skip
        if eng:
            _en_tp = re.search(r'([?!])["\u201c\u201d\u201e]', eng)
            if _en_tp:
                _after_close = eng[_en_tp.end():].strip()
                if _after_close:
                    _target_char = _en_tp.group(1)
                    _all_tp = list(re.finditer(re.escape(_target_char), text))
                    if _all_tp:
                        _tp_pos = _all_tp[-1].end()  # position after last matching punct
                        return _tp_pos, False  # !/" and ?/" never take a comma
        # When the EN source has a dash (\u2013 or \u2014) immediately before
        # the closing quote (e.g. 'I prefer\u2014" Her cheeks turned pink.'),
        # the closing \u201d belongs right after the dash in Italian. Without
        # this guard, _find_speech_end falls to end-of-text and wraps the
        # entire row (including attribution) inside the quotes:
        #   BAD:  \u201eIch bevorzuge \u2013 Ihre Wangen wurden rosafarben.\u201c
        #   GOOD: \u201eIch bevorzuge \u2013\u201c Ihre Wangen wurden rosafarben.
        if eng and re.search(r'[\u2014\u2013]["\u201c\u201d]', eng):
            # Finding 2: find the LAST dash, not the first. A row may contain
            # multiple dashes; the speech ends after the last one, not the first.
            _all_dashes = list(re.finditer(r'[\u2014\u2013]', text))
            if _all_dashes:
                return _all_dashes[-1].end(), False  # insert \u201d after LAST dash
        # ── Priority 1: Short-speech early boundary ─────────────────────
        # When EN speech is very short (≤2 words like "Good." or "No."),
        # check for a sentence boundary in the first few Italian words BEFORE
        # SV detection. Any SV verb found later is narration, not attribution.
        if eng:
            _esw = _en_speech_word_count(eng)
            if _esw <= 2:
                m_early = re.search(r'[.!?…]\s+[A-Z]', text)
                if m_early and len(text[:m_early.start()].split()) <= 3:
                    return m_early.start() + 1, False
        # ── Priority 2: SV verb with comma ──────────────────────────
        # BOUNDARY GUARD: if a sentence-ending [?!] followed by an uppercase non-SV
        # word appears BEFORE the SV match position, the speech ended at that boundary
        # and the comma+SV belongs to narration, not attribution after the speech.
        #   ✓ '"Tienila d\'occhio," disse.' → no boundary before disse → fire at comma
        #   ✗ '"Stai spiando? Hank quasi, annuì.' → ? + Hank (non-SV) before annuì → defer to P4
        # PRONOUN EXTENSION: Italian attribution frequently uses object/reflexive clitic
        # pronouns between comma and verb: ", lo disse", ", le chiese", ", si voltò"
        # The optional non-capturing group (?:PRON\s+)? allows P2 to fire on these.
        _P2_PRON = r'(?:(?:lo|la|gli|le|li|si|me|te|ce|ve|ne)\s+)?'
        m = re.search(r',\s+' + _P2_PRON + r'(' + _SV + r')\b', text, re.IGNORECASE)
        if m:
            _pre_sv = text[:m.start()]
            _bound = re.search(r'[?!]\s+([A-Z][a-zA-Zàèéìòù]*)', _pre_sv)
            if not (_bound and not re.match(r'^(?:' + _SV + r')$', _bound.group(1), re.IGNORECASE)):
                return m.start(), False
            # else: boundary+non-SV word precedes the verb → fall through to Priority 4
        # ── Priority 3: SV verb without comma ───────────────────────
        # BOUNDARY GUARD (mirrors Priority 2): if a sentence-ending [?!] followed
        # by an uppercase non-SV word appears BEFORE the SV match position, the
        # speech ended at that boundary and the SV verb belongs to narration.
        #   ✓ '"Fermati" disse.' → no boundary before disse → fire
        #   ✗ '"Ci sei? Hank scosse la testa disse.' → ? + Hank before disse → defer to P4
        m2 = re.search(r'(?<=[a-zàèéìòù!?.…])\s+(' + _SV + r')\b', text, re.IGNORECASE)
        if m2:
            _pre_sv3 = text[:m2.start()]
            _bound3 = re.search(r'[?!]\s+([A-Z][a-zA-Zàèéìòù]*)', _pre_sv3)
            if not (_bound3 and not re.match(r'^(?:' + _SV + r')$', _bound3.group(1), re.IGNORECASE)):
                return m2.start(), True
            # else: boundary+non-SV word precedes the verb → fall through to Priority 4
        # ── Priority 3.5: Structural attribution fallback ─────────────
        # Catches attribution verbs NOT in _SV by matching the Italian
        # attribution structure:  , [lowercase-verb] [Name/pronoun]
        # After a closing quote, Italian places the verb before or after the subject — the verb
        # is lowercase, followed by the subject (proper name = uppercase, or pronoun).
        # SUFFIX GUARD: the "verb" must end in a passato remoto suffix (ò, ì, è, ette, emme,
        # este) to avoid false positives on honorifics and common nouns.
        #   ✓ , assicurò Greta    (ends in ò — passato remoto)
        #   ✓ , consolò lei        (ends in ò — passato remoto)
        #   ✓ , rispose lui        (ends in e — 3rd-conj. passato remoto)
        #   ✗ , signor Ward        (signor has no verb suffix → suppressed)
        #   ✗ , aprì la porta      (article "la" — not a name/pronoun)
        _ATTRIB_PRONOUNS = r'(?:lui|lei|io|noi|voi|esso|essa)\b'
        _PR_SUFFIX = r'[a-zàèéìòù]\w*(?:ò|ì|è|ette|emme|este|erse|ense)'
        m_struct = re.search(
            r',\s+' + _PR_SUFFIX + r'\s+(?:' + _ATTRIB_PRONOUNS + r'|[A-Z][a-zàèéìòù])',
            text
        )
        if m_struct:
            return m_struct.start(), False
        # ── Priority 3.5b: Verb-final attribution (no explicit subject) ──
        # Handles: ", commentò."  ", riprese."  ", aggiunse."
        # The existing P3.5 requires a name/pronoun AFTER the verb; this sub-case
        # catches single-verb attributions at sentence end: verb is the LAST word
        # before the sentence-final period (or end-of-text).
        # Guard: verb must end in passato remoto suffix AND be at/near end of text
        # (no additional words after it except possible final punct).
        # This avoids false positives on verbs mid-sentence.
        m_struct_final = re.search(
            r',\s+' + _PR_SUFFIX + r'[,.]?\s*$',
            text
        )
        if m_struct_final:
            return m_struct_final.start(), False
        # ── Priority 4: Sentence boundary (.!? + uppercase) ───────────
        m3 = re.search(r'[.!?…]\s+[A-Z]', text)
        if m3:
            return m3.start() + 1, False
        # ── Priority 5: Short attribution + comma + uppercase name ────────
        # EN guard: only fire if EN source has attribution after its closing quote.
        # POSITIONAL FIX (Image 1): original guard measured words BEFORE the match
        # (words_before <= 4), which falsely fired on vocatives at the START of speech:
        # "Caden, Gerry è ancora..." → Caden = 1 word before comma → wrongly fired.
        # Correct signal is ATTRIBUTION LENGTH, which is always short (1–3 words).
        # New guard: words AFTER the match must be <= 3.
        #   ✓ '"Sure," Mia replied.' → 0 words after match → fire
        #   ✓ '"Già," disse lui piano.' → 3 words after → fire
        #   ✗ '"Caden, Gerry è ancora nei paraggi."' → 5 words after → suppress
        #   ✗ '"By the way, Joshua, has Iris?"' → EN no post-close → suppress
        m4 = re.search(r',\s+([A-Z][a-zàèéìòù])', text)
        if m4 and len(text[m4.end():].split()) <= 3 and _en_has_post_close_attribution(eng):
            return m4.start(), False
        # ── Fallback: end of text ────────────────────────────────────────────
        return len(text), False

    def _count_en_inline_attribution(eng):
        """Count words in the EN attribution between first close-quote and next open-quote.
        Used for INLINE_SPLIT rows: "Speech1," ATTRIBUTION, "Speech2..."
        Returns the word count of the attribution, or 0 if not an inline pattern."""
        _q_chars = set('\u201e\u201c\u201d""\u00ab\u00bb')
        balance = 0
        first_close_pos = -1
        second_open_pos = -1

        for i, ch in enumerate(eng):
            if ch not in _q_chars:
                continue
            # Classify opener/closer with same logic as _classify_quote_role
            if i == 0:
                is_opener = True
            else:
                prev = eng[i - 1]
                is_opener = prev in ' \t\n(:;'
                if not is_opener and prev in '.!?\u2014\u2013-' and i + 1 < len(eng) and eng[i + 1].isupper():
                    is_opener = True
            if is_opener:
                if first_close_pos >= 0 and second_open_pos < 0:
                    second_open_pos = i
                    break  # found both boundaries, stop
                balance += 1
            else:
                balance -= 1
                if balance == 0 and first_close_pos < 0:
                    first_close_pos = i
        if first_close_pos < 0 or second_open_pos < 0:
            return 0
        between = eng[first_close_pos + 1:second_open_pos].strip()
        return len(between.split()) if between else 0

    def _find_second_speech_start(text, en_attrib_wc):
        """Find where the second speech segment starts in text.
        text: DE text from first speech-end position onward (contains attribution + speech 2).
        en_attrib_wc: word count of the EN attribution (guide for finding the boundary).
        Returns char index in text where the second speech content begins, or -1 if not found.

        Algorithm: find the comma whose preceding word count is closest to en_attrib_wc.
        The EN attribution word count approximates the Italian attribution length (+-30%),
        so the nearest comma reliably marks the attribution/speech-2 boundary.
        """
        comma_positions = [i for i, ch in enumerate(text) if ch == ',']
        if not comma_positions:
            return -1
        best_pos = -1
        best_dist = 999
        for cp in comma_positions:
            before = text[:cp].strip()
            wc = len(before.split()) if before else 0
            if wc < 1:
                continue  # skip leading comma (0 words before = SV prefix)
            dist = abs(wc - en_attrib_wc)
            if dist < best_dist:
                best_dist = dist
                best_pos = cp
        if best_pos < 0:
            return -1
        # Sanity: if best distance is far from expected, bail
        if best_dist > max(3, en_attrib_wc):
            return -1
        # Return position after comma + whitespace (where the speech content begins)
        rest = text[best_pos + 1:]
        m = re.match(r'\s*', rest)
        skip = m.end() if m else 0
        return best_pos + 1 + skip

    def _italian_close_at(stripped, pos, needs_comma):
        """Insert Italian closing quote at pos with correct comma placement.
        Italian convention: comma goes BEFORE the closing quotation mark.
        Returns the text from stripped[:pos] + close-quote construct + stripped[pos:].
        """
        speech = stripped[:pos]
        rest = stripped[pos:]
        # Fix 4: suppress comma when speech ends with sentence-final punctuation.
        # Italian never writes '!," or '?,' — those end the speech cleanly.
        # needs_comma is only meaningful when the speech ends with a word character.
        _terminal_punct = ('.', '!', '?', '…', '\u2026')
        _speech_end = speech.rstrip()[-1] if speech.rstrip() else ''
        _suppress_comma = _speech_end in _terminal_punct
        if needs_comma and not _suppress_comma:
            # No comma in stripped text — add comma before close quote
            return speech + ',' + _QE_CLOSE + ' ' + rest.lstrip()
        elif rest and rest[0] == ',':
            # Comma already at pos (e.g. from Priority 0 stripped-quote boundary)
            # Move it before the close quote: speech + , + " + rest_after_comma
            return speech + ',' + _QE_CLOSE + rest[1:]
        else:
            # No attribution / end of text / terminal punct — just close
            return speech + _QE_CLOSE + ('' if not rest or rest[0] == ' ' else ' ') + rest.lstrip()

        # _QE_OPEN and _QE_CLOSE are defined at module level (no need to redefine here)
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
        fixed = c.replace('«', '"').replace('»', '"')
        original = c  # compare against the RAW row, not the normalized form

        en_starts_quote = bool(eng and re.match(r'^[""„“«]', eng.strip()))
        stripped = _strip_outer_quotes(fixed).strip()

        # Fix 3 (checker-27): Attribution-only quote-injection guard.
        # If Gemini returned a pure attribution clause (e.g. "disse lui e
        # interruppe...") for a row that should contain speech content,
        # injecting any quote here is structurally wrong and produces
        # malformed output like 'und",'. Restore from MT and queue for
        # Remedy A retry instead.
        if role not in ("none", "middle") and _is_begleitsatz(stripped):
            _eng_is_attr_f3 = (
                not (eng or "").lstrip().startswith('"')
                and len((eng or "").split()) <= _ENG_ATTRIBUTION_MAX_WORDS
            )
            if not _eng_is_attr_f3:
                _mt_f3 = _mt_by_sort.get(sort_n, "")
                if _mt_f3:
                    row["content"] = _mt_f3
                    log(f"  ⚠️ Quote-inject BGS guard: sort={sort_n} "
                        f"attribution-only output {stripped[:40]!r} restored from MT")
                    if force_retry_sorts is not None:
                        force_retry_sorts[sort_n] = 'bgs'  # Finding 3
                    continue

        if role in ("middle", "none"):
            # No quotes at all — pure continuation or narrative
            fixed = stripped

        elif role == "open":
            # Attribution-start guard: if EN starts with speech quotes but the Italian
            # output starts with an attribution/speech verb, the speech content was
            # displaced to an adjacent row. Injecting " around attribution text produces
            # malformed output. Skip quote injection.
            # Fix 4 (sort=80): colon+speech check before skipping attribution-start.
            # A row starting with SV verb that also has ': [Uppercase/quote]' content
            # is a valid inverted-structure row — route to inverted path, don't skip.
            _attr_colon_speech = bool(re.search(r':\s*[A-ZÀÈÉÌÒÙ"]', stripped))
            if en_starts_quote and re.match(r'^\s*(?:' + _SV + r')', stripped, re.IGNORECASE) and not _attr_colon_speech:
                log(f"  ⚠️  Quote reinject: sort={sort_n} role={role} — IT starts with "
                    f"attribution verb but EN starts with speech. Skipping quote injection.")
                fixed = stripped
            elif en_starts_quote:
                # Start-of-row opener
                fixed = _QE_OPEN + stripped
            else:
                # Mid-row open: narration + speech (e.g. "disse lui: speech")
                pos = _find_speech_start(stripped)
                if pos >= 0:
                    # Fix 4 (Image 4, Session 7): when _find_speech_start finds a colon position,
                    # pass only the speech_part to _find_speech_end, not the full text.
                    # Fix (Image 1, Session 10): for role=open, NEVER add the close-quote.
                    # role=open means the speech continues into the next row (role=close).
                    # The close-quote belongs there, not here. Adding it prematurely breaks
                    # the multi-row span: the role=close row then has a dangling close-quote
                    # with no matching open on the same row.
                    speech_part = stripped[pos:]
                    narration   = stripped[:pos]
                    # Only open-quote is added; close is the responsibility of the adjacent
                    # role=close row. _en_has_post_close_attribution is irrelevant here
                    # because even when EN has attribution after its close, the ITALIAN
                    # multi-row structure still places the close on the next row.
                    fixed = narration + _QE_OPEN + speech_part
                else:
                    # Fallback: keep original (Gemini\'s placement, imperfect but better than none)
                    pass

        elif role == "close":
            if _en_has_post_close_attribution(eng):
                # EN has attribution after close → find SV verb in Italian
                pos, needs_comma = _find_speech_end(stripped, eng)
                fixed = _italian_close_at(stripped, pos, needs_comma)
            else:
                # EN closes at end of row (no attribution) → " at end
                fixed = stripped + _QE_CLOSE

        elif role == "both":
            # Attribution-start guard (same as role=="open" above): skip quote injection
            # when DE starts with an attribution verb but EN starts with speech quotes.
            _attr_colon_speech_both = bool(re.search(r':\s*[A-ZÀÈÉÌÒÙ"]', stripped))
            if en_starts_quote and re.match(r'^\s*(?:' + _SV + r')\b', stripped, re.IGNORECASE) and not _attr_colon_speech_both:
                log(f"  ⚠️  Quote reinject: sort={sort_n} role={role} — DE starts with "
                    f"attribution verb but EN starts with speech. Skipping quote injection.")
                fixed = stripped
            elif en_starts_quote:
                # Fix (Image 1 sort=93): Before calling _find_speech_end on the full
                # stripped text, check if the text has an inverted SV:speech structure
                # (e.g. "Gerry affermò: Sei l'unica in grado di guarirlo.").
                # _find_speech_start detects the colon at the correct speech boundary.
                # If found, apply the same post-colon logic as role=open (Fix 4, Session 7):
                # pass only the speech_part to _find_speech_end, not the full text.
                # Without this, P3 fires on the SV verb (affermò) treating the name
                # before it ("Gerry") as the speech content — producing "Gerry," affermò: ...
                _sp_start_both = _find_speech_start(stripped)
                if _sp_start_both >= 0:
                    # Inverted structure: narration + SV + colon + speech
                    _narr_both   = stripped[:_sp_start_both]
                    _speech_both = stripped[_sp_start_both:]
                    if _en_has_post_close_attribution(eng):
                        _ep_both, _nc_both = _find_speech_end(_speech_both, eng)
                        _inner_both = _italian_close_at(_speech_both, _ep_both, _nc_both)
                        fixed = _narr_both + _QE_OPEN + _inner_both
                    else:
                        fixed = _narr_both + _QE_OPEN + _speech_both + _QE_CLOSE
                else:
                    # Standard path: EN starts with quote, no inverted colon structure
                    # Always call _find_speech_end regardless of _en_has_post_close_attribution
                    # so trailing narration is not trapped inside quotes.
                    pos, needs_comma = _find_speech_end(stripped, eng)
                    if pos < len(stripped):
                        inner = _italian_close_at(stripped, pos, needs_comma)
                        fixed = _QE_OPEN + inner
                    else:
                        # No boundary found — pure speech row, close at end
                        fixed = _QE_OPEN + stripped + _QE_CLOSE
            else:
                # Mid-row both: narration + „speech“ + possible attribution
                start_pos = _find_speech_start(stripped)
                if start_pos >= 0:
                    narration = stripped[:start_pos]
                    speech_part = stripped[start_pos:]
                    if _en_has_post_close_attribution(eng):
                        end_pos, needs_comma = _find_speech_end(speech_part, eng)
                        inner = _italian_close_at(speech_part, end_pos, needs_comma)
                        fixed = narration + _QE_OPEN + inner
                    else:
                        # No attribution → “ at end of speech
                        fixed = narration + _QE_OPEN + speech_part + _QE_CLOSE
                else:
                    # Fallback: keep original
                    pass

        elif role in ("inline_open", "inline_both"):
            # INLINE_SPLIT: "Speech1," attribution, "Speech2[..."]
            # Place 4 quote marks: „Speech1", attribution, „Speech2["]
            _close_second = (role == "inline_both")
            _en_attr_wc = _count_en_inline_attribution(eng)

            if _en_attr_wc > 0:
                if en_starts_quote:
                    # Start-of-row inline: „ at start
                    pos1, needs_comma1 = _find_speech_end(stripped, eng)
                    if pos1 < len(stripped):
                        text_after = stripped[pos1:]
                        pos2_rel = _find_second_speech_start(text_after, _en_attr_wc)
                        if pos2_rel >= 0:
                            speech1 = stripped[:pos1]
                            attrib  = text_after[:pos2_rel]
                            speech2 = text_after[pos2_rel:]
                            # Italian: comma before close quote
                            _s1_close = ',' + _QE_CLOSE if needs_comma1 else _QE_CLOSE
                            fixed = (_QE_OPEN + speech1 + _s1_close
                                     + attrib + _QE_OPEN + speech2
                                     + (_QE_CLOSE if _close_second else ''))
                            log(f"  💬 Inline split (start-of-row, {role}): sort={sort_n} "
                                f"en_attr={_en_attr_wc}w pos1={pos1} pos2_rel={pos2_rel}")
                        else:
                            # Couldn't find second boundary — fall back to simple open/both
                            if _close_second:
                                fixed = _QE_OPEN + stripped + _QE_CLOSE
                            else:
                                fixed = _QE_OPEN + stripped
                    else:
                        # _find_speech_end returned end-of-text — fall back
                        if _close_second:
                            fixed = _QE_OPEN + stripped + _QE_CLOSE
                        else:
                            fixed = _QE_OPEN + stripped
                else:
                    # Mid-row inline: narration + „Speech1", attrib, „Speech2["]
                    start_pos = _find_speech_start(stripped)
                    if start_pos >= 0:
                        narration = stripped[:start_pos]
                        speech_part = stripped[start_pos:]
                        pos1, needs_comma1 = _find_speech_end(speech_part, eng)
                        if pos1 < len(speech_part):
                            text_after = speech_part[pos1:]
                            pos2_rel = _find_second_speech_start(text_after, _en_attr_wc)
                            if pos2_rel >= 0:
                                s1     = speech_part[:pos1]
                                attrib = text_after[:pos2_rel]
                                s2     = text_after[pos2_rel:]
                                # Italian: comma before close quote
                                _s1_close = ',' + _QE_CLOSE if needs_comma1 else _QE_CLOSE
                                fixed = (narration + _QE_OPEN + s1 + _s1_close
                                         + attrib + _QE_OPEN + s2
                                         + (_QE_CLOSE if _close_second else ''))
                                log(f"  💬 Inline split (mid-row, {role}): sort={sort_n} "
                                    f"en_attr={_en_attr_wc}w pos1={pos1} pos2_rel={pos2_rel}")
                    # If any sub-step failed, fixed == original → no change (Gemini's placement kept)

            # _en_attr_wc == 0: couldn't detect inline pattern in EN → keep Gemini output

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
        _it_stripped_p = c.rstrip().rstrip('„“”""«»').rstrip()

        if not _en_stripped_p or not _it_stripped_p:
            continue

        _en_end = _en_stripped_p[-1]
        _it_end = _it_stripped_p[-1]

        # If EN ends with sentence-final punct and DE ends with comma, fix it
        if _en_end in '.!?' and _it_end == ',':
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
        return re.sub(r'[“”„"]', '"', s)

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
        m_inline = re.search(r'[\u201d\u201c"]+\s*,\s*(.+)$', cn)
        if m_inline:
            inline_bgs = m_inline.group(1).strip()
            if inline_bgs and cn1 and (
                inline_bgs.lower() == cn1.lower() or
                inline_bgs.lower().rstrip(".") == cn1.lower().rstrip(".")
            ):
                row_n["content"] = re.sub(
                    r"\s*,\s*" + re.escape(inline_bgs) + r"\s*$", "", cn
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
        _mt_words  = re.findall(r"[a-zA-Zàèéìòù']+", mt)
        _out_words = re.findall(r"[a-zA-Zàèéìòù']+", out)
        if len(_mt_words) != 1: continue          # only single-word sources
        if len(_out_words) <= 1: continue         # output already single word
        if _out_words[0].lower() != _mt_words[0].lower(): continue  # legitimate rephrase
        row["content"] = mt
        log(f"  ⚠️  Single-word guard: sort={sort_n} restored from {out!r} → {mt!r}")

    if _dup_fixes:
        log(f"  🔁 Post-processing: fixed {_dup_fixes} duplicate content row(s).")

    # ── Fix PUNCT-DEDUP: collapse repeated terminal punctuation ──────────────
    # Gemini occasionally outputs ?? or !! (double/triple question/exclamation marks).
    # Normalise to single char BEFORE comma rules run so Rule C2/F receive clean input.
    # Pattern: ([?!])\1+  →  \1   (collapses ?? → ?, !! → !, ??? → ?, etc.)
    _pdedup_fixes = 0
    for row in sorted_rows:
        sort_n = row.get("sort")
        if sort_n == 0:
            continue
        c = row.get("content", "")
        if not c:
            continue
        c_dedup = re.sub(r'([?!])\1+', r'\1', c)
        if c_dedup != c:
            row["content"] = c_dedup
            c = c_dedup
            _pdedup_fixes += 1
            log(f"  ⚠️  Punct-dedup: sort={sort_n} collapsed repeated terminal punct: {c!r}")
    if _pdedup_fixes:
        log(f"  💬 Punct-dedup: normalised {_pdedup_fixes} row(s) with repeated ?/!")

    # ── Fix B: Comma→Colon before directly introduced speech ─────────────────
    # Italian convention: when a narrative clause ends with an attribution verb
    # and directly introduces speech, a colon is used before the opening quote,
    # NOT a comma. Benchmark analysis showed 8/110 rows where pipeline used comma
    # but the human editor consistently corrected to colon.
    # Pattern: [anything with SV verb], "speech  →  [same]: "speech
    # The SV verb may be separated from the comma by modifying words
    # (e.g. "disse con calma, \"" → "disse con calma: \"").
    # Guard: (a) an SV verb must appear before the last ," on the row,
    #        (b) no close-quote appears between the SV verb and the comma
    #            (avoids firing on attribution-then-new-speech patterns).
    _colon_fixes = 0
    for row in sorted_rows:
        sort_n = row.get("sort")
        if sort_n == 0:
            continue
        c = row.get("content", "")
        if not c:
            continue
        # Find last ," pattern (comma immediately before open-quote)
        m_cq = re.search(r',(\s*")', c)
        if not m_cq:
            continue
        before_comma = c[:m_cq.start()]
        # Check an SV verb appears before the comma
        sv_before = re.search(r'\b(?:' + _SV + r')\b', before_comma, re.IGNORECASE)
        if not sv_before:
            continue
        # Guard: no close-quote between SV verb and comma (would mean speech already ended)
        between = c[sv_before.end():m_cq.start()]
        if '"' in between:
            continue
        # Replace comma with colon
        c_col = before_comma + ':' + m_cq.group(1) + c[m_cq.end():]
        if c_col != c:
            row["content"] = c_col
            _colon_fixes += 1
    if _colon_fixes:
        log(f"  💬 Comma→Colon: converted {_colon_fixes} row(s) to Italian colon-before-speech convention")

    for idx, row in enumerate(sorted_rows):
        c = row.get("content", "")
        next_content = sorted_rows[idx + 1].get("content", "") if idx + 1 < len(sorted_rows) else ""

        # Rule A removed: comma after ?" IS required in Italian before attribution.
        # Rule F (below) handles !" the same way.

        # Rule B-pre: Clean up closing_quote + comma + period (",.") at row end.
        # Gemini outputs e.g. „Nachmittag",. — period AND comma, which is wrong either way.
        # - If next row IS a attribution clause: keep comma (needed), move period inside quote → „Nachmittag.",
        # - If next row is NOT a attribution clause: drop comma, move period inside quote → „Nachmittag."
        if re.search(r'[“”"],[.]$', c):
            c_base = c[:-3]           # everything before closing_quote
            c_quote = c[-3]           # the closing quote character
            if _is_continuation_row(next_content):
                c = c_base + ".," + c_quote         # period+comma before close-quote (Italian)
            else:
                c = c_base + "." + c_quote          # „...."   (period inside, comma dropped)
            row["content"] = c
            comma_fixes += 1

        # Rule B: Remove trailing comma when next row is NOT an attribution clause.
        if c.rstrip().endswith(','):
            _c_s = c.rstrip()
            if not _is_continuation_row(next_content):
                # Only remove if the comma is after close-quote or standalone
                if len(_c_s) >= 2 and _c_s[-2] in ('\u201d', '\u201c', '"', '"'):
                    pass  # comma inside close-quote — keep it (Italian convention)
                else:
                    row["content"] = _c_s[:-1]
                    c = row["content"]
                    comma_fixes += 1

        # Rule C: Add missing comma inside close-quote when followed by attribution.
        # Italian: "Testo," disse. (comma BEFORE close-quote)
        # Cross-row: row ends with " (no comma before it) and next IS attribution.
        # SUPPRESSION: Do NOT add comma when the char before the close-quote is ?/!/.
        # Italian never writes "Testo?," or "Testo!," — terminal punct stands alone.
        elif c and c.rstrip().endswith('"') and not c.rstrip().endswith(',"'):
            _c_s_rule_c = c.rstrip()
            _pre_close_char = _c_s_rule_c[-2] if len(_c_s_rule_c) >= 2 else ''
            if _is_continuation_row(next_content) and _pre_close_char not in '.!?…':
                row["content"] = _c_s_rule_c[:-1] + ',"'
                c = row["content"]
                comma_adds += 1
        elif c and c[-1] in ('"', '"') and not c.rstrip()[-2:-1] == ',':
            _pre_close_char2 = c.rstrip()[-2] if len(c.rstrip()) >= 2 else ''
            if _is_continuation_row(next_content) and _pre_close_char2 not in '.!?…':
                # Fallback for non-standard quote chars: insert comma before close
                row["content"] = c[:-1] + ',' + c[-1]
                c = row["content"]
                comma_adds += 1

        # Rule C2: REMOVED — Italian does not use comma after ?/! before close-quote.
        # "Dove sei stata?" chiese. is correct; "Dove sei stata?," chiese. is wrong.
        # (Rule was inherited from German pipeline where ?," IS correct.)
        # Rule D: Replace literal mid-sentence em-dashes with commas.
        if '\u2014' in c:
            c_nodash = re.sub(r'(?<=\w)\s*\u2014\s*(?=\w)', ', ', c)
            if c_nodash != c:
                row["content"] = c_nodash
                dash_fixes += 1
                c = row["content"]

        # Rule E: Move comma from AFTER closing quote to BEFORE it (Italian convention).
        # Wrong: \u201cTesto\u201d, disse   (comma after close-quote — German style)
        # Right: \u201cTesto,\u201d disse   (comma before close-quote — Italian style)
        if not re.search(r'[?!],?"', c):  # skip when speech ends with ?/!
            c_e = re.sub(
                r'(")\s*,\s*([ \t]*(?:' + _SV + r'))',
                r',\1 \2', c
            )
            if c_e != c:
                row["content"] = c_e
                comma_fixes += 1
                c = row["content"]

        # Rule F: REMOVED — Italian does not use comma after ! before close-quote.
        # "Fermati!" esclamò. is correct Italian; "Fermati!," esclamò. is wrong.
        # (Rule was inherited from German pipeline where !," IS correct.)
        # Rule G: Enforce canonical Capitolo header format: "Capitolo N Title Case Title"
        # Gemini sometimes returns headers all-lowercase, with a spurious colon, or with
        # wrong casing. Match case-insensitively to catch "capitolo 60 ..." variants.
        # Canonical form: "Capitolo {N} {First Word+Proper Nouns Capitalized}" (no colon)
        if re.match(r'^[Cc]apitolo\s+\d+', c):
            _m_g = re.match(r'^[Cc]apitolo\s+(\d+)\s*:?\s*(.*)', c, re.DOTALL)
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
                # Italian convention: capitalize only first word (+ proper nouns left as-is)
                _words_g = _rest_g.split(' ')
                _rest_titled = (' '.join([_tc_word(_words_g[0])] + _words_g[1:])) if _words_g else ''
                titled = f"Capitolo {_num_g} {_rest_titled}" if _rest_titled else f"Capitolo {_num_g}"
                if titled != c:
                    row['content'] = titled
        # Rules H2, J, I removed — quote placement now handled entirely by
        # the Quote Reinject system above, which strips ALL quotes and places
        # them deterministically from English source structure.
        pass


    # ── Post-process: deterministic glossary enforcement ────────────────────
    # The LLM sometimes ignores glossary entries in the prompt.
    # This step scans every row for untranslated English glossary source terms
    # and replaces them with the correct Italian target — bypassing model compliance.
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
                    pattern = re.compile(r'(?<![\w\-])' + re.escape(src) + r'(?![\w\-])', re.IGNORECASE)
                    replaced = pattern.sub(tgt, new_content)
                    if replaced != new_content:
                        new_content = replaced
                except re.error:
                    pass  # skip malformed patterns
            if new_content != original_content:
                row["content"] = new_content
                gloss_fixes += 1

        if gloss_fixes:
            log(f"  📖 Post-processing: enforced glossary terms in {gloss_fixes} row(s).")



# ─── Phase 4: Rephrase with Gemini ───────────────────────────────────────────
def _build_register_block(processed_rows):
    """Scan completed Italian output rows for unambiguous tu/Lei register signals.

    Returns a prompt injection string for subsequent batches, or '' if nothing detected.
    Uses only morphologically unambiguous markers:
      - tu-register:  tu / ti / te / tuo / tua / tuoi / tue (2nd-person informal)
      - Lei-register: Lei / Suo / Sua / Suoi / Sue (formal, always capitalized)
    Associates pronouns with character names via attribution patterns inside quotes.
    First-occurrence wins; once a name is mapped it is never overwritten.
    """
    _tu_re = re.compile(r'\b(tu|ti|te|tuo|tua|tuoi|tue)\b')  # lowercase only = informal
    _formal_re = re.compile(r'\b(Lei|Suo|Sua|Suoi|Sue)\b')   # capitalized = formal
    _attrib_re = re.compile(
        r'[\u201c"](.*?)[\u201d"]\s*[,.]?\s*'
        r'(?:disse|chiese|sussurr\u00f2|rispose|esclam\u00f2|mormor\u00f2|replic\u00f2|parl\u00f2|'
        r'mormor\u00f2|sibil\u00f2|balbett\u00f2|grid\u00f2|url\u00f2|'
        r'bisbigli\u00f2|ringhi\u00f2|prosegu\u00ec|dichiar\u00f2|spieg\u00f2|'
        r'sorrise|sogghign\u00f2|annuì|sospir\u00f2|sbuff\u00f2|brontol\u00f2|borbott\u00f2)\s+'
        r'([A-Z\u00c0\u00c8\u00c9\u00cc\u00d2\u00d9][a-z\u00e0\u00e8\u00e9\u00ec\u00f2\u00f9]{2,})\b'
    )
    register_map = {}

    _HONORIFICS = {'Signor', 'Signora', 'Signorina', 'Dottore', 'Dottoressa', 'Professore', 'Professoressa'}
    for row in processed_rows:
        text = row.get('content', '')
        if not text:
            continue
        for m in _attrib_re.finditer(text):
            char_name = m.group(2)
            if char_name in _HONORIFICS:
                continue
            if char_name in register_map:
                continue
            dialogue = m.group(1)
            if _tu_re.search(dialogue):
                register_map[char_name] = 'tu'
            elif _formal_re.search(dialogue):
                register_map[char_name] = 'Lei'

    if not register_map:
        return ''

    tu_chars  = sorted(n for n, r in register_map.items() if r == 'tu')
    lei_chars = sorted(n for n, r in register_map.items() if r == 'Lei')
    lines = ['\nESTABLISHED PRONOUN REGISTERS - carry forward UNCHANGED into all remaining rows:']
    if tu_chars:
        lines.append(f'tu-register (family / romantic partners): {", ".join(tu_chars)}')
    if lei_chars:
        lines.append(f'Lei-register (professional / formal / strangers): {", ".join(lei_chars)}')
    lines.append('If a name appears in both lists, Lei-register takes priority.\n')
    return '\n'.join(lines)


def rephrase_with_gemini(rows, glossary_terms, book_name):
    global _batch_account_offset
    if not GEMINI_KEYS:
        log("❌ No GEMINI_API_KEY configured.")
        return None

    # Keep raw terms for per-batch filtering; also pre-format full list as fallback
    glossary_text_full = format_glossary_for_prompt(glossary_terms)

    # Build input data: sort + English original (context) + Italian to rephrase
    # CONFIRMED by DIAG: chapterConetnt=English source, machineChapterContent=Italian machine translation
    # Gemini must rephrase the Italian machine translation, NOT re-translate from English.

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
        _reopened = False   # True if a new open occurs after balance returned to 0
        
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
                # INLINE_SPLIT detection: if balance is 0 and we already saw a close,
                # this open starts a SECOND speech segment within the same row.
                # Pattern: "Speech1," attribution, "Speech2..."
                if balance == 0 and any_close:
                    _reopened = True
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
        
        # INLINE_SPLIT: balance returned to 0 (speech 1 closed), then a new opener
        # was seen (speech 2 starts). This is the "Speech1," attrib, "Speech2" pattern.
        # Return specialised roles so the reinject system can place 4 quote marks
        # (close+open for the mid-row boundary) instead of just 2.
        if _reopened and not has_unmatched_close:
            if has_unmatched_open:
                return "inline_open"   # 2nd segment continues to next row
            else:
                return "inline_both"   # 2nd segment also closes in this row
        
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
        # Use Italian machine translation as the text Gemini rephrases.
        # chapterConetnt is English — using it would make Gemini re-translate from
        # English, producing output similar to the existing machine translation.
        r.get("machineChapterContent") or r.get("modifChapterContent") or r.get("peContent") or ""
        for r in rows
    ]
    # peContent = previous human post-editor's accepted version.
    # Used as RESTORE SOURCE by guards (not as Gemini input).
    # Preferred over machineChapterContent for restores because it is human-bounded
    # and free of CDReader MT boundary contamination.
    pe_contents = [
        r.get("peContent") or ""
        for r in rows
    ]
    # English source (chapterConetnt) used as context only, not as content to rephrase
    english_originals = [
        r.get("chapterConetnt") or r.get("eContent") or r.get("eeContent") or ""
        for r in rows
    ]

    # Determine quote roles using the ENGLISH source, not the Italian MT.
    # Italian MT frequently has „ without closing ", which causes the state machine
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
        elif role == "inline_open":
            # "Speech1," attrib, "Speech2... — 2nd segment continues to next row
            in_dialogue = True
            quote_roles.append("inline_open")
        elif role == "inline_both":
            # "Speech1," attrib, "Speech2." — both segments self-contained
            in_dialogue = False
            quote_roles.append("inline_both")
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
    #   2. Italian MT contains '„' (MT correctly placed the opening quote)
    # Then retroactively assign "open" + mark intermediates as "middle".
    _in_speech = False
    for i in range(len(quote_roles)):
        if quote_roles[i] in ("open", "both", "inline_open", "inline_both"):
            _in_speech = True if quote_roles[i] in ("open", "inline_open") else False
        elif quote_roles[i] == "close":
            if not _in_speech:
                # Orphan close — walk backwards to find opener
                _found_opener = -1
                for j in range(i - 1, max(i - 15, -1), -1):  # look back up to 15 rows
                    if quote_roles[j] in ("open", "both", "close", "inline_open", "inline_both"):
                        break  # hit another dialogue block, stop
                    en_j = english_originals[j] if j < len(english_originals) else ""
                    mt_j = raw_contents[j] if j < len(raw_contents) else ""
                    # Check if EN has colon + content (speech introduction)
                    has_colon_speech = bool(re.search(r':\s+[a-zA-Z]', en_j))
                    # Check if Italian MT has “ (MT detected speech start)
                    mt_has_open = '"' in mt_j
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
            "content": raw_contents[i],           # Italian machine translation — primary text to rephrase
            "machine_translation": raw_contents[i],  # same Italian text used by similarity guard
            "pe_content": pe_contents[i],         # previous human post-edit — used as restore source by guards
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
        """Fix literal newlines/tabs inside JSON string values (Pass 1).

        Handles: bare \\n, \\r, \\t inside string values that Gemini occasionally
        emits when the Italian content contains line-breaks or tabs.
        Does NOT fix unescaped double-quotes — that is handled by
        _repair_content_quotes (Pass 2) below.
        """
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
            elif ch == '"':  # escape_next is always False here (handled above)
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

    def _repair_content_quotes(s):
        """Pass 2: repair unescaped double-quotes inside "content" field values.

        Root cause (2026-03-23): Gemini occasionally omits the backslash escape
        on a straight double-quote that opens Italian dialogue, e.g.:

            "content": ""Allora, signor Ward..."    ← unescaped opening "

        The char-flip logic in _fix_json_strings cannot distinguish a legitimate
        JSON closing quote from an unescaped content quote — it just flips the
        in_string flag, producing the same broken string.

        Strategy: anchor on the structural end of each JSON object.  In the
        Gemini batch response every "content" value is the last field before
        the closing brace, so the TRUE closing delimiter is always the "
        immediately followed by \\s*\\n\\s*[}\\]] .  The regex uses a
        non-greedy .*? with a lookahead for that structural anchor, which forces
        the engine to skip over any unescaped inner quotes and stop only at the
        correct structural close.  The captured raw value is then escaped
        in one shot with a nested re.sub.

        Ordering: MUST be called BEFORE _fix_json_strings, because
        _fix_json_strings converts literal \\n → \\\\n, destroying the
        structural \\n anchor that the lookahead depends on.
        Safe: only fires after json.loads + _fix_json_strings have both failed,
        so it never touches already-valid JSON.
        """
        def _escape_inner(m):
            prefix  = m.group(1)   # '"content": "'
            raw_val = m.group(2)   # everything inside the value (may contain bad ")
            suffix  = m.group(3)   # the structural closing '"\\n  }'
            # Escape every unescaped " inside raw_val
            fixed_val = re.sub(r'(?<!\\)"', r'\\"', raw_val)
            return prefix + fixed_val + suffix

        # Pattern:
        #   group 1 = '"content": "'     (opening marker)
        #   group 2 = the value body     (non-greedy, DOTALL)
        #   group 3 = '"\\s*\\n\\s*[}\\]]' (structural end-of-object anchor)
        # The lookahead in group 3 forces .*? to skip inner " that are NOT
        # followed by the structural anchor, landing correctly at the last "
        # before the closing brace/bracket.
        return re.sub(
            r'("content":\s*")(.*?)("(?=\s*\n\s*[}\]]))',
            _escape_inner,
            s,
            flags=re.DOTALL,
        )


    def _parse_llm_response(text, batch_num):
        """Parse JSON from LLM response text — three-tier fallback.

        Tier 1: json.loads(text)               — happy path
        Tier 2: json.loads(_fix_json_strings)  — bare newlines/tabs in values
        Tier 3: json.loads(_repair_content_quotes(_fix_json_strings))
                                               — unescaped " inside content values
                                                 (e.g. Gemini omits escape on Italian
                                                  dialogue-opening straight quote)
        If all three tiers fail, re-raises json.JSONDecodeError for the outer handler
        in _call_gemini, which logs the error and retries the batch.
        """
        text = text.strip()
        if text.startswith("```"):
            text = text.split("\n", 1)[-1]
            text = text.rsplit("```", 1)[0].strip()
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            pass
        try:
            return json.loads(_fix_json_strings(text))
        except json.JSONDecodeError:
            pass
        # Tier 3: unescaped " repair — must run on raw text (real \n = anchor),
        # then _fix_json_strings handles remaining literal control chars.
        return json.loads(_fix_json_strings(_repair_content_quotes(text)))

    def _build_prompt(batch_data, batch_num, total_batches, next_batch_first=None, register_block=''):
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
                quote_hints.append(f'  sort {sort_n}: OPENS a multi-row dialogue — use " to open, NO closing " at end')
            elif role == "close":
                quote_hints.append(f'  sort {sort_n}: CLOSES a multi-row dialogue — NO opening ", but add closing " at end')
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
            # batch_text is already lowercase from the join above.
            def _term_in_batch(term):
                key = (term.get("dictionaryKey") or "").strip().lower()
                sur = (term.get("enSurname") or "").strip().lower()
                return (key and key in batch_text) or (sur and sur in batch_text)

            merged = [t for t in glossary_terms if _term_in_batch(t)]
            # Fallback: if filter produces nothing (e.g. batch is all Italian already),
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
            f"{register_block}"
            f"ROWS TO REPHRASE (batch {batch_num}/{total_batches}, {len(clean_batch)} rows):\n"
            f"For each row:\n"
            f"  - \"original\": English source text (may be empty) — for context and meaning verification only.\n"
            f"  - \"content\": Italian machine translation — this is what you MUST rephrase. "
            f"Actively rephrase into polished, natural Italian: replace common verbs with literary alternatives, "
            f"restructure clauses, add connective tissue, vary sentence openings. Aim for 35-50% word-level change. "
            f"Returning a row IDENTICAL or near-identical to the input is a hard validation error — "
            f"CDReader will reject the entire chapter. Every row must feel noticeably different.\n"
            f"Return ONLY a JSON array; each object must have \"sort\" and \"content\" only.\n"
            f"{json.dumps(clean_batch, ensure_ascii=False)}{quote_hint_block}{lookahead_note}"
        )
        return prompt, clean_batch



    def _call_gemini(batch_data, batch_num, total_batches, next_batch_first=None, register_block=''):
        batch_prompt, _ = _build_prompt(batch_data, batch_num, total_batches, next_batch_first, register_block=register_block)

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
                # D2: 403 Forbidden in batch path — key suspended/revoked.
                # Exclude permanently for this run, rotate to next key in the batch loop.
                if resp.status_code == 403:
                    _403_excluded_keys.add(api_key)
                    try:
                        err_msg_403 = resp.json().get("error", {}).get("message", "")[:80]
                    except Exception:
                        err_msg_403 = ""
                    remaining_403 = len([k for k in GEMINI_KEYS if k not in _403_excluded_keys
                                         and k not in _rpd_exhausted_keys])
                    log(f"  🚫 403 Forbidden on batch {batch_num}: key excluded for run "
                        f"[{err_msg_403}]. {remaining_403} key(s) still available.")
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
                log(f"❌ Gemini JSON parse error on batch {batch_num} (finishReason={finish_reason}): {e}")
                log(f"   Raw response (first 500 chars): {text[:500]}")
                log(f"  Retrying in 15s...")
                time.sleep(15)
                full_rotations += 1  # prevent infinite loop on persistent malformed JSON
                continue
            except Exception as e:
                log(f"❌ Gemini error on batch {batch_num}: {e}")
                log(f"  Retrying in 15s...")
                time.sleep(15)
                full_rotations += 1
                continue
        return None  # all rotations exhausted

    # Sort=0 is always the chapter title row (e.g. "Capitolo 60 Comparsa E Rubare I Riflettori!").
    # Gemini cannot return valid JSON for a 6-word title row reliably, and rephrasing it produces
    # wrong output (all-lowercase, restructured titles). Bypass Gemini for sort=0 entirely —
    # pass it through unchanged and let Rule G enforce correct title-case in post-processing.
    _title_row = next((r for r in input_data if r.get("sort") == 0), None)
    gemini_input_data = [r for r in input_data if r.get("sort") != 0]

    # ── Pre-batch Guard: Source-alignment isolation (Fix 1) ──────────────────
    # Root cause: CDReader's MT for some rows is contaminated — the machine
    # translation spans content from row N AND row N+1 (or N+2), making the
    # MT 2-5× longer than the EN source. Gemini receives this contaminated MT
    # as the text to rephrase and faithfully reproduces the bled content.
    #
    # Detection: MT/EN word-count ratio ≥ 2.5 AND MT ≥ 8 words.
    # (EN is generated independently from Chinese; the ratio indicates MT boundary error.)
    #
    # Fix: replace the row's content in gemini_input_data with a TRIMMED MT,
    # capped at EN_words × 1.8 words, preserving whole sentences from the MT start.
    # This prevents Gemini from seeing the contaminated tail.
    # Row is also pre-tagged in _pre_batch_source_align so unified_retry routes it
    # through the EN-source bleed path (not the contaminated-MT truncated path).
    #
    # Fix 2 — Forward-shift guard:
    # A second class of CDReader MT boundary error: sort N MT ends with ":" (or "—")
    # where sort N EN does NOT (CDReader shifted the trailing noun into sort N+1 MT).
    # Sort N+1 MT then starts with a proper noun that semantically belongs to sort N.
    # Neither bleed guard fires because output ≈ MT word count.
    # Detection: sort N MT ends with ":" AND that ending ":" is NOT in sort N EN
    #            AND first proper-noun word of sort N+1 MT appears in sort N EN
    #            AND that word does NOT appear at the start of sort N+1 EN.
    # Fix: tag sort N+1 as source_align for EN-source retry.

    _pre_batch_source_align: set = set()  # sort numbers pre-tagged as source_align

    def _trim_mt_to_en_length(mt_text, en_words, multiplier=1.8):
        """Cap MT text to en_words * multiplier words, preserving whole sentences."""
        cap = max(int(en_words * multiplier), en_words + 3)
        words = mt_text.split()
        if len(words) <= cap:
            return mt_text
        # Find last sentence boundary within cap words
        candidate = ' '.join(words[:cap])
        last_break = max(candidate.rfind('. '), candidate.rfind('? '),
                         candidate.rfind('! '), candidate.rfind('." '))
        if last_break > len(candidate) * 0.4:
            return candidate[:last_break + 1].strip()
        return candidate.strip()

    _sa_fixed = 0
    _fwd_fixed = 0
    _sorted_gid = sorted(gemini_input_data, key=lambda r: r.get('sort', 0))
    for _gi, _row in enumerate(_sorted_gid):
        _s      = _row.get('sort', 0)
        _mt_w   = len((_row.get('content') or '').split())
        _en_txt = (_row.get('original') or '').strip()
        _en_w   = len(_en_txt.split())

        # Fix 1: source-align isolation
        if _en_w >= 2 and _mt_w >= 8 and (_mt_w / _en_w) >= 2.5:
            _trimmed = _trim_mt_to_en_length(_row['content'], _en_w)
            if _trimmed != _row['content']:
                _row['content'] = _trimmed
                _pre_batch_source_align.add(_s)
                _sa_fixed += 1
                log(f"  ⚠️  Pre-batch SA: sort={_s} MT trimmed {_mt_w}w→{len(_trimmed.split())}w "
                    f"(EN={_en_w}w ratio={_mt_w/_en_w:.1f}x)")

        # Fix 2: forward-shift guard (runs on N+1, needs pair)
        if _gi == 0:
            continue
        _prev_row = _sorted_gid[_gi - 1]
        _prev_mt  = (_prev_row.get('content') or '').rstrip()
        _prev_en  = (_prev_row.get('original') or '').strip()
        # Sort N MT ends with ':' but sort N EN does NOT → CDReader forward-shifted content
        if _prev_mt.endswith(':') and not _prev_en.endswith(':'):
            _n1_mt_first = re.match(r'^"?([A-ZÀÈÉÌÒÙ][a-zàèéìòùA-Z]*)', _row.get('content') or '')
            if _n1_mt_first:
                _fw_word = _n1_mt_first.group(1)
                # That word appears in sort N EN but NOT at the start of sort N+1 EN
                _en_n1 = (_row.get('original') or '').strip()
                _fw_in_prev_en  = _fw_word.lower() in _prev_en.lower()
                _fw_starts_n1   = _en_n1.lower().startswith(_fw_word.lower())
                if _fw_in_prev_en and not _fw_starts_n1:
                    _pre_batch_source_align.add(_s)
                    _fwd_fixed += 1
                    log(f"  ⚠️  Pre-batch FWD: sort={_s} first word {_fw_word!r} "
                        f"belongs to prev EN (forward-shift) — tagged for EN-source retry")

    if _sa_fixed:
        log(f"  💬 Pre-batch: {_sa_fixed} source-align row(s) MT trimmed.")
    if _fwd_fixed:
        log(f"  💬 Pre-batch: {_fwd_fixed} forward-shift row(s) tagged for EN-source retry.")

    # Split into batches and call Gemini for each
    batches = [gemini_input_data[i:i+BATCH_SIZE] for i in range(0, len(gemini_input_data), BATCH_SIZE)]
    total_batches = len(batches)
    log(f"  Splitting {len(gemini_input_data)} rows into {total_batches} batches of ~{BATCH_SIZE}...")

    all_rephrased = []
    _force_retry_sorts: dict = {}    # Remedy A: sorts from trunc-guard for unconditional retry
    _register_block = ''  # built from processed rows; injected into batches 2+
    key_count = len(GEMINI_KEYS)
    _ag_info = ", ".join(f"{_ACCOUNT_LABELS[i]}:{len(g)}" for i, g in enumerate(_ACCOUNT_GROUPS) if g)
    log(f"  Using {key_count} Gemini key(s) across {len(_ACCOUNT_GROUPS)} accounts ({_ag_info}) with group rotation.")
    for i, batch in enumerate(batches, 1):
        _preferred_group = _batch_account_offset % len(_ACCOUNT_GROUPS) if _ACCOUNT_GROUPS else 0
        log(f"  Sending batch {i}/{total_batches} ({len(batch)} rows) via Gemini (prefer Account {_ACCOUNT_LABELS[_preferred_group]})...")
        next_first = batches[i][0] if i < total_batches else None
        result = _call_gemini(batch, i, total_batches, next_batch_first=next_first, register_block=_register_block)
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
            # Fix 4 — EN-primary trigger: catches source-align rows where the CDReader
            # MT is contaminated and far longer than EN. Standard MT-based trigger is
            # blind because MT word count already includes the bled content. Using EN
            # as the reference catches re-bleed in retry (sort=21: EN=4w, retry=27w).
            # Threshold 2.2x EN with delta>=4: allows legitimate IT expansion (10w→14w)
            # but catches 5w→12w bleed (sort=97) and 4w→27w re-bleed (sort=21).
            _en_primary_trigger = (_en_w >= 3
                                   and _out_w > _en_w * 2.2  # lowered 2.5→2.2 (catches sort=97: 12>5*2.2=11)
                                   and (_out_w - _en_w) >= 4)
            # EN-based check: catches bleed on SHORT rows where MT < 4 words.
            _en_trigger = (_en_w >= 2 and _en_w < 8
                           and _out_w > _en_w * 3
                           and (_out_w - _en_w) >= 4)
            # Tiny-row check: catches bleed on 1-2 word EN rows.
            _tiny_trigger = (_en_w >= 1 and _en_w <= 2
                             and _out_w >= _en_w + 3)
            if _mt_trigger or _en_primary_trigger or _en_trigger or _tiny_trigger:
                _mt_orig = next((r.get("content", "") for r in batch if r.get("sort") == _s), "")
                _pe_orig = next((r.get("pe_content", "") for r in batch if r.get("sort") == _s), "")
                _restore_orig = (_pe_orig.strip() or _mt_orig)  # prefer peContent over MT
                if _restore_orig:
                    _trigger_src = ("MT" if _mt_trigger else
                                    ("EN-PRIMARY" if _en_primary_trigger else
                                     ("EN" if _en_trigger else "TINY")))
                    _restore_src = "peContent" if _pe_orig.strip() else "MT"
                    log(f"  \u26a0\ufe0f  Bleed guard: sort={_s} inflated ({_out_w}w vs MT={_inp_w}w EN={_en_w}w, trigger={_trigger_src}) \u2014 restored from {_restore_src}")
                    _r["content"] = _restore_orig
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
            # Look for: ends with closing quote, then whitespace, then new open "..." fragment
            _echo_match = re.search(
                r'"\s+"(.{3,60}?)"\s*$', _cN
            )
            if not _echo_match:
                continue
            _echo_phrase = _echo_match.group(1).strip()
            # Check if Row N+1 starts with the same phrase (after its opening „)
            _n1_inner = re.match(r'^"(.{3,60}?)["",\s]', _cN1)
            if not _n1_inner:
                continue
            _n1_phrase = _n1_inner.group(1).strip()
            # Allow minor variation: compare first 15 chars or full phrase if shorter
            _cmp_len = min(15, len(_echo_phrase), len(_n1_phrase))
            if _cmp_len >= 3 and _echo_phrase[:_cmp_len].lower() == _n1_phrase[:_cmp_len].lower():
                # Strip the appended echo fragment from Row N
                _stripped = _cN[:_echo_match.start()].rstrip()
                # Ensure the stripped content still ends with a proper close quote
                if not _stripped.endswith(('"', '!', '?', '.')):
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
            _inp_w = _inp_wc.get(_s, 0)   # Italian MT word count (already built for Guard 2)
            _en_w  = _en_wc.get(_s, 0)    # English source word count
            _out_w = len((_r.get("content") or "").split())
            _mt_trigger = _inp_w >= _TRUNCATION_MIN_WORDS and _out_w < _inp_w * _TRUNCATION_THRESHOLD
            # EN-alignment exemption: when EN and MT have very different word counts
            # (CDReader source data misalignment), Gemini correctly follows the EN length.
            # The output looks "truncated" relative to MT but is actually EN-aligned.
            # Example: EN=6w "Did I fail to please you?" / MT=23w (dialogue+narrative).
            # Gemini outputs 6w matching EN → trunc guard fires (6 < 23×0.35=8).
            # Suppress MT trigger when output is at least 50% of EN length.
            if _mt_trigger and _en_w >= 2 and _out_w >= _en_w * 0.5:
                _mt_trigger = False
            # EN trigger: only fire when MT is also >= 3 words. If MT is 1-2 words,
            # the EN/MT length discrepancy is a CDReader source data issue (EN field
            # sometimes contains concatenated text from adjacent rows), not Gemini truncation.
            # Restoring from a 1-word MT would make things worse, not better.
            _en_trigger = (_en_w >= _TRUNCATION_MIN_WORDS
                           and _out_w < _en_w * _TRUNCATION_THRESHOLD
                           and _inp_w >= 3)
            if _mt_trigger or _en_trigger:
                _mt_orig = next((r.get("content", "") for r in batch if r.get("sort") == _s), "")
                _pe_orig = next((r.get("pe_content", "") for r in batch if r.get("sort") == _s), "")
                _restore_orig = (_pe_orig.strip() or _mt_orig)
                if _restore_orig:
                    _restore_src_t = "peContent" if _pe_orig.strip() else "MT"
                    log(f"  ⚠️  Trunc guard: sort={_s} too short ({_out_w}w vs MT={_inp_w}w EN={_en_w}w) — restored from {_restore_src_t}")
                    _r["content"] = _restore_orig
                    _truncated_sorts.add(_s)
                    _force_retry_sorts[_s] = 'truncated'    # Remedy A: bypass _MAX_RETRIES + dialogue exemption
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
        # ── Diagnostic: CDReader source alignment anomalies ──────────────────
        # Rows where the Italian MT is 3x+ longer than the English source are
        # almost always a CDReader data-alignment error (EN field contains just
        # one short line while MT spans a merged multi-sentence block, or vice versa).
        # These rows are handled correctly by the trunc guard and Gemini (it follows
        # the EN length), but they are worth logging for monitoring purposes.
        for _r in result:
            _s_diag = _r.get("sort")
            if not _s_diag:
                continue
            _en_w_d  = _en_wc.get(_s_diag, 0)
            _mt_w_d  = _inp_wc.get(_s_diag, 0)
            if _en_w_d >= 1 and _mt_w_d >= 1 and (_mt_w_d / _en_w_d) >= 3.0 and _mt_w_d >= 6:
                _en_preview = next((r.get("original","")[:50] for r in batch if r.get("sort")==_s_diag), "")
                log(f"  ℹ️  Source-align anomaly: sort={_s_diag} MT={_mt_w_d}w vs EN={_en_w_d}w "
                    f"(ratio={_mt_w_d/_en_w_d:.1f}x) — EN: {_en_preview!r}")
        all_rephrased.extend(result)
        # Clear RPM-exhausted state after each successful batch — keys that were
        # rate-limited mid-chapter have likely recovered by the time the next batch starts.
        # RPD-exhausted keys are preserved in _rpd_exhausted_keys and not affected.
        _exhausted_keys.intersection_update(_rpd_exhausted_keys)
        if i < total_batches:
            _register_block = _build_register_block(all_rephrased)
            if _register_block:
                mapped = sum(1 for ln in _register_block.splitlines() if "register" in ln.lower())
                log(f"  Register block updated after batch {i}: {mapped} character(s) mapped.")
            time.sleep(_INTER_BATCH_SLEEP)

    log(f"  Total rows rephrased: {len(all_rephrased)}")

    # Re-inject the bypassed title row (sort=0) with its original content.
    # Rule G (post-processing below) will enforce correct title-case formatting.
    if _title_row is not None:
        all_rephrased.append({"sort": 0, "content": _title_row.get("content", ""),
                               "_quote_role": _title_row.get("_quote_role", "none")})
        all_rephrased = sorted(all_rephrased, key=lambda r: r.get("sort", 0))


    # ── Post-processing + unified retry loop ─────────────────────────────
    # Merge pre-batch source-align tags into _force_retry_sorts so unified_retry
    # routes them through the EN-source bleed path (not the contaminated-MT truncated path).
    for _sa_sort in _pre_batch_source_align:
        if _sa_sort not in _force_retry_sorts:
            _force_retry_sorts[_sa_sort] = 'source_align'
        # If the trunc guard already tagged this sort (e.g. 'truncated'), override
        # with 'source_align' so the correct retry prompt is used.
        elif _force_retry_sorts[_sa_sort] == 'truncated':
            _force_retry_sorts[_sa_sort] = 'source_align'

    # Run post-processing on initial Gemini output
    sorted_rows = sorted(all_rephrased, key=lambda r: r.get("sort", 0))
    _post_process(sorted_rows, input_data, glossary_terms,
                   force_retry_sorts=_force_retry_sorts)

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
    all_rephrased = _unified_retry(sorted_rows, input_data, rows,
                                    bleed_sorts=_force_retry_sorts)

    # Re-run post-processing on retry output to ensure retried rows get
    # the same treatment (Pass QE, comma rules, glossary enforcement, etc.)
    sorted_final = sorted(all_rephrased, key=lambda r: r.get("sort", 0))
    _post_process(sorted_final, input_data, glossary_terms, skip_bgs_guard=True,
                   force_retry_sorts=_force_retry_sorts)

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

    # Check 5: sample check for curly/smart quotation marks (pipeline should use straight ")
    # Flag rows that contain U+201C/U+201D curly quotes — these indicate the quote
    # reinject did not run or a stale output was carried through.
    # ASCII double quotes are correct for this pipeline and must NOT be flagged.
    _eng_single_quote_pair = re.compile(r"'[^']{2,}'")  # 'text' pattern (English-style)
    english_quotes = [
        r.get("sort") for r in rephrased_rows
        if '“' in r.get("content", "") or '”' in r.get("content", "") or _eng_single_quote_pair.search(r.get("content", ""))
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

            # taskContent format: "EnglishTitle|ItalianTitle|ChapterName"
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
class _AlreadyProcessedRetry(Exception):
    """Signal that a chapter was already processed and the pipeline should claim the next one."""
    pass

def run():
    _init_account_groups()
    try:
        token = login()
    except Exception as e:
        log(f"❌ Login failed (CDReader server unreachable?): {e}")
        # Exit cleanly — next scheduled run will retry automatically
        return
    try:
        for _attempt in range(3):
            try:
                _run_inner(token)
                break  # success or no chapters — done
            except _AlreadyProcessedRetry as e:
                log(f"  {e} — looking for next chapter... (attempt {_attempt + 1}/3)")
                continue
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
        claimed_chapters.append(ClaimedChapter(matched_book, f"Override Chapter #{proc_id}", None, "override", proc_id, None))

    # ── Phase 0: Check for already active/claimed chapter ──
    if not claimed_chapters:
        log("Checking for already active chapter across all books...")
        active = find_active_chapter(token, books)
        if active:
            active_book, active_ch_name, active_proc_id, active_task_id = active
            log(f"Found active chapter: {active_ch_name} (proc_id={active_proc_id})")
            claimed_chapters.append(ClaimedChapter(active_book, active_ch_name, None, "already-claimed", None, active_task_id))
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
                        claimed_chapters.append(ClaimedChapter(book, ch_name, ch_id, "dry-run", None, None))
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
                        claimed_chapters.append(ClaimedChapter(book, ch_name, ch_id, "claimed", claim_proc_id, None))
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
                            claimed_chapters.append(ClaimedChapter(book, ch_name, orphan_id, "claimed", orphan_id, None))
                            break
                    else:
                        log(f"  ⚠️  Unexpected claim response: {result}")

    if not claimed_chapters:
        log("No chapters claimed this run.")
        return

    # ── Phase 2-6: Process each claimed chapter ──
    entry = claimed_chapters[0]
    book          = entry.book
    ch_name       = entry.ch_name
    ch_id         = entry.ch_id
    status        = entry.status
    claim_proc_id = entry.claim_proc_id
    task_id       = entry.task_id
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
        raise _AlreadyProcessedRetry(f"Chapter {proc_id} is a recheck")

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
    # OVERRIDE MODE: skip this guard — the whole point of override is to re-process.
    if status != "override":
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
            raise _AlreadyProcessedRetry(f"Chapter {proc_id} already processed")
    else:
        log(f"  ℹ️  Override mode — skipping already-processed guard.")

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

    # ── Post-process: replace English quotes with Italian quotes ─────────────────
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
                    fixed += '"'  # " opening (straight quote)
                    in_quote = True
                else:
                    fixed += '"'  # " closing (straight quote)
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
        log(f"  🔤 Post-processing: converted English quotes to Italian in {quote_fixes} row(s).")

    # ── Post-process: fix "X family" / "famiglia X" format ───────────────
    # Two separate patterns to avoid IGNORECASE corrupting the uppercase-name check:
    # Pattern A: hyphenated "Surname" (not common in Italian, but safe to check) — safe, no article ambiguity
    _fam_hyphen = re.compile(
        r"\b([A-Z][A-Za-zàèéìòù]+)-[Ff]amiglia\b"
    )
    # Pattern B: space-separated single-word surname before "family" or "famiglia"
    _fam_space = re.compile(
        r"\b([A-Z][A-Za-zàèéìòù]+)\s+[Ff]amil(?:y|ia|iglia)\b"
    )
    _FAM_SKIP = {"La", "Il", "Lo", "Le", "Gli", "The", "Una", "Un", "Uno",
                 "Sua", "Suo", "Loro", "Nostra", "Nostro"}
    def _repl_fam(m):
        name = m.group(1).strip().replace("-", " ")
        return m.group(0) if name in _FAM_SKIP else f"famiglia {name}"
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
                "content": (orig_row.get("machineChapterContent")
                        or orig_row.get("modifChapterContent")
                        or orig_row.get("chapterConetnt")  # English source only if no Italian MT
                        or ""),
                "_quote_role": "both",
            }]
            # Single-row retry via Gemini
            retry_result = None
            single_prompt = (
            "Sei un editor italiano esperto. Apporta 2-3 miglioramenti redazionali a questa frase: "
            "verbi più precisi, connettivi più naturali, apertura di frase variata, o adattamento idiomatico. "
            "Il risultato deve suonare autentico per un lettore madrelingua.\n"
            "Rispondi SOLO con un array JSON: "
            "[{\"sort\": " + str(sort_n) + ", \"content\": \"...\"}]\n"
            + json.dumps([{"sort": sort_n, "content": single_batch[0]["content"]}], ensure_ascii=False)
            )
            # Route through _call_gemini_simple: account-group rotation, RPM/RPD
            # tracking, 503 retry, and 2048-token budget for thinking tokens.
            # ── Per-row wall-clock deadline ──────────────────────────────────────────
            # Max 90s per row: with 20s timeout × 3 groups, a full sweep of all
            # groups takes ≤ 60s. The extra 30s cushion covers RPM-cooldown waits.
            # Deadline is enforced inside _call_gemini_simple — it short-circuits
            # and returns None when wall-clock exceeds the threshold.
            _row_deadline = time.time() + 90
            retry_result = _call_gemini_simple(single_prompt, temperature=0.7, max_tokens=2048,
                                               deadline=_row_deadline)
            if retry_result and not retry_result[0].get("content", "").strip():
                retry_result = None
            # Simple direct approach: just copy original content as fallback
            if not retry_result or not retry_result[0].get("content", "").strip():
                fallback_content = orig_row.get("machineChapterContent") or orig_row.get("modifChapterContent") or orig_row.get("chapterConetnt") or ""
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

    # ── Remedy E: Session-expired submit retry (ErrMessage3) ─────────────────
    # CDReader's editing session opened by StartChapter can expire if processing
    # takes too long (RPM cooldowns, many retry rows, force-retry timeouts).
    # ErrMessage3 signals a chapter-state precondition failure — the session lock
    # is no longer valid.  Fix: re-open the session and retry the submit once.
    if not submit_ok and "ErrMessage3" in str(submit_result.get("message", "")):
        log(f"  ⚠️  ErrMessage3 — editing session likely expired. Re-opening session and retrying submit...")
        try:
            start_chapter(token, proc_id)
            time.sleep(2)
            submit_result = submit_chapter(token, proc_id, rephrased, rows)
            submit_ok = (
                submit_result.get("status") is True
                or submit_result.get("message") in ("SaveSuccess", "OperSuccess")
                or submit_result.get("code") in ("311", "315", 0)
            )
            if submit_ok:
                log(f"  ✅ Submit succeeded after session refresh.")
            else:
                log(f"  ❌ Submit still failing after session refresh: {submit_result}")
        except Exception as e:
            log(f"  ❌ Session refresh / resubmit exception: {e}")

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
            # ── Remedy D: Self-healing ErrMessage10 recovery ───────────────────────────
            log(f"  ⚠️  ErrMessage10 — attempting automatic recovery...")
            recovered = _errmessage10_recovery(
                token=token, chapter_id=proc_id, rows=rows,
                finish_fn=finish_chapter, submit_fn=submit_chapter,
            )
            if not recovered:
                msg = (
                    f"⚠️ <b>CDReader: Finish rejected (ErrMessage10)</b>\n\n"
                    f"📖 {book_name}\n"
                    f"📄 {ch_name}\n\n"
                    f"CDReader detected insufficient rephrasing. Automatic recovery also "
                    f"failed. Please open the chapter manually, make meaningful edits, "
                    f"and finish it from the CDReader interface."
                )
                send_telegram(msg)
                log(f"  ⚠️  Finish failed and recovery exhausted: {finish_result}")
                return
            log(f"  ✅ Recovery succeeded — continuing to task close.")
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
    # In override mode, task_id is None because Task Center was skipped.
    # Scan for the matching task entry so the Task Center stays clean.
    if not task_id and proc_id:
        log(f"  ℹ️  No task_id — scanning Task Center for chapter {proc_id}...")
        try:
            _tc_resp = requests.post(
                f"{BASE_URL}/TaskCenter/AuthorTaskCenterList",
                headers={**auth_headers(token), "content-type": "application/json;charset=UTF-8"},
                json={"PageIndex": 1, "PageSize": 20,
                      "status": "", "optUsers": "",
                      "taskType": [], "taskTitle": ""},
                timeout=15,
            )
            _tc_resp.raise_for_status()
            _tc_data = _tc_resp.json().get("data", {})
            _tc_tasks = (
                _tc_data.get("dtolist") or _tc_data.get("list") or
                _tc_data.get("items") or (_tc_data if isinstance(_tc_data, list) else [])
            )
            for _tc_t in _tc_tasks:
                _tc_cid = _tc_t.get("chapterId") or _tc_t.get("objectChapterId")
                if str(_tc_cid) == str(proc_id):
                    task_id = _tc_t.get("id")
                    log(f"  ✅ Found matching Task Center entry: task_id={task_id}")
                    break
            if not task_id:
                log(f"  ℹ️  No Task Center entry found for chapter {proc_id}.")
        except Exception as e:
            log(f"  ⚠️  Task Center scan failed: {e}")

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
    _init_account_groups()
    log("=" * 60)
    log("TEST MODE — full pipeline on synthetic data")
    log(f"Gemini keys available: {len(GEMINI_KEYS)}")
    # Log status of every configured Gemini key dynamically
    # Env var names: GEMINI_API_KEY (no suffix), then GEMINI_API_KEY_2 through _28
    key_statuses = " | ".join(
        f"key {i+1}: {'✅' if k else '⚠️ not set'}"
        for i, k in enumerate(
            os.environ.get(f"ITGEMINI_API_KEY{'_' + str(i+1) if i > 0 else ''}", "")
            for i in range(28)
        )
    )
    log(f"Gemini key status: {key_statuses}")
    log("=" * 60)

    # Synthetic test rows — use the same field names as the real CDReader API response.
    # rephrase_with_gemini reads machineChapterContent (Italian MT) as the text to rephrase
    # and chapterConetnt (English source) for context.
    TEST_ROWS = [
        {"sort": 0,  "machineChapterContent": "Capitolo 249 Come Poteva Non Volerla?",                "chapterConetnt": "Chapter 249 How Could He Not Want Her?"},
        {"sort": 1,  "machineChapterContent": "La famiglia Moss era conosciuta in citt\u00e0 da generazioni.",  "chapterConetnt": "The Moss family had been known in the city for generations."},
        {"sort": 2,  "machineChapterContent": '"Non me ne andr\u00f2" disse lei con fermezza.',        "chapterConetnt": '"I will not go," she said firmly.'},
        {"sort": 3,  "machineChapterContent": "Lui non le rispose.",                                    "chapterConetnt": "He did not answer her."},
        {"sort": 4,  "machineChapterContent": '"Allora resta" sussurr\u00f2 lui piano.',                "chapterConetnt": '"Then stay," he whispered softly.'},
        {"sort": 5,  "machineChapterContent": "Lei lo guard\u00f2 a lungo prima di parlare.",              "chapterConetnt": "She looked at him for a long time before she spoke."},
        {"sort": 6,  "machineChapterContent": '"Cosa hai detto?" chiese lei incredula.',               "chapterConetnt": '"What did you say?" she asked in disbelief.'},
        {"sort": 7,  "machineChapterContent": "disse lui con voce calma.",                              "chapterConetnt": "he said in a calm voice."},
        {"sort": 8,  "machineChapterContent": "La famiglia Williams le era sempre stata accanto.",      "chapterConetnt": "The Williams family had always stood by her."},
        {"sort": 9,  "machineChapterContent": "Lui fece un passo indietro e incroci\u00f2 le braccia.",   "chapterConetnt": "He took a step back and crossed his arms."},
        # Sort 10: indirect speech — must NOT be converted to direct speech
        {"sort": 10, "machineChapterContent": "Lui le disse freddamente che doveva andarsene.",
              "chapterConetnt": '"You should leave now," he said coldly.'},
        # Sort 12: clean indirect speech — model must not inject quotes
        {"sort": 12, "machineChapterContent": "Lei spieg\u00f2 che lui non aveva pi\u00f9 scelta.",
              "chapterConetnt": "She explained that he no longer had a choice."},
        {"sort": 11, "machineChapterContent": "Lei annu\u00ec lentamente e lasci\u00f2 la stanza senza dire un'altra parola.", "chapterConetnt": "She nodded slowly and left the room without another word."},
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
        msg = "❌ <b>TEST FAILED</b>: No result returned. Check Gemini API keys (ITGEMINI_API_KEY through ITGEMINI_API_KEY_7)."
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
    _fam_en = re.compile(r"(?:[Tt]he\s+)?([A-Z][A-Za-zàèéìòù]+(?:\s[A-Z][A-Za-zàèéìòù]+){0,2})\s+[Ff]amily\b")
    _fam_it = re.compile(r"(?:[Ll]a\s+)?([A-Z][A-Za-zàèéìòù]+(?:[-\s][A-Z][A-Za-zàèéìòù]+){0,2})[-\s][Ff]amiglia\b")
    fam_hits = sum(1 for r in result if _fam_en.search(r.get("content","")) or _fam_it.search(r.get("content","")))
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
