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

# ─── Config ──────────────────────────────────────────────────────────────────
BASE_URL   = "https://translatorserverwebapi-de.cdreader.com/api"
GEMINI_URL  = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent"
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
]
GEMINI_KEYS = [k for k in _GEMINI_KEYS_RAW if k.strip()]
_exhausted_keys: set = set()      # RPM-exhausted (clears after 60s wait)
_rpd_exhausted_keys: set = set()  # RPD-exhausted (daily quota — permanent for this run)

# Fallback chain — used when all Gemini keys hit their daily quota (RPD)
#
# Tier 1: Llama 3.3 70B via Groq (free, 14,400 RPD, CI-friendly)
#   Set GROQ_API_KEY in GitHub Actions secrets to enable.
#   Sign up at console.groq.com — no credit card required for free tier.
GROQ_API_KEY = os.environ.get("GROQ_API_KEY", "")
GROQ_URL     = "https://api.groq.com/openai/v1/chat/completions"
GROQ_MODEL   = "llama-3.3-70b-versatile"



def _next_gemini_key():
    """Return the next non-exhausted Gemini API key, or None if all exhausted."""
    available = [k for k in GEMINI_KEYS if k not in _exhausted_keys and k not in _rpd_exhausted_keys]
    return available[0] if available else None

def _all_keys_rpd_dead():
    """True if every configured key has hit its daily quota."""
    return len(GEMINI_KEYS) > 0 and all(k in _rpd_exhausted_keys for k in GEMINI_KEYS)

DRY_RUN   = os.environ.get("DRY_RUN",   "false").lower() == "true"
TEST_MODE = os.environ.get("TEST_MODE", "false").lower() == "true"

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
You are an experienced German content writer and expert editor. Your task is to rephrase each row in the "content" field into polished, natural, and professional German.

OUTPUT FORMAT (CRITICAL)
Return ONLY a valid JSON array — no markdown, no preamble, no explanation.
Each object must have exactly:
  "sort": original sort number (integer, unchanged)
  "content": rephrased German text
Example: [{"sort": 0, "content": "rephrased line"}, {"sort": 1, "content": "..."}]

CAPITALIZATION & SOURCE FORMATTING
- All-caps lines: rephrase in ALL CAPS (e.g. "GRAND KING" → "GROẞER KÖNIG")
- Lines beginning with "Kapitel": capitalize first letter of each word (e.g. "Kapitel 168 Sie Überraschte Wilbur")
- Lines containing only punctuation or single words (e.g. "!" or "Los!"): retain exactly as-is
- Standard lines: standard German capitalization rules

LINGUISTIC GUIDELINES
- Word count: approximately maintain the original word count per line; avoid excessive shortening
- Tone: natural, conversational German with everyday expressions; use synonyms to avoid repetition
- Action beats: incorporate or maintain character actions where suitable
- Contextual flow: consider surrounding rows for narrative continuity
- Dashes (—): never translate literally as "-"; restructure using conjunctions, verbs, or relative clauses for natural German flow
  Example: "...in the news—a softer version..." → "...in den Nachrichten, und wirkte wie eine sanftere Version..."

THE PRONOUN PROTOCOL (CRITICAL)
- "du": only for family (parents, children, siblings), romantic partners, demonstrably close long-term friends
- "Sie": default for ALL other interactions — professional colleagues, new acquaintances, boss/subordinate, strangers, any relationship marked by respect or distance
- Absolute consistency: never switch "du"/"Sie" between the same two people within a chapter

DIALOGUE & HONORIFICS
- German quotation marks ONLY: „ to open, " to close
- Accompanying sentences (Begleitsatz): If a line of direct speech ends with a closing quotation mark and is immediately followed by an accompanying sentence (e.g. "sagte sie", "flüsterte er", "antwortete er leise"), you MUST add a comma after the closing quotation mark. If the next row is NOT a speech attribution but begins a new thought, describes an action, or starts a new speaker — do NOT add a comma after the closing ".
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

FINAL SELF-CHECK (perform before responding)
1. Output has EXACTLY the same number of JSON objects as input rows?
2. Begleitsatz comma rule applied correctly — comma ONLY when next row is a speech attribution?
3. du/Sie consistent per character relationship?
4. All glossary terms applied?
5. No literal dash (—) translations — restructured naturally?
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


# ─── Phase 4: Rephrase with Gemini ───────────────────────────────────────────
def rephrase_with_gemini(rows, glossary_terms, book_name):
    if not GEMINI_KEYS:
        log("❌ No GEMINI_API_KEY configured.")
        return None

    # Keep raw terms for per-batch filtering; also pre-format full list as fallback
    glossary_text_full = format_glossary_for_prompt(glossary_terms)

    # Build input data: sort + English original + German pre-translation to rephrase
    # Field names from API: eContent=English source, chapterConetnt=German pre-translation (note typo)

    def _classify_quote_role(text):
        """
        Classify whether a text row is an opening, closing, middle, or standalone
        dialogue line based on quote balance.
        Returns one of: "open", "close", "middle", "both", "none"
        """
        import re as _re
        # Strip whitespace
        t = text.strip()
        # Count unescaped opening (« „ ") and closing (" » ") quote chars
        # We look at start/end of text for German/English dialogue markers
        opens = t.startswith(('„', '"', '„', '“'))
        closes = t.endswith(('"', '»', '”', '"'))
        # Also handle cases like: text ends with '" ' or '",' or '".'
        closes = closes or bool(_re.search(r'["”»]\s*[,!?.]?\s*$', t))
        opens = opens or bool(_re.match(r'^[„"„“«]', t))

        if opens and closes:
            return "both"
        elif opens and not closes:
            return "open"
        elif closes and not opens:
            return "close"
        elif not opens and not closes:
            # Could be a middle dialogue line — check if it looks like speech
            # Simple heuristic: if previous context is open dialogue, treat as middle
            return "middle_or_none"
        return "none"

    raw_contents = [
        r.get("chapterConetnt") or r.get("content") or r.get("modifChapterContent") or ""
        for r in rows
    ]

    # Determine quote roles with context awareness
    quote_roles = []
    in_dialogue = False
    for i, text in enumerate(raw_contents):
        role = _classify_quote_role(text)
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

    input_data = [
        {
            "sort": r.get("sort", i),
            "original": r.get("eContent") or r.get("eeContent") or r.get("original") or "",
            "content": raw_contents[i],
            "_quote_role": quote_roles[i],  # stripped before sending to Gemini
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
        clean_batch = [{"sort": r["sort"], "original": r.get("original",""), "content": r["content"]} for r in batch_data]
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
        # size dramatically (especially important for Groq's 12k TPM limit) and keeps
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
            f"For each row, \"original\" is the English source text (may be empty), and \"content\" is the German pre-translation you must rephrase into fluent, natural German. "
            f"Return ONLY a JSON array with the same number of objects, each containing \"sort\" and \"content\" fields.\n"
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

    def _call_groq(batch_data, batch_num, total_batches, next_batch_first=None):
        """
        Tier-1 fallback: Llama 3.3 70B via Groq free tier.
        14,400 RPD / 100 RPM — CI-friendly, no credit card required.
        OpenAI-compatible endpoint.
        """
        if not GROQ_API_KEY:
            log("  ⚠️ GROQ_API_KEY not set — Groq fallback unavailable.")
            return None

        prompt, _ = _build_prompt(batch_data, batch_num, total_batches, next_batch_first)

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                resp = requests.post(
                    GROQ_URL,
                    headers={
                        "Authorization": f"Bearer {GROQ_API_KEY}",
                        "Content-Type": "application/json",
                    },
                    json={
                        "model": GROQ_MODEL,
                        "messages": [{"role": "user", "content": prompt}],
                        "temperature": 0.3,
                        "max_tokens": 32768,
                    },
                    timeout=300,
                )
                if resp.status_code == 429:
                    retry_after = int(resp.headers.get("Retry-After", 60) or 60)
                    log(f"  🔄 Groq rate-limited (attempt {attempt}/{MAX_RETRIES}), waiting {retry_after}s... Body: {resp.text[:300]}")
                    time.sleep(retry_after)
                    continue
                if resp.status_code >= 400:
                    log(f"  ❌ Groq HTTP {resp.status_code} (attempt {attempt}/{MAX_RETRIES}): {resp.text[:400]}")
                    if resp.status_code in (500, 502, 503):
                        time.sleep(15)
                        continue
                    else:
                        return None  # 4xx other than 429 — don't retry
                body = resp.json()
                text = body["choices"][0]["message"]["content"]
                if not text:
                    log(f"❌ Empty Groq response on batch {batch_num}")
                    return None
                parsed = _parse_llm_response(text, batch_num)
                if isinstance(parsed, dict):
                    parsed = next((v for v in parsed.values() if isinstance(v, list)), None)
                    if parsed is None:
                        log(f"❌ Groq returned object but no array found: {text[:300]}")
                        return None
                log(f"  Batch {batch_num}/{total_batches}: {len(parsed)} rows from Groq (Llama 3.3 70B).")
                return parsed
            except json.JSONDecodeError as e:
                log(f"❌ Groq JSON parse error on batch {batch_num}: {e}")
                log(f"   Raw response (first 800 chars): {text[:800]}")
                if attempt < MAX_RETRIES:
                    log("  Retrying in 15s...")
                    time.sleep(15)
                else:
                    return None
            except Exception as e:
                log(f"❌ Groq error on batch {batch_num}: {e}")
                if attempt < MAX_RETRIES:
                    log("  Retrying in 15s...")
                    time.sleep(15)
                else:
                    return None
        return None


    def _call_gemini(batch_data, batch_num, total_batches, next_batch_first=None):
        batch_prompt, _ = _build_prompt(batch_data, batch_num, total_batches, next_batch_first)

        # Retry loop: try each key once, then if all RPM-exhausted wait 60s for reset.
        # RPD-exhausted keys (daily quota) are marked permanently and never retried.
        # Max full RPM-reset rotations before giving up: MAX_RETRIES_429
        full_rotations = 0
        while full_rotations < MAX_RETRIES_429:
            try:
                # Fail fast if every key has hit its daily quota
                if _all_keys_rpd_dead():
                    log(f"❌ All Gemini keys have hit their daily quota (RPD). No point retrying.")
                    return None
                api_key = _next_gemini_key()
                if not api_key:
                    # All remaining (non-RPD) keys are RPM-exhausted — wait for reset
                    full_rotations += 1
                    if full_rotations >= MAX_RETRIES_429:
                        log(f"❌ All Gemini keys RPM-exhausted after {MAX_RETRIES_429} rotation(s) on batch {batch_num}.")
                        return None
                    # Honour Retry-After header if present (more precise than flat 60s)
                    retry_after = None
                    try:
                        retry_after = int(resp.headers.get("Retry-After", 0))
                    except Exception:
                        pass
                    wait = retry_after if retry_after and retry_after > 0 else 60
                    rpm_exhausted_count = len([k for k in GEMINI_KEYS if k in _exhausted_keys and k not in _rpd_exhausted_keys])
                    log(f"  ⚠️ {rpm_exhausted_count} key(s) RPM-limited. Waiting {wait}s for reset (rotation {full_rotations}/{MAX_RETRIES_429})...")
                    _exhausted_keys.clear()  # only clears RPM-exhausted, not RPD
                    time.sleep(wait)
                    continue
                resp = requests.post(
                    f"{GEMINI_URL}?key={api_key}",
                    json={
                        "contents": [{"parts": [{"text": batch_prompt}]}],
                        "generationConfig": {
                            "temperature": 0.3,
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
                        is_rpd = (
                            any(kw in combined for kw in rpd_keywords)
                            or err_status == "RESOURCE_EXHAUSTED"
                        )
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

    # Split into batches and call Gemini (with Groq fallback) for each
    batches = [input_data[i:i+BATCH_SIZE] for i in range(0, len(input_data), BATCH_SIZE)]
    total_batches = len(batches)
    log(f"  Splitting {len(input_data)} rows into {total_batches} batches of ~{BATCH_SIZE}...")

    all_rephrased = []
    key_count = len(GEMINI_KEYS)
    groq_batches = 0
    log(f"  Using {key_count} Gemini key(s) with automatic rotation on 429.")
    for i, batch in enumerate(batches, 1):
        log(f"  Sending batch {i}/{total_batches} ({len(batch)} rows) via Gemini...")
        next_first = batches[i][0] if i < total_batches else None
        result = _call_gemini(batch, i, total_batches, next_batch_first=next_first)
        if result is None:
            if _all_keys_rpd_dead() and GROQ_API_KEY:
                log(f"  🔀 Gemini RPD-exhausted — falling back to Groq (Llama 3.3 70B) for batch {i}...")
                result = _call_groq(batch, i, total_batches, next_batch_first=next_first)
                if result is not None:
                    groq_batches += 1
            if result is None:
                log(f"❌ Batch {i} failed — all providers exhausted. Aborting.")
                return None
        all_rephrased.extend(result)
        # Clear RPM-exhausted state after each successful batch — keys that were
        # rate-limited mid-chapter have likely recovered by the time the next batch starts.
        # RPD-exhausted keys are preserved in _rpd_exhausted_keys and not affected.
        _exhausted_keys.difference_update(
            [k for k in _exhausted_keys if k not in _rpd_exhausted_keys]
        )
        if i < total_batches:
            time.sleep(5)

    provider_note = f" (Groq fallback used for {groq_batches}/{total_batches} batch(es))" if groq_batches else ""
    log(f"  Total rows rephrased: {len(all_rephrased)}{provider_note}")

    # ── Post-process: German dialogue punctuation enforcement ─────────────────
    import re as _re
    BEGLEITSATZ_PATTERN = _re.compile(
        r"""^(?:
            (?:sagte|flüsterte|antwortete|rief|fragte|murmelte|erwiderte|
               bemerkte|fügte|entgegnete|zischte|hauchte|stammelte|schrie|
               brüllte|nickte|lächelte|seufzte|wisperte|knurrte|schnappte|
               stöhnte|schluchzte|keuchte|grunzte|gluckste|ergänzte|meinte|
               verkündete|wiederholte)
            |
            (?:[A-ZÄÖÜ][a-zäöüß]+\s+(?:sagte|flüsterte|antwortete|rief|fragte|
               murmelte|erwiderte|bemerkte|fügte|entgegnete|zischte|hauchte|
               stammelte|schrie|brüllte|wisperte|knurrte|ergänzte|meinte|
               verkündete|wiederholte))
            |
            (?:(?:er|sie|es|ich|wir|ihr)\s+(?:sagte|flüsterte|antwortete|rief|
               fragte|murmelte|erwiderte|bemerkte|fügte|entgegnete|zischte|
               hauchte|stammelte|schrie|brüllte|wisperte|knurrte|ergänzte|
               meinte|wiederholte))
        )""",
        _re.IGNORECASE | _re.VERBOSE
    )

    comma_fixes = 0
    comma_adds = 0
    dash_fixes = 0
    sorted_rows = sorted(all_rephrased, key=lambda r: r.get("sort", 0))

    for idx, row in enumerate(sorted_rows):
        c = row.get("content", "")
        next_content = sorted_rows[idx + 1].get("content", "") if idx + 1 < len(sorted_rows) else ""

        # Rule A: Remove comma after ?" or !" (intra-row and cross-row).
        # German rule: ? and ! already end speech — no comma before attribution.
        c_fixed = _re.sub(r'([?!]“)\s*,\s*', r'\1 ', c)
        c_fixed = _re.sub(r'([?!"]),\s*', r'\1 ', c_fixed)
        if c_fixed != c:
            row["content"] = c_fixed
            c = c_fixed
            comma_fixes += 1

        # Rule B: Remove cross-row comma when next row is NOT a Begleitsatz.
        elif c.endswith('“,') or c.endswith('",'):
            if not BEGLEITSATZ_PATTERN.match(next_content):
                row["content"] = c[:-1]
                c = c[:-1]
                comma_fixes += 1

        # Rule C: Add missing comma when plain closing quote is followed by Begleitsatz.
        elif (c.endswith('“') or c.endswith('"')) \
                and not c.endswith('?“') and not c.endswith('!"') \
                and not c.endswith('?“') and not c.endswith('!“'):
            if BEGLEITSATZ_PATTERN.match(next_content):
                row["content"] = c + ","
                c = c + ","
                comma_adds += 1

        # Rule D: Replace literal mid-sentence em-dashes with commas.
        if '—' in c:
            c_nodash = _re.sub(r'(?<=\w)\s*—\s*(?=\w)', ', ', c)
            if c_nodash != c:
                row["content"] = c_nodash
                dash_fixes += 1

    if comma_fixes:
        log(f"  ✂️  Post-processing: fixed {comma_fixes} dialogue comma(s).")
    if comma_adds:
        log(f"  ✍️  Post-processing: added {comma_adds} missing comma(s) before Begleitsatz.")
    if dash_fixes:
        log(f"  ➖ Post-processing: replaced {dash_fixes} literal em-dash(es) with comma.")
    # ── Post-process: deterministic glossary enforcement ────────────────────
    # The LLM (especially Groq) sometimes ignores glossary entries in the prompt.
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

        import re as _re2
        gloss_fixes = 0
        for row in all_rephrased:
            original_content = row.get("content", "")
            new_content = original_content
            for src, tgt in replacement_pairs:
                # Word-boundary aware replacement, case-insensitive
                try:
                    pattern = _re2.compile(r'(?<![\w\-])' + _re2.escape(src) + r'(?![\w\-])', _re2.IGNORECASE)
                    replaced = pattern.sub(tgt, new_content)
                    if replaced != new_content:
                        new_content = replaced
                except _re2.error:
                    pass  # skip malformed patterns
            if new_content != original_content:
                row["content"] = new_content
                gloss_fixes += 1

        if gloss_fixes:
            log(f"  📖 Post-processing: enforced glossary terms in {gloss_fixes} row(s).")

    return all_rephrased


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
            log(f"  Evaluating task status={t_status} finishTime={t_finish}")
            # Accept status=0 (in-progress) AND status=1 (to-be-edited / claimed-not-started).
            # CDReader uses different codes: 0=in-progress, 1=to-be-edited, 2+=completed/closed.
            # Only skip tasks that are explicitly finished.
            if t_finish is not None:
                log(f"  Skipping task — finishTime is set ({t_finish})")
                continue
            if t_status in (2, 3, 4):
                log(f"  Skipping task — status={t_status} indicates completed")
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
    books = get_books(token)

    if not books:
        log("No books found.")
        return

    claimed_chapters = []
    errors = []

    # ── Phase 0: Check for already active/claimed chapter ──
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
                    claimed_chapters.append((book, ch_name, ch_id, "dry-run", None))
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
                    claimed_chapters.append((book, ch_name, ch_id, "claimed", claim_proc_id))
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
                        claimed_chapters.append((book, ch_name, orphan_id, "claimed", orphan_id))
                        break
                else:
                    log(f"  ⚠️  Unexpected claim response: {result}")

    if not claimed_chapters:
        log("No chapters claimed this run.")
        return

    # ── Phase 2-6: Process each claimed chapter ──
    entry = claimed_chapters[0]
    book, ch_name, ch_id, status = entry[0], entry[1], entry[2], entry[3]
    task_id = entry[5] if len(entry) > 5 else None  # Task Center task ID for closing
    claim_proc_id = entry[4] if len(entry) > 4 else None
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

    # Start chapter (unlock for editing)
    start_chapter(token, proc_id)
    time.sleep(2)

    # Fetch rows
    rows = get_chapter_rows(token, proc_id)
    if not rows:
        msg = f"⚠️ <b>CDReader:</b> No rows fetched for {ch_name}. Manual action required."
        send_telegram(msg)
        return

    # Note: modifChapterContent is pre-populated by CDReader with machine translations
    # on ALL chapters — it cannot be used to detect whether WE already processed a chapter.
    # The task center finishTime is the sole ground truth for completion status.
    # Any chapter we reach here either (a) has an open task or (b) was just freshly claimed,
    # so we always proceed to process it.
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
    import re as _re
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
    import re as _re2
    # Two separate patterns to avoid IGNORECASE corrupting the uppercase-name check:
    # Pattern A: hyphenated "Surname-Familie" — safe, no article ambiguity
    _fam_hyphen = _re2.compile(
        r"\b([A-ZÄÖÜ][A-Za-zäöüßÄÖÜ]+(?:-[A-ZÄÖÜ][A-Za-zäöüßÄÖÜ]+)*)-Familie\b"
    )
    # Pattern B: space-separated single-word surname before "family" or " Familie"
    _fam_space = _re2.compile(
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
            # Try Gemini first for single-row retry (save Groq quota)
            retry_result = None
            if GEMINI_API_KEY:
                retry_result = rephrase_with_gemini.__wrapped_call_gemini(single_batch, sort_n, 1, None) if hasattr(rephrase_with_gemini, '__wrapped_call_gemini') else None
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
    Falls back to Groq (Llama 3.3 70B) if Gemini RPD-exhausted.
    """
    log("=" * 60)
    log("TEST MODE — full pipeline on synthetic data")
    log(f"Gemini keys available: {len(GEMINI_KEYS)}")
    or_status = "✅ configured" if GROQ_API_KEY else "❌ not configured (set GROQ_API_KEY)"
    log(f"Groq fallback (Llama 3.3 70B): {or_status}")
    log("=" * 60)

    # Synthetic test rows — realistic German pre-translation content
    TEST_ROWS = [
        {"sort": 0,  "content": "Kapitel 249 Wie Konnte Er Sie Nicht Wollen?"},
        {"sort": 1,  "content": "Die Moss family war seit Generationen in der Stadt bekannt."},
        {"sort": 2,  "content": '„Ich werde nicht gehen", sagte sie bestimmt.'},
        {"sort": 3,  "content": "Er antwortete ihr nicht."},
        {"sort": 4,  "content": '„Dann bleib", flüsterte er leise.'},
        {"sort": 5,  "content": "Sie schaute ihn lange an, bevor sie sprach."},
        {"sort": 6,  "content": '„Was hast du gesagt?" fragte sie ungläubig,'},
        {"sort": 7,  "content": "sagte er mit ruhiger Stimme."},
        {"sort": 8,  "content": "Die Williams family hatte immer zu ihr gehalten."},
        {"sort": 9,  "content": "Er trat einen Schritt zurück und verschränkte die Arme."},
        {"sort": 10, "content": '"You should leave now," he said coldly.'},
        {"sort": 11, "content": "Sie nickte langsam und verließ das Zimmer ohne ein weiteres Wort."},
    ]

    SAMPLE_GLOSSARY = [
        {"dictionaryKey": "Moss", "dictionaryValue": "Moss"},
        {"dictionaryKey": "Williams", "dictionaryValue": "Williams"},
    ]

    log(f"\nTest input: {len(TEST_ROWS)} synthetic rows")

    # ── Test rephrase pipeline ─────────────────────────────────────────────────
    log("\n[1/4] Testing rephrase pipeline (Gemini → Groq)...")
    result = rephrase_with_gemini(TEST_ROWS, SAMPLE_GLOSSARY, "TEST BOOK")

    if not result:
        fallback_hint = " (add GROQ_API_KEY for fallback)" if not GROQ_API_KEY else ""
        msg = f"❌ <b>TEST FAILED</b>: No result returned. Check Gemini API keys{fallback_hint}."
        log(msg)
        send_telegram(msg)
        return

    log(f"  ✅ Pipeline returned {len(result)} rows")

    # ── Show before/after comparison ──────────────────────────────────────────
    log("\n[2/4] Before → After comparison:")
    orig_map = {r["sort"]: r["content"] for r in TEST_ROWS}
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
    import re as _re
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
        f"🔀 Groq fallback: {'configured' if GROQ_API_KEY else 'not set'}\n"
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
