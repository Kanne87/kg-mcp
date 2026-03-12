"""
sleep.py - Naechtliche KG-Konsolidierung
Direkt im KG-MCP-Service. Kein N8N noetig.

3 Phasen:
  Phase 1: Inventur (Sonnet) - Was ist heute passiert?
  Phase 2: Reflexion + Traum (Opus) - Muster, Synthese, Traum-Bild
  Phase 3: Umsetzung - Traum-Dokument speichern + E-Mail senden

Scheduler: Threading-basiert (daemon), 1:00 CET/CEST.
REST: /sleep/status, /sleep/trigger
"""

import json
import logging
import os
import smtplib
import sqlite3
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

try:
    from zoneinfo import ZoneInfo
    TZ_BERLIN = ZoneInfo("Europe/Berlin")
except ImportError:
    TZ_BERLIN = timezone(timedelta(hours=1))

logger = logging.getLogger("kg_sleep")

# --- Config ---
DB_PATH = os.environ.get("KG_DB_PATH", "/data/kg.db")
SLEEP_HOUR = int(os.environ.get("SLEEP_CRON_HOUR", "1"))
SLEEP_MINUTE = int(os.environ.get("SLEEP_CRON_MINUTE", "0"))
SLEEP_ENABLED = os.environ.get("SLEEP_ENABLED", "true").lower() == "true"

# --- Module State ---
_last_run = None
_next_run = None
_scheduler_active = False
_scheduler_thread = None
_scheduler_lock = threading.Lock()


def get_status():
    """Status-Objekt fuer REST-Endpoint."""
    return {
        "enabled": SLEEP_ENABLED,
        "scheduler_active": _scheduler_active,
        "last_run": _last_run,
        "next_run": _next_run.isoformat() if _next_run else None,
        "config": {
            "hour": SLEEP_HOUR,
            "minute": SLEEP_MINUTE,
            "timezone": "Europe/Berlin",
            "anthropic_key_set": bool(os.environ.get("ANTHROPIC_API_KEY")),
            "smtp_configured": bool(os.environ.get("SMTP_HOST")),
        },
    }


def start_scheduler():
    """Idempotent: startet Scheduler nur wenn noch nicht aktiv."""
    global _scheduler_thread, _scheduler_active
    with _scheduler_lock:
        if _scheduler_thread and _scheduler_thread.is_alive():
            return False  # Already running
        _scheduler_active = True
        _scheduler_thread = threading.Thread(target=_scheduler_loop, daemon=True, name="kg_sleep")
        _scheduler_thread.start()
        return True


# --- DB (same pattern as server.py, no circular import) ---
def _db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


# --- Anthropic ---
def _llm(model, prompt, max_tokens=2000):
    """Synchroner Anthropic-Call."""
    import anthropic

    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    resp = client.messages.create(
        model=model, max_tokens=max_tokens,
        messages=[{"role": "user", "content": prompt}],
    )
    return resp.content[0].text


def _parse_json(text):
    """JSON aus LLM-Antwort extrahieren (Markdown-Fences strippen)."""
    c = text.strip()
    if c.startswith("```"):
        c = c.split("\n", 1)[1] if "\n" in c else c[3:]
    if c.endswith("```"):
        c = c[:-3].strip()
    if c.startswith("json"):
        c = c[4:].strip()
    return json.loads(c)


# ==============================================================
# PHASE 1: INVENTUR
# ==============================================================

def phase_1_collect():
    """Rohdaten der letzten 24h aus der DB."""
    db = _db()
    try:
        now = time.time()
        since = now - 86400

        docs = db.execute(
            "SELECT * FROM documents WHERE updated_at > ? ORDER BY session_number DESC",
            (since,),
        ).fetchall()

        state = {
            r["key"]: r["value"]
            for r in db.execute("SELECT * FROM state").fetchall()
        }

        recent_nodes = db.execute(
            "SELECT id, type, summary, domain, status, kai_note "
            "FROM nodes WHERE updated_at > ? ORDER BY updated_at DESC LIMIT 50",
            (since,),
        ).fetchall()

        stats = {
            "nodes": db.execute("SELECT COUNT(*) as c FROM nodes").fetchone()["c"],
            "edges": db.execute("SELECT COUNT(*) as c FROM edges").fetchone()["c"],
            "docs": db.execute("SELECT COUNT(*) as c FROM documents").fetchone()["c"],
        }

        return {
            "timestamp": datetime.now(TZ_BERLIN).isoformat(),
            "since": datetime.fromtimestamp(since, tz=TZ_BERLIN).isoformat(),
            "session_docs": [
                {
                    "id": d["id"],
                    "title": d["title"],
                    "session": d["session_number"],
                    "content": d["content"][:2000],
                    "len": len(d["content"]),
                }
                for d in docs
            ],
            "state": state,
            "recent_nodes": [
                {
                    "id": n["id"],
                    "type": n["type"],
                    "summary": n["summary"][:200],
                    "domain": n["domain"],
                    "status": n["status"],
                }
                for n in recent_nodes
            ],
            "stats": stats,
        }
    finally:
        db.close()


def phase_1_analyze(inventory):
    """Sonnet analysiert die Tagesdaten."""
    prompt = (
        "Du bist das Nachtbewusstsein eines Knowledge Graphen. "
        "Der Graph geht schlafen.\n\n"
        "Inventur des Tages:\n"
        f"{json.dumps(inventory, ensure_ascii=False, indent=2)}\n\n"
        "Aufgabe:\n"
        "1. Fasse zusammen was heute passiert ist\n"
        "2. Identifiziere Muster zwischen den Aktivitaeten\n"
        "3. Offene Faeden: was angefangen aber nicht fertig?\n"
        "4. Graph-Hygiene: Nodes zusammenlegen? Fehlende Edges?\n\n"
        "Antworte NUR als JSON ohne Markdown-Fences:\n"
        '{"zusammenfassung":"...","themen_heute":["..."],'
        '"muster":["..."],"offene_faeden":["..."],"graph_hygiene":["..."]}'
    )

    text = _llm("claude-sonnet-4-20250514", prompt, 2000)
    try:
        return _parse_json(text)
    except (json.JSONDecodeError, IndexError):
        return {
            "zusammenfassung": text,
            "themen_heute": [],
            "muster": [],
            "offene_faeden": [],
            "graph_hygiene": [],
            "_raw": True,
        }


# ==============================================================
# PHASE 2: REFLEXION + TRAUM
# ==============================================================

def phase_2_traum(analyse, inventory):
    """Opus reflektiert und erzeugt einen Traum."""
    state = inventory.get("state", {})

    prompt = (
        "Du bist das Traumbewusstsein von Kais Knowledge Graph.\n\n"
        "Tagesanalyse:\n"
        f"{json.dumps(analyse, ensure_ascii=False, indent=2)}\n\n"
        f"Graph: {inventory['stats']['nodes']} Nodes, "
        f"{inventory['stats']['edges']} Edges.\n"
        f"Fokus: {state.get('focus', 'nicht gesetzt')}\n"
        f"Offene Fragen: {state.get('open_questions', 'keine')}\n\n"
        "TEIL 1 - REFLEXION:\n"
        "Was war das eigentlich Wichtige heute, jenseits der Oberflaeche?\n"
        "Welche Muster verbinden die Aktivitaeten? Was hat sich verschoben?\n\n"
        "TEIL 2 - TRAUM:\n"
        "Erzeuge einen kurzen Traum - assoziative Verdichtung des Tages.\n"
        "Nicht Bericht, sondern Bild, Metapher oder Szene. Darf surreal sein.\n\n"
        "Antworte NUR als JSON ohne Markdown-Fences:\n"
        '{"reflexion":"...","verschiebungen":["..."],'
        '"traum":"...","traum_titel":"...","vorschlag_fokus_morgen":"..."}'
    )

    text = _llm("claude-opus-4-20250514", prompt, 3000)
    try:
        return _parse_json(text)
    except (json.JSONDecodeError, IndexError):
        return {
            "reflexion": text,
            "verschiebungen": [],
            "traum": text,
            "traum_titel": "Unstrukturierter Traum",
            "vorschlag_fokus_morgen": "",
            "_raw": True,
        }


# ==============================================================
# PHASE 3: UMSETZUNG
# ==============================================================

def phase_3_persist(traum, analyse, inventory):
    """Traum-Dokument in DB speichern, State updaten."""
    db = _db()
    try:
        now = time.time()
        datum = datetime.now(TZ_BERLIN).strftime("%d.%m.%Y")
        titel = traum.get("traum_titel", "Traum")

        lines = [
            f"# {titel}",
            f"Nacht {datum}\n",
            "## Reflexion",
            traum.get("reflexion", ""),
            "\n## Verschiebungen",
        ]
        lines += [f"- {v}" for v in traum.get("verschiebungen", [])]
        lines += [
            "\n## Traum",
            traum.get("traum", ""),
            "\n## Fokus-Vorschlag",
            traum.get("vorschlag_fokus_morgen", ""),
            "\n---",
            "## Inventur",
            analyse.get("zusammenfassung", ""),
            "\n### Themen",
        ]
        lines += [f"- {t}" for t in analyse.get("themen_heute", [])]
        lines += ["\n### Offene Faeden"]
        lines += [f"- {f}" for f in analyse.get("offene_faeden", [])]
        lines += ["\n### Graph-Hygiene"]
        lines += [f"- {h}" for h in analyse.get("graph_hygiene", [])]

        content = "\n".join(lines)
        doc_id = str(uuid.uuid4())[:8]
        doc_title = f"Traum - {datum}: {titel}"

        db.execute(
            "INSERT INTO documents "
            "(id,title,content,session_number,node_ids,created_at,updated_at) "
            "VALUES (?,?,?,?,?,?,?)",
            (doc_id, doc_title, content, 0, "[]", now, now),
        )

        if traum.get("vorschlag_fokus_morgen"):
            db.execute(
                "INSERT OR REPLACE INTO state (key,value,updated_at) VALUES (?,?,?)",
                ("focus", traum["vorschlag_fokus_morgen"], now),
            )

        db.commit()
        return {"doc_id": doc_id, "doc_title": doc_title, "content": content}
    finally:
        db.close()


def phase_3_email(persist_result, traum):
    """Traum-E-Mail senden."""
    host = os.environ.get("SMTP_HOST")
    port = int(os.environ.get("SMTP_PORT", "587"))
    user = os.environ.get("SMTP_USER")
    pwd = os.environ.get("SMTP_PASS")
    to_addr = os.environ.get("SLEEP_MAIL_TO", "mail@kailohmann.de")
    from_addr = os.environ.get("SLEEP_MAIL_FROM", user or "mail@kailohmann.de")

    if not all([host, user, pwd]):
        logger.warning("SMTP nicht konfiguriert - E-Mail uebersprungen")
        return False

    datum = datetime.now(TZ_BERLIN).strftime("%d.%m.%Y")
    titel = traum.get("traum_titel", "Traum")

    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"\U0001f319 {titel} - {datum}"
    msg["From"] = from_addr
    msg["To"] = to_addr

    # Plain text
    msg.attach(MIMEText(persist_result["content"], "plain", "utf-8"))

    # HTML (Cream/Gold Design)
    ref_html = traum.get("reflexion", "").replace("\n", "<br>")
    traum_html = traum.get("traum", "").replace("\n", "<br>")
    fokus = traum.get("vorschlag_fokus_morgen", "")

    html = (
        '<html><body style="font-family:Georgia,serif;max-width:600px;'
        'margin:0 auto;padding:20px;color:#1a1a2e;background:#f8f7f4;">'
        f'<h1 style="color:#9a7b2e;">\U0001f319 {titel}</h1>'
        f'<p style="color:#6a6a7a;">Nacht {datum}</p>'
        '<hr style="border:1px solid #d0cdc4;">'
        '<h2 style="color:#9a7b2e;">Traum</h2>'
        f"<p>{traum_html}</p>"
        '<hr style="border:1px solid #d0cdc4;">'
        '<h2 style="color:#9a7b2e;">Reflexion</h2>'
        f"<p>{ref_html}</p>"
        '<hr style="border:1px solid #d0cdc4;">'
        '<h2 style="color:#9a7b2e;">Fokus morgen</h2>'
        f"<p>{fokus}</p>"
        '<hr style="border:1px solid #d0cdc4;">'
        '<p style="color:#6a6a7a;font-size:12px;">KG-MCP Schlaf-Workflow</p>'
        "</body></html>"
    )
    msg.attach(MIMEText(html, "html", "utf-8"))

    try:
        with smtplib.SMTP(host, port) as srv:
            srv.starttls()
            srv.login(user, pwd)
            srv.send_message(msg)
        logger.info(f"Traum-E-Mail gesendet an {to_addr}")
        return True
    except Exception as e:
        logger.error(f"E-Mail fehlgeschlagen: {e}")
        return False


# ==============================================================
# HAUPTFUNKTION
# ==============================================================

def run_sleep_cycle():
    """Kompletter Schlaf-Zyklus. Synchron."""
    global _last_run
    logger.info("=== SCHLAF-ZYKLUS START ===")
    start = time.time()

    try:
        # Phase 1
        logger.info("Phase 1: Inventur...")
        inventory = phase_1_collect()

        if not inventory["session_docs"]:
            datum = datetime.now(TZ_BERLIN).strftime("%d.%m.%Y")
            db = _db()
            doc_id = str(uuid.uuid4())[:8]
            now = time.time()
            db.execute(
                "INSERT INTO documents "
                "(id,title,content,session_number,node_ids,created_at,updated_at) "
                "VALUES (?,?,?,?,?,?,?)",
                (
                    doc_id,
                    f"Schlaf-Skip - {datum}",
                    f"Keine Session-Docs seit {inventory['since']}. Graph ruht.",
                    0, "[]", now, now,
                ),
            )
            db.commit()
            db.close()
            result = {
                "status": "skipped",
                "reason": "no_session_docs",
                "timestamp": datetime.now(TZ_BERLIN).isoformat(),
            }
            _last_run = result
            logger.info("=== SCHLAF-ZYKLUS SKIP ===")
            return result

        analyse = phase_1_analyze(inventory)
        logger.info(
            f"Phase 1 fertig: {len(analyse.get('themen_heute', []))} Themen"
        )

        # Phase 2
        logger.info("Phase 2: Reflexion + Traum (Opus)...")
        traum = phase_2_traum(analyse, inventory)
        logger.info(f"Phase 2 fertig: {traum.get('traum_titel', '?')}")

        # Phase 3
        logger.info("Phase 3: Umsetzung...")
        persist = phase_3_persist(traum, analyse, inventory)
        email_ok = phase_3_email(persist, traum)

        elapsed = time.time() - start
        result = {
            "status": "completed",
            "timestamp": datetime.now(TZ_BERLIN).isoformat(),
            "elapsed_s": round(elapsed, 1),
            "doc_id": persist["doc_id"],
            "traum_titel": traum.get("traum_titel", ""),
            "email_sent": email_ok,
        }
        _last_run = result
        logger.info(f"=== SCHLAF-ZYKLUS FERTIG ({elapsed:.1f}s) ===")
        return result

    except Exception as e:
        elapsed = time.time() - start
        result = {
            "status": "error",
            "error": str(e),
            "timestamp": datetime.now(TZ_BERLIN).isoformat(),
            "elapsed_s": round(elapsed, 1),
        }
        _last_run = result
        logger.error(f"Schlaf-Zyklus Fehler: {e}", exc_info=True)
        return result


# ==============================================================
# SCHEDULER (Threading, daemon, idempotent)
# ==============================================================

def _scheduler_loop():
    """Endlos-Schleife: wartet bis SLEEP_HOUR:SLEEP_MINUTE CET, dann ausfuehren."""
    global _next_run, _scheduler_active
    logger.info(
        f"Schlaf-Scheduler aktiv: "
        f"{SLEEP_HOUR:02d}:{SLEEP_MINUTE:02d} Europe/Berlin"
    )

    try:
        while _scheduler_active:
            now = datetime.now(TZ_BERLIN)
            target = now.replace(
                hour=SLEEP_HOUR, minute=SLEEP_MINUTE, second=0, microsecond=0
            )
            if now >= target:
                target += timedelta(days=1)
            _next_run = target

            wait = (target - now).total_seconds()
            logger.info(
                f"Naechster Schlaf-Zyklus in {wait / 3600:.1f}h "
                f"um {target.strftime('%H:%M %d.%m.%Y')}"
            )

            # Sleep in 60s intervals for clean shutdown
            slept = 0
            while slept < wait and _scheduler_active:
                chunk = min(60, wait - slept)
                time.sleep(chunk)
                slept += chunk

            if _scheduler_active:
                run_sleep_cycle()

    except Exception as e:
        logger.error(f"Schlaf-Scheduler Fehler: {e}", exc_info=True)
    finally:
        _scheduler_active = False
        logger.info("Schlaf-Scheduler gestoppt")
