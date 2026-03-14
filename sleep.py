"""
sleep.py - Naechtliche KG-Konsolidierung
Direkt im KG-MCP-Service. Kein N8N noetig.

5 Phasen:
  Phase 0: Graph-Hygiene (rein SQL) - Zyklen, Phantome, Waisen, Stale Seeds
  Phase 1: Inventur (Sonnet) - Was ist heute passiert?
  Phase 2: Reflexion + Traum (Opus) - Muster, Synthese, Traum-Bild
  Phase 3: Umsetzung - Traum-Dokument speichern + E-Mail senden
  Phase 3b: Tagebuch - Menschenlesbares Projekt-Update fuer Kais Dashboard

Scheduler: Threading-basiert (daemon), 1:00 CET/CEST.
REST: /sleep/status, /sleep/trigger, /sleep/hygiene
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
            return False
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


def phase_0_hygiene(auto_fix=True):
    """Strukturelle Graph-Hygiene. Rein SQL-basiert, kein LLM.

    Checks:
      1. Zyklen: bidirektionale contains-Edges (A contains B AND B contains A)
      2. Duplikate: identische Edges (src+tgt+relation mehrfach)
      3. Verwaiste Nodes: keine einzige Edge (weder src noch tgt)
      4. Stale Seeds: seed-Nodes die seit >14 Tagen nicht aktualisiert wurden
      5. Phantom-Edges: Edges die auf nicht-existente Nodes zeigen

    Returns dict mit findings + fixes.
    """
    db = _db()
    findings = {
        "cycles": [],
        "duplicates": [],
        "orphans": [],
        "stale_seeds": [],
        "phantom_edges": [],
        "auto_fixed": [],
    }

    try:
        now = time.time()
        fourteen_days_ago = now - (14 * 86400)

        # --- 1. Zyklen: bidirektionale contains ---
        cycles = db.execute("""
            SELECT e1.source_id AS a, e1.target_id AS b,
                   e1.weight AS w1, e2.weight AS w2
            FROM edges e1
            JOIN edges e2
              ON e1.source_id = e2.target_id
             AND e1.target_id = e2.source_id
             AND e1.relation = e2.relation
            WHERE e1.relation = 'contains'
              AND e1.source_id < e1.target_id
        """).fetchall()

        for c in cycles:
            pair = {"a": c["a"], "b": c["b"], "w1": c["w1"], "w2": c["w2"]}
            findings["cycles"].append(pair)
            if auto_fix:
                # Behalte die Edge mit höherem Weight, lösche die andere
                if c["w1"] >= c["w2"]:
                    db.execute(
                        "DELETE FROM edges WHERE source_id=? AND target_id=? AND relation='contains'",
                        (c["b"], c["a"]),
                    )
                    findings["auto_fixed"].append(
                        f"Zyklus {c['a']}<->contains->{c['b']}: "
                        f"behielt {c['a']}→{c['b']} (w={c['w1']}), "
                        f"löschte {c['b']}→{c['a']} (w={c['w2']})"
                    )
                else:
                    db.execute(
                        "DELETE FROM edges WHERE source_id=? AND target_id=? AND relation='contains'",
                        (c["a"], c["b"]),
                    )
                    findings["auto_fixed"].append(
                        f"Zyklus {c['a']}<->contains->{c['b']}: "
                        f"behielt {c['b']}→{c['a']} (w={c['w2']}), "
                        f"löschte {c['a']}→{c['b']} (w={c['w1']})"
                    )

        # --- 2. Phantom-Edges: zeigen auf nicht-existente Nodes ---
        phantoms = db.execute("""
            SELECT e.source_id, e.target_id, e.relation
            FROM edges e
            LEFT JOIN nodes n1 ON e.source_id = n1.id
            LEFT JOIN nodes n2 ON e.target_id = n2.id
            WHERE n1.id IS NULL OR n2.id IS NULL
        """).fetchall()

        for p in phantoms:
            findings["phantom_edges"].append({
                "src": p["source_id"],
                "tgt": p["target_id"],
                "rel": p["relation"],
            })
            if auto_fix:
                db.execute(
                    "DELETE FROM edges WHERE source_id=? AND target_id=? AND relation=?",
                    (p["source_id"], p["target_id"], p["relation"]),
                )
                findings["auto_fixed"].append(
                    f"Phantom-Edge gelöscht: {p['source_id']}→{p['relation']}→{p['target_id']}"
                )

        # --- 3. Verwaiste Nodes: kein einziger Edge ---
        orphans = db.execute("""
            SELECT n.id, n.domain, n.status, n.summary, n.kai_note, n.type,
                   CAST((? - n.updated_at) / 86400 AS INTEGER) AS days_stale
            FROM nodes n
            LEFT JOIN edges e1 ON n.id = e1.source_id
            LEFT JOIN edges e2 ON n.id = e2.target_id
            WHERE e1.source_id IS NULL AND e2.target_id IS NULL
            ORDER BY n.updated_at ASC
        """, (now,)).fetchall()

        # Build domain→parent mapping for recommendations
        domain_parents = {}
        parent_rows = db.execute("""
            SELECT DISTINCT n.id, n.domain, n.summary
            FROM nodes n
            JOIN edges e ON n.id = e.source_id AND e.relation = 'contains'
            WHERE n.domain != ''
            ORDER BY n.domain
        """).fetchall()
        for p in parent_rows:
            dom = p["domain"].split(".")[0] if p["domain"] else ""
            if dom not in domain_parents:
                domain_parents[dom] = []
            domain_parents[dom].append({"id": p["id"], "summary": (p["summary"] or "")[:60]})

        for o in orphans:
            nid = o["id"]
            dom = (o["domain"] or "").split(".")[0]
            kai_note = o["kai_note"] or ""

            # Heuristic recommendations
            suggested = domain_parents.get(dom, [])[:5]
            rec_type = "review"  # default
            rec_text = "Manuell prüfen und einordnen oder archivieren."

            if nid.startswith("pattern_") or nid.startswith("workaround_"):
                rec_type = "archive"
                rec_text = "Dokumentiertes Muster/Workaround. Inhalt ggf. in kai_note eines verwandten Nodes einarbeiten, dann archivieren."
            elif nid.startswith("bug_") or nid.startswith("fix_"):
                rec_type = "archive"
                rec_text = "Bug-Fix-Dokumentation. Prüfen ob noch relevant, sonst archivieren."
            elif o["status"] == "verified" and o["days_stale"] > 14:
                rec_type = "wire"
                rec_text = "Verifizierter Node ohne Verbindung. Wahrscheinlich an einen WBS-Elternknoten verdrahten."
            elif o["status"] == "seed" and o["days_stale"] > 21:
                rec_type = "archive"
                rec_text = "Alter Seed ohne Edges. Entweder weiterentwickeln und verdrahten, oder archivieren."

            findings["orphans"].append({
                "id": nid,
                "domain": o["domain"],
                "status": o["status"],
                "type": o["type"],
                "summary": o["summary"],
                "kai_note": kai_note[:200] if kai_note else "",
                "days_stale": o["days_stale"],
                "rec_type": rec_type,
                "rec_text": rec_text,
                "suggested_parents": suggested,
            })
        # Verwaiste Nodes werden NICHT auto-gelöscht - zu gefährlich

        # --- 4. Stale Seeds: seed seit >14 Tagen ohne Update ---
        stale = db.execute("""
            SELECT id, domain, summary,
                   CAST((? - updated_at) / 86400 AS INTEGER) AS days_stale
            FROM nodes
            WHERE status = 'seed' AND updated_at < ?
            ORDER BY updated_at ASC
            LIMIT 30
        """, (now, fourteen_days_ago)).fetchall()

        for s in stale:
            # Check if stale seed has edges (it has, otherwise it would be orphan)
            edge_count = db.execute(
                "SELECT COUNT(*) as c FROM edges WHERE source_id=? OR target_id=?",
                (s["id"], s["id"])
            ).fetchone()["c"]
            rec_text = "Alter Seed – Status zu explored/verified aktualisieren oder archivieren."
            if edge_count == 0:
                rec_text = "Alter Seed ohne Verbindungen – verdrahten oder archivieren."
            elif s["days_stale"] > 30:
                rec_text = "Seit >30 Tagen unverändert. Entweder aktivieren oder bewusst parken."
            findings["stale_seeds"].append({
                "id": s["id"],
                "domain": s["domain"],
                "summary": s["summary"],
                "days_stale": s["days_stale"],
                "edge_count": edge_count,
                "rec_text": rec_text,
            })
        # Stale Seeds werden NICHT auto-geändert - braucht menschliche Entscheidung

        if auto_fix and findings["auto_fixed"]:
            db.commit()

        # Zusammenfassung
        findings["summary"] = {
            "cycles_found": len(findings["cycles"]),
            "phantoms_found": len(findings["phantom_edges"]),
            "orphans_found": len(findings["orphans"]),
            "stale_seeds_found": len(findings["stale_seeds"]),
            "auto_fixes_applied": len(findings["auto_fixed"]),
        }

        logger.info(
            f"Phase 0 Hygiene: {findings['summary']['cycles_found']} Zyklen, "
            f"{findings['summary']['phantoms_found']} Phantome, "
            f"{findings['summary']['orphans_found']} Waisen, "
            f"{findings['summary']['stale_seeds_found']} stale Seeds, "
            f"{findings['summary']['auto_fixes_applied']} Auto-Fixes"
        )

        return findings

    except Exception as e:
        logger.error(f"Phase 0 Fehler: {e}", exc_info=True)
        return {"error": str(e), "summary": {"error": True}}
    finally:
        db.close()


# === HYGIENE ANALYSE (nach phase_0_hygiene, vor Phase 1) ===

def hygiene_analyze_node(node_id):
    """Sonnet-basierte Tiefenanalyse eines Hygiene-Eintrags.
    
    Sammelt Kontext: Node-Daten, Domain-Nachbarn, potenzielle Eltern,
    ähnliche Nodes. Lässt Sonnet ein strukturiertes Reasoning erstellen.
    
    Returns: {node_id, reasoning, action, action_detail, confidence}
    """
    db = _db()
    try:
        now = time.time()
        
        # 1. Vollständige Node-Daten
        node = db.execute(
            "SELECT id, domain, status, type, summary, kai_note, "
            "CAST((? - updated_at) / 86400 AS INTEGER) AS days_stale, "
            "CAST((? - created_at) / 86400 AS INTEGER) AS days_old "
            "FROM nodes WHERE id=?",
            (now, now, node_id)
        ).fetchone()
        if not node:
            return {"error": f"Node {node_id} not found"}
        
        # 2. Edges dieses Nodes (falls doch welche existieren – für stale seeds)
        edges = db.execute(
            "SELECT source_id, target_id, relation FROM edges "
            "WHERE source_id=? OR target_id=? LIMIT 10",
            (node_id, node_id)
        ).fetchall()
        edge_list = [{"src": e["source_id"], "tgt": e["target_id"], "rel": e["relation"]} for e in edges]
        
        # 3. Potenzielle Eltern-Nodes (gleiche Domain, haben contains-Edges)
        dom_base = (node["domain"] or "").split(".")[0]
        parents = db.execute("""
            SELECT DISTINCT n.id, n.summary, n.status,
                   COUNT(e.target_id) as child_count
            FROM nodes n
            JOIN edges e ON n.id = e.source_id AND e.relation = 'contains'
            WHERE n.domain LIKE ? AND n.id != ?
            GROUP BY n.id
            ORDER BY child_count DESC
            LIMIT 8
        """, (f"{dom_base}%", node_id)).fetchall()
        parent_list = [{"id": p["id"], "summary": (p["summary"] or "")[:80], "status": p["status"], "children": p["child_count"]} for p in parents]
        
        # 4. Ähnliche Nodes (Namens-Ähnlichkeit)
        # Einfache Heuristik: Wörter aus der Node-ID suchen
        id_parts = node_id.replace("-", "_").split("_")
        similar = []
        for part in id_parts:
            if len(part) < 4:
                continue
            matches = db.execute(
                "SELECT id, summary, status, domain FROM nodes "
                "WHERE id LIKE ? AND id != ? LIMIT 3",
                (f"%{part}%", node_id)
            ).fetchall()
            for m in matches:
                if m["id"] not in [s["id"] for s in similar]:
                    similar.append({"id": m["id"], "summary": (m["summary"] or "")[:80], "status": m["status"], "domain": m["domain"]})
        similar = similar[:6]
        
        # 5. Gesetze aus state
        state_row = db.execute("SELECT value FROM state WHERE key='custom'").fetchone()
        gesetze_text = ""
        if state_row and state_row["value"]:
            try:
                custom = json.loads(state_row["value"])
                gesetze = custom.get("gesetze", [])
                gesetze_text = "\n".join(f"G{i+1}: {g}" for i, g in enumerate(gesetze))
            except:
                pass
        
        # 6. Prompt bauen
        prompt = f"""Du analysierst einen Node im Knowledge-Graphen von Kai Lohmann.
Kai ist Versicherungsmakler und baut ein digitales Ökosystem (lo-board CRM, K-AI Orchestrator, diverse Tools).
Der Graph bildet sein gesamtes Denk- und Arbeitsfeld ab.

## Node
- ID: {node["id"]}
- Domain: {node["domain"]}
- Status: {node["status"]}
- Typ: {node["type"]}
- Alter: {node["days_old"]} Tage (seit Erstellung)
- Letzte Aktualisierung: vor {node["days_stale"]} Tagen
- Summary: {node["summary"] or "(leer)"}
- kai_note: {(node["kai_note"] or "(leer)")[:500]}

## Aktuelle Edges ({len(edge_list)})
{json.dumps(edge_list, ensure_ascii=False) if edge_list else "(keine – verwaister Node)"}

## Potenzielle Eltern in Domain "{dom_base}" (haben contains-Edges)
{json.dumps(parent_list, ensure_ascii=False, indent=1) if parent_list else "(keine gefunden)"}

## Ähnliche Nodes (Namens-Ähnlichkeit)
{json.dumps(similar, ensure_ascii=False, indent=1) if similar else "(keine gefunden)"}

## Kais Arbeitsgesetze
{gesetze_text[:1000] if gesetze_text else "(nicht geladen)"}

## Aufgabe
Analysiere diesen Node und gib eine fundierte Empfehlung. Beantworte:

1. **Was ist das?** – Worum geht es bei diesem Node? (1-2 Sätze)
2. **Warum ist er verwaist/stale?** – Hypothese warum keine Edges existieren oder er veraltet ist
3. **Empfehlung** – Eine von: ARCHIVE, WIRE, PROMOTE, REVIEW, MERGE
   - ARCHIVE: Node hat keinen weiteren Nutzen, Wissen ggf. in anderen Node übernehmen
   - WIRE: Node gehört an einen bestimmten Eltern-Node (nenne welchen und warum)
   - PROMOTE: Status sollte aktualisiert werden (z.B. seed→explored)
   - REVIEW: Braucht Kais persönliche Entscheidung, nicht automatisierbar
   - MERGE: Inhalt ist Duplikat eines anderen Nodes (nenne welchen)
4. **Begründung** – 2-4 Sätze warum genau diese Empfehlung

Antworte NUR als JSON:
{{"what": "...", "why_orphan": "...", "action": "ARCHIVE|WIRE|PROMOTE|REVIEW|MERGE", "target_node": "node_id oder null", "reasoning": "...", "confidence": "high|medium|low"}}"""

        # 7. LLM Call
        raw = _llm("claude-sonnet-4-20250514", prompt, max_tokens=600)
        result = _parse_json(raw)
        result["node_id"] = node_id
        
        logger.info(f"Hygiene-Analyse {node_id}: {result.get('action')} ({result.get('confidence')})")
        return result
        
    except json.JSONDecodeError as e:
        logger.error(f"Hygiene-Analyse JSON-Fehler für {node_id}: {e}")
        return {"node_id": node_id, "error": f"LLM gab kein valides JSON: {str(e)}", "raw": raw[:500] if 'raw' in locals() else ""}
    except Exception as e:
        logger.error(f"Hygiene-Analyse Fehler für {node_id}: {e}", exc_info=True)
        return {"node_id": node_id, "error": str(e)}
    finally:
        db.close()


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

    msg.attach(MIMEText(persist_result["content"], "plain", "utf-8"))

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


def phase_3b_diary(analyse, inventory, traum):
    """Menschenlesbares Projekt-Tagebuch fuer Kais Dashboard."""
    session_titles = [d["title"] for d in inventory.get("session_docs", [])]
    node_changes = [
        f"- {n['id']} ({n['domain']}/{n['status']}): {n['summary'][:80]}"
        for n in inventory.get("recent_nodes", [])[:20]
    ]

    prompt = (
        "Du schreibst ein kurzes Projekt-Tagebuch fuer Kai Lohmann. "
        "Kai ist Versicherungsmakler und baut ein digitales Oekosystem "
        "(lo-board, Knowledge Graph, Rechner-Tools, Vertrieb). "
        "Schreibe in der Du-Form, direkt an Kai gerichtet.\n\n"
        "Tagesanalyse:\n"
        f"{json.dumps(analyse, ensure_ascii=False, indent=2)}\n\n"
        "Heutige Sessions:\n"
        + "\n".join(f"- {t}" for t in session_titles) + "\n\n"
        "Geaenderte Nodes:\n"
        + "\n".join(node_changes) + "\n\n"
        f"Traum-Titel der Nacht: {traum.get('traum_titel', '')}\n"
        f"Fokus-Vorschlag: {traum.get('vorschlag_fokus_morgen', '')}\n\n"
        "Schreibe ein kompaktes Projekt-Tagebuch (max 300 Woerter). "
        "Struktur mit Markdown-Headern:\n"
        "## Fortschritte heute\n"
        "Was wurde konkret erreicht oder umgesetzt?\n\n"
        "## Offene Punkte\n"
        "Was wartet noch auf Arbeit?\n\n"
        "## Naechste Schritte\n"
        "Was sollte als naechstes passieren?\n\n"
        "Halte es kurz, konkret, nuetzlich. Keine Poesie, kein Fuelltext. "
        "Verwende Markdown-Listen wo sinnvoll."
    )

    try:
        content = _llm("claude-sonnet-4-20250514", prompt, 1500)
    except Exception as e:
        logger.error(f"Tagebuch-LLM-Fehler: {e}")
        content = (
            "## Fortschritte heute\n"
            + analyse.get("zusammenfassung", "Keine Zusammenfassung verfuegbar.")
            + "\n\n## Offene Punkte\n"
            + "\n".join(f"- {f}" for f in analyse.get("offene_faeden", []))
        )

    db = _db()
    try:
        now = time.time()
        datum = datetime.now(TZ_BERLIN).strftime("%d.%m.%Y")
        doc_id = str(uuid.uuid4())[:8]
        doc_title = f"Tagebuch - {datum}"

        db.execute(
            "INSERT INTO documents "
            "(id,title,content,session_number,node_ids,created_at,updated_at) "
            "VALUES (?,?,?,?,?,?,?)",
            (doc_id, doc_title, content, 0, "[]", now, now),
        )
        db.commit()
        logger.info(f"Tagebuch gespeichert: {doc_title}")
        return {"doc_id": doc_id, "doc_title": doc_title}
    finally:
        db.close()


# ==============================================================
# HAUPTFUNKTION
# ==============================================================

def run_sleep_cycle():
    """Kompletter Schlaf-Zyklus. Synchron."""
    global _last_run
    logger.info("=== SCHLAF-ZYKLUS START ===")
    start = time.time()

    try:
        # Phase 0: Graph-Hygiene (strukturell, kein LLM)
        logger.info("Phase 0: Graph-Hygiene...")
        hygiene = phase_0_hygiene(auto_fix=True)
        logger.info(
            f"Phase 0 fertig: {hygiene.get('summary', {}).get('auto_fixes_applied', 0)} Fixes"
        )

        # Phase 1
        logger.info("Phase 1: Inventur...")
        inventory = phase_1_collect()
        inventory["hygiene"] = hygiene

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

        # Phase 3b: Projekt-Tagebuch
        logger.info("Phase 3b: Projekt-Tagebuch...")
        diary = phase_3b_diary(analyse, inventory, traum)
        logger.info(f"Tagebuch: {diary.get('doc_title', '?')}")

        elapsed = time.time() - start
        result = {
            "status": "completed",
            "timestamp": datetime.now(TZ_BERLIN).isoformat(),
            "elapsed_s": round(elapsed, 1),
            "doc_id": persist["doc_id"],
            "traum_titel": traum.get("traum_titel", ""),
            "diary_id": diary.get("doc_id", ""),
            "email_sent": email_ok,
            "hygiene": hygiene.get("summary", {}),
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


# ==============================================================
# AUTO-START: Scheduler startet beim ersten Import
# ==============================================================

if SLEEP_ENABLED:
    started = start_scheduler()
    if started:
        logger.info("Scheduler auto-gestartet via Modul-Import")
