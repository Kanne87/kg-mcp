"""
kg_mcp - Knowledge Graph MCP Server
Optimized for LLM context efficiency. All outputs are compact structured data.
No prose, no markdown, no human formatting. Pure graph.

Schicht 1: Nodes + Edges (assoziativ, komprimiert)
Schicht 2: Documents (episodisch, Session-Destillate)

Architecture: Lean boot + domain-based lazy loading.
Boot returns only state, domain index, and meta-nodes.
Use kg_load_domain() to pull specific knowledge areas into context.
"""

import json
import sqlite3
import os
import time
import uuid
from contextlib import asynccontextmanager
from typing import Optional

from mcp.server.fastmcp import FastMCP
from mcp.server.fastmcp.server import TransportSecuritySettings

# --- Constants ---

DB_PATH = os.environ.get("KG_DB_PATH", "/data/kg.db")

# --- Database ---

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn

def init_db():
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    conn = get_db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS nodes (
            id TEXT PRIMARY KEY,
            type TEXT NOT NULL DEFAULT 'concept',
            summary TEXT NOT NULL DEFAULT '',
            bands TEXT NOT NULL DEFAULT '[]',
            domain TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT 'seed',
            kai_note TEXT NOT NULL DEFAULT '',
            meta TEXT NOT NULL DEFAULT '{}',
            created_at REAL NOT NULL,
            updated_at REAL NOT NULL
        );
        CREATE TABLE IF NOT EXISTS edges (
            source_id TEXT NOT NULL,
            target_id TEXT NOT NULL,
            relation TEXT NOT NULL,
            weight REAL NOT NULL DEFAULT 1.0,
            note TEXT NOT NULL DEFAULT '',
            created_at REAL NOT NULL,
            PRIMARY KEY (source_id, target_id, relation),
            FOREIGN KEY (source_id) REFERENCES nodes(id) ON DELETE CASCADE,
            FOREIGN KEY (target_id) REFERENCES nodes(id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS state (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            updated_at REAL NOT NULL
        );
        CREATE TABLE IF NOT EXISTS documents (
            id TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            content TEXT NOT NULL DEFAULT '',
            session_number INTEGER,
            node_ids TEXT NOT NULL DEFAULT '[]',
            created_at REAL NOT NULL,
            updated_at REAL NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_edges_source ON edges(source_id);
        CREATE INDEX IF NOT EXISTS idx_edges_target ON edges(target_id);
        CREATE INDEX IF NOT EXISTS idx_nodes_type ON nodes(type);
        CREATE INDEX IF NOT EXISTS idx_nodes_status ON nodes(status);
        CREATE INDEX IF NOT EXISTS idx_docs_session ON documents(session_number);
    """)
    # Migration: add domain column if missing (existing databases)
    cols = [r[1] for r in conn.execute("PRAGMA table_info(nodes)").fetchall()]
    if "domain" not in cols:
        conn.execute("ALTER TABLE nodes ADD COLUMN domain TEXT NOT NULL DEFAULT ''")
        conn.commit()
    # Domain index: always ensure it exists (after migration)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_nodes_domain ON nodes(domain)")
    conn.commit()
    now = time.time()
    for k, v in {
        "focus": "HOLOFEELING-Analyse: Muster aus 8 Baenden als Knowledge Graph externalisieren",
        "open_questions": "[]", "last_session": "", "session_count": "0"
    }.items():
        conn.execute("INSERT OR IGNORE INTO state (key, value, updated_at) VALUES (?,?,?)", (k, v, now))
    conn.commit()
    conn.close()

# --- Helpers ---

def _c(d):
    return json.dumps(d, ensure_ascii=False, separators=(',',':'))

def _node(r):
    return {"id":r["id"],"t":r["type"],"s":r["summary"],"b":json.loads(r["bands"]),
            "d":r["domain"],"st":r["status"],"k":r["kai_note"],"m":json.loads(r["meta"])}

def _edge(r):
    return {"src":r["source_id"],"rel":r["relation"],"tgt":r["target_id"],"w":r["weight"],"n":r["note"]}

def _idx(r):
    return {"id":r["id"],"t":r["type"],"st":r["status"],"d":r["domain"]}

def _doc(r):
    return {"id":r["id"],"title":r["title"],"content":r["content"],
            "session":r["session_number"],"node_ids":json.loads(r["node_ids"]),
            "created":r["created_at"],"updated":r["updated_at"]}

def _doc_idx(r):
    return {"id":r["id"],"title":r["title"],"session":r["session_number"],
            "len":len(r["content"]),"updated":r["updated_at"]}

# --- Lifespan ---

@asynccontextmanager
async def app_lifespan(app):
    init_db()
    yield {}

# --- Server ---

mcp = FastMCP(
    "kg_mcp",
    lifespan=app_lifespan,
    host=os.environ.get("KG_HOST", "0.0.0.0"),
    port=int(os.environ.get("KG_PORT", "8099")),
    stateless_http=True,
    transport_security=TransportSecuritySettings(enable_dns_rebinding_protection=False),
)

# ============================================================
# GRAPH TOOLS
# ============================================================

@mcp.tool(name="kg_boot")
async def kg_boot(include_edges: bool = False) -> str:
    """Lean session init. Returns {state:{},domains:[{name,count,last_updated}],meta_nodes:[{full nodes}],edge_count:N,docs:[...]}.
    Only meta-domain nodes are fully loaded. Use kg_load_domain() for other domains. Call FIRST."""
    db = get_db()
    try:
        state = {r["key"]:r["value"] for r in db.execute("SELECT key,value FROM state").fetchall()}

        # Domain index: count + last_updated per domain
        domain_rows = db.execute("""
            SELECT domain, COUNT(*) as cnt, MAX(updated_at) as last_upd
            FROM nodes GROUP BY domain ORDER BY last_upd DESC
        """).fetchall()
        domains = [{"name":r["domain"] or "(unassigned)","count":r["cnt"],"last_updated":r["last_upd"]} for r in domain_rows]

        # Meta-nodes: always loaded (core principles, architecture)
        meta_nodes = [_node(r) for r in db.execute("SELECT * FROM nodes WHERE domain='meta' ORDER BY updated_at DESC").fetchall()]

        ec = db.execute("SELECT COUNT(*) as c FROM edges").fetchone()["c"]
        docs = [_doc_idx(r) for r in db.execute("SELECT * FROM documents ORDER BY session_number DESC, updated_at DESC LIMIT 20").fetchall()]

        res = {"state":state,"domains":domains,"meta_nodes":meta_nodes,"edge_count":ec,"docs":docs}

        if include_edges:
            res["edges"] = [_edge(r) for r in db.execute("SELECT * FROM edges").fetchall()]

        sc = int(state.get("session_count","0")) + 1
        db.execute("INSERT OR REPLACE INTO state (key,value,updated_at) VALUES (?,?,?)", ("session_count",str(sc),time.time()))
        db.commit()
        return _c(res)
    finally:
        db.close()

@mcp.tool(name="kg_load_domain")
async def kg_load_domain(name: str, depth: str = "full") -> str:
    """Load a domain into context. Returns {domain,nodes:[{full}],edges:[],sub_domains:[]}.
    Supports dot-notation: 'holofeeling' loads all, 'holofeeling.baende' loads sub-domain only.
    depth: 'full' (nodes+edges) or 'index' (just node list without summaries)."""
    db = get_db()
    try:
        # Match exact domain or sub-domains (dot-notation)
        pattern = name + "%"
        if depth == "index":
            rows = db.execute("SELECT id, type, status, domain FROM nodes WHERE domain LIKE ? ORDER BY domain, updated_at DESC", (pattern,)).fetchall()
            nodes = [_idx(r) for r in rows]
        else:
            rows = db.execute("SELECT * FROM nodes WHERE domain LIKE ? ORDER BY domain, updated_at DESC", (pattern,)).fetchall()
            nodes = [_node(r) for r in rows]

        # Edges between loaded nodes
        node_ids = {r["id"] for r in rows}
        edges = []
        if node_ids and depth == "full":
            placeholders = ",".join("?" * len(node_ids))
            edge_rows = db.execute(f"""
                SELECT * FROM edges
                WHERE source_id IN ({placeholders}) AND target_id IN ({placeholders})
            """, list(node_ids) + list(node_ids)).fetchall()
            edges = [_edge(r) for r in edge_rows]

        # Sub-domains within this domain
        sub_rows = db.execute("""
            SELECT domain, COUNT(*) as cnt FROM nodes
            WHERE domain LIKE ? AND domain != ?
            GROUP BY domain ORDER BY domain
        """, (pattern, name)).fetchall()
        subs = [{"name":r["domain"],"count":r["cnt"]} for r in sub_rows]

        return _c({"domain":name,"node_count":len(nodes),"edge_count":len(edges),
                    "nodes":nodes,"edges":edges,"sub_domains":subs})
    finally:
        db.close()

@mcp.tool(name="kg_unload_domain")
async def kg_unload_domain(name: str) -> str:
    """Signal that a domain is no longer needed in context. No server-side effect.
    Returns confirmation for conversation flow."""
    return _c({"op":"unloaded","domain":name,"note":"Domain removed from active context. Use kg_load_domain() to reload."})

@mcp.tool(name="kg_list_domains")
async def kg_list_domains() -> str:
    """Full domain tree with counts and last activity. Returns [{name,count,last_updated,sub_domains:[]}]."""
    db = get_db()
    try:
        rows = db.execute("""
            SELECT domain, COUNT(*) as cnt, MAX(updated_at) as last_upd
            FROM nodes GROUP BY domain ORDER BY domain
        """).fetchall()
        # Build tree from flat list
        tree = {}
        for r in rows:
            d = r["domain"] or "(unassigned)"
            parts = d.split(".")
            root = parts[0]
            if root not in tree:
                tree[root] = {"name":root,"count":0,"last_updated":0,"sub_domains":[]}
            if len(parts) == 1:
                tree[root]["count"] += r["cnt"]
                tree[root]["last_updated"] = max(tree[root]["last_updated"], r["last_upd"])
            else:
                tree[root]["sub_domains"].append({"name":d,"count":r["cnt"],"last_updated":r["last_upd"]})
                tree[root]["count"] += r["cnt"]
                tree[root]["last_updated"] = max(tree[root]["last_updated"], r["last_upd"])
        return _c({"domains":list(tree.values())})
    finally:
        db.close()

@mcp.tool(name="kg_get")
async def kg_get(id: str, hops: int = 1) -> str:
    """Node + N-hop neighborhood. Returns {node:{},edges:[],neighbors:{}}. hops:0-3"""
    db = get_db()
    try:
        row = db.execute("SELECT * FROM nodes WHERE id=?", (id,)).fetchone()
        if not row: return _c({"error":f"not_found:{id}"})
        edges, nids = [], set()
        visited, frontier = {id}, {id}
        for _ in range(min(hops,3)):
            nf = set()
            for nid in frontier:
                for e in db.execute("SELECT * FROM edges WHERE source_id=?", (nid,)).fetchall():
                    edges.append(_edge(e))
                    if e["target_id"] not in visited: nf.add(e["target_id"]); nids.add(e["target_id"])
                for e in db.execute("SELECT * FROM edges WHERE target_id=?", (nid,)).fetchall():
                    edges.append(_edge(e))
                    if e["source_id"] not in visited: nf.add(e["source_id"]); nids.add(e["source_id"])
            visited.update(nf); frontier = nf
        seen, ue = set(), []
        for e in edges:
            k=(e["src"],e["rel"],e["tgt"])
            if k not in seen: seen.add(k); ue.append(e)
        nb = {}
        for nid in nids:
            nr = db.execute("SELECT * FROM nodes WHERE id=?", (nid,)).fetchone()
            if nr: nb[nid] = _node(nr)
        return _c({"node":_node(row),"edges":ue,"neighbors":nb})
    finally:
        db.close()

@mcp.tool(name="kg_search")
async def kg_search(q: str, limit: int = 10) -> str:
    """Fulltext search nodes by id/summary/kai_note/meta."""
    db = get_db()
    try:
        p = f"%{q}%"
        rows = db.execute("SELECT * FROM nodes WHERE id LIKE ? OR summary LIKE ? OR kai_note LIKE ? OR meta LIKE ? LIMIT ?", (p,p,p,p,limit)).fetchall()
        return _c({"q":q,"count":len(rows),"nodes":[_node(r) for r in rows]})
    finally:
        db.close()

@mcp.tool(name="kg_put_node")
async def kg_put_node(id: str, type: str = "concept", summary: str = "", bands: str = "[]",
                      domain: str = "", status: str = "seed", kai_note: str = "", meta: str = "{}") -> str:
    """Upsert node. domain: dot-notation e.g. 'infra', 'holofeeling.baende', 'meta'. bands: JSON '[1,3]'. meta: JSON '{}'.
    type: concept|metaphor|principle|model|person|band|insight|pattern|question. status: seed|explored|deep|verified|archived"""
    db = get_db()
    try:
        now = time.time()
        bl = json.loads(bands) if bands else []
        md = json.loads(meta) if meta else {}
        ex = db.execute("SELECT * FROM nodes WHERE id=?", (id,)).fetchone()
        if ex:
            u = {"updated_at":now}
            if type: u["type"]=type
            if summary: u["summary"]=summary
            if bands and bands != "[]": u["bands"]=json.dumps(bl)
            if domain is not None and domain != "": u["domain"]=domain
            if status: u["status"]=status
            if kai_note: u["kai_note"]=kai_note
            if meta and meta != "{}":
                old = json.loads(ex["meta"]); old.update(md)
                u["meta"]=json.dumps(old, ensure_ascii=False)
            sc = ",".join(f"{k}=?" for k in u)
            db.execute(f"UPDATE nodes SET {sc} WHERE id=?", list(u.values())+[id])
            db.commit(); return _c({"op":"updated","id":id,"domain":u.get("domain",ex["domain"])})
        else:
            db.execute("INSERT INTO nodes (id,type,summary,bands,domain,status,kai_note,meta,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
                (id,type,summary,json.dumps(bl),domain,status,kai_note,json.dumps(md,ensure_ascii=False),now,now))
            db.commit(); return _c({"op":"created","id":id,"domain":domain})
    finally:
        db.close()

@mcp.tool(name="kg_put_edge")
async def kg_put_edge(source_id: str, target_id: str, relation: str, weight: float = 1.0, note: str = "") -> str:
    """Upsert edge. relation: contains|contrasts|becomes|mirrors|requires|extends|instantiates|refines|grounds|maps_to|emerges_from|dissolves_into|polarizes"""
    db = get_db()
    try:
        db.execute("INSERT OR REPLACE INTO edges (source_id,target_id,relation,weight,note,created_at) VALUES (?,?,?,?,?,?)",
            (source_id,target_id,relation,weight,note,time.time()))
        db.commit(); return _c({"op":"edge_set","src":source_id,"rel":relation,"tgt":target_id})
    finally:
        db.close()

@mcp.tool(name="kg_delete_node")
async def kg_delete_node(id: str) -> str:
    """Delete node + edges. Destructive."""
    db = get_db()
    try:
        db.execute("DELETE FROM nodes WHERE id=?", (id,)); db.commit()
        return _c({"op":"deleted","id":id})
    finally:
        db.close()

@mcp.tool(name="kg_delete_edge")
async def kg_delete_edge(source_id: str, target_id: str, relation: str) -> str:
    """Delete edge."""
    db = get_db()
    try:
        db.execute("DELETE FROM edges WHERE source_id=? AND target_id=? AND relation=?", (source_id,target_id,relation))
        db.commit(); return _c({"op":"edge_deleted"})
    finally:
        db.close()

@mcp.tool(name="kg_state")
async def kg_state(key: str, value: str) -> str:
    """Set state. Keys: focus|open_questions|last_session|session_count|custom."""
    db = get_db()
    try:
        db.execute("INSERT OR REPLACE INTO state (key,value,updated_at) VALUES (?,?,?)", (key,value,time.time()))
        db.commit(); return _c({"op":"state_set","key":key})
    finally:
        db.close()

@mcp.tool(name="kg_bulk")
async def kg_bulk(operations: str) -> str:
    """Batch ops. Input JSON: {nodes:[{id,type,summary,bands,domain,status,kai_note,meta}],edges:[{source_id,target_id,relation,weight,note}]}"""
    db = get_db()
    try:
        ops = json.loads(operations); now = time.time(); nc=ec=0
        for n in ops.get("nodes",[]):
            ex = db.execute("SELECT id FROM nodes WHERE id=?", (n["id"],)).fetchone()
            if ex:
                db.execute("UPDATE nodes SET type=?,summary=?,bands=?,domain=?,status=?,kai_note=?,meta=?,updated_at=? WHERE id=?",
                    (n.get("type","concept"),n.get("summary",""),json.dumps(n.get("bands",[])),
                     n.get("domain",""),n.get("status","seed"),n.get("kai_note",""),
                     json.dumps(n.get("meta",{}),ensure_ascii=False),now,n["id"]))
            else:
                db.execute("INSERT INTO nodes (id,type,summary,bands,domain,status,kai_note,meta,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
                    (n["id"],n.get("type","concept"),n.get("summary",""),json.dumps(n.get("bands",[])),
                     n.get("domain",""),n.get("status","seed"),n.get("kai_note",""),
                     json.dumps(n.get("meta",{}),ensure_ascii=False),now,now))
            nc+=1
        for e in ops.get("edges",[]):
            db.execute("INSERT OR REPLACE INTO edges (source_id,target_id,relation,weight,note,created_at) VALUES (?,?,?,?,?,?)",
                (e["source_id"],e["target_id"],e["relation"],e.get("weight",1.0),e.get("note",""),now))
            ec+=1
        db.commit(); return _c({"op":"bulk","nodes":nc,"edges":ec})
    finally:
        db.close()

@mcp.tool(name="kg_bulk_set_domain")
async def kg_bulk_set_domain(domain: str, node_ids: str) -> str:
    """Set domain for multiple nodes at once. node_ids: JSON array of node IDs. For migration."""
    db = get_db()
    try:
        ids = json.loads(node_ids)
        now = time.time()
        for nid in ids:
            db.execute("UPDATE nodes SET domain=?, updated_at=? WHERE id=?", (domain, now, nid))
        db.commit()
        return _c({"op":"bulk_domain_set","domain":domain,"count":len(ids)})
    finally:
        db.close()

@mcp.tool(name="kg_traverse")
async def kg_traverse(start_id: str, max_hops: int = 2, relation_filter: str = "") -> str:
    """BFS subgraph from node. Returns {nodes:{},edges:[]}. relation_filter: only follow this type (empty=all)."""
    db = get_db()
    try:
        vn, ce = {}, []
        frontier, visited = {start_id}, set()
        rf = relation_filter or None
        for hop in range(max_hops+1):
            nf = set()
            for nid in frontier:
                if nid in visited: continue
                visited.add(nid)
                row = db.execute("SELECT * FROM nodes WHERE id=?", (nid,)).fetchone()
                if row: vn[nid] = _node(row)
                if hop < max_hops:
                    eq = " AND relation=?" if rf else ""
                    ep = [nid]+([rf] if rf else [])
                    for e in db.execute(f"SELECT * FROM edges WHERE source_id=?{eq}", ep).fetchall():
                        ce.append(_edge(e))
                        if e["target_id"] not in visited: nf.add(e["target_id"])
                    for e in db.execute(f"SELECT * FROM edges WHERE target_id=?{eq}", ep).fetchall():
                        ce.append(_edge(e))
                        if e["source_id"] not in visited: nf.add(e["source_id"])
            frontier = nf
        seen, ue = set(), []
        for e in ce:
            k=(e["src"],e["rel"],e["tgt"])
            if k not in seen: seen.add(k); ue.append(e)
        return _c({"start":start_id,"hops":max_hops,"node_count":len(vn),"edge_count":len(ue),"nodes":vn,"edges":ue})
    finally:
        db.close()

# ============================================================
# DOCUMENT TOOLS (Schicht 2 - Session-Destillate)
# ============================================================

@mcp.tool(name="kg_doc_create")
async def kg_doc_create(title: str, session_number: int = 0, content: str = "", node_ids: str = "[]") -> str:
    """Create session document. Returns {id,title,session}. node_ids: JSON array of related graph node IDs."""
    db = get_db()
    try:
        doc_id = str(uuid.uuid4())[:8]
        now = time.time()
        nids = json.loads(node_ids) if node_ids else []
        db.execute("INSERT INTO documents (id,title,content,session_number,node_ids,created_at,updated_at) VALUES (?,?,?,?,?,?,?)",
            (doc_id, title, content, session_number, json.dumps(nids), now, now))
        db.commit()
        return _c({"op":"doc_created","id":doc_id,"title":title,"session":session_number})
    finally:
        db.close()

@mcp.tool(name="kg_doc_append")
async def kg_doc_append(id: str, content: str, node_ids: str = "[]") -> str:
    """Append to existing document. Adds content + merges node_ids. Primary tool for session destillation."""
    db = get_db()
    try:
        row = db.execute("SELECT * FROM documents WHERE id=?", (id,)).fetchone()
        if not row: return _c({"error":f"doc_not_found:{id}"})
        now = time.time()
        new_content = row["content"] + "\n" + content
        old_nids = set(json.loads(row["node_ids"]))
        new_nids = json.loads(node_ids) if node_ids else []
        merged = list(old_nids | set(new_nids))
        db.execute("UPDATE documents SET content=?, node_ids=?, updated_at=? WHERE id=?",
            (new_content, json.dumps(merged), now, id))
        db.commit()
        return _c({"op":"doc_appended","id":id,"len":len(new_content),"nodes":len(merged)})
    finally:
        db.close()

@mcp.tool(name="kg_doc_read")
async def kg_doc_read(id: str) -> str:
    """Read full document by ID. Returns {id,title,content,session,node_ids}."""
    db = get_db()
    try:
        row = db.execute("SELECT * FROM documents WHERE id=?", (id,)).fetchone()
        if not row: return _c({"error":f"doc_not_found:{id}"})
        return _c(_doc(row))
    finally:
        db.close()

@mcp.tool(name="kg_doc_search")
async def kg_doc_search(q: str = "", session: int = 0, limit: int = 10) -> str:
    """Search documents by text or session number. Returns [{id,title,session,len}]."""
    db = get_db()
    try:
        if session > 0:
            rows = db.execute("SELECT * FROM documents WHERE session_number=? ORDER BY updated_at DESC LIMIT ?", (session, limit)).fetchall()
        elif q:
            p = f"%{q}%"
            rows = db.execute("SELECT * FROM documents WHERE title LIKE ? OR content LIKE ? ORDER BY updated_at DESC LIMIT ?", (p, p, limit)).fetchall()
        else:
            rows = db.execute("SELECT * FROM documents ORDER BY session_number DESC, updated_at DESC LIMIT ?", (limit,)).fetchall()
        return _c({"q":q or f"session:{session}","count":len(rows),"docs":[_doc_idx(r) for r in rows]})
    finally:
        db.close()

@mcp.tool(name="kg_doc_delete")
async def kg_doc_delete(id: str) -> str:
    """Delete document by ID. Destructive."""
    db = get_db()
    try:
        row = db.execute("SELECT id, title FROM documents WHERE id=?", (id,)).fetchone()
        if not row: return _c({"error":f"doc_not_found:{id}"})
        db.execute("DELETE FROM documents WHERE id=?", (id,))
        db.commit()
        return _c({"op":"doc_deleted","id":id,"title":row["title"]})
    finally:
        db.close()

# --- Resources ---

@mcp.resource("kg://schema")
async def get_schema() -> str:
    """Format reference."""
    return json.dumps({
        "node":{"id":"slug","t":"type","s":"summary","b":"bands[]","d":"domain","st":"status","k":"kai_note","m":"meta{}"},
        "edge":{"src":"source_id","rel":"relation","tgt":"target_id","w":"weight","n":"note"},
        "doc":{"id":"uuid8","title":"str","content":"str","session":"int","node_ids":"[node_id,...]"},
        "types":"concept|metaphor|principle|model|person|band|insight|pattern|question",
        "statuses":"seed|explored|deep|verified|archived",
        "domains":"meta|infra|holofeeling|holofeeling.baende|lo-board|kanzlei|recht-privat|ki-theorie|voice|... (dot-notation for hierarchy)",
        "rels":"contains|contrasts|becomes|mirrors|requires|extends|instantiates|refines|grounds|maps_to|emerges_from|dissolves_into|polarizes"
    }, indent=2)

# --- Entry ---

if __name__ == "__main__":
    mcp.run(transport=os.environ.get("KG_TRANSPORT", "streamable-http"))
