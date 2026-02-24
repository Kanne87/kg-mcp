"""
kg_mcp - Knowledge Graph MCP Server
Optimized for LLM context efficiency. All outputs are compact structured data.
No prose, no markdown, no human formatting. Pure graph.
"""

import json
import sqlite3
import os
import time
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
        CREATE INDEX IF NOT EXISTS idx_edges_source ON edges(source_id);
        CREATE INDEX IF NOT EXISTS idx_edges_target ON edges(target_id);
        CREATE INDEX IF NOT EXISTS idx_nodes_type ON nodes(type);
        CREATE INDEX IF NOT EXISTS idx_nodes_status ON nodes(status);
    """)
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
            "st":r["status"],"k":r["kai_note"],"m":json.loads(r["meta"])}

def _edge(r):
    return {"src":r["source_id"],"rel":r["relation"],"tgt":r["target_id"],"w":r["weight"],"n":r["note"]}

def _idx(r):
    return {"id":r["id"],"t":r["type"],"st":r["status"]}

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
# TOOLS
# ============================================================

@mcp.tool(name="kg_boot")
async def kg_boot(include_edges: bool = False) -> str:
    """Session init. Returns {state:{},nodes:[{id,t,st}],edge_count:N}. Call FIRST."""
    db = get_db()
    try:
        state = {r["key"]:r["value"] for r in db.execute("SELECT key,value FROM state").fetchall()}
        nodes = [_idx(r) for r in db.execute("SELECT id,type,status FROM nodes ORDER BY updated_at DESC").fetchall()]
        ec = db.execute("SELECT COUNT(*) as c FROM edges").fetchone()["c"]
        res = {"state":state,"nodes":nodes,"edge_count":ec}
        if include_edges:
            res["edges"] = [_edge(r) for r in db.execute("SELECT * FROM edges").fetchall()]
        sc = int(state.get("session_count","0")) + 1
        db.execute("INSERT OR REPLACE INTO state (key,value,updated_at) VALUES (?,?,?)", ("session_count",str(sc),time.time()))
        db.commit()
        return _c(res)
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
async def kg_put_node(id: str, type: str = "concept", summary: str = "", bands: str = "[]", status: str = "seed", kai_note: str = "", meta: str = "{}") -> str:
    """Upsert node. bands: JSON '[1,3]'. meta: JSON '{}'. type: concept|metaphor|principle|model|person|band|insight|pattern|question. status: seed|explored|deep|verified|archived"""
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
            if status: u["status"]=status
            if kai_note: u["kai_note"]=kai_note
            if meta and meta != "{}":
                old = json.loads(ex["meta"]); old.update(md)
                u["meta"]=json.dumps(old, ensure_ascii=False)
            sc = ",".join(f"{k}=?" for k in u)
            db.execute(f"UPDATE nodes SET {sc} WHERE id=?", list(u.values())+[id])
            db.commit(); return _c({"op":"updated","id":id})
        else:
            db.execute("INSERT INTO nodes (id,type,summary,bands,status,kai_note,meta,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?)",
                (id,type,summary,json.dumps(bl),status,kai_note,json.dumps(md,ensure_ascii=False),now,now))
            db.commit(); return _c({"op":"created","id":id})
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
    """Batch ops. Input JSON: {nodes:[{id,type,summary,bands,status,kai_note,meta}],edges:[{source_id,target_id,relation,weight,note}]}"""
    db = get_db()
    try:
        ops = json.loads(operations); now = time.time(); nc=ec=0
        for n in ops.get("nodes",[]):
            ex = db.execute("SELECT id FROM nodes WHERE id=?", (n["id"],)).fetchone()
            if ex:
                db.execute("UPDATE nodes SET type=?,summary=?,bands=?,status=?,kai_note=?,meta=?,updated_at=? WHERE id=?",
                    (n.get("type","concept"),n.get("summary",""),json.dumps(n.get("bands",[])),
                     n.get("status","seed"),n.get("kai_note",""),json.dumps(n.get("meta",{}),ensure_ascii=False),now,n["id"]))
            else:
                db.execute("INSERT INTO nodes (id,type,summary,bands,status,kai_note,meta,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?)",
                    (n["id"],n.get("type","concept"),n.get("summary",""),json.dumps(n.get("bands",[])),
                     n.get("status","seed"),n.get("kai_note",""),json.dumps(n.get("meta",{}),ensure_ascii=False),now,now))
            nc+=1
        for e in ops.get("edges",[]):
            db.execute("INSERT OR REPLACE INTO edges (source_id,target_id,relation,weight,note,created_at) VALUES (?,?,?,?,?,?)",
                (e["source_id"],e["target_id"],e["relation"],e.get("weight",1.0),e.get("note",""),now))
            ec+=1
        db.commit(); return _c({"op":"bulk","nodes":nc,"edges":ec})
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

# --- Resources ---

@mcp.resource("kg://schema")
async def get_schema() -> str:
    """Format reference."""
    return json.dumps({
        "node":{"id":"slug","t":"type","s":"summary","b":"bands[]","st":"status","k":"kai_note","m":"meta{}"},
        "edge":{"src":"source_id","rel":"relation","tgt":"target_id","w":"weight","n":"note"},
        "types":"concept|metaphor|principle|model|person|band|insight|pattern|question",
        "statuses":"seed|explored|deep|verified|archived",
        "rels":"contains|contrasts|becomes|mirrors|requires|extends|instantiates|refines|grounds|maps_to|emerges_from|dissolves_into|polarizes"
    }, indent=2)

# --- Entry ---

if __name__ == "__main__":
    mcp.run(transport=os.environ.get("KG_TRANSPORT", "streamable-http"))
