# kg-mcp

Knowledge Graph MCP Server. Maschinenoptimiert – kein Prosa, kein Markdown. Kompaktes JSON für LLM-Kontexteffizienz.

## Deployment (Coolify)

1. GitHub Repo erstellen und Code pushen
2. In Coolify: Neue Application → GitHub → dieses Repo
3. Build Pack: Dockerfile
4. Port: 8099
5. Domain: `kg-mcp.kailohmann.de`
6. Volume: `/data` (persistent für SQLite)
7. Keine weiteren Env-Vars nötig (Defaults sind gesetzt)

## Claude MCP Integration

Nach Deployment in Claude.ai unter Settings → MCP Servers hinzufügen:

```
URL: https://kg-mcp.kailohmann.de/mcp
Name: kg-mcp
```

## Tools

| Tool | Funktion |
|------|----------|
| `kg_boot` | Session-Start: State + Node-Index |
| `kg_get` | Node + N-Hop Nachbarschaft |
| `kg_search` | Volltext-Suche |
| `kg_put_node` | Node erstellen/aktualisieren |
| `kg_put_edge` | Edge erstellen/aktualisieren |
| `kg_delete_node` | Node löschen |
| `kg_delete_edge` | Edge löschen |
| `kg_state` | Projektstatus setzen |
| `kg_bulk` | Batch-Operationen |
| `kg_traverse` | BFS Subgraph-Traversierung |

## Compact Format

```json
Node: {"id":"schwamm","t":"metaphor","s":"...","b":[1,3,6],"st":"explored","k":"...","m":{}}
Edge: {"src":"schwamm","rel":"contrasts","tgt":"basic_programm","w":1.0,"n":"..."}
Index: {"id":"schwamm","t":"metaphor","st":"explored"}
```

## Lokal testen

```bash
KG_DB_PATH=./test.db KG_TRANSPORT=streamable-http python server.py
```
