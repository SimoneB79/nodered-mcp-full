# nodered-mcp-full

[![CI](https://github.com/SimoneB79/nodered-mcp-full/actions/workflows/ci.yml/badge.svg)](https://github.com/SimoneB79/nodered-mcp-full/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![npm](https://img.shields.io/badge/npm-nodered--mcp--full-red.svg)](https://www.npmjs.com/package/nodered-mcp-full)

**Full-featured MCP (Model Context Protocol) server for Node-RED** — gives AI agents complete, safe access to any Node-RED instance through its Admin API: read flows, nodes and settings; deploy patches and full flow sets with automatic backups; call custom HTTP endpoints; restore in one command.

Built for real-world setups:

- direct `http://localhost:1880`
- behind a **reverse proxy** (Caddy, nginx, Traefik) with private-CA or self-signed HTTPS
- with **adminAuth / OAuth bearer token**
- **read-only** deployments you do not want touched
- **production-grade writes**: every deploy requires an explicit `confirm: true`, takes an automatic pre-deploy backup (with rotation), and deploys the complete flow set — the only deploy type that is safe with config nodes

## Tools

| Tool | Description |
|------|-------------|
| `get_settings` | Runtime settings: version, palette editability, contextStorage — check limits first |
| `list_flows` | All tabs with node counts; `detail:true` adds type histogram, entry points (injects, mqtt topics, http endpoints, HA entities), config refs, subflow instances |
| `get_flow_map` | **AI-readable map of one tab/subflow**: `outline` (indented execution order, wiring resolved to names, config nodes inlined), `mermaid` (flowchart), `json` |
| `get_flow` | Nodes of one tab (by id), or the whole flow document; `compact:true` strips coordinates, `offset`/`limit` paginate |
| `get_node` | Single node by id (full config, function code, wiring); `resolve_refs:true` inlines referenced config nodes |
| `search_nodes` | Find nodes by text across name/type/topic/entity/url **and function code** (code matches with snippet) |
| `list_config_nodes` | Config nodes (brokers, HA servers, ui config) with usage counts |
| `patch_node` | Merge fields into one node (func, wires, x/y, ...) — `dry_run` preview, auto backup, needs `confirm:true` |
| `patch_batch` | Many node patches in ONE deploy — server merges + validates + backs up; you send only diffs |
| `upsert_node` | Create a node (auto id/position, optional `wire_from`) or update by id — needs `confirm:true` |
| `delete_node` | Delete a node and clean its wires; `splice:true` rewires inputs to outputs — needs `confirm:true` |
| `validate_flows` | Validate a candidate flow array without deploying (dup ids, dangling wires, legacy HA v3 fields, `flow.get(...,'file')` traps) |
| `deploy_flows` | Deploy a complete flow array — `dry_run` validation, auto backup, needs `confirm:true` |
| `call_endpoint` | GET any HTTP endpoint served by your flows (custom diagnostics, APIs) |
| `get_errors` | **Live runtime errors/warnings**: red/yellow node statuses + error notifications from the editor comms channel, node and tab resolved - no SSH needed |
| `list_backups` | List automatic pre-deploy backups |
| `restore_backup` | Restore a backup in one command — needs `confirm:true` |
| `list_node_types` | Installed palette modules and node types |

## Configuration (environment variables)

| Variable | Default | Description |
|----------|---------|-------------|
| `NODE_RED_URL` | `http://localhost:1880` | Base URL of the editor/admin API (no trailing slash) |
| `NODE_RED_TOKEN` | — | Static bearer token (if you manage tokens yourself) |
| `NODE_RED_USER` / `NODE_RED_PASS` | — | **Recommended with adminAuth**: the server logs in automatically (`POST /auth/token`), caches and renews the token |
| `NODE_RED_CA_FILE` | — | Path to a CA certificate file for HTTPS behind a private reverse proxy |
| `NODE_RED_INSECURE_TLS` | `false` | Skip TLS verification (self-signed, testing only) |
| `NODE_RED_READ_ONLY` | `false` | Disable every write tool |
| `NODE_RED_BACKUP_DIR` | tmp dir | Where pre-deploy backups are stored |
| `NODE_RED_BACKUP_KEEP` | `20` | Backup rotation count |
| `NODE_RED_TIMEOUT_MS` | `20000` | HTTP timeout |
| `NODE_RED_MAX_RESPONSE_CHARS` | `300000` | Response size guard before truncation |
| `NODE_RED_HEADERS` | `{}` | Extra JSON headers (e.g. `{"X-Proxy-Auth": "..."}`) |

## Usage

### Any MCP client (stdio)

```json
{
  "mcpServers": {
    "nodered": {
      "command": "npx",
      "args": ["-y", "nodered-mcp-full"],
      "env": { "NODE_RED_URL": "http://localhost:1880" }
    }
  }
}
```

See [`examples/`](examples/) for Claude Desktop, Cursor and the mcp-proxy pattern (multiple servers behind one SSE endpoint).

### Docker (standalone)

```bash
docker compose up -d   # uses docker-compose.yml example
```

## Security notes

- The Node-RED Admin API controls everything: expose this server only on trusted networks.
- Prefer HTTPS with a real or private CA (`NODE_RED_CA_FILE`) and `NODE_RED_TOKEN`.
- For instances you do not want modified, set `NODE_RED_READ_ONLY=true`.
- Writes are always guarded by `confirm: true` + automatic backup + restore.

## Development & tests

```bash
npm install
npm test        # end-to-end suite: spawns the server against a mock Node-RED Admin API
```

CI runs the suite on Node 18/20/22 (`.github/workflows/ci.yml`).

## License

MIT — Simone Bonizzardi
