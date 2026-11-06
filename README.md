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
| `list_flows` | All tabs with node counts and disabled flags |
| `get_flow` | Nodes of one tab (by id), or the whole flow document |
| `get_node` | Single node by id (full config, function code, wiring) |
| `patch_node` | Merge fields into one node (func, wires, x/y, ...) — auto backup, needs `confirm:true` |
| `deploy_flows` | Deploy a complete flow array — auto backup, needs `confirm:true` |
| `call_endpoint` | GET any HTTP endpoint served by your flows (custom diagnostics, APIs) |
| `list_backups` | List automatic pre-deploy backups |
| `restore_backup` | Restore a backup in one command — needs `confirm:true` |
| `list_node_types` | Installed palette modules and node types |

## Configuration (environment variables)

| Variable | Default | Description |
|----------|---------|-------------|
| `NODE_RED_URL` | `http://localhost:1880` | Base URL of the editor/admin API (no trailing slash) |
| `NODE_RED_TOKEN` | — | Bearer token when Node-RED `adminAuth` is enabled |
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
