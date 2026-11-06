# Changelog

## 0.1.0 (2026-11-06)
- 10 tools: get_settings, list_flows, get_flow, get_node, patch_node, deploy_flows, call_endpoint, list_backups, restore_backup, list_node_types
- Safe writes: confirm flag + automatic pre-deploy backups with rotation + one-command restore
- env-driven: URL, Bearer token, custom CA, insecure TLS, read-only mode, timeout, response cap, extra headers
- Tested end-to-end against a production Node-RED behind a Caddy reverse proxy with local CA
