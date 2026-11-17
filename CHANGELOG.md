# Changelog

## 0.4.0 (2026-11-17)
- get_errors: live runtime error monitor via the editor /comms websocket (auth-packet login, status + notification subscriptions, auto-reconnect, heartbeat echo). Rolling 500-event buffer of red/yellow node statuses and error notifications with node name/type/tab resolved; filters: limit, tab_id, include_warnings, clear. No more SSH/docker to read why a node is in error.

## 0.3.0 (2026-11-17)
- AI-first comprehension layer: get_flow_map (outline / mermaid / json, wiring resolved to node names), list_flows detail:true (type histogram, entry points, config refs, subflow instances), search_nodes (fields + function code), list_config_nodes (usage counts)
- Surgical writes: patch_batch (one deploy for many patches), upsert_node (auto id/position, optional wire_from), delete_node (+splice bypass) - the model sends only diffs, the server merges and deploys the full array
- Safety: dry_run with before/after diffs on every write tool, validate_flows standalone (duplicate ids, dangling wires, legacy HA v3 fields on v6 nodes, flow.get 'file' trap), get_flow compact/pagination, get_node resolve_refs
- 17 tools total; all v0.2 tools and behaviors preserved

## 0.1.0 (2026-11-06)
- 10 tools: get_settings, list_flows, get_flow, get_node, patch_node, deploy_flows, call_endpoint, list_backups, restore_backup, list_node_types
- Safe writes: confirm flag + automatic pre-deploy backups with rotation + one-command restore
- env-driven: URL, Bearer token, custom CA, insecure TLS, read-only mode, timeout, response cap, extra headers
- Tested end-to-end against a production Node-RED behind a Caddy reverse proxy with local CA
