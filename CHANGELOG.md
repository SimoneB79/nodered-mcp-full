# Changelog

## 0.5.0 (2026-11-21)
Single release: safe concurrency + layout + flow hygiene (28 tools).

- revision guard (admin api v2): every deploy carries the flow revision; if the flows changed server-side (concurrent editor deploy) Node-RED answers 409 version_mismatch and NOTHING is written - the tool error explains the safe retry. deploy_flows force:true skips it deliberately; restore_backup skips it by design (explicit rollback). Verified against Node-RED 5.0.10 runtime source.
- layout engine: upsert_node/import_nodes place new nodes in the first FREE canvas region (no overlaps); layout_audit reports per-tab group overlaps and group-escape (exact, error) plus stray-in-group and estimated node overlaps (warnings), worst tabs first; layout gate refuses any write that would introduce NEW group geometry errors (allow_overlap:true overrides).
- node utilities: set_node_enabled (d:true nodes / disabled:true tabs, batch, one deploy), bypass_node (rewires inputs straight to the outputs of a single-output node and keeps it on the canvas disabled - non-destructive splice).
- manage_tabs: create (returns the new tab id), rename, reorder (index/after_tab_id), set_info, delete (refuses non-empty tabs without force).
- groups: manage_groups (create bounds from members, add/remove with single-membership detach + auto refit, rename, refit, delete keeps nodes), decouple_tab (cross-group wires become link out/in pairs named after the far end, groups refitted and membership updated), arrange_tab (repack groups into balanced columns; members and nested groups move together; ungrouped nodes never moved; dry-run plan by default).
- import_nodes / export_nodes: clipboard-style JSON; id remapping, wires kept inside the set (external ends dropped with a list), identical config nodes already on the instance are REUSED instead of cloned, free-region or explicit placement.
- audit_flows: unused config nodes, empty tabs, disabled tabs/nodes, orphans, dangling outputs, cross-tab direct wires, duplicate type+name, unknown z.
- validation: link out targets must exist; unknown group refs warned; disabled state read from d OR disabled everywhere (outline, mermaid, search, summaries).
- tests: mock admin api v2 with revision + one-shot 409 injection; full coverage of the new tools; 60+ checks green.

## 0.4.1 (2026-11-17)
- get_errors: include_last_status:true adds nodes_in_error_now (nodes whose latest status is red) and last_status_seen (latest status per node seen by the monitor). Verified against real Node-RED: the comms channel has NO status replay on subscribe, so silent nodes appear only after they emit again.

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
