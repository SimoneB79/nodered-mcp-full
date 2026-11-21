#!/usr/bin/env node
/**
 * nodered-mcp-full - MCP server for the Node-RED Admin API
 *
 * v0.5.0 - safe concurrency + layout + flow hygiene (single release):
 *   - revision guard: every deploy carries the admin-api v2 flow revision; a
 *     concurrent editor deploy => HTTP 409 version_mismatch, nothing is written
 *   - layout engine: free-region placement (no overlaps), layout_audit,
 *     layout gate on every write (new group-geometry errors are refused)
 *   - groups: manage_groups (create/add/remove/rename/refit/delete),
 *     decouple_tab (cross-group wires -> link in/out pairs), arrange_tab
 *     (repack groups into columns, dry-run plan)
 *   - node utilities: set_node_enabled, bypass_node (reroute around, keep node)
 *   - manage_tabs (create/rename/reorder/set_info/delete)
 *   - import_nodes / export_nodes (id remap, config dedupe, free placement)
 *   - audit_flows: unused configs, empty tabs, orphans, dangling outputs,
 *     cross-tab wires, duplicate names, unknown z
 * v0.4.0 - get_errors: live runtime error monitor via the editor /comms websocket
 * v0.3.0 - AI-first flow comprehension layer (get_flow_map, list_flows detail,
 *   search_nodes, surgical writes patch_batch/upsert_node/delete_node)
 *
 * Generic, environment-driven. Works with:
 *  - direct Node-RED (http/https)
 *  - Node-RED behind a reverse proxy (Caddy/nginx/traefik) with custom CA
 *  - Node-RED with adminAuth enabled (Bearer token)
 *  - read-only deployments
 *
 * All writes require an explicit confirm flag and take an automatic backup first.
 */
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { z } from "zod";
import http from "node:http";
import https from "node:https";
import fs from "node:fs";
import path from "node:path";
import os from "node:os";
import crypto from "node:crypto";
import WebSocket from "ws";

// ---------------- environment ----------------
const URL_BASE = (process.env.NODE_RED_URL || "http://localhost:1880").replace(/\/+$/, "");
const TOKEN = process.env.NODE_RED_TOKEN || "";
const AUTH_USER = process.env.NODE_RED_USER || "";
const AUTH_PASS = process.env.NODE_RED_PASS || "";
const CA_FILE = process.env.NODE_RED_CA_FILE || "";
const INSECURE_TLS = process.env.NODE_RED_INSECURE_TLS === "true";
const READ_ONLY = process.env.NODE_RED_READ_ONLY === "true";
const BACKUP_DIR = process.env.NODE_RED_BACKUP_DIR || path.join(os.tmpdir(), "nodered-mcp-backups");
const BACKUP_KEEP = parseInt(process.env.NODE_RED_BACKUP_KEEP || "20", 10);
const TIMEOUT_MS = parseInt(process.env.NODE_RED_TIMEOUT_MS || "20000", 10);
const MAX_RESPONSE_CHARS = parseInt(process.env.NODE_RED_MAX_RESPONSE_CHARS || "300000", 10);
let EXTRA_HEADERS = {};
try { EXTRA_HEADERS = JSON.parse(process.env.NODE_RED_HEADERS || "{}"); } catch { /* ignore */ }

let ca = null;
if (CA_FILE) {
  try { ca = fs.readFileSync(CA_FILE); }
  catch (e) { console.error("nodered-mcp: cannot read NODE_RED_CA_FILE: " + CA_FILE); process.exit(1); }
}

// ---------------- auth token (auto login + cache) ----------------
let cachedToken = null, tokenExp = 0;
async function getToken(force) {
  if (!AUTH_USER) return TOKEN;
  const now = Date.now();
  if (!force && cachedToken && now < tokenExp - 60000) return cachedToken;
  return new Promise((resolve, reject) => {
    let u;
    try { u = new URL(URL_BASE + "/auth/token"); } catch (e) { reject(e); return; }
    const mod = u.protocol === "https:" ? https : http;
    const form = new URLSearchParams({ client_id: "node-red-editor", grant_type: "password", scope: "*", username: AUTH_USER, password: AUTH_PASS }).toString();
    const req = mod.request({ hostname: u.hostname, port: u.port || (u.protocol === "https:" ? 443 : 80), path: u.pathname, method: "POST", headers: Object.assign({}, EXTRA_HEADERS, { "Content-Type": "application/x-www-form-urlencoded", "Content-Length": Buffer.byteLength(form) }), ca: ca || undefined, rejectUnauthorized: !INSECURE_TLS }, (res) => {
      let data = ""; res.setEncoding("utf8");
      res.on("data", c => data += c);
      res.on("end", () => {
        if (res.statusCode !== 200) { reject(new Error("auth login failed HTTP " + res.statusCode)); return; }
        try { const j = JSON.parse(data); cachedToken = j.access_token; tokenExp = now + (j.expires_in || 604800) * 1000; resolve(cachedToken); }
        catch (e) { reject(new Error("auth login: bad response")); }
      });
    });
    req.setTimeout(TIMEOUT_MS, () => req.destroy(new Error("auth login timeout")));
    req.on("error", reject);
    req.write(form); req.end();
  });
}

// ---------------- runtime error monitor (editor comms websocket) ----------------
// Connects to the same /comms websocket the Node-RED editor uses, authenticates
// with the admin token (auth packet), subscribes to status + notification and
// keeps a rolling buffer of red/yellow node statuses and error notifications.
const MONITOR_MAX = 500;
const monitor = { ws: null, state: "idle", since: null, buffer: [], lastFill: new Map(), lastStatus: new Map(), reconnectMs: 4000, reconnectTimer: null, closedByUs: false };
let nodeNames = null; let nodeNamesAt = 0;

function monitorRecord(kind, entry) {
  monitor.buffer.push(Object.assign({ ts: Date.now(), kind }, entry));
  if (monitor.buffer.length > MONITOR_MAX) monitor.buffer.splice(0, monitor.buffer.length - MONITOR_MAX);
}

async function nodeNameMap() {
  if (nodeNames && Date.now() - nodeNamesAt < 60000) return nodeNames;
  try {
    const flows = await fetchFlows();
    const tabs = new Map();
    flows.forEach(function (f) { if (f.type === "tab") tabs.set(f.id, f.label || f.id); });
    const map = new Map();
    flows.forEach(function (f) {
      if (f.type === "tab" || f.type === "subflow") return;
      map.set(f.id, { name: f.name || f.type || f.id, type: f.type, tab_id: f.z || null, tab_label: f.z ? (tabs.get(f.z) || f.z) : "(config)" });
    });
    nodeNames = { map: map }; nodeNamesAt = Date.now();
    return nodeNames;
  } catch (e) { return nodeNames || { map: new Map() }; }
}

function monitorConnect() {
  if (monitor.ws && (monitor.ws.readyState === 0 || monitor.ws.readyState === 1)) return;
  monitor.closedByUs = false;
  monitor.state = "connecting";
  const wsUrl = URL_BASE.replace(/^http/, "ws") + "/comms";
  let ws;
  try { ws = new WebSocket(wsUrl, { headers: Object.assign({}, EXTRA_HEADERS), ca: ca || undefined, rejectUnauthorized: !INSECURE_TLS }); }
  catch (e) { monitor.state = "error: " + e.message; scheduleReconnect(); return; }
  ws.on("open", async function () {
    monitor.ws = ws;
    monitor.state = "authorizing";
    try {
      const t = AUTH_USER ? await getToken() : (TOKEN || "");
      ws.send(JSON.stringify({ auth: t || "" }));
    } catch (e) { monitor.state = "auth-error"; monitor.closedByUs = true; try { ws.close(); } catch (e2) {} }
  });
  ws.on("message", function (raw) {
    let m; try { m = JSON.parse(raw.toString()); } catch (e) { return; }
    if (m && m.auth) {
      if (m.auth === "ok") {
        monitor.state = "open"; monitor.since = Date.now();
        try { ws.send(JSON.stringify({ subscribe: "status" })); ws.send(JSON.stringify({ subscribe: "notification" })); } catch (e) {}
      } else { monitor.state = "auth-fail"; monitor.closedByUs = true; try { ws.close(); } catch (e) {} }
      return;
    }
    const arr = Array.isArray(m) ? m : [m];
    for (const one of arr) {
      if (!one || typeof one !== "object") continue;
      const topic = String(one.topic || "");
      if (topic === "hb") { try { ws.send(JSON.stringify({ topic: "hb" })); } catch (e) {} continue; }
      if (topic.indexOf("status/") === 0) {
        const id = topic.slice(7);
        const d = one.data || {};
        if (d.fill) {
          monitor.lastFill.set(id, d.fill);
          monitor.lastStatus.set(id, { ts: Date.now(), fill: d.fill, shape: d.shape || null, text: String(d.text || "").slice(0, 200) });
          if (monitor.lastStatus.size > 1000) {
            const sorted = Array.from(monitor.lastStatus.entries()).sort(function (a, b) { return a[1].ts - b[1].ts; });
            for (let i = 0; i < 200; i++) monitor.lastStatus.delete(sorted[i][0]);
          }
        }
        if (d.fill === "red" || d.fill === "yellow") monitorRecord("status", { node_id: id, fill: d.fill, shape: d.shape || null, text: String(d.text || "") });
        continue;
      }
      if (topic.indexOf("notification/") === 0) {
        const d = one.data || {};
        if (d && (d.type === "error" || d.type === "warning")) monitorRecord("notification", { level: d.type, text: String(d.text || "") });
        continue;
      }
    }
  });
  ws.on("close", function () {
    if (monitor.ws === ws) {
      monitor.ws = null;
      if (!monitor.closedByUs) { monitor.state = "reconnect-wait"; scheduleReconnect(); }
      else monitor.state = "closed";
    }
  });
  ws.on("error", function () { /* close handler reschedules */ });
}

function scheduleReconnect() {
  if (monitor.reconnectTimer) return;
  monitor.reconnectTimer = setTimeout(function () { monitor.reconnectTimer = null; monitorConnect(); }, monitor.reconnectMs);
}

async function ensureMonitor() {
  if (monitor.ws && monitor.ws.readyState === 1) return;
  if (monitor.ws && monitor.ws.readyState === 0) {
    const t0 = Date.now();
    while (Date.now() - t0 < 3000 && monitor.state !== "open") await new Promise(function (r) { setTimeout(r, 100); });
    return;
  }
  if (AUTH_USER) { try { await getToken(true); } catch (e) { /* proceed, auth packet will fail visibly */ } }
  monitorConnect();
  const t0 = Date.now();
  while (Date.now() - t0 < 4000 && monitor.state !== "open") await new Promise(function (r) { setTimeout(r, 100); });
}

// ---------------- http client ----------------
async function request(method, urlPath, body, extraHeaders) {
  const tk = await getToken(false);
  return new Promise((resolve, reject) => {
    let u;
    try { u = new URL(URL_BASE + urlPath); }
    catch (e) { reject(new Error("invalid path or base URL")); return; }
    const mod = u.protocol === "https:" ? https : http;
    const headers = Object.assign({}, EXTRA_HEADERS, extraHeaders || {}, { Accept: "application/json" });
    if (tk) headers.Authorization = "Bearer " + tk;
    let payload;
    if (body !== undefined) { payload = JSON.stringify(body); headers["Content-Type"] = "application/json"; headers["Content-Length"] = Buffer.byteLength(payload); }
    const req = mod.request({
      hostname: u.hostname,
      port: u.port || (u.protocol === "https:" ? 443 : 80),
      path: u.pathname + u.search,
      method,
      headers,
      ca: ca || undefined,
      rejectUnauthorized: !INSECURE_TLS,
    }, (res) => {
      let data = ""; res.setEncoding("utf8");
      res.on("data", (c) => { data += c; });
      res.on("end", () => resolve({ status: res.statusCode, body: data }));
    });
    req.setTimeout(TIMEOUT_MS, () => req.destroy(new Error("request timeout after " + TIMEOUT_MS + "ms")));
    req.on("error", reject);
    if (payload) req.write(payload);
    req.end();
  });
}

async function apiGet(p) {
  let r = await request("GET", p);
  if (r.status === 401 && AUTH_USER) { await getToken(true); r = await request("GET", p); }
  if (r.status !== 200) throw new Error("GET " + p + " -> HTTP " + r.status + ": " + r.body.slice(0, 400));
  return r.body;
}
async function apiGetJson(p) { return JSON.parse(await apiGet(p)); }

// ---------------- flows + revision (admin api v2) ----------------
// Node-RED >= 1.x: GET /flows with Node-RED-API-Version: v2 returns
// { flows: [...], rev: "<id>" }; POST /flows with { flows, rev } is REJECTED
// with 409 version_mismatch when the flows changed server-side meanwhile
// (verified against Node-RED 5.0.10 runtime source). Old instances answer
// with a plain array: no revision guard there, we degrade gracefully.
let lastRev = null;
async function fetchFlows() {
  const hdr = { "Node-RED-API-Version": "v2" };
  let r = await request("GET", "/flows", undefined, hdr);
  if (r.status === 401 && AUTH_USER) { await getToken(true); r = await request("GET", "/flows", undefined, hdr); }
  if (r.status !== 200) throw new Error("GET /flows -> HTTP " + r.status + ": " + r.body.slice(0, 400));
  let j;
  try { j = JSON.parse(r.body); } catch (e) { throw new Error("GET /flows: invalid JSON response"); }
  if (Array.isArray(j)) { lastRev = null; return j; }
  if (j && Array.isArray(j.flows)) { lastRev = j.rev || null; return j.flows; }
  throw new Error("GET /flows: unexpected response shape");
}

function clip(text, what) {
  if (text.length <= MAX_RESPONSE_CHARS) return text;
  return text.slice(0, MAX_RESPONSE_CHARS) + "\n...[truncated " + what + "; raise NODE_RED_MAX_RESPONSE_CHARS if needed]";
}
function txt(obj) { return clip(typeof obj === "string" ? obj : JSON.stringify(obj, null, 1), "response"); }

// ---------------- backups ----------------
function listBackupFiles() {
  try { return fs.readdirSync(BACKUP_DIR).filter(f => f.startsWith("flows-") && f.endsWith(".json")).sort(); }
  catch { return []; }
}
function rotateBackups() {
  const files = listBackupFiles();
  while (files.length > BACKUP_KEEP) {
    const f = files.shift();
    try { fs.unlinkSync(path.join(BACKUP_DIR, f)); } catch { /* ignore */ }
  }
}
async function backupFlows(label) {
  const body = await apiGet("/flows");
  fs.mkdirSync(BACKUP_DIR, { recursive: true });
  const ts = new Date().toISOString().replace(/[:.]/g, "-");
  const name = "flows-" + ts + (label ? "-" + label : "") + ".json";
  fs.writeFileSync(path.join(BACKUP_DIR, name), body);
  rotateBackups();
  return name;
}

// ---------------- deploy (full array, revision-guarded, layout-gated) ----------------
// opts.previous   the flows array the edit was based on (enables the layout gate)
// opts.allowOverlap  pass through the layout gate even if new error-level
//                    group geometry would appear (deliberate overlaps)
// opts.force      skip the revision guard (deliberate overwrite; used by
//                 restore_backup and by deploy_flows force:true)
async function deployFlows(flows, opts) {
  opts = opts || {};
  let gate = null;
  if (Array.isArray(opts.previous) && !opts.allowOverlap) {
    gate = gateCheck(opts.previous, flows);
    if (gate.blocked) {
      const e = new Error("layout gate: this edit would introduce NEW group geometry errors:\n  " + gate.newErrors.join("\n  ") + "\nNothing was written. Fix the geometry, or pass allow_overlap:true if the overlap is deliberate.");
      e.code = "layout_gate";
      throw e;
    }
  }
  const backup = await backupFlows("pre-deploy");
  const useRev = !opts.force && lastRev;
  const body = useRev ? { flows: flows, rev: lastRev } : flows;
  const hdr = { "Node-RED-Deployment-Type": "flows" };
  if (useRev) hdr["Node-RED-API-Version"] = "v2";
  let r = await request("POST", "/flows", body, hdr);
  if (r.status === 401 && AUTH_USER) { await getToken(true); r = await request("POST", "/flows", body, hdr); }
  if (r.status === 409) {
    throw new Error("CONFLICT (409 version_mismatch): the flows changed on the Node-RED server after this tool read them (concurrent deploy from the editor?). Nothing was written; a backup was still kept: " + backup + ". Safe to retry the same tool call now - it re-reads the current flows and re-applies the edit on top.");
  }
  if (r.status !== 204 && r.status !== 200) {
    throw new Error("deploy failed HTTP " + r.status + " (backup kept: " + backup + "): " + r.body.slice(0, 400));
  }
  if (r.status === 200 && useRev) {
    try { const j2 = JSON.parse(r.body); if (j2 && j2.rev) lastRev = j2.rev; } catch (e) { /* keep old rev */ }
  }
  const out = { ok: true, http: r.status, backup: backup, revision_guard: useRev ? "on" : "off", nodes: Array.isArray(flows) ? flows.length : "n/a" };
  if (gate && gate.newWarnings.length) out.layout_warnings = gate.newWarnings;
  return out;
}

function readOnlyError() { return "NODE_RED_READ_ONLY=true: write tools are disabled on this instance."; }

// ---------------- flow comprehension helpers ----------------
function isDisabled(n) { return !!(n && (n.d === true || n.disabled === true)); }
function buildIndex(flows) {
  const byId = new Map();
  for (const n of flows) if (n && n.id) byId.set(n.id, n);
  return byId;
}
function shortId(id) { return String(id || "").slice(0, 8); }
function trunc(s, n) { s = String(s); return s.length <= n ? s : s.slice(0, n - 3) + "..."; }
function displayName(n) {
  if (!n) return "?";
  if (n.name && String(n.name).trim()) return String(n.name).trim();
  if (n.type === "tab" || n.type === "subflow" || n.type === "group") return n.label || n.name || shortId(n.id);
  return n.type + ":" + shortId(n.id);
}
function isConfigType(t) {
  return t === "mqtt-broker" || t === "server" || t === "websocket-listener" || t === "websocket-client" || String(t).indexOf("ui_") === 0;
}
const ENTRY_TYPES = new Set(["inject", "mqtt in", "http in", "server-state-changed", "link in", "cronplus", "bigtimer", "poll-state", "websocket in", "udp in", "tcp in", "serial in"]);
const PARAM_SKIP = new Set(["id", "type", "name", "x", "y", "z", "w", "h", "g", "d", "wires", "credentials", "env", "info", "func", "template", "rules", "status", "label", "icon", "order", "links", "nodes"]);

function paramLine(n, byId) {
  const out = [];
  const keys = Object.keys(n);
  for (let i = 0; i < keys.length && out.length < 8; i++) {
    const k = keys[i];
    if (PARAM_SKIP.has(k)) continue;
    const v = n[k];
    if (v === "" || v === null || v === undefined || v === false) continue;
    let s;
    if (typeof v === "string") { s = byId && byId.has(v) ? displayName(byId.get(v)) + "(" + shortId(v) + ")" : trunc(v, 50); }
    else if (typeof v === "number" || typeof v === "boolean") { s = String(v); }
    else { s = trunc(JSON.stringify(v), 50); }
    out.push(k + "=" + s);
  }
  if (typeof n.func === "string") out.push("func=" + n.func.split("\n").length + "L");
  if (Array.isArray(n.rules)) out.push("rules=" + n.rules.length);
  return out;
}

function outlineTab(flows, byId, containerId) {
  const container = byId.get(containerId);
  const nodes = flows.filter(function (n) { return n.z === containerId && n.type !== "tab" && n.type !== "group"; });
  const ids = new Set(nodes.map(function (n) { return n.id; }));
  const indeg = new Map(); nodes.forEach(function (n) { indeg.set(n.id, 0); });
  nodes.forEach(function (n) {
    (n.wires || []).forEach(function (w) { (w || []).forEach(function (t) { if (ids.has(t) && t !== n.id) indeg.set(t, indeg.get(t) + 1); }); });
  });
  const depth = new Map();
  const q = [];
  nodes.forEach(function (n) { if (indeg.get(n.id) === 0) q.push([n, 0]); });
  while (q.length) {
    const it = q.shift(); const n = it[0]; const d = it[1];
    if (depth.has(n.id)) continue;
    depth.set(n.id, d);
    (n.wires || []).forEach(function (w) { (w || []).forEach(function (t) { if (ids.has(t) && !depth.has(t) && t !== n.id) q.push([byId.get(t), d + 1]); }); });
  }
  const sorted = nodes.slice().sort(function (a, b) {
    const da = depth.has(a.id) ? depth.get(a.id) : 99; const db = depth.has(b.id) ? depth.get(b.id) : 99;
    if (da !== db) return da - db;
    const ya = a.y || 0, yb = b.y || 0;
    if (ya !== yb) return ya - yb;
    return (a.x || 0) - (b.x || 0);
  });
  const disabledCount = nodes.filter(function (n) { return isDisabled(n); }).length;
  const kind = container && container.type === "subflow" ? "subflow " : "tab ";
  const lines = ["# " + kind + displayName(container) + " (id " + containerId + ", " + nodes.length + " nodes" + (disabledCount ? ", " + disabledCount + " disabled" : "") + ")"];
  sorted.forEach(function (n) {
    const d = depth.has(n.id) ? Math.min(depth.get(n.id), 8) : 0;
    let label = n.type;
    if (String(n.type).indexOf("subflow:") === 0) {
      const def = byId.get(String(n.type).slice(9));
      label = "subflow " + (def ? displayName(def) : String(n.type).slice(9));
    }
    const parts = [shortId(n.id), label];
    if (n.name) parts.push(JSON.stringify(n.name));
    if (isDisabled(n)) parts.push("[disabled]");
    const ps = paramLine(n, byId);
    if (ps.length) parts.push("| " + ps.join(" "));
    const outs = [];
    (n.wires || []).forEach(function (w, i) {
      const tg = (w || []).map(function (t) {
        const tn = byId.get(t);
        if (tn && ids.has(t)) return displayName(tn) + "(" + shortId(t) + ")";
        if (tn) return "[ext] " + displayName(tn) + " (link/group)";
        return "[missing:" + t + "]";
      });
      outs.push(((n.wires || []).length > 1 ? "[" + (i + 1) + "] " : "") + "-> " + (tg.join(", ") || "-"));
    });
    lines.push("  ".repeat(d) + parts.join(" ") + (outs.length ? " " + outs.join(" ") : ""));
  });
  return lines.join("\n");
}

function mermaidEscape(s) { return String(s).replace(/[\\\"[\]]/g, " ").replace(/[\r\n]+/g, " ").slice(0, 60); }
function mermaidTab(flows, byId, containerId) {
  const nodes = flows.filter(function (n) { return n.z === containerId && n.type !== "tab" && n.type !== "group"; });
  const ids = new Set(nodes.map(function (n) { return n.id; }));
  const mid = function (n) { return "n" + String(n.id).replace(/[^a-zA-Z0-9_]/g, "_"); };
  const xid = function (t) { return "x" + String(t).replace(/[^a-zA-Z0-9_]/g, "_"); };
  const lines = ["flowchart TD"];
  nodes.forEach(function (n) {
    let label = n.name ? n.name + " [" + n.type + "]" : n.type;
    if (String(n.type).indexOf("subflow:") === 0) {
      const def = byId.get(String(n.type).slice(9));
      label = (n.name || (def ? displayName(def) : "subflow")) + " [subflow]";
    }
    if (isDisabled(n)) label = label + " (disabled)";
    lines.push("  " + mid(n) + '["' + mermaidEscape(label) + '"]');
  });
  const extDefs = new Map();
  const edges = [];
  nodes.forEach(function (n) {
    (n.wires || []).forEach(function (w, i) {
      (w || []).forEach(function (t) {
        const tn = byId.get(t);
        if (tn && ids.has(t)) {
          edges.push("  " + mid(n) + ((n.wires || []).length > 1 ? " -->|out" + (i + 1) + "| " : " --> ") + mid(tn));
        } else {
          const k = xid(t);
          if (!extDefs.has(k)) {
            extDefs.set(k, "  " + k + '["' + mermaidEscape(tn ? displayName(tn) + " (" + (tn.type === "tab" ? tn.label || "tab" : tn.type) + ")" : "missing " + String(t).slice(0, 12)) + '"]');
          }
          edges.push("  " + mid(n) + " -.-> " + k);
        }
      });
    });
  });
  return lines.concat(Array.from(extDefs.values()), edges).join("\n");
}

function tabSummary(flows, byId, tab, detail) {
  const nodes = flows.filter(function (n) { return n.z === tab.id && n.type !== "tab" && n.type !== "group"; });
  const base = { id: tab.id, label: tab.label || tab.id, disabled: isDisabled(tab), nodes: nodes.length, disabled_nodes: nodes.filter(function (n) { return isDisabled(n); }).length };
  if (!detail) return base;
  const types = {};
  const entries = [];
  const configs = {};
  const sub = [];
  nodes.forEach(function (n) {
    types[n.type] = (types[n.type] || 0) + 1;
    if (ENTRY_TYPES.has(n.type)) {
      const e = { id: n.id, type: n.type, name: n.name || null };
      ["topic", "url", "method", "repeat", "crontab", "entities", "entity_id", "entity"].forEach(function (k) { if (n[k] !== undefined && n[k] !== "") e[k] = n[k]; });
      entries.push(e);
    }
    if (String(n.type).indexOf("subflow:") === 0) sub.push({ id: n.id, subflow: String(n.type).slice(9), name: n.name || null });
    Object.keys(n).forEach(function (k) {
      if (k === "wires" || k === "z" || k === "id") return;
      const v = n[k];
      if (typeof v !== "string") return;
      const t = byId.get(v);
      if (t && t.type !== "tab" && t.type !== "subflow" && String(t.type).indexOf("subflow:") !== 0 && (t.z === undefined || t.z === null || isConfigType(t.type))) configs[t.id] = displayName(t) + " (" + t.type + ", " + t.id + ")";
    });
  });
  base.types = types;
  base.entry_points = entries;
  base.subflow_instances = sub;
  base.config_refs = Object.values(configs);
  return base;
}

function searchNodes(flows, a) {
  const byId = buildIndex(flows);
  const q = String(a.query || "").toLowerCase();
  const limit = a.limit || 30;
  const results = [];
  const code = [];
  flows.forEach(function (n) {
    if (n.type === "tab" || n.type === "group") return;
    if (a.type && n.type !== a.type && !(a.type === "subflow" && String(n.type).indexOf("subflow:") === 0)) return;
    if (a.tab_id && n.z !== a.tab_id) return;
    const tabNode = n.z ? byId.get(n.z) : null;
    const tab = tabNode ? (tabNode.label || tabNode.name || tabNode.id) : (n.z || "config/global");
    const hay = [n.id, n.type, n.name || "", n.topic || "", n.entity_id || "", n.url || ""].join(" ").toLowerCase();
    let hit = hay.indexOf(q) >= 0;
    let snippet = null;
    if (!hit && typeof n.func === "string") {
      const low = n.func.toLowerCase(); const i2 = low.indexOf(q);
      if (i2 >= 0) snippet = trunc(n.func.slice(Math.max(0, i2 - 30), i2 + q.length + 30).replace(/[\r\n]+/g, " "), 90);
    }
    if (!hit && snippet === null) return;
    const item = { id: n.id, type: n.type, name: n.name || null, tab: tab };
    if (n.topic) item.topic = n.topic;
    if (n.entity_id) item.entity_id = n.entity_id;
    if (n.url) item.url = n.url;
    if (isDisabled(n)) item.disabled = true;
    item.match = hit ? "fields" : "func";
    if (snippet !== null) item.snippet = snippet;
    (hit ? results : code).push(item);
  });
  return { query: a.query, matches: results.length + code.length, results: results.slice(0, limit), code_matches: code.slice(0, limit) };
}

// ---------------- validation + diff ----------------
function validateFlows(flows) {
  const errors = []; const warnings = [];
  if (!Array.isArray(flows)) return { ok: false, errors: ["flows must be an array"], warnings: warnings };
  const ids = new Set();
  flows.forEach(function (n) {
    if (!n || typeof n !== "object") { errors.push("non-object entry in flows array"); return; }
    if (!n.id) errors.push(String(n.type || "?") + " node missing id");
    else if (ids.has(n.id)) errors.push("duplicate id " + n.id);
    else ids.add(n.id);
    if (!n.type) errors.push("node " + String(n.id) + " missing type");
  });
  flows.forEach(function (n) {
    if (!n || !Array.isArray(n.wires)) return;
    n.wires.forEach(function (w, i) {
      if (!Array.isArray(w)) { errors.push(String(n.id) + " wires[" + i + "] is not an array"); return; }
      (w || []).forEach(function (t) { if (!ids.has(t)) errors.push(String(n.id) + " wires[" + i + "] -> missing target " + String(t)); });
    });
  });
  flows.forEach(function (n) {
    if (!n || typeof n !== "object") return;
    if (n.z && !ids.has(n.z)) warnings.push(String(n.id) + " z=" + n.z + " not a known tab/subflow id (ok for scoped config nodes)");
    if (Array.isArray(n.links) && n.type === "link out") n.links.forEach(function (l) { if (!ids.has(l)) errors.push(String(n.id) + " links -> missing link in " + String(l)); });
    if (n.g && !ids.has(n.g)) warnings.push(String(n.id) + " g=" + n.g + " not a known group id");
  });
  flows.forEach(function (n) {
    if (!n || typeof n !== "object") return;
    if (n.type === "server-state-changed" && n.entityidfilter !== undefined && !n.entities) warnings.push(String(n.id) + " server-state-changed has legacy v3 field entityidfilter - contrib v6 expects entities.entity; the node may never fire");
    if (typeof n.func === "string" && /flow\.get\([^)]*,\s*["']file["']\)/.test(n.func)) warnings.push(String(n.id) + " function uses flow.get(key, \"file\") - default instances have no file context store; use plain flow.get(key)");
  });
  return { ok: errors.length === 0, errors: errors, warnings: warnings };
}

function nodeDiff(before, after) {
  const keys = new Set(Object.keys(before || {}));
  Object.keys(after || {}).forEach(function (k) { keys.add(k); });
  const out = [];
  keys.forEach(function (k) {
    const a = JSON.stringify(before ? before[k] : undefined);
    const b = JSON.stringify(after ? after[k] : undefined);
    if (a !== b) out.push(k + ": " + trunc(a, 90) + "  ->  " + trunc(b, 90));
  });
  return out;
}

function newId() { return crypto.randomBytes(8).toString("hex"); }
function removeNode(flows, nodeId) {
  return flows.filter(function (n) { return n.id !== nodeId; }).map(function (n) {
    if (!Array.isArray(n.wires)) return n;
    const c = Object.assign({}, n);
    c.wires = n.wires.map(function (w) { return (w || []).filter(function (t) { return t !== nodeId; }); });
    return c;
  });
}
// Rewire every wire pointing AT nodeId so it points to nodeId's (single) output
// targets instead - the shared core of delete_node(splice) and bypass_node.
function spliceOutWires(flows, nodeId) {
  const n = flows.find(function (x) { return x.id === nodeId; });
  const outs = (n && n.wires && n.wires[0]) || [];
  return flows.map(function (m) {
    if (!Array.isArray(m.wires)) return m;
    let changed = false;
    const c = Object.assign({}, m);
    c.wires = m.wires.map(function (w) {
      const ix = (w || []).indexOf(nodeId);
      if (ix < 0) return w;
      changed = true;
      const nw = (w || []).slice();
      nw.splice.apply(nw, [ix, 1].concat(outs));
      return nw;
    });
    return changed ? c : m;
  });
}

// ---------------- canvas geometry (free regions, overlap findings, gate) ----------------
// Node-RED flow JSON: node x/y are the node CENTER (default footprint 100x30,
// only resized nodes carry explicit w/h); group x/y is the TOP-LEFT corner
// with exact w/h. Node footprints are therefore ESTIMATES -> node-level
// findings are warnings only; group-level findings are exact -> errors.
const NODE_DEF_W = 100, NODE_DEF_H = 30, CANVAS_MARGIN = 20, GRID = 20;

function nodeRect(n) {
  if (!n || typeof n.x !== "number" || typeof n.y !== "number") return null;
  let w = NODE_DEF_W, h = NODE_DEF_H;
  if (n.type === "link in" || n.type === "link out" || n.l === false) { w = 30; h = 30; } // small canvas nodes
  if (typeof n.w === "number") w = n.w;
  if (typeof n.h === "number") h = n.h;
  return { x: n.x - w / 2, y: n.y - h / 2, w: w, h: h };
}
function groupRect(g) {
  return { x: typeof g.x === "number" ? g.x : 0, y: typeof g.y === "number" ? g.y : 0, w: g.w || NODE_DEF_W, h: g.h || NODE_DEF_H };
}
function rectsHit(a, b, pad) {
  pad = pad || 0;
  return a.x < b.x + b.w + pad && b.x < a.x + a.w + pad && a.y < b.y + b.h + pad && b.y < a.y + a.h + pad;
}
function rectInside(inner, outer) {
  return inner.x >= outer.x && inner.y >= outer.y && inner.x + inner.w <= outer.x + outer.w && inner.y + inner.h <= outer.y + outer.h;
}

// First free canvas slot in a tab (scans top-left downward); returns a CENTER
// position ready to assign to a node.
function freeRegion(flows, zid, wantW, wantH) {
  wantW = wantW || NODE_DEF_W; wantH = wantH || NODE_DEF_H;
  const rects = [];
  flows.forEach(function (n) {
    if (n.z !== zid) return;
    if (n.type === "group") { if (typeof n.x === "number") rects.push(groupRect(n)); return; }
    const r = nodeRect(n); if (r) rects.push(r);
  });
  const busy = function (x, y) {
    return rects.some(function (r) {
      return x < r.x + r.w + CANVAS_MARGIN && r.x < x + wantW + CANVAS_MARGIN && y < r.y + r.h + CANVAS_MARGIN && r.y < y + wantH + CANVAS_MARGIN;
    });
  };
  for (let y = 40; y < 80000; y += GRID) {
    for (let x = 40; x < 4000; x += GRID) {
      if (!busy(x, y)) return { x: Math.round(x + wantW / 2), y: Math.round(y + wantH / 2) };
    }
  }
  return { x: 100, y: 100 };
}

// Overlap / membership findings per tab.
function layoutFindings(flows) {
  const byTab = {};
  function add(tabId, sev, msg) {
    const t = byTab[tabId] = byTab[tabId] || { errors: [], warnings: [] };
    (sev === "error" ? t.errors : t.warnings).push(msg);
  }
  const tabs = flows.filter(function (n) { return n.type === "tab"; });
  for (const tab of tabs) {
    const inTab = flows.filter(function (n) { return n.z === tab.id; });
    const groups = inTab.filter(function (n) { return n.type === "group"; });
    const nodes = inTab.filter(function (n) { return n.type !== "group"; });
    const gById = new Map(groups.map(function (g) { return [g.id, g]; }));
    for (let i = 0; i < groups.length; i++) for (let j = i + 1; j < groups.length; j++) {
      const a = groups[i], b = groups[j];
      if ((a.g && a.g === b.id) || (b.g && b.g === a.id)) continue; // nested groups are fine
      if (typeof a.x !== "number" || typeof b.x !== "number") continue;
      if (rectsHit(groupRect(a), groupRect(b))) add(tab.id, "error", "group-overlap: " + displayName(a) + " <-> " + displayName(b));
    }
    for (const n of nodes) {
      const g = n.g ? gById.get(n.g) : null;
      const r = nodeRect(n);
      if (g) {
        if (typeof g.x === "number" && r && !rectInside(r, groupRect(g))) add(tab.id, "error", "group-escape: " + displayName(n) + " outside its group " + displayName(g));
        if (Array.isArray(g.nodes) && g.nodes.indexOf(n.id) < 0) add(tab.id, "warning", "membership mismatch: " + displayName(n) + " has g=" + shortId(g.id) + " but is not listed in the group");
      } else if (r) {
        for (const gg of groups) {
          if (typeof gg.x !== "number") continue;
          if (rectsHit(r, groupRect(gg))) add(tab.id, "warning", "stray-in-group: " + displayName(n) + " sits inside " + displayName(gg) + " but is not a member");
        }
      }
    }
    for (let i = 0; i < nodes.length; i++) for (let j = i + 1; j < nodes.length; j++) {
      const a = nodes[i], b = nodes[j];
      if (typeof a.x !== "number" || typeof b.x !== "number") continue;
      if (Math.abs(a.x - b.x) < 50 && Math.abs(a.y - b.y) < 16) add(tab.id, "warning", "node-overlap (estimated): " + displayName(a) + " <-> " + displayName(b));
    }
  }
  return byTab;
}

// The layout gate: compare findings before/after an edit; ONLY new
// error-level (group geometry) findings block the write. New warnings are
// reported but never block.
function gateCheck(previous, next) {
  const a = layoutFindings(previous), b = layoutFindings(next);
  const oldErr = new Set(), oldWarn = new Set();
  Object.keys(a).forEach(function (t) { a[t].errors.forEach(function (e) { oldErr.add(t + "|" + e); }); a[t].warnings.forEach(function (w) { oldWarn.add(t + "|" + w); }); });
  const newErrors = [], newWarnings = [];
  Object.keys(b).forEach(function (t) {
    b[t].errors.forEach(function (e) { const k = t + "|" + e; if (!oldErr.has(k)) newErrors.push(t + " " + e); });
    b[t].warnings.forEach(function (w) { const k = t + "|" + w; if (!oldWarn.has(k)) newWarnings.push(t + " " + w); });
  });
  return { blocked: newErrors.length > 0, newErrors: newErrors, newWarnings: newWarnings };
}

// ---------------- group helpers ----------------
function boundsFromNodes(nodes) {
  const rects = nodes.map(nodeRect).filter(Boolean);
  if (!rects.length) return null;
  const minX = Math.min.apply(null, rects.map(function (r) { return r.x; }));
  const minY = Math.min.apply(null, rects.map(function (r) { return r.y; }));
  const maxX = Math.max.apply(null, rects.map(function (r) { return r.x + r.w; }));
  const maxY = Math.max.apply(null, rects.map(function (r) { return r.y + r.h; }));
  return { x: Math.round(minX - CANVAS_MARGIN), y: Math.round(minY - CANVAS_MARGIN), w: Math.round(maxX - minX + 2 * CANVAS_MARGIN), h: Math.round(maxY - minY + 2 * CANVAS_MARGIN) };
}
function refitGroups(flows, groupIds) {
  const byId = buildIndex(flows);
  return flows.map(function (m) {
    if (m.type === "group" && groupIds.has(m.id)) {
      const members = (m.nodes || []).map(function (id) { return byId.get(id); }).filter(function (n) { return n && n.type !== "group"; });
      const b = boundsFromNodes(members);
      return b ? Object.assign({}, m, b) : m;
    }
    return m;
  });
}
// All descendants of a group (members + nested groups + their members...).
function groupDescendants(flows, byId, groupId) {
  const out = [];
  const walk = function (gid) {
    flows.forEach(function (n) {
      if (n.g === gid && out.indexOf(n) < 0) { out.push(n); if (n.type === "group") walk(n.id); }
    });
  };
  walk(groupId);
  return out;
}

// ---------------- export / import helpers ----------------
function isConfigCandidate(t) {
  return t && t.type !== "tab" && t.type !== "subflow" && t.type !== "group" && String(t.type).indexOf("subflow:") !== 0 && (t.z === undefined || t.z === null || isConfigType(t.type));
}
function collectExportSet(flows, ids, includeConfigs, includeSubflows) {
  const byId = buildIndex(flows);
  const out = new Map();
  const queue = ids.slice();
  while (queue.length) {
    const id = queue.shift();
    if (out.has(id)) continue;
    const n = byId.get(id);
    if (!n) continue;
    out.set(id, n);
    if (includeSubflows && String(n.type).indexOf("subflow:") === 0) {
      const d = String(n.type).slice(9);
      if (!out.has(d)) queue.push(d);
    }
    if (includeConfigs) {
      Object.keys(n).forEach(function (k) {
        if (k === "wires" || k === "z" || k === "g" || k === "id") return;
        const v = n[k];
        if (typeof v === "string" && v !== n.id && byId.has(v)) {
          const t = byId.get(v);
          if (isConfigCandidate(t) && !out.has(v)) queue.push(v);
        }
      });
    }
  }
  return Array.from(out.values());
}
function configSignature(n) {
  const c = Object.assign({}, n);
  ["id", "z", "x", "y", "w", "h", "g", "d", "wires", "links"].forEach(function (k) { delete c[k]; });
  const keys = Object.keys(c).sort();
  return n.type + "|" + keys.map(function (k) { return k + "=" + JSON.stringify(c[k]); }).join("&");
}

// ---------------- flow hygiene audit ----------------
function auditReport(flows) {
  const byId = buildIndex(flows);
  const cfgIds = new Set(flows.filter(function (n) { return isConfigCandidate(n); }).map(function (n) { return n.id; }));
  const users = new Map(); cfgIds.forEach(function (id) { users.set(id, 0); });
  flows.forEach(function (n) {
    if (cfgIds.has(n.id)) return;
    Object.keys(n).forEach(function (k) {
      if (k === "wires" || k === "z" || k === "id" || k === "g") return;
      const v = n[k];
      if (typeof v === "string" && cfgIds.has(v)) users.set(v, users.get(v) + 1);
    });
  });
  const out = { unused_config_nodes: [], empty_tabs: [], disabled_tabs: [], disabled_nodes: [], orphan_nodes: [], dangling_outputs: [], cross_tab_wires: [], duplicate_names: [], unknown_z: [] };
  cfgIds.forEach(function (id) { if (users.get(id) === 0) { const n = byId.get(id); out.unused_config_nodes.push({ id: id, type: n.type, name: n.name || null }); } });
  const tabs = flows.filter(function (n) { return n.type === "tab"; });
  tabs.forEach(function (t) {
    const cnt = flows.filter(function (n) { return n.z === t.id && n.type !== "group"; }).length;
    if (cnt === 0) out.empty_tabs.push({ id: t.id, label: t.label });
    if (isDisabled(t)) out.disabled_tabs.push({ id: t.id, label: t.label });
  });
  const wireTargets = new Set();
  flows.forEach(function (n) { (n.wires || []).forEach(function (w) { (w || []).forEach(function (t) { wireTargets.add(t); }); }); });
  const seenNames = new Map();
  const TERMINAL = new Set(["debug", "link out", "comment", "http response", "mqtt out", "ha-fire-event", "api-call-service", "call-service"]);
  const tabOf = function (z) { const c = z ? byId.get(z) : null; return c ? (c.label || z) : (z || null); };
  flows.forEach(function (n) {
    if (n.type === "tab" || n.type === "group" || n.type === "subflow") return;
    if (n.z && !byId.has(n.z)) out.unknown_z.push({ id: n.id, z: n.z });
    if (String(n.type).indexOf("subflow:") === 0) return;
    const where = { id: n.id, type: n.type, name: n.name || null, tab: tabOf(n.z) };
    if (isDisabled(n)) out.disabled_nodes.push(where);
    const hasIn = wireTargets.has(n.id);
    const hasOut = (n.wires || []).some(function (w) { return (w || []).length > 0; });
    if (!hasIn && !hasOut && n.type !== "comment") out.orphan_nodes.push(where);
    if (!hasOut && !TERMINAL.has(n.type) && n.type !== "comment") out.dangling_outputs.push(where);
    if (n.name) {
      const nk = n.type + " :: " + n.name;
      if (!seenNames.has(nk)) seenNames.set(nk, []);
      seenNames.get(nk).push(where);
    }
    (n.wires || []).forEach(function (w) {
      (w || []).forEach(function (t) {
        const tg = byId.get(t);
        if (!tg || !n.z || !tg.z || n.z === tg.z) return;
        const cz = byId.get(n.z), tz = byId.get(tg.z);
        if (cz && tz && cz.type === "tab" && tz.type === "tab") out.cross_tab_wires.push({ from: displayName(n) + " (" + shortId(n.id) + ")", to: displayName(tg) + " (" + shortId(t.id) + ")" });
      });
    });
  });
  seenNames.forEach(function (arr, k) { if (arr.length > 1) out.duplicate_names.push({ key: k, nodes: arr }); });
  out.summary = {};
  Object.keys(out).forEach(function (k) { if (k !== "summary" && out[k].length) out.summary[k] = out[k].length; });
  return out;
}

// ---------------- server ----------------
const server = new McpServer({ name: "nodered-mcp", version: "0.5.0" });
const log = (msg) => console.error("[nodered-mcp] " + msg);
log("target " + URL_BASE + (TOKEN ? " (token)" : "") + (READ_ONLY ? " READ-ONLY" : ""));

server.tool("get_settings", "Node-RED runtime settings (version, palette editability, contextStorage) - check limits before advanced work", {}, async () => {
  const s = await apiGetJson("/settings");
  return { content: [{ type: "text", text: txt({ version: s.version, paletteEditable: s.paletteEditable ?? null, contextStorage: s.contextStorage ?? null, httpAdminRoot: s.httpAdminRoot ?? null }) }] };
});

server.tool("list_flows", "List flow tabs with node counts. detail:true adds per-tab node-type histogram, entry points (injects + intervals, mqtt topics, http endpoints, HA entities), config refs and subflow instances - the one-call overview of the whole instance.", { detail: z.boolean().optional() }, async ({ detail }) => {
  const flows = await fetchFlows();
  const byId = buildIndex(flows);
  const tabs = flows.filter(function (n) { return n.type === "tab"; });
  const out = tabs.map(function (t) { return tabSummary(flows, byId, t, !!detail); });
  return { content: [{ type: "text", text: txt(out) }] };
});

server.tool("get_flow_map", "Render one tab or subflow as an AI-readable map - use this FIRST to understand a flow before editing. format=outline (default): indented node lines in execution order, wiring resolved to node names (-> targets), config nodes inlined, functions summarized as line counts. format=mermaid: flowchart diagram. format=json: raw node array.", { tab_id: z.string(), format: z.enum(["outline", "mermaid", "json"]).optional() }, async ({ tab_id, format }) => {
  const flows = await fetchFlows();
  const byId = buildIndex(flows);
  if (!byId.has(tab_id)) throw new Error("tab/subflow not found: " + tab_id + " (use list_flows for ids)");
  const f = format || "outline";
  if (f === "mermaid") return { content: [{ type: "text", text: txt(mermaidTab(flows, byId, tab_id)) }] };
  if (f === "json") return { content: [{ type: "text", text: txt(flows.filter(function (n) { return n.z === tab_id; })) }] };
  return { content: [{ type: "text", text: txt(outlineTab(flows, byId, tab_id)) }] };
});

server.tool("get_flow", "Get nodes of one flow tab (by tab id) or the whole flow document as JSON - for exact patching. compact:true strips x/y/w/h; offset/limit paginate large tabs. For understanding a flow prefer get_flow_map; for finding nodes prefer search_nodes.", {
  tab_id: z.string().optional(), full: z.boolean().optional(), compact: z.boolean().optional(), offset: z.number().int().optional(), limit: z.number().int().optional()
}, async ({ tab_id, full, compact, offset, limit }) => {
  const flows = await fetchFlows();
  let out = full || !tab_id ? flows : flows.filter(function (n) { return n.z === tab_id; });
  if (compact) out = out.map(function (n) { const c = Object.assign({}, n); delete c.x; delete c.y; delete c.w; delete c.h; return c; });
  const total = out.length;
  if (typeof offset === "number" || typeof limit === "number") {
    const o = offset || 0;
    const l = limit || Math.max(total - o, 0);
    const page = out.slice(o, o + l);
    return { content: [{ type: "text", text: txt({ total: total, offset: o, count: page.length, nodes: page }) }] };
  }
  return { content: [{ type: "text", text: txt(out) }] };
});

server.tool("get_node", "Get a single node by id (exact config, function code, wiring). resolve_refs:true also inlines the config nodes it references (HA server, mqtt broker...).", { node_id: z.string(), resolve_refs: z.boolean().optional() }, async ({ node_id, resolve_refs }) => {
  const flows = await fetchFlows();
  const n = flows.find(function (x) { return x.id === node_id; });
  if (!n) throw new Error("node not found: " + node_id);
  let out = n;
  if (resolve_refs) {
    const byId = buildIndex(flows);
    const refs = {};
    Object.keys(n).forEach(function (k) {
      if (k === "wires" || k === "z") return;
      const v = n[k];
      if (typeof v === "string" && v !== n.id && byId.has(v)) {
        const t = byId.get(v);
        if (t.type !== "tab" && t.type !== "subflow" && t.type !== "group") refs[k] = t;
      }
    });
    out = { node: n, refs: refs };
  }
  return { content: [{ type: "text", text: txt(out) }] };
});

server.tool("search_nodes", "Find nodes by text across name, type, id, topic, entity, url AND function code (code matches returned separately with a snippet). Much cheaper than get_flow when hunting specific nodes. Optional type and tab_id filters; limit default 30.", {
  query: z.string(), type: z.string().optional(), tab_id: z.string().optional(), limit: z.number().int().optional()
}, async (a) => {
  const flows = await fetchFlows();
  return { content: [{ type: "text", text: txt(searchNodes(flows, a)) }] };
});

server.tool("list_config_nodes", "List config nodes (mqtt brokers, HA servers, dashboard/ui config) with usage counts - who references each one.", {}, async () => {
  const flows = await fetchFlows();
  const byId = buildIndex(flows);
  const cfgIds = new Set(flows.filter(function (n) { return isConfigCandidate(n); }).map(function (n) { return n.id; }));
  const users = new Map(); cfgIds.forEach(function (id2) { users.set(id2, 0); });
  flows.forEach(function (n) {
    if (cfgIds.has(n.id)) return;
    Object.keys(n).forEach(function (k) {
      if (k === "wires" || k === "z" || k === "id" || k === "g") return;
      const v = n[k];
      if (typeof v === "string" && cfgIds.has(v)) users.set(v, users.get(v) + 1);
    });
  });
  const out = [];
  cfgIds.forEach(function (id2) { const n = byId.get(id2); out.push({ id: id2, type: n.type, name: n.name || null, users: users.get(id2) }); });
  out.sort(function (a, b) { return b.users - a.users; });
  return { content: [{ type: "text", text: txt(out) }] };
});

server.tool("patch_node", "Patch ONE node by id (merge fields: func, wires, name, x, y, ...). dry_run:true previews diff + validation without writing. Applying requires confirm:true. Automatic backup + full-array deploy server-side; every deploy is revision-guarded (409 on concurrent editor deploys).", {
  node_id: z.string(), patch: z.record(z.unknown()), confirm: z.boolean().optional(), dry_run: z.boolean().optional(), allow_overlap: z.boolean().optional()
}, async ({ node_id, patch, confirm, dry_run, allow_overlap }) => {
  if (READ_ONLY) throw new Error(readOnlyError());
  const flows = await fetchFlows();
  const i = flows.findIndex(function (x) { return x.id === node_id; });
  if (i < 0) throw new Error("node not found: " + node_id);
  const merged = Object.assign({}, flows[i], patch);
  const next = flows.slice(); next[i] = merged;
  const val = validateFlows(next);
  if (dry_run) return { content: [{ type: "text", text: txt({ dry_run: true, diff: nodeDiff(flows[i], merged), validation: val }) }] };
  if (!confirm) throw new Error("pass confirm:true to apply this patch");
  const res = await deployFlows(next, { previous: flows, allowOverlap: allow_overlap });
  return { content: [{ type: "text", text: txt(Object.assign({ patched: node_id, fields: Object.keys(patch), validation: val }, res)) }] };
});

server.tool("patch_batch", "Apply several node patches in ONE deploy: patches=[{node_id, patch}, ...]. Server merges into the full flow set, validates, backs up and deploys once - you never send the whole array. dry_run:true previews all diffs. Applying requires confirm:true.", {
  patches: z.array(z.object({ node_id: z.string(), patch: z.record(z.unknown()) })), confirm: z.boolean().optional(), dry_run: z.boolean().optional(), allow_overlap: z.boolean().optional()
}, async ({ patches, confirm, dry_run, allow_overlap }) => {
  if (READ_ONLY) throw new Error(readOnlyError());
  if (!Array.isArray(patches) || patches.length === 0) throw new Error("patches must be a non-empty array");
  const flows = await fetchFlows();
  const next = flows.slice();
  const diffs = [];
  for (const p of patches) {
    const i = next.findIndex(function (x) { return x.id === p.node_id; });
    if (i < 0) throw new Error("node not found: " + p.node_id);
    const merged = Object.assign({}, next[i], p.patch);
    diffs.push({ node_id: p.node_id, diff: nodeDiff(next[i], merged) });
    next[i] = merged;
  }
  const val = validateFlows(next);
  if (dry_run) return { content: [{ type: "text", text: txt({ dry_run: true, diffs: diffs, validation: val }) }] };
  if (!confirm) throw new Error("pass confirm:true to apply these patches");
  const res = await deployFlows(next, { previous: flows, allowOverlap: allow_overlap });
  return { content: [{ type: "text", text: txt(Object.assign({ patched: patches.length + " nodes", diffs: diffs, validation: val }, res)) }] };
});

server.tool("upsert_node", "Create a new node (auto id; position chosen in the first FREE canvas region of the tab, no overlaps) or update an existing one when node.id matches. Optional wire_from {node_id, output} appends this node to that output wire. dry_run:true previews. Applying requires confirm:true. Backup + full deploy server-side.", {
  node: z.record(z.unknown()), tab_id: z.string().optional(), wire_from: z.object({ node_id: z.string(), output: z.number().int().optional() }).optional(), confirm: z.boolean().optional(), dry_run: z.boolean().optional(), allow_overlap: z.boolean().optional()
}, async ({ node, tab_id, wire_from, confirm, dry_run, allow_overlap }) => {
  if (READ_ONLY) throw new Error(readOnlyError());
  if (!node || typeof node !== "object" || !node.type) throw new Error("node.type is required");
  const flows = await fetchFlows();
  const byId = buildIndex(flows);
  const exists = node.id && byId.has(node.id);
  const next = flows.slice();
  let action; let theNode;
  if (exists) {
    const i = next.findIndex(function (x) { return x.id === node.id; });
    theNode = Object.assign({}, next[i], node);
    next[i] = theNode; action = "updated";
  } else {
    const zid = tab_id || node.z || null;
    if (!zid) throw new Error("tab_id required for new nodes");
    if (!byId.has(zid)) throw new Error("tab/subflow not found: " + zid);
    let pos;
    if (typeof node.x === "number" && typeof node.y === "number") pos = { x: node.x, y: node.y };
    else pos = freeRegion(flows, zid, typeof node.w === "number" ? node.w : NODE_DEF_W, typeof node.h === "number" ? node.h : NODE_DEF_H);
    theNode = Object.assign({ id: newId(), x: pos.x, y: pos.y, wires: [] }, node, { z: zid });
    next.push(theNode); action = "created";
  }
  if (wire_from) {
    const wi = next.findIndex(function (x) { return x.id === wire_from.node_id; });
    if (wi < 0) throw new Error("wire_from node not found: " + wire_from.node_id);
    const src = Object.assign({}, next[wi]);
    if (!Array.isArray(src.wires)) src.wires = [];
    const o = wire_from.output || 0;
    while (src.wires.length <= o) src.wires.push([]);
    src.wires[o] = (src.wires[o] || []).concat([theNode.id]);
    next[wi] = src;
  }
  const val = validateFlows(next);
  if (dry_run) return { content: [{ type: "text", text: txt({ dry_run: true, action: action, node: theNode, validation: val }) }] };
  if (!confirm) throw new Error("pass confirm:true to apply");
  const res = await deployFlows(next, { previous: flows, allowOverlap: allow_overlap });
  return { content: [{ type: "text", text: txt(Object.assign({ action: action, node_id: theNode.id, validation: val }, res)) }] };
});

server.tool("delete_node", "Delete one node by id and clean every wire pointing to it. splice:true (single-output nodes only) rewires its inputs directly to its outputs - bypass semantics. dry_run:true previews. Applying requires confirm:true.", {
  node_id: z.string(), splice: z.boolean().optional(), confirm: z.boolean().optional(), dry_run: z.boolean().optional()
}, async ({ node_id, splice, confirm, dry_run }) => {
  if (READ_ONLY) throw new Error(readOnlyError());
  const flows = await fetchFlows();
  const n = flows.find(function (x) { return x.id === node_id; });
  if (!n) throw new Error("node not found: " + node_id);
  let next = flows.slice();
  if (splice) {
    if ((n.wires || []).length > 1) throw new Error("splice supports single-output nodes only");
    next = spliceOutWires(next, node_id);
  }
  next = removeNode(next, node_id);
  const val = validateFlows(next);
  if (dry_run) return { content: [{ type: "text", text: txt({ dry_run: true, deleting: n, validation: val }) }] };
  if (!confirm) throw new Error("pass confirm:true to delete");
  const res = await deployFlows(next, { previous: flows });
  return { content: [{ type: "text", text: txt(Object.assign({ deleted: node_id, spliced: !!splice, validation: val }, res)) }] };
});

server.tool("set_node_enabled", "Enable or disable nodes/tabs WITHOUT deleting them (nodes: d:true, tabs: disabled:true - disabled nodes stay on the canvas and are skipped by the runtime). Accepts many ids in ONE deploy. dry_run:true previews.", {
  node_ids: z.array(z.string()).min(1), enabled: z.boolean(), confirm: z.boolean().optional(), dry_run: z.boolean().optional()
}, async ({ node_ids, enabled, confirm, dry_run }) => {
  if (READ_ONLY) throw new Error(readOnlyError());
  const flows = await fetchFlows();
  const byId = buildIndex(flows);
  const missing = node_ids.filter(function (id) { return !byId.has(id); });
  if (missing.length) throw new Error("ids not found: " + missing.join(", "));
  const next = flows.map(function (n) {
    if (node_ids.indexOf(n.id) < 0) return n;
    const c = Object.assign({}, n);
    if (c.type === "tab") c.disabled = !enabled;
    else if (enabled) delete c.d;
    else c.d = true;
    return c;
  });
  const changed = flows.filter(function (n) { return node_ids.indexOf(n.id) >= 0; }).map(function (n) { return { id: n.id, type: n.type, name: n.name || null, disabled: !enabled }; });
  const val = validateFlows(next);
  if (dry_run) return { content: [{ type: "text", text: txt({ dry_run: true, changed: changed, validation: val }) }] };
  if (!confirm) throw new Error("pass confirm:true to apply");
  const res = await deployFlows(next, { previous: flows });
  return { content: [{ type: "text", text: txt(Object.assign({ changed: changed.length + " nodes", validation: val }, res)) }] };
});

server.tool("bypass_node", "Reroute every wire AROUND one node (single-output nodes only): its inputs are rewired straight to its outputs, the node itself stays on the canvas but is disabled (d:true). Non-destructive alternative to delete+splice. dry_run:true previews.", {
  node_id: z.string(), confirm: z.boolean().optional(), dry_run: z.boolean().optional()
}, async ({ node_id, confirm, dry_run }) => {
  if (READ_ONLY) throw new Error(readOnlyError());
  const flows = await fetchFlows();
  const n = flows.find(function (x) { return x.id === node_id; });
  if (!n) throw new Error("node not found: " + node_id);
  if (n.type === "tab" || n.type === "group") throw new Error("bypass_node works on flow nodes, not tabs/groups");
  if ((n.wires || []).length > 1) throw new Error("bypass_node supports single-output nodes only (this node has " + (n.wires || []).length + " outputs)");
  let next = spliceOutWires(flows, node_id);
  const idx = next.findIndex(function (x) { return x.id === node_id; });
  next[idx] = Object.assign({}, next[idx], { d: true });
  const val = validateFlows(next);
  if (dry_run) return { content: [{ type: "text", text: txt({ dry_run: true, bypassed: node_id, rewired_to: (n.wires && n.wires[0]) || [], validation: val }) }] };
  if (!confirm) throw new Error("pass confirm:true to apply");
  const res = await deployFlows(next, { previous: flows });
  return { content: [{ type: "text", text: txt(Object.assign({ bypassed: node_id, now_disabled: true, rewired_to: (n.wires && n.wires[0]) || [], validation: val }, res)) }] };
});

server.tool("deploy_flows", "Deploy a FULL flow array (the only safe deploy type). Validates it is an array, takes an automatic backup first, and carries the flow revision: a concurrent editor deploy results in 409 and NOTHING is written. force:true skips the revision guard (use only to overwrite a concurrent deploy on purpose). dry_run:true validates without deploying. Deploying requires confirm:true.", {
  flows: z.array(z.unknown()), confirm: z.boolean().optional(), backup_label: z.string().optional(), dry_run: z.boolean().optional(), force: z.boolean().optional(), allow_overlap: z.boolean().optional()
}, async ({ flows, confirm, backup_label, dry_run, force, allow_overlap }) => {
  if (READ_ONLY) throw new Error(readOnlyError());
  if (!Array.isArray(flows)) throw new Error("flows must be an array (the complete flow document)");
  const val = validateFlows(flows);
  if (dry_run) return { content: [{ type: "text", text: txt(Object.assign({ dry_run: true, nodes: flows.length }, val)) }] };
  if (!confirm) throw new Error("pass confirm:true to deploy");
  const cur = await fetchFlows(); // gate baseline + refresh revision
  const res = await deployFlows(flows, { previous: cur, force: force, allowOverlap: allow_overlap });
  return { content: [{ type: "text", text: txt(Object.assign({ validation: val }, res)) }] };
});

server.tool("validate_flows", "Validate a candidate full flow array WITHOUT deploying: duplicate ids, dangling wires, unknown z, broken link-out targets, unknown group refs, legacy HA v3 fields on v6 contrib nodes, flow.get(key, \"file\") traps. Run it on any edit built by hand.", { flows: z.array(z.unknown()) }, async ({ flows }) => {
  return { content: [{ type: "text", text: txt(validateFlows(flows)) }] };
});

server.tool("manage_tabs", "Tab lifecycle: create (returns the new tab id - use it as tab_id in upsert_node), rename, reorder (index or after_tab_id), set_info (tab description), delete (refuses non-empty tabs unless force:true). All in ONE deploy; dry_run previews.", {
  op: z.enum(["create", "rename", "reorder", "set_info", "delete"]),
  tab_id: z.string().optional(), label: z.string().optional(), info: z.string().optional(),
  index: z.number().int().optional(), after_tab_id: z.string().optional(),
  force: z.boolean().optional(), confirm: z.boolean().optional(), dry_run: z.boolean().optional()
}, async (a) => {
  if (READ_ONLY) throw new Error(readOnlyError());
  const flows = await fetchFlows();
  const tabs = flows.filter(function (n) { return n.type === "tab"; });
  let next = flows.slice();
  let result;
  if (a.op === "create") {
    const tab = { id: newId(), type: "tab", label: a.label || "New tab", disabled: false, info: a.info || "" };
    let at = tabs.length ? next.indexOf(tabs[tabs.length - 1]) + 1 : 0;
    if (a.after_tab_id) {
      const t = tabs.find(function (x) { return x.id === a.after_tab_id; });
      if (!t) throw new Error("after_tab_id not found: " + a.after_tab_id);
      at = next.indexOf(t) + 1;
    } else if (typeof a.index === "number") {
      const cl = Math.min(Math.max(a.index, 0), Math.max(tabs.length - 1, 0));
      at = tabs.length ? next.indexOf(tabs[cl]) + (a.index >= tabs.length ? 1 : 0) : 0;
    }
    next.splice(at, 0, tab);
    result = { created: tab };
  } else {
    if (!a.tab_id) throw new Error("tab_id required for op " + a.op);
    const ti = next.findIndex(function (n) { return n.id === a.tab_id && n.type === "tab"; });
    if (ti < 0) throw new Error("tab not found: " + a.tab_id);
    if (a.op === "rename") {
      next[ti] = Object.assign({}, next[ti], { label: a.label || next[ti].label });
      result = { renamed: a.tab_id, label: next[ti].label };
    } else if (a.op === "set_info") {
      next[ti] = Object.assign({}, next[ti], { info: a.info == null ? "" : String(a.info) });
      result = { tab_id: a.tab_id, info: next[ti].info };
    } else if (a.op === "reorder") {
      const tab = next.splice(ti, 1)[0];
      const rest = next.filter(function (n) { return n.type === "tab"; });
      let at;
      if (a.after_tab_id) {
        const t = rest.find(function (x) { return x.id === a.after_tab_id; });
        if (!t) throw new Error("after_tab_id not found: " + a.after_tab_id);
        at = next.indexOf(t) + 1;
      } else if (typeof a.index === "number") {
        const cl = Math.min(Math.max(a.index, 0), rest.length);
        at = cl < rest.length ? next.indexOf(rest[cl]) : (rest.length ? next.indexOf(rest[rest.length - 1]) + 1 : 0);
      } else throw new Error("reorder needs index or after_tab_id");
      next.splice(at, 0, tab);
      result = { moved: a.tab_id, new_position: at };
    } else if (a.op === "delete") {
      const members = flows.filter(function (n) { return n.z === a.tab_id && n.type !== "tab"; });
      if (members.length && !a.force) throw new Error("tab " + a.tab_id + " still has " + members.length + " nodes; pass force:true to delete the tab AND its nodes");
      const kill = new Set([a.tab_id].concat(members.map(function (n) { return n.id; })));
      next = next.filter(function (n) { return !kill.has(n.id); }).map(function (n) {
        if (!Array.isArray(n.wires)) return n;
        const c = Object.assign({}, n);
        c.wires = n.wires.map(function (w) { return (w || []).filter(function (t) { return !kill.has(t); }); });
        return c;
      });
      result = { deleted: a.tab_id, nodes_removed: members.length };
    }
  }
  const val = validateFlows(next);
  if (a.dry_run) return { content: [{ type: "text", text: txt({ dry_run: true, op: a.op, result: result, validation: val }) }] };
  if (!a.confirm) throw new Error("pass confirm:true to apply");
  const res = await deployFlows(next, { previous: flows });
  return { content: [{ type: "text", text: txt(Object.assign({ op: a.op, result: result, validation: val }, res)) }] };
});

server.tool("manage_groups", "Group lifecycle: create (bounds computed from member nodes), add/remove members (auto refit), rename, refit (resize around members), delete (keeps the nodes). Group geometry is exact in Node-RED: writes that would create NEW group overlaps or escape errors are refused by the layout gate (override: allow_overlap:true). dry_run previews.", {
  op: z.enum(["create", "add", "remove", "rename", "refit", "delete"]),
  tab_id: z.string().optional(), group_id: z.string().optional(), name: z.string().optional(),
  node_ids: z.array(z.string()).optional(), refit: z.boolean().optional(),
  confirm: z.boolean().optional(), dry_run: z.boolean().optional(), allow_overlap: z.boolean().optional()
}, async (a) => {
  if (READ_ONLY) throw new Error(readOnlyError());
  const flows = await fetchFlows();
  const byId = buildIndex(flows);
  let next = flows.slice();
  let result;
  const setG = function (arr, gid) {
    const ids = new Set(arr);
    next = next.map(function (n) {
      if (!ids.has(n.id)) return n;
      const c = Object.assign({}, n);
      if (gid) c.g = gid; else delete c.g;
      return c;
    });
  };
  // a node can belong to ONE group: detach it from its current one first
  // (keepGroupId = the group the node is being moved INTO)
  const detachFromCurrent = function (ids, keepGroupId) {
    const idSet = new Set(ids);
    next = next.map(function (n) {
      if (n.type !== "group" || !Array.isArray(n.nodes) || n.id === keepGroupId) return n;
      if (!n.nodes.some(function (id) { return idSet.has(id); })) return n;
      const c = Object.assign({}, n);
      c.nodes = n.nodes.filter(function (id) { return !idSet.has(id); });
      return c;
    });
  };
  if (a.op === "create") {
    if (!a.tab_id) throw new Error("tab_id required to create a group");
    if (!Array.isArray(a.node_ids) || !a.node_ids.length) throw new Error("node_ids required to create a group (initial members)");
    const tab = byId.get(a.tab_id);
    if (!tab || tab.type !== "tab") throw new Error("tab not found: " + a.tab_id);
    const members = a.node_ids.map(function (id) { return byId.get(id); });
    members.forEach(function (m, i) {
      if (!m || m.z !== a.tab_id || m.type === "group" || m.type === "tab") throw new Error("node_ids[" + i + "] is not a node in tab " + a.tab_id + ": " + a.node_ids[i]);
    });
    const b = boundsFromNodes(members);
    if (!b) throw new Error("members carry no x/y coordinates");
    const group = { id: newId(), type: "group", z: a.tab_id, name: a.name || "Group", style: {}, nodes: a.node_ids.slice(), x: b.x, y: b.y, w: b.w, h: b.h };
    next.push(group);
    detachFromCurrent(a.node_ids, group.id);
    setG(members.map(function (m) { return m.id; }), group.id);
    result = { created: group };
  } else {
    if (!a.group_id) throw new Error("group_id required for op " + a.op);
    const gi = next.findIndex(function (n) { return n.id === a.group_id && n.type === "group"; });
    if (gi < 0) throw new Error("group not found: " + a.group_id);
    const g = next[gi];
    if (a.op === "rename") {
      next[gi] = Object.assign({}, g, { name: a.name || g.name });
      result = { renamed: a.group_id, name: next[gi].name };
    } else if (a.op === "add" || a.op === "remove") {
      if (!Array.isArray(a.node_ids) || !a.node_ids.length) throw new Error("node_ids required for op " + a.op);
      a.node_ids.forEach(function (id, i) {
        const m = byId.get(id);
        if (!m || m.type === "group" || m.type === "tab") throw new Error("node_ids[" + i + "] is not a flow node: " + id);
        if (a.op === "add" && m.z !== g.z) throw new Error("node " + id + " is in another tab (" + m.z + "), group is in " + g.z);
      });
      let members = (g.nodes || []).slice();
      if (a.op === "add") {
        a.node_ids.forEach(function (id) { if (members.indexOf(id) < 0) members.push(id); });
        detachFromCurrent(a.node_ids, g.id);
      } else {
        members = members.filter(function (id) { return a.node_ids.indexOf(id) < 0; });
      }
      next[gi] = Object.assign({}, g, { nodes: members });
      setG(a.node_ids, a.op === "add" ? g.id : null);
      if (a.refit !== false) next = refitGroups(next, new Set([g.id]));
      result = { group_id: g.id, members: members.length };
    } else if (a.op === "refit") {
      next = refitGroups(next, new Set([g.id]));
      const ng = next.find(function (n) { return n.id === g.id; });
      result = { group_id: g.id, bounds: { x: ng.x, y: ng.y, w: ng.w, h: ng.h } };
    } else if (a.op === "delete") {
      const members = (g.nodes || []).slice();
      next = next.filter(function (n) { return n.id !== g.id; });
      setG(members, null);
      result = { deleted: g.id, nodes_kept: members.length };
    }
  }
  const val = validateFlows(next);
  if (a.dry_run) return { content: [{ type: "text", text: txt({ dry_run: true, op: a.op, result: result, validation: val }) }] };
  if (!a.confirm) throw new Error("pass confirm:true to apply");
  const res = await deployFlows(next, { previous: flows, allowOverlap: a.allow_overlap });
  return { content: [{ type: "text", text: txt(Object.assign({ op: a.op, result: result, validation: val }, res)) }] };
});

server.tool("import_nodes", "Import a set of nodes (as JSON objects, editor-clipboard style) into a tab: id remapping (default), wires kept inside the set (external ends dropped with a warning), referenced config nodes reused when an identical one already exists, free-region placement (or explicit x/y). dry_run previews the id map.", {
  nodes: z.array(z.record(z.unknown())).min(1), tab_id: z.string(),
  x: z.number().optional(), y: z.number().optional(), remap_ids: z.boolean().optional(),
  confirm: z.boolean().optional(), dry_run: z.boolean().optional(), allow_overlap: z.boolean().optional()
}, async (a) => {
  if (READ_ONLY) throw new Error(readOnlyError());
  const flows = await fetchFlows();
  const byId = buildIndex(flows);
  const tab = byId.get(a.tab_id);
  if (!tab || (tab.type !== "tab" && tab.type !== "subflow")) throw new Error("tab_id must be an existing tab/subflow: " + a.tab_id);
  const remap = a.remap_ids !== false;
  const entries = a.nodes.filter(function (n) { return n && typeof n === "object" && n.type && n.type !== "tab" && n.type !== "group"; });
  if (!entries.length) throw new Error("no importable node objects found (tab/group entries cannot be imported; subflow definitions can)");
  const idMap = new Map();
  for (const n of entries) {
    if (!n.id) throw new Error("import entry missing id (type " + n.type + ")");
    if (remap) idMap.set(n.id, newId());
    else {
      if (idMap.has(n.id)) throw new Error("duplicate id inside the import set: " + n.id);
      if (byId.has(n.id)) throw new Error("id already exists on this instance (pass remap_ids:true): " + n.id);
      idMap.set(n.id, n.id);
    }
  }
  // config dedupe: an identical config node already on the instance is REUSED, not cloned
  const skipIds = new Set(); const cfgMap = new Map();
  for (const n of entries) {
    if (n.type === "subflow") continue;
    if (n.z !== undefined && n.z !== null && !isConfigType(n.type)) continue; // regular nodes always cloned
    const sig = configSignature(n);
    const existing = flows.find(function (f) { return isConfigCandidate(f) && configSignature(f) === sig; });
    if (existing) { skipIds.add(n.id); cfgMap.set(n.id, existing.id); }
  }
  const mapId = function (id) {
    if (cfgMap.has(id)) return cfgMap.get(id);
    return idMap.has(id) ? idMap.get(id) : null;
  };
  const dropped = [];
  const made = [];
  for (const n of entries) {
    if (skipIds.has(n.id)) continue;
    const c = JSON.parse(JSON.stringify(n));
    c.id = mapId(n.id);
    if (c.type === "subflow") { delete c.z; delete c.g; }
    else {
      if (String(c.type).indexOf("subflow:") === 0) {
        const d = String(c.type).slice(9);
        if (idMap.has(d)) c.type = "subflow:" + idMap.get(d);
      }
      c.z = a.tab_id;
      if (c.g) { if (idMap.has(c.g)) c.g = idMap.get(c.g); else delete c.g; }
    }
    if (Array.isArray(c.wires)) {
      c.wires = c.wires.map(function (w) {
        return (w || []).map(function (t) {
          const m = mapId(t);
          if (m) return m;
          dropped.push(String(t));
          return null;
        }).filter(Boolean);
      });
    }
    if (Array.isArray(c.links)) c.links = c.links.map(function (l) { return mapId(l); }).filter(Boolean);
    made.push(c);
  }
  // placement: translate the bbox of positioned nodes to a free region
  const withPos = made.filter(function (m) { return m.type !== "subflow" && typeof m.x === "number" && typeof m.y === "number"; });
  if (withPos.length) {
    const rects = withPos.map(nodeRect).filter(Boolean);
    const minX = Math.min.apply(null, rects.map(function (r) { return r.x; }));
    const minY = Math.min.apply(null, rects.map(function (r) { return r.y; }));
    const maxX = Math.max.apply(null, rects.map(function (r) { return r.x + r.w; }));
    const maxY = Math.max.apply(null, rects.map(function (r) { return r.y + r.h; }));
    const bw = maxX - minX, bh = maxY - minY;
    let tlx, tly;
    if (typeof a.x === "number" && typeof a.y === "number") { tlx = a.x; tly = a.y; }
    else { const c = freeRegion(flows, a.tab_id, bw, bh); tlx = c.x - bw / 2; tly = c.y - bh / 2; }
    const dx = Math.round(tlx - minX), dy = Math.round(tly - minY);
    if (dx || dy) withPos.forEach(function (m) { m.x += dx; m.y += dy; });
  } else {
    let base = freeRegion(flows, a.tab_id, NODE_DEF_W, NODE_DEF_H);
    made.forEach(function (m) {
      if (m.type === "subflow") return;
      m.x = base.x; m.y = base.y; base = { x: base.x, y: base.y + 40 };
    });
  }
  const next = flows.concat(made);
  const val = validateFlows(next);
  const summary = {
    imported: made.length,
    reused_configs: cfgMap.size ? Array.from(cfgMap.values()) : 0,
    dropped_external_wires: dropped.length ? dropped : 0,
    id_map: Array.from(idMap.entries()).reduce(function (o, kv) { o[kv[0]] = kv[1]; return o; }, {})
  };
  if (a.dry_run) return { content: [{ type: "text", text: txt(Object.assign({ dry_run: true }, summary, { validation: val })) }] };
  if (!a.confirm) throw new Error("pass confirm:true to apply");
  const res = await deployFlows(next, { previous: flows, allowOverlap: a.allow_overlap });
  return { content: [{ type: "text", text: txt(Object.assign(summary, { validation: val }, res)) }] };
});

server.tool("export_nodes", "Export a set of nodes as an editor-clipboard-style JSON array: selected nodes + (by default) the config nodes they reference and any subflow definitions they use. Output feeds import_nodes or the editor import dialog. Read-only.", {
  node_ids: z.array(z.string()).min(1), include_configs: z.boolean().optional(), include_subflows: z.boolean().optional()
}, async (a) => {
  const flows = await fetchFlows();
  const byId = buildIndex(flows);
  const missing = a.node_ids.filter(function (id) { return !byId.has(id); });
  if (missing.length) throw new Error("ids not found: " + missing.join(", "));
  const out = collectExportSet(flows, a.node_ids, a.include_configs !== false, a.include_subflows !== false);
  return { content: [{ type: "text", text: txt({ count: out.length, nodes: out }) }] };
});

server.tool("layout_audit", "Canvas health report per tab: group-vs-group overlaps (exact, error), nodes escaped from their group (exact, error), stray nodes inside groups they do not belong to and estimated node overlaps (warnings). Tabs ranked worst-first; tall tabs flagged. Read-only.", {
  tab_id: z.string().optional()
}, async ({ tab_id }) => {
  const flows = await fetchFlows();
  const byId = buildIndex(flows);
  const byTab = layoutFindings(flows);
  let tabs = flows.filter(function (n) { return n.type === "tab"; });
  if (tab_id) tabs = tabs.filter(function (t) { return t.id === tab_id; });
  const rows = tabs.map(function (t) {
    const f = byTab[t.id] || { errors: [], warnings: [] };
    const inTab = flows.filter(function (n) { return n.z === t.id; });
    const rects = inTab.map(function (n) { return n.type === "group" ? groupRect(n) : nodeRect(n); }).filter(Boolean);
    const maxY = rects.length ? Math.max.apply(null, rects.map(function (r) { return r.y + r.h; })) : 0;
    return {
      id: t.id, label: t.label || t.id, nodes: inTab.filter(function (n) { return n.type !== "group"; }).length, groups: inTab.filter(function (n) { return n.type === "group"; }).length,
      extent_y: maxY, tall_tab: maxY > 3000 ? maxY : undefined,
      errors: f.errors.length ? f.errors : undefined, warnings: f.warnings.length ? f.warnings : undefined
    };
  });
  rows.sort(function (x, y2) { return (y2.errors ? y2.errors.length : 0) - (x.errors ? x.errors.length : 0) || (y2.warnings ? y2.warnings.length : 0) - (x.warnings ? x.warnings.length : 0); });
  const totalE = rows.reduce(function (s, r) { return s + (r.errors ? r.errors.length : 0); }, 0);
  const totalW = rows.reduce(function (s, r) { return s + (r.warnings ? r.warnings.length : 0); }, 0);
  return { content: [{ type: "text", text: txt({ summary: { tabs: rows.length, errors: totalE, warnings: totalW }, tabs: rows }) }] };
});

server.tool("arrange_tab", "Repack the groups of one tab into balanced columns (default target aspect 1.6). Groups keep their relative order; ungrouped nodes are NEVER moved - the packed block drops below them if needed. Members and nested groups move together with their group. Default is a dry-run plan; apply:true + confirm:true executes. The layout gate still guards the write.", {
  tab_id: z.string(), apply: z.boolean().optional(), target_ratio: z.number().optional(), columns: z.number().int().optional(),
  confirm: z.boolean().optional(), allow_overlap: z.boolean().optional()
}, async (a) => {
  if (READ_ONLY) throw new Error(readOnlyError());
  const flows = await fetchFlows();
  const byId = buildIndex(flows);
  const tab = byId.get(a.tab_id);
  if (!tab || tab.type !== "tab") throw new Error("tab not found: " + a.tab_id);
  const all = flows.filter(function (n) { return n.z === a.tab_id; });
  const groups = all.filter(function (n) { return n.type === "group" && !n.g; });
  if (groups.length < 2) throw new Error("arrange_tab needs >= 2 top-level groups in the tab (found " + groups.length + ")");
  const GAP = 60;
  const sorted = groups.slice().sort(function (x, y2) { return (x.y || 0) - (y2.y || 0) || (x.x || 0) - (y2.x || 0); });
  const totalW = sorted.reduce(function (s, g) { return s + (g.w || 0); }, 0);
  const totalH = sorted.reduce(function (s, g) { return s + (g.h || 0); }, 0);
  const ratio = a.target_ratio || 1.6;
  let ncols = a.columns || 1;
  if (!a.columns) {
    const area = (totalW + GAP * sorted.length) * (totalH + GAP * sorted.length);
    const canvasW = Math.sqrt(area * ratio);
    ncols = Math.max(1, Math.min(sorted.length, Math.round(canvasW / Math.max(1, totalW / sorted.length + GAP))));
  }
  ncols = Math.max(1, Math.min(sorted.length, ncols));
  const cols = [];
  for (let i = 0; i < ncols; i++) cols.push({ h: 0, w: 0 });
  const placement = [];
  sorted.forEach(function (g) {
    let ci = 0;
    for (let i = 1; i < ncols; i++) if (cols[i].h < cols[ci].h) ci = i;
    cols[ci].h += (g.h || 0) + GAP;
    cols[ci].w = Math.max(cols[ci].w, g.w || 0);
    placement.push({ g: g, ci: ci });
  });
  let x = 40;
  const colX = [];
  for (let i = 0; i < ncols; i++) { colX[i] = x; x += cols[i].w + GAP; }
  const colH = new Array(ncols).fill(0);
  placement.forEach(function (p) { colH[p.ci] += (p.g.h || 0) + GAP; });
  const blockW = colX[ncols - 1] + cols[ncols - 1].w - 40;
  const blockH = Math.max.apply(null, colH) - GAP;
  // never move ungrouped nodes: drop the whole block below them if they collide
  const unRects = all.filter(function (n) { return n.type !== "group" && !n.g; }).map(nodeRect).filter(Boolean);
  let startY = 40, guard = 0;
  while (guard++ < 200 && unRects.some(function (r) { return rectsHit({ x: 40, y: startY, w: blockW, h: blockH }, r, 20); })) {
    const hit2 = unRects.filter(function (r) { return rectsHit({ x: 40, y: startY, w: blockW, h: blockH }, r, 20); });
    startY = Math.max.apply(null, hit2.map(function (r) { return r.y + r.h; })) + GAP;
  }
  const cy = new Array(ncols).fill(startY);
  const moves = [];
  placement.forEach(function (p) {
    const nx = colX[p.ci], ny = cy[p.ci];
    cy[p.ci] += (p.g.h || 0) + GAP;
    moves.push({ id: p.g.id, name: displayName(p.g), from: { x: p.g.x, y: p.g.y }, to: { x: Math.round(nx), y: Math.round(ny) }, dx: Math.round(nx - (p.g.x || 0)), dy: Math.round(ny - (p.g.y || 0)) });
  });
  const plan = { columns: ncols, footprint: { w: Math.round(blockW), h: Math.round(blockH) }, moves: moves };
  if (!a.apply) return { content: [{ type: "text", text: txt(Object.assign({ dry_run: true, tab: a.tab_id }, plan, { note: "pass apply:true + confirm:true to repack" })) }] };
  if (!a.confirm) throw new Error("pass confirm:true to apply the plan");
  // move each group AND its descendants (members + nested groups) by the same delta
  const deltas = new Map();
  moves.forEach(function (m) { deltas.set(m.id, { dx: m.dx, dy: m.dy }); });
  const next = flows.map(function (n) {
    if (deltas.has(n.id)) { const d = deltas.get(n.id); return Object.assign({}, n, { x: (n.x || 0) + d.dx, y: (n.y || 0) + d.dy }); }
    if (n.g && deltas.has(n.g)) { const d = deltas.get(n.g); return Object.assign({}, n, { x: (n.x || 0) + d.dx, y: (n.y || 0) + d.dy }); }
    // nested group inside a moved group
    let p = n.g ? byId.get(n.g) : null;
    let hops = 0;
    while (p && hops < 5) {
      if (deltas.has(p.id)) { const d = deltas.get(p.id); return Object.assign({}, n, { x: (n.x || 0) + d.dx, y: (n.y || 0) + d.dy }); }
      p = p.g ? byId.get(p.g) : null; hops++;
    }
    return n;
  });
  const val = validateFlows(next);
  const res = await deployFlows(next, { previous: flows, allowOverlap: a.allow_overlap });
  return { content: [{ type: "text", text: txt(Object.assign({ arranged: moves.length, plan: plan, validation: val }, res)) }] };
});

server.tool("decouple_tab", "Replace wires that CROSS group boundaries with link out / link in pairs (one pair per crossing, named after the far end) so each group becomes independently movable. Scope: one tab (tab_id) or one group (group_id). Default is a dry-run listing of the crossings; apply:true + confirm:true inserts the link nodes and refits both groups.", {
  tab_id: z.string().optional(), group_id: z.string().optional(), apply: z.boolean().optional(),
  confirm: z.boolean().optional(), allow_overlap: z.boolean().optional()
}, async (a) => {
  if (READ_ONLY) throw new Error(readOnlyError());
  const flows = await fetchFlows();
  const byId = buildIndex(flows);
  let scope;
  if (a.group_id) {
    const g = byId.get(a.group_id);
    if (!g || g.type !== "group") throw new Error("group not found: " + a.group_id);
    scope = flows.filter(function (n) { return n.z === g.z; });
  } else if (a.tab_id) {
    if (!byId.has(a.tab_id)) throw new Error("tab not found: " + a.tab_id);
    scope = flows.filter(function (n) { return n.z === a.tab_id; });
  } else throw new Error("tab_id or group_id required");
  const crossings = [];
  scope.forEach(function (s) {
    if (s.type === "group" || !Array.isArray(s.wires)) return;
    s.wires.forEach(function (w, oi) {
      (w || []).forEach(function (t) {
        const tgt = byId.get(t);
        if (!tgt || tgt.type === "group") return;
        if (!s.g || !tgt.g || s.g === tgt.g) return;
        const sg = byId.get(s.g), tg = byId.get(tgt.g);
        if (!sg || !tg || sg.type !== "group" || tg.type !== "group") return;
        if (a.group_id && s.g !== a.group_id && tgt.g !== a.group_id) return;
        crossings.push({ src: s, out: oi, tgt: tgt, sg: sg, tg: tg });
      });
    });
  });
  if (!crossings.length) return { content: [{ type: "text", text: txt({ crossings: 0, note: "no wires cross group boundaries in this scope - nothing to decouple" }) }] };
  if (!a.apply) {
    return { content: [{ type: "text", text: txt({
      crossings: crossings.map(function (c) {
        return { from: displayName(c.src) + " (" + shortId(c.src.id) + ")", to: displayName(c.tgt) + " (" + shortId(c.tgt.id) + ")", src_group: displayName(c.sg), tgt_group: displayName(c.tg) };
      }),
      note: "dry run - pass apply:true + confirm:true to insert one link in/out pair per crossing"
    }) }] };
  }
  if (!a.confirm) throw new Error("pass confirm:true to apply");
  const additions = [];
  const replaceWire = []; // each crossing: source output -> link out (same group), link in (target group) -> original target
  const addMap = new Map(); // groupId -> new member ids (the link nodes)
  crossings.forEach(function (c) {
    const inId = newId(), outId = newId();
    additions.push({ id: outId, type: "link out", z: c.src.z, name: "-> " + displayName(c.tgt), links: [inId], x: Math.round(c.sg.x + c.sg.w - 30), y: c.src.y, g: c.sg.id, wires: [] });
    additions.push({ id: inId, type: "link in", z: c.tgt.z, name: displayName(c.src) + " ->", links: [outId], x: Math.round(c.tg.x + 30), y: c.tgt.y, g: c.tg.id, wires: [[c.tgt.id]] });
    replaceWire.push({ srcId: c.src.id, out: c.out, tgtId: c.tgt.id, outNodeId: outId });
    if (!addMap.has(c.sg.id)) addMap.set(c.sg.id, []);
    addMap.get(c.sg.id).push(outId);
    if (!addMap.has(c.tg.id)) addMap.set(c.tg.id, []);
    addMap.get(c.tg.id).push(inId);
  });
  let next = flows.map(function (m) {
    const rws = replaceWire.filter(function (r) { return r.srcId === m.id; });
    if (!rws.length) return m;
    const c = Object.assign({}, m);
    c.wires = m.wires.map(function (w, i) {
      const hits = rws.filter(function (r) { return r.out === i; });
      if (!hits.length) return w;
      const swap = new Map(hits.map(function (h) { return [h.tgtId, h.outNodeId]; }));
      return (w || []).map(function (t) { return swap.has(t) ? swap.get(t) : t; });
    });
    return c;
  });
  additions.forEach(function (x2) { next.push(x2); });
  // register the new link nodes as group members (editor consistency)
  next = next.map(function (m) {
    if (m.type === "group" && addMap.has(m.id)) {
      const ids = (m.nodes || []).slice();
      addMap.get(m.id).forEach(function (id) { if (ids.indexOf(id) < 0) ids.push(id); });
      return Object.assign({}, m, { nodes: ids });
    }
    return m;
  });
  const gIds = new Set();
  crossings.forEach(function (c) { gIds.add(c.sg.id); gIds.add(c.tg.id); });
  next = refitGroups(next, gIds);
  const val = validateFlows(next);
  const res = await deployFlows(next, { previous: flows, allowOverlap: a.allow_overlap });
  return { content: [{ type: "text", text: txt(Object.assign({ decoupled: crossings.length, link_pairs: crossings.length, validation: val }, res)) }] };
});

server.tool("audit_flows", "Flow hygiene audit (read-only): unused config nodes, empty tabs, disabled tabs/nodes, orphan nodes (no wiring at all), dangling outputs (non-terminal nodes wired to nothing), cross-tab direct wires (candidates for link nodes / decouple_tab), duplicate type+name pairs, nodes with unknown z. Empty sections are omitted.", {}, async () => {
  const flows = await fetchFlows();
  return { content: [{ type: "text", text: txt(auditReport(flows)) }] };
});

server.tool("call_endpoint", "GET an HTTP endpoint served by Node-RED flows (e.g. custom diagnostic nodes). Path must start with /.", {
  path: z.string(),
}, async ({ path: p }) => {
  if (!p.startsWith("/")) throw new Error("path must start with /");
  const body = await apiGet(p);
  return { content: [{ type: "text", text: txt(body) }] };
});

server.tool("list_backups", "List automatic pre-deploy backups available for restore", {}, async () => {
  return { content: [{ type: "text", text: txt(listBackupFiles()) }] };
});

server.tool("restore_backup", "Restore a backup file (name from list_backups). Requires confirm:true. This is a deliberate rollback: it skips the revision guard by design.", {
  file: z.string(),
  confirm: z.boolean()
}, async ({ file, confirm }) => {
  if (READ_ONLY) throw new Error(readOnlyError());
  if (!confirm) throw new Error("pass confirm:true to restore");
  const safe = path.basename(file);
  const full = path.join(BACKUP_DIR, safe);
  if (!fs.existsSync(full)) throw new Error("backup not found: " + safe);
  const flows = JSON.parse(fs.readFileSync(full, "utf8"));
  const res = await deployFlows(flows, { force: true });
  return { content: [{ type: "text", text: txt(Object.assign({ restored: safe }, res)) }] };
});

server.tool("list_node_types", "List installed palette modules and node types", {}, async () => {
  const nodes = await apiGetJson("/nodes");
  return { content: [{ type: "text", text: txt(nodes.map(m => ({ module: m.module, version: m.version, types: m.types, local: m.local, disabled: m.disabled }))) }] };
});

server.tool("get_errors", "Live runtime errors and warnings from the Node-RED editor comms channel: node statuses with red/yellow fill (e.g. 'Call-service error...', rejected commands) and runtime error notifications, with node name/type/tab resolved. The monitor connects on first call and keeps a rolling in-memory buffer (reconnects automatically); events are captured from the moment it first connects - there is no history before that. include_warnings:false hides yellow; tab_id filters one tab; clear:true empties the buffer after reading. include_last_status:true adds nodes_in_error_now (nodes whose latest status is red) and last_status_seen (latest status per node seen by the monitor - Node-RED has no status replay: silent nodes show nothing until they emit again).", {
  limit: z.number().int().min(1).max(200).optional(),
  tab_id: z.string().optional(),
  include_warnings: z.boolean().optional(),
  clear: z.boolean().optional(),
  include_last_status: z.boolean().optional()
}, async ({ limit, tab_id, include_warnings, clear, include_last_status }) => {
  await ensureMonitor();
  const names = await nodeNameMap();
  const wanted = limit || 30;
  const entries = [];
  const reversed = monitor.buffer.slice().reverse();
  for (const e of reversed) {
    if (entries.length >= wanted) break;
    const isWarn = (e.kind === "status" && e.fill === "yellow") || (e.kind === "notification" && e.level === "warning");
    if (include_warnings === false && isWarn) continue;
    const info = e.node_id ? names.map.get(e.node_id) : null;
    if (tab_id && (!info || (info.tab_id !== tab_id && info.tab_label !== tab_id))) continue;
    entries.push({
      time: new Date(e.ts).toISOString(),
      kind: e.kind,
      node_id: e.node_id || undefined,
      node: info ? info.name : undefined,
      node_type: info ? info.type : undefined,
      tab: info ? info.tab_label : undefined,
      fill: e.fill || undefined,
      level: e.level || undefined,
      still_current: e.kind === "status" ? ((monitor.lastStatus.get(e.node_id) || {}).fill) === e.fill : undefined,
      text: e.text
    });
  }
  if (clear) monitor.buffer.length = 0;
  const out = {
    monitor: { state: monitor.state, connected_since: monitor.since ? new Date(monitor.since).toISOString() : null, captured_events: monitor.buffer.length },
    errors: entries
  };
  if (include_last_status) {
    const seen = [];
    const nowRed = [];
    for (const [id, st] of monitor.lastStatus.entries()) {
      const info = names.map.get(id);
      if (tab_id && (!info || (info.tab_id !== tab_id && info.tab_label !== tab_id))) continue;
      const row = { node_id: id, node: info ? info.name : undefined, node_type: info ? info.type : undefined, tab: info ? info.tab_label : undefined, fill: st.fill, shape: st.shape || undefined, text: st.text, seen_at: new Date(st.ts).toISOString() };
      seen.push(row);
      if (st.fill === "red") nowRed.push(row);
    }
    seen.sort(function (a, b) { return a.seen_at < b.seen_at ? 1 : -1; });
    out.nodes_in_error_now = nowRed;
    out.last_status_seen = seen.slice(0, 50);
  }
  return { content: [{ type: "text", text: txt(out) }] };
});

await server.connect(new StdioServerTransport());
log("ready: 28 tools (v0.5.0)");
