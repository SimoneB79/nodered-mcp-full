#!/usr/bin/env node
/**
 * nodered-mcp-full - MCP server for the Node-RED Admin API
 *
 * v0.4.0 - get_errors: live runtime error monitor via the editor /comms websocket
 *          (red/yellow node statuses + error notifications, node/tab resolved)
 * v0.3.0 - AI-first flow comprehension layer:
 *   - get_flow_map: tab/subflow rendered as outline or Mermaid, wiring resolved to names
 *   - list_flows detail, search_nodes, list_config_nodes
 *   - surgical writes: patch_batch, upsert_node, delete_node (+splice) - the model
 *     sends only diffs, the server merges and deploys the full array
 *   - validation + dry_run diffs on every write tool
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
const monitor = { ws: null, state: "idle", since: null, buffer: [], lastFill: new Map(), reconnectMs: 4000, reconnectTimer: null, closedByUs: false };
let nodeNames = null; let nodeNamesAt = 0;

function monitorRecord(kind, entry) {
  monitor.buffer.push(Object.assign({ ts: Date.now(), kind }, entry));
  if (monitor.buffer.length > MONITOR_MAX) monitor.buffer.splice(0, monitor.buffer.length - MONITOR_MAX);
}

async function nodeNameMap() {
  if (nodeNames && Date.now() - nodeNamesAt < 60000) return nodeNames;
  try {
    const flows = await apiGetJson("/flows");
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
        if (d.fill) monitor.lastFill.set(id, d.fill);
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

// ---------------- deploy (full, the safe way) ----------------
async function deployFlows(flows) {
  const backup = await backupFlows("pre-deploy");
  let r = await request("POST", "/flows", flows, { "Node-RED-Deployment-Type": "flows" });
  if (r.status === 401 && AUTH_USER) { await getToken(true); r = await request("POST", "/flows", flows, { "Node-RED-Deployment-Type": "flows" }); }
  if (r.status !== 204 && r.status !== 200) {
    throw new Error("deploy failed HTTP " + r.status + " (backup kept: " + backup + "): " + r.body.slice(0, 400));
  }
  return { ok: true, http: r.status, backup: backup, nodes: Array.isArray(flows) ? flows.length : "n/a" };
}

function readOnlyError() { return "NODE_RED_READ_ONLY=true: write tools are disabled on this instance."; }

// ---------------- flow comprehension helpers ----------------
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
  if (n.type === "tab" || n.type === "subflow") return n.label || shortId(n.id);
  return n.type + ":" + shortId(n.id);
}
function isConfigType(t) {
  return t === "mqtt-broker" || t === "server" || t === "websocket-listener" || t === "websocket-client" || String(t).indexOf("ui_") === 0;
}
const ENTRY_TYPES = new Set(["inject", "mqtt in", "http in", "server-state-changed", "link in", "cronplus", "bigtimer", "poll-state", "websocket in", "udp in", "tcp in", "serial in"]);
const PARAM_SKIP = new Set(["id", "type", "name", "x", "y", "z", "w", "h", "g", "wires", "credentials", "env", "info", "func", "template", "rules", "status", "label", "icon", "order"]);

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
  const nodes = flows.filter(function (n) { return n.z === containerId && n.type !== "tab"; });
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
  const disabledCount = nodes.filter(function (n) { return n.disabled; }).length;
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
    if (n.disabled) parts.push("[disabled]");
    const ps = paramLine(n, byId);
    if (ps.length) parts.push("| " + ps.join(" "));
    const outs = [];
    (n.wires || []).forEach(function (w, i) {
      const tg = (w || []).map(function (t) {
        const tn = byId.get(t);
        if (tn && ids.has(t)) return displayName(tn) + "(" + shortId(t) + ")";
        if (tn) return "[ext] " + displayName(tn) + " (" + (tn.type === "tab" ? tn.label || "tab" : tn.type) + ")";
        return "[missing:" + t + "]";
      });
      outs.push(((n.wires || []).length > 1 ? "[" + (i + 1) + "] " : "") + "-> " + (tg.join(", ") || "-"));
    });
    lines.push("  ".repeat(d) + parts.join(" ") + (outs.length ? " " + outs.join(" ") : ""));
  });
  return lines.join("\n");
}

function mermaidEscape(s) { return String(s).replace(/[\\"[\]"]/g, " ").replace(/[\r\n]+/g, " ").slice(0, 60); }
function mermaidTab(flows, byId, containerId) {
  const nodes = flows.filter(function (n) { return n.z === containerId && n.type !== "tab"; });
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
    if (n.disabled) label = label + " (disabled)";
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
  const nodes = flows.filter(function (n) { return n.z === tab.id && n.type !== "tab"; });
  const base = { id: tab.id, label: tab.label || tab.id, disabled: !!tab.disabled, nodes: nodes.length, disabled_nodes: nodes.filter(function (n) { return n.disabled; }).length };
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
    if (n.type === "tab") return;
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
    if (n.disabled) item.disabled = true;
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
    if (n && n.z && !ids.has(n.z)) warnings.push(String(n.id) + " z=" + n.z + " not a known tab/subflow id (ok for scoped config nodes)");
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

// ---------------- server ----------------
const server = new McpServer({ name: "nodered-mcp", version: "0.4.0" });
const log = (msg) => console.error("[nodered-mcp] " + msg);
log("target " + URL_BASE + (TOKEN ? " (token)" : "") + (READ_ONLY ? " READ-ONLY" : ""));

server.tool("get_settings", "Node-RED runtime settings (version, palette editability, contextStorage) - check limits before advanced work", {}, async () => {
  const s = await apiGetJson("/settings");
  return { content: [{ type: "text", text: txt({ version: s.version, paletteEditable: s.paletteEditable ?? null, contextStorage: s.contextStorage ?? null, httpAdminRoot: s.httpAdminRoot ?? null }) }] };
});

server.tool("list_flows", "List flow tabs with node counts. detail:true adds per-tab node-type histogram, entry points (injects + intervals, mqtt topics, http endpoints, HA entities), config refs and subflow instances - the one-call overview of the whole instance.", { detail: z.boolean().optional() }, async ({ detail }) => {
  const flows = await apiGetJson("/flows");
  const byId = buildIndex(flows);
  const tabs = flows.filter(function (n) { return n.type === "tab"; });
  const out = tabs.map(function (t) { return tabSummary(flows, byId, t, !!detail); });
  return { content: [{ type: "text", text: txt(out) }] };
});

server.tool("get_flow_map", "Render one tab or subflow as an AI-readable map - use this FIRST to understand a flow before editing. format=outline (default): indented node lines in execution order, wiring resolved to node names (-> targets), config nodes inlined, functions summarized as line counts. format=mermaid: flowchart diagram. format=json: raw node array.", { tab_id: z.string(), format: z.enum(["outline", "mermaid", "json"]).optional() }, async ({ tab_id, format }) => {
  const flows = await apiGetJson("/flows");
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
  const flows = await apiGetJson("/flows");
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
  const flows = await apiGetJson("/flows");
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
        if (t.type !== "tab" && t.type !== "subflow") refs[k] = t;
      }
    });
    out = { node: n, refs: refs };
  }
  return { content: [{ type: "text", text: txt(out) }] };
});

server.tool("search_nodes", "Find nodes by text across name, type, id, topic, entity, url AND function code (code matches returned separately with a snippet). Much cheaper than get_flow when hunting specific nodes. Optional type and tab_id filters; limit default 30.", {
  query: z.string(), type: z.string().optional(), tab_id: z.string().optional(), limit: z.number().int().optional()
}, async (a) => {
  const flows = await apiGetJson("/flows");
  return { content: [{ type: "text", text: txt(searchNodes(flows, a)) }] };
});

server.tool("list_config_nodes", "List config nodes (mqtt brokers, HA servers, dashboard/ui config) with usage counts - who references each one.", {}, async () => {
  const flows = await apiGetJson("/flows");
  const byId = buildIndex(flows);
  const cfgIds = new Set(flows.filter(function (n) {
    return n.type !== "tab" && n.type !== "subflow" && String(n.type).indexOf("subflow:") !== 0 && (n.z === undefined || n.z === null || isConfigType(n.type));
  }).map(function (n) { return n.id; }));
  const users = new Map(); cfgIds.forEach(function (id2) { users.set(id2, 0); });
  flows.forEach(function (n) {
    if (cfgIds.has(n.id)) return;
    Object.keys(n).forEach(function (k) {
      if (k === "wires" || k === "z" || k === "id") return;
      const v = n[k];
      if (typeof v === "string" && cfgIds.has(v)) users.set(v, users.get(v) + 1);
    });
  });
  const out = [];
  cfgIds.forEach(function (id2) { const n = byId.get(id2); out.push({ id: id2, type: n.type, name: n.name || null, users: users.get(id2) }); });
  out.sort(function (a, b) { return b.users - a.users; });
  return { content: [{ type: "text", text: txt(out) }] };
});

server.tool("patch_node", "Patch ONE node by id (merge fields: func, wires, name, x, y, ...). dry_run:true previews diff + validation without writing. Applying requires confirm:true. Automatic backup + full-array deploy server-side.", {
  node_id: z.string(), patch: z.record(z.unknown()), confirm: z.boolean().optional(), dry_run: z.boolean().optional()
}, async ({ node_id, patch, confirm, dry_run }) => {
  if (READ_ONLY) throw new Error(readOnlyError());
  const flows = await apiGetJson("/flows");
  const i = flows.findIndex(function (x) { return x.id === node_id; });
  if (i < 0) throw new Error("node not found: " + node_id);
  const merged = Object.assign({}, flows[i], patch);
  const next = flows.slice(); next[i] = merged;
  const val = validateFlows(next);
  if (dry_run) return { content: [{ type: "text", text: txt({ dry_run: true, diff: nodeDiff(flows[i], merged), validation: val }) }] };
  if (!confirm) throw new Error("pass confirm:true to apply this patch");
  const res = await deployFlows(next);
  return { content: [{ type: "text", text: txt(Object.assign({ patched: node_id, fields: Object.keys(patch), validation: val }, res)) }] };
});

server.tool("patch_batch", "Apply several node patches in ONE deploy: patches=[{node_id, patch}, ...]. Server merges into the full flow set, validates, backs up and deploys once - you never send the whole array. dry_run:true previews all diffs. Applying requires confirm:true.", {
  patches: z.array(z.object({ node_id: z.string(), patch: z.record(z.unknown()) })), confirm: z.boolean().optional(), dry_run: z.boolean().optional()
}, async ({ patches, confirm, dry_run }) => {
  if (READ_ONLY) throw new Error(readOnlyError());
  if (!Array.isArray(patches) || patches.length === 0) throw new Error("patches must be a non-empty array");
  const flows = await apiGetJson("/flows");
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
  const res = await deployFlows(next);
  return { content: [{ type: "text", text: txt(Object.assign({ patched: patches.length + " nodes", diffs: diffs, validation: val }, res)) }] };
});

server.tool("upsert_node", "Create a new node (auto id, z and position below the tab content) or update an existing one when node.id matches. Optional wire_from {node_id, output} appends this node to that output wire. dry_run:true previews. Applying requires confirm:true. Backup + full deploy server-side.", {
  node: z.record(z.unknown()), tab_id: z.string().optional(), wire_from: z.object({ node_id: z.string(), output: z.number().int().optional() }).optional(), confirm: z.boolean().optional(), dry_run: z.boolean().optional()
}, async ({ node, tab_id, wire_from, confirm, dry_run }) => {
  if (READ_ONLY) throw new Error(readOnlyError());
  if (!node || typeof node !== "object" || !node.type) throw new Error("node.type is required");
  const flows = await apiGetJson("/flows");
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
    const inTab = flows.filter(function (n) { return n.z === zid && typeof n.y === "number"; });
    const maxY = inTab.length ? Math.max.apply(null, inTab.map(function (n) { return n.y; })) : 100;
    theNode = Object.assign({ id: newId(), x: 100, y: maxY + 100, wires: [] }, node, { z: zid });
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
  const res = await deployFlows(next);
  return { content: [{ type: "text", text: txt(Object.assign({ action: action, node_id: theNode.id, validation: val }, res)) }] };
});

server.tool("delete_node", "Delete one node by id and clean every wire pointing to it. splice:true (single-output nodes only) rewires its inputs directly to its outputs - bypass semantics. dry_run:true previews. Applying requires confirm:true.", {
  node_id: z.string(), splice: z.boolean().optional(), confirm: z.boolean().optional(), dry_run: z.boolean().optional()
}, async ({ node_id, splice, confirm, dry_run }) => {
  if (READ_ONLY) throw new Error(readOnlyError());
  const flows = await apiGetJson("/flows");
  const n = flows.find(function (x) { return x.id === node_id; });
  if (!n) throw new Error("node not found: " + node_id);
  let next = flows.slice();
  if (splice) {
    if ((n.wires || []).length > 1) throw new Error("splice supports single-output nodes only");
    const outs = (n.wires && n.wires[0]) || [];
    next = next.map(function (m) {
      if (!Array.isArray(m.wires)) return m;
      const c = Object.assign({}, m);
      c.wires = m.wires.map(function (w) {
        const ix = (w || []).indexOf(node_id);
        if (ix < 0) return w;
        const nw = (w || []).slice();
        nw.splice.apply(nw, [ix, 1].concat(outs));
        return nw;
      });
      return c;
    });
  }
  next = removeNode(next, node_id);
  const val = validateFlows(next);
  if (dry_run) return { content: [{ type: "text", text: txt({ dry_run: true, deleting: n, validation: val }) }] };
  if (!confirm) throw new Error("pass confirm:true to delete");
  const res = await deployFlows(next);
  return { content: [{ type: "text", text: txt(Object.assign({ deleted: node_id, spliced: !!splice, validation: val }, res)) }] };
});

server.tool("deploy_flows", "Deploy a FULL flow array (the only safe deploy type). Validates it is an array, takes an automatic backup first. dry_run:true validates without deploying. Deploying requires confirm:true.", {
  flows: z.array(z.unknown()), confirm: z.boolean().optional(), backup_label: z.string().optional(), dry_run: z.boolean().optional()
}, async ({ flows, confirm, backup_label, dry_run }) => {
  if (READ_ONLY) throw new Error(readOnlyError());
  if (!Array.isArray(flows)) throw new Error("flows must be an array (the complete flow document)");
  const val = validateFlows(flows);
  if (dry_run) return { content: [{ type: "text", text: txt(Object.assign({ dry_run: true, nodes: flows.length }, val)) }] };
  if (!confirm) throw new Error("pass confirm:true to deploy");
  const res = await deployFlows(flows);
  return { content: [{ type: "text", text: txt(Object.assign({ validation: val }, res)) }] };
});

server.tool("validate_flows", "Validate a candidate full flow array WITHOUT deploying: duplicate ids, dangling wires, unknown z, legacy HA v3 fields on v6 contrib nodes, flow.get(key, \"file\") traps. Run it on any edit built by hand.", { flows: z.array(z.unknown()) }, async ({ flows }) => {
  return { content: [{ type: "text", text: txt(validateFlows(flows)) }] };
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

server.tool("restore_backup", "Restore a backup file (name from list_backups). Requires confirm:true.", {
  file: z.string(),
  confirm: z.boolean()
}, async ({ file, confirm }) => {
  if (READ_ONLY) throw new Error(readOnlyError());
  if (!confirm) throw new Error("pass confirm:true to restore");
  const safe = path.basename(file);
  const full = path.join(BACKUP_DIR, safe);
  if (!fs.existsSync(full)) throw new Error("backup not found: " + safe);
  const flows = JSON.parse(fs.readFileSync(full, "utf8"));
  const res = await deployFlows(flows);
  return { content: [{ type: "text", text: txt(Object.assign({ restored: safe }, res)) }] };
});

server.tool("list_node_types", "List installed palette modules and node types", {}, async () => {
  const nodes = await apiGetJson("/nodes");
  return { content: [{ type: "text", text: txt(nodes.map(m => ({ module: m.module, version: m.version, types: m.types, local: m.local, disabled: m.disabled }))) }] };
});

server.tool("get_errors", "Live runtime errors and warnings from the Node-RED editor comms channel: node statuses with red/yellow fill (e.g. 'Call-service error...', rejected commands) and runtime error notifications, with node name/type/tab resolved. The monitor connects on first call and keeps a rolling in-memory buffer (reconnects automatically); events are captured from the moment it first connects - there is no history before that. include_warnings:false hides yellow; tab_id filters one tab; clear:true empties the buffer after reading.", {
  limit: z.number().int().min(1).max(200).optional(),
  tab_id: z.string().optional(),
  include_warnings: z.boolean().optional(),
  clear: z.boolean().optional()
}, async ({ limit, tab_id, include_warnings, clear }) => {
  await ensureMonitor();
  const names = await nodeNameMap();
  const wanted = limit || 30;
  let seen = 0;
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
      still_current: e.kind === "status" ? monitor.lastFill.get(e.node_id) === e.fill : undefined,
      text: e.text
    });
  }
  if (clear) monitor.buffer.length = 0;
  const out = {
    monitor: { state: monitor.state, connected_since: monitor.since ? new Date(monitor.since).toISOString() : null, captured_events: monitor.buffer.length },
    errors: entries
  };
  return { content: [{ type: "text", text: txt(out) }] };
});

await server.connect(new StdioServerTransport());
log("ready: 18 tools (v0.4.0)");
