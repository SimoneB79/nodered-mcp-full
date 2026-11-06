#!/usr/bin/env node
/**
 * nodered-mcp — MCP server for the Node-RED Admin API
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

// ---------------- environment ----------------
const URL_BASE = (process.env.NODE_RED_URL || "http://localhost:1880").replace(/\/+$/, "");
const TOKEN = process.env.NODE_RED_TOKEN || "";
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

// ---------------- http client ----------------
function request(method, urlPath, body, extraHeaders) {
  return new Promise((resolve, reject) => {
    let u;
    try { u = new URL(URL_BASE + urlPath); }
    catch (e) { reject(new Error("invalid path or base URL")); return; }
    const mod = u.protocol === "https:" ? https : http;
    const headers = Object.assign({}, EXTRA_HEADERS, extraHeaders || {}, { Accept: "application/json" });
    if (TOKEN) headers.Authorization = "Bearer " + TOKEN;
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
      let data = "";
      res.setEncoding("utf8");
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
  const r = await request("GET", p);
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
  const r = await request("POST", "/flows", flows, { "Node-RED-Deployment-Type": "flows" });
  if (r.status !== 204 && r.status !== 200) {
    throw new Error("deploy failed HTTP " + r.status + " (backup kept: " + backup + "): " + r.body.slice(0, 400));
  }
  return { ok: true, http: r.status, backup: backup, nodes: Array.isArray(flows) ? flows.length : "n/a" };
}

function readOnlyError() { return "NODE_RED_READ_ONLY=true: write tools are disabled on this instance."; }

// ---------------- server ----------------
const server = new McpServer({ name: "nodered-mcp", version: "0.1.0" });
const log = (msg) => console.error("[nodered-mcp] " + msg);
log("target " + URL_BASE + (TOKEN ? " (token)" : "") + (READ_ONLY ? " READ-ONLY" : ""));

server.tool("get_settings", "Node-RED runtime settings (version, palette editability, contextStorage) — check limits before advanced work", {}, async () => {
  const s = await apiGetJson("/settings");
  return { content: [{ type: "text", text: txt({ version: s.version, paletteEditable: s.paletteEditable ?? null, contextStorage: s.contextStorage ?? null, httpAdminRoot: s.httpAdminRoot ?? null }) }] };
});

server.tool("list_flows", "List flow tabs with node counts and disabled flags", {}, async () => {
  const flows = await apiGetJson("/flows");
  const tabs = flows.filter(n => n.type === "tab");
  const out = tabs.map(t => {
    const nodes = flows.filter(n => n.z === t.id);
    return { id: t.id, label: t.label, disabled: t.disabled || false, nodes: nodes.length, subflows: nodes.filter(n => n.type === "subflow").length };
  });
  return { content: [{ type: "text", text: txt(out) }] };
});

server.tool("get_flow", "Get nodes of one flow tab (by tab id) or the whole flow document", { tab_id: z.string().optional(), full: z.boolean().optional() }, async ({ tab_id, full }) => {
  const flows = await apiGetJson("/flows");
  if (full || !tab_id) return { content: [{ type: "text", text: txt(flows) }] };
  const nodes = flows.filter(n => n.z === tab_id);
  return { content: [{ type: "text", text: txt(nodes) }] };
});

server.tool("get_node", "Get a single node by id (exact config, function code, wiring)", { node_id: z.string() }, async ({ node_id }) => {
  const flows = await apiGetJson("/flows");
  const n = flows.find(x => x.id === node_id);
  if (!n) throw new Error("node not found: " + node_id);
  return { content: [{ type: "text", text: txt(n) }] };
});

server.tool("patch_node", "Patch one node by id (merge fields: func, wires, name, x, y, ...). Requires confirm:true. Takes automatic backup and deploys the full flow set.", {
  node_id: z.string(),
  patch: z.record(z.unknown()),
  confirm: z.boolean()
}, async ({ node_id, patch, confirm }) => {
  if (READ_ONLY) throw new Error(readOnlyError());
  if (!confirm) throw new Error("pass confirm:true to apply this patch");
  const flows = await apiGetJson("/flows");
  const i = flows.findIndex(x => x.id === node_id);
  if (i < 0) throw new Error("node not found: " + node_id);
  flows[i] = Object.assign({}, flows[i], patch);
  const res = await deployFlows(flows);
  return { content: [{ type: "text", text: txt(Object.assign({ patched: node_id, fields: Object.keys(patch) }, res)) }] };
});

server.tool("deploy_flows", "Deploy a FULL flow array (the only safe deploy type). Validates it is an array, takes automatic backup first. Requires confirm:true.", {
  flows: z.array(z.unknown()),
  confirm: z.boolean(),
  backup_label: z.string().optional()
}, async ({ flows, confirm, backup_label }) => {
  if (READ_ONLY) throw new Error(readOnlyError());
  if (!confirm) throw new Error("pass confirm:true to deploy");
  if (!Array.isArray(flows)) throw new Error("flows must be an array (the complete flow document)");
  const res = await deployFlows(flows);
  return { content: [{ type: "text", text: txt(res) }] };
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

await server.connect(new StdioServerTransport());
log("ready: 10 tools");
