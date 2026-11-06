// End-to-end test: spawns the MCP server against a mock Node-RED Admin API.
// No external dependencies: run with "npm test".
import http from "node:http";
import { spawn } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

const PORT = 18899;
const BACKUP_DIR = fs.mkdtempSync(path.join(os.tmpdir(), "nr-mcp-test-"));

// ---- mock Node-RED ----
const flows = [
  { id: "tab1", type: "tab", label: "Demo" },
  { id: "n1", type: "inject", z: "tab1", name: "tick" },
  { id: "n2", type: "debug", z: "tab1", name: "log" }
];
let deployed = 0;
const srv = http.createServer((req, res) => {
  const url = req.url.split("?")[0];
  if (req.method === "GET" && url === "/flows") { res.setHeader("content-type", "application/json"); res.end(JSON.stringify(flows)); }
  else if (req.method === "POST" && url === "/flows") {
    let b = ""; req.on("data", c => b += c);
    req.on("end", () => {
      deployed++;
      const ok = req.headers["node-red-deployment-type"] === "flows";
      try { flows.length = 0; flows.push(...JSON.parse(b)); } catch {}
      res.statusCode = ok ? 204 : 400; res.end();
    });
  }
  else if (req.method === "GET" && url === "/settings") { res.end(JSON.stringify({ version: "4.0.0-mock", httpAdminRoot: "/" })); }
  else if (req.method === "GET" && url === "/nodes") { res.end(JSON.stringify([{ module: "node-red", version: "4.0.0", types: ["inject"], local: false, enabled: true }])); }
  else if (req.method === "GET" && url === "/diag") { res.end("diag-ok"); }
  else { res.statusCode = 404; res.end("{}"); }
});
await new Promise(r => srv.listen(PORT, r));

// ---- spawn MCP server ----
const child = spawn(process.execPath, ["index.js"], {
  env: { ...process.env, NODE_RED_URL: "http://127.0.0.1:" + PORT, NODE_RED_BACKUP_DIR: BACKUP_DIR },
  stdio: ["pipe", "pipe", "inherit"]
});
let buf = "";
const pending = new Map();
let nextId = 1;
child.stdout.on("data", d => {
  buf += d;
  let i;
  while ((i = buf.indexOf("\n")) >= 0) {
    const line = buf.slice(0, i).trim(); buf = buf.slice(i + 1);
    if (!line) continue;
    try { const m = JSON.parse(line); if (m.id && pending.has(m.id)) { pending.get(m.id)(m); pending.delete(m.id); } } catch {}
  }
});
const call = (method, params) => new Promise(res => { const id = nextId++; pending.set(id, res); child.stdin.write(JSON.stringify({ jsonrpc: "2.0", id, method, params }) + "\n"); });

let failures = 0;
const check = (name, cond) => { console.log((cond ? "PASS" : "FAIL") + " - " + name); if (!cond) failures++; };

try {
  const init = await call("initialize", { protocolVersion: "2024-11-05", capabilities: {}, clientInfo: { name: "t", version: "0" } });
  check("initialize", init.result.serverInfo.name === "nodered-mcp");
  child.stdin.write(JSON.stringify({ jsonrpc: "2.0", method: "notifications/initialized" }) + "\n");

  const tools = await call("tools/list", {});
  check("10 tools", tools.result.tools.length === 10);

  const lf = await call("tools/call", { name: "list_flows", arguments: {} });
  const tabs = JSON.parse(lf.result.content[0].text);
  check("list_flows sees Demo tab", Array.isArray(tabs) && tabs[0].label === "Demo" && tabs[0].nodes === 2);

  const st = await call("tools/call", { name: "get_settings", arguments: {} });
  check("get_settings", JSON.parse(st.result.content[0].text).version === "4.0.0-mock");

  const ce = await call("tools/call", { name: "call_endpoint", arguments: { path: "/diag" } });
  check("call_endpoint", ce.result.content[0].text.includes("diag-ok"));

  const nc = await call("tools/call", { name: "deploy_flows", arguments: { flows: [], confirm: false } });
  check("deploy without confirm rejected", nc.result.isError === true);

  const dc = await call("tools/call", { name: "deploy_flows", arguments: { flows: [{ id: "tab9", type: "tab", label: "X" }], confirm: true } });
  check("deploy with confirm ok", JSON.parse(dc.result.content[0].text).ok === true && deployed === 1);

  const backups = fs.readdirSync(BACKUP_DIR).filter(f => f.endsWith(".json"));
  check("backup created", backups.length >= 1);

  const lb = await call("tools/call", { name: "list_backups", arguments: {} });
  check("list_backups", JSON.parse(lb.result.content[0].text).length >= 1);

  const rb = await call("tools/call", { name: "restore_backup", arguments: { file: backups[0], confirm: true } });
  check("restore_backup", JSON.parse(rb.result.content[0].text).ok === true && deployed === 2);

  const nd = await call("tools/call", { name: "get_node", arguments: { node_id: "tab1" } });
  check("get_node (restored state)", JSON.parse(nd.result.content[0].text).label === "Demo");
  const nt = await call("tools/call", { name: "list_node_types", arguments: {} });
  check("list_node_types", Array.isArray(JSON.parse(nt.result.content[0].text)) && JSON.parse(nt.result.content[0].text)[0].module === "node-red");
} catch (e) {
  console.log("FAIL - unexpected:", e.message); failures++;
} finally {
  child.kill(); srv.close();
}
console.log(failures === 0 ? "\nALL TESTS PASSED" : "\n" + failures + " FAILURES");
process.exit(failures === 0 ? 0 : 1);
