// End-to-end test: spawns the MCP server against a mock Node-RED Admin API.
// No external dependencies: run with "npm test".
import http from "node:http";
import { WebSocketServer } from "ws";
import { spawn } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

const PORT = 18899;
const BACKUP_DIR = fs.mkdtempSync(path.join(os.tmpdir(), "nr-mcp-test-"));

// ---- mock Node-RED ----
const initialFlows = [
  { id: "tab1", type: "tab", label: "Demo" },
  { id: "cfg1", type: "mqtt-broker", name: "Broker LAN" },
  { id: "n1", type: "inject", z: "tab1", name: "tick", topic: "demo/tick", repeat: "15", once: false, wires: [["n2"]] },
  { id: "n2", type: "debug", z: "tab1", name: "log", active: true, wires: [] },
  { id: "n3", type: "mqtt in", z: "tab1", name: "frigate events", topic: "frigate/events", broker: "cfg1", datatype: "json", wires: [["n4"]] },
  { id: "n4", type: "switch", z: "tab1", name: "route", property: "payload.type", rules: [{ t: "eq", v: "new" }], outputs: 1, wires: [["n5"]] },
  { id: "n5", type: "function", z: "tab1", name: "parse", func: "const v = parseFloat(msg.payload);\nreturn msg;", outputs: 1, wires: [["n2"]] }
];
const flows = JSON.parse(JSON.stringify(initialFlows));
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
// ---- mock editor /comms websocket ----
const wss = new WebSocketServer({ noServer: true });
let commsEvents = 0;
srv.on("upgrade", (req, socket, head) => {
  const u = new URL(req.url, "http://x");
  if (u.pathname !== "/comms") { socket.destroy(); return; }
  wss.handleUpgrade(req, socket, head, ws => {
    ws.on("message", raw => {
      let m; try { m = JSON.parse(raw.toString()); } catch { return; }
      if (m.auth !== undefined) { ws.send(JSON.stringify({ auth: "ok" })); return; }
      if (m.topic === "hb") { ws.send(JSON.stringify({ topic: "hb" })); return; }
      if (m.subscribe === "status") {
        setTimeout(() => {
          ws.send(JSON.stringify({ topic: "status/n1", data: { fill: "red", shape: "ring", text: "Call-service error. boom" } }));
          ws.send(JSON.stringify({ topic: "status/n2", data: { fill: "yellow", shape: "ring", text: "warn status" } }));
          ws.send(JSON.stringify({ topic: "notification/mock", data: { type: "error", text: "mock notif error" } }));
          commsEvents++;
        }, 150);
      }
    });
  });
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
const tool = (name, args) => call("tools/call", { name, arguments: args });
const text = (r) => r.result.content[0].text;
const j = (r) => JSON.parse(text(r));

let failures = 0;
const check = (name, cond) => { console.log((cond ? "PASS" : "FAIL") + " - " + name); if (!cond) failures++; };

try {
  const init = await call("initialize", { protocolVersion: "2024-11-05", capabilities: {}, clientInfo: { name: "t", version: "0" } });
  check("initialize", init.result.serverInfo.name === "nodered-mcp");
  child.stdin.write(JSON.stringify({ jsonrpc: "2.0", method: "notifications/initialized" }) + "\n");

  const tools = await call("tools/list", {});
  check("18 tools", tools.result.tools.length === 18);

  // --- overview + comprehension ---
  const lf = await tool("list_flows", {});
  check("list_flows sees Demo tab", Array.isArray(j(lf)) && j(lf)[0].label === "Demo" && j(lf)[0].nodes === 5);

  const lfd = await tool("list_flows", { detail: true });
  const d0 = j(lfd)[0];
  check("list_flows detail: entries", d0.entry_points.length === 2 && d0.entry_points.some(e => e.type === "inject" && e.repeat === "15") && d0.entry_points.some(e => e.type === "mqtt in" && e.topic === "frigate/events"));
  check("list_flows detail: types histogram", d0.types["switch"] === 1 && d0.types["function"] === 1);
  check("list_flows detail: config refs", JSON.stringify(d0.config_refs).includes("Broker LAN"));

  const map = await tool("get_flow_map", { tab_id: "tab1" });
  check("get_flow_map outline: nodes + wiring resolved", text(map).includes("tick") && text(map).includes("frigate events") && text(map).includes("-> log") && text(map).includes("parse"));
  check("get_flow_map outline: broker inlined", text(map).includes("Broker LAN"));

  const mm = await tool("get_flow_map", { tab_id: "tab1", format: "mermaid" });
  check("get_flow_map mermaid", text(mm).includes("flowchart TD") && text(mm).includes("-->"));

  const sn = await tool("search_nodes", { query: "frigate" });
  check("search_nodes by fields", j(sn).results[0].id === "n3" && j(sn).results[0].topic === "frigate/events");
  const sc = await tool("search_nodes", { query: "parseFloat" });
  check("search_nodes in function code", j(sc).code_matches.length === 1 && j(sc).code_matches[0].id === "n5");
  const snf = await tool("search_nodes", { query: "events", type: "mqtt in" });
  check("search_nodes type filter", j(snf).results.length === 1);

  const lc = await tool("list_config_nodes", {});
  const cfg = j(lc).find(c => c.id === "cfg1");
  check("list_config_nodes with usage", cfg && cfg.users === 1);

  const gn = await tool("get_node", { node_id: "n3", resolve_refs: true });
  check("get_node resolve_refs", j(gn).refs.broker && j(gn).refs.broker.name === "Broker LAN");

  // --- validation ---
  const vf = await tool("validate_flows", { flows: [{ id: "x", type: "inject" }, { id: "x", type: "debug" }] });
  check("validate_flows duplicate id", j(vf).ok === false && JSON.stringify(j(vf).errors).includes("duplicate"));
  const vw = await tool("validate_flows", { flows: [{ id: "a", type: "server-state-changed", z: "nope", entityidfilter: "binary_sensor.x", wires: [] }] });
  check("validate_flows v3 field + z warnings", j(vw).warnings.some(w => w.includes("entityidfilter")) && j(vw).warnings.some(w => w.includes("z=nope")));

  // --- writes: dry_run never deploys ---
  const dr = await tool("patch_node", { node_id: "n1", patch: { name: "tick2" }, dry_run: true });
  check("patch_node dry_run diff", text(dr).includes("tick2") && deployed === 0);

  const nc = await tool("deploy_flows", { flows: [], confirm: false });
  check("deploy without confirm rejected", nc.result.isError === true);

  const pn = await tool("patch_node", { node_id: "n1", patch: { name: "tick2" }, confirm: true });
  check("patch_node applies", j(pn).ok === true && deployed === 1);
  const gn2 = await tool("get_node", { node_id: "n1" });
  check("patch_node effect", j(gn2).name === "tick2");

  const pb = await tool("patch_batch", { patches: [{ node_id: "n4", patch: { property: "payload.kind" } }, { node_id: "n2", patch: { name: "logger" } }], confirm: true });
  check("patch_batch one deploy", j(pb).ok === true && deployed === 2);
  const gn4 = await tool("get_node", { node_id: "n4" });
  check("patch_batch effect", j(gn4).property === "payload.kind");

  // --- upsert + wiring ---
  const up = await tool("upsert_node", { node: { type: "debug", name: "dbg2" }, tab_id: "tab1", confirm: true });
  check("upsert_node creates", j(up).action === "created" && deployed === 3);
  const newId2 = j(up).node_id;
  const gnn = await tool("get_node", { node_id: newId2 });
  check("upsert_node z + position", j(gnn).z === "tab1" && typeof j(gnn).y === "number");
  const up2 = await tool("upsert_node", { node: { type: "status", name: "st" }, tab_id: "tab1", wire_from: { node_id: "n1" }, confirm: true });
  check("upsert_node wire_from", j(up2).action === "created" && deployed === 4);
  const gn1 = await tool("get_node", { node_id: "n1" });
  check("wire_from appended", JSON.stringify(j(gn1).wires).includes(j(up2).node_id));

  // --- delete with splice: n3 -> n4 -> n5 becomes n3 -> n5 ---
  const dn = await tool("delete_node", { node_id: "n4", splice: true, confirm: true });
  check("delete_node splice", j(dn).ok === true && deployed === 5);
  const gn3 = await tool("get_node", { node_id: "n3" });
  check("splice rewired", JSON.stringify(j(gn3).wires) === JSON.stringify([["n5"]]));

  // --- legacy deploy/backup/restore path ---
  const dc = await tool("deploy_flows", { flows: [{ id: "tab9", type: "tab", label: "X" }], confirm: true });
  check("deploy with confirm ok", j(dc).ok === true && deployed === 6);

  const backups = fs.readdirSync(BACKUP_DIR).filter(f => f.endsWith(".json"));
  check("backups created", backups.length >= 1);
  const lb = await tool("list_backups", {});
  check("list_backups", JSON.parse(text(lb)).length >= 1);
  const rb = await tool("restore_backup", { file: backups[0], confirm: true });
  check("restore_backup restores initial state", j(rb).ok === true && deployed === 7);
  const gt = await tool("get_node", { node_id: "tab1" });
  check("restored label", j(gt).label === "Demo");
  const st = await tool("get_settings", {});
  check("get_settings", j(st).version === "4.0.0-mock");
  const ce = await tool("call_endpoint", { path: "/diag" });
  check("call_endpoint", text(ce).includes("diag-ok"));
  const nt = await tool("list_node_types", {});
  check("list_node_types", j(nt)[0].module === "node-red");

  // --- get_errors: comms monitor ---
  const ge0 = await tool("get_errors", {}); // avvia il monitor
  await new Promise(r => setTimeout(r, 900)); // attende status/invii del mock
  const ge = await tool("get_errors", {});
  const gej = j(ge);
  check("get_errors monitor open", gej.monitor.state === "open" && typeof gej.monitor.connected_since === "string");
  const errN1 = gej.errors.find(x => x.node_id === "n1");
  check("get_errors red status captured", errN1 && errN1.fill === "red" && errN1.text.includes("boom") && errN1.tab === "Demo" && errN1.still_current === true);
  check("get_errors notification captured", gej.errors.some(x => x.kind === "notification" && x.text.includes("mock notif")));
  const gw = await tool("get_errors", { include_warnings: false });
  check("get_errors warnings filtered", j(gw).errors.every(x => x.fill !== "yellow"));
  const gc = await tool("get_errors", { clear: true });
  const gAfter = await tool("get_errors", {});
  check("get_errors clear empties buffer", j(gAfter).errors.length === 0);
} catch (e) {
  console.log("FAIL - unexpected:", e.message); failures++;
} finally {
  child.kill(); srv.close(); try { wss.close(); } catch {}
}
console.log(failures === 0 ? "\nALL TESTS PASSED" : "\n" + failures + " FAILURES");
process.exit(failures === 0 ? 0 : 1);
