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
// Admin API v2: GET /flows (v2 header) -> {flows, rev}; POST /flows (v2) with
// stale rev -> 409; /bump simulates a concurrent editor deploy.
const initialFlows = [
  { id: "tab1", type: "tab", label: "Demo" },
  { id: "cfg1", type: "mqtt-broker", name: "Broker LAN" },
  { id: "cfg2", type: "mqtt-broker", name: "Unused Broker" },
  { id: "n1", type: "inject", z: "tab1", name: "tick", topic: "demo/tick", repeat: "15", once: false, wires: [["n2"]] },
  { id: "n2", type: "debug", z: "tab1", name: "log", active: true, wires: [] },
  { id: "n3", type: "mqtt in", z: "tab1", name: "frigate events", topic: "frigate/events", broker: "cfg1", datatype: "json", wires: [["n4"]] },
  { id: "n4", type: "switch", z: "tab1", name: "route", property: "payload.type", rules: [{ t: "eq", v: "new" }], outputs: 1, wires: [["n5"]] },
  { id: "n5", type: "function", z: "tab1", name: "parse", func: "const v = parseFloat(msg.payload);\nreturn msg;", outputs: 1, wires: [["n2", "m1"]] },
  { id: "tab2", type: "tab", label: "Layout Lab" },
  { id: "ga", type: "group", z: "tab2", name: "GA", style: {}, nodes: ["m1", "m2"], x: 70, y: 85, w: 260, h: 70 },
  { id: "gb", type: "group", z: "tab2", name: "GB", style: {}, nodes: ["m3"], x: 430, y: 85, w: 140, h: 70 },
  { id: "m1", type: "inject", z: "tab2", name: "src", x: 140, y: 120, g: "ga", wires: [["m2"]] },
  { id: "m2", type: "function", z: "tab2", name: "prep", func: "return msg;", outputs: 1, x: 260, y: 120, g: "ga", wires: [["m3"]] },
  { id: "m3", type: "debug", z: "tab2", name: "out", x: 500, y: 120, g: "gb", wires: [] },
  { id: "m4", type: "function", z: "tab2", name: "unused orphan", func: "return msg;", outputs: 1, x: 140, y: 400, d: true, wires: [] },
  { id: "tab3", type: "tab", label: "Empty" }
];
const flows = JSON.parse(JSON.stringify(initialFlows));
let deployed = 0;
let rev = "r0";
let revN = 0;
let armConflict = false; // one-shot: next v2 POST gets 409 (simulates a deploy landing between read and write)
const srv = http.createServer((req, res) => {
  const url = req.url.split("?")[0];
  if (req.method === "GET" && url === "/flows") {
    res.setHeader("content-type", "application/json");
    if (req.headers["node-red-api-version"] === "v2") res.end(JSON.stringify({ flows: flows, rev: rev }));
    else res.end(JSON.stringify(flows));
  }
  else if (req.method === "POST" && url === "/flows") {
    let b = ""; req.on("data", c => b += c);
    req.on("end", () => {
      const ok = req.headers["node-red-deployment-type"] === "flows";
      if (!ok) { res.statusCode = 400; res.end(); return; }
      if (req.headers["node-red-api-version"] === "v2") {
        let body; try { body = JSON.parse(b); } catch { res.statusCode = 400; res.end(); return; }
        if (body && typeof body === "object" && Array.isArray(body.flows)) {
          if (armConflict) { armConflict = false; res.statusCode = 409; res.end(JSON.stringify({ code: "version_mismatch" })); return; }
          if (body.rev && body.rev !== rev) { res.statusCode = 409; res.end(JSON.stringify({ code: "version_mismatch" })); return; }
          flows.length = 0; flows.push(...body.flows);
          rev = "r" + (++revN);
          deployed++;
          res.setHeader("content-type", "application/json");
          res.end(JSON.stringify({ rev: rev }));
          return;
        }
        res.statusCode = 400; res.end(); return;
      }
      try { flows.length = 0; flows.push(...JSON.parse(b)); } catch {}
      deployed++;
      res.statusCode = 204; res.end();
    });
  }
  else if (req.method === "GET" && url === "/arm-conflict") { armConflict = true; res.end("armed"); }
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
  check("28 tools", tools.result.tools.length === 28);

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
  const vl = await tool("validate_flows", { flows: [{ id: "lo", type: "link out", links: ["ghost"], wires: [] }, { id: "a", type: "inject", g: "ghostgroup", wires: [["lo"]] }] });
  check("validate_flows link target + group ref", JSON.stringify(j(vl).errors).includes("missing link in") && JSON.stringify(j(vl).warnings).includes("not a known group"));

  // --- writes: dry_run never deploys ---
  const dr = await tool("patch_node", { node_id: "n1", patch: { name: "tick2" }, dry_run: true });
  check("patch_node dry_run diff", text(dr).includes("tick2") && deployed === 0);

  const nc = await tool("deploy_flows", { flows: [], confirm: false });
  check("deploy without confirm rejected", nc.result.isError === true);

  const pn = await tool("patch_node", { node_id: "n1", patch: { name: "tick2" }, confirm: true });
  check("patch_node applies", j(pn).ok === true && j(pn).revision_guard === "on" && deployed === 1);
  const gn2 = await tool("get_node", { node_id: "n1" });
  check("patch_node effect", j(gn2).name === "tick2");

  const pb = await tool("patch_batch", { patches: [{ node_id: "n4", patch: { property: "payload.kind" } }, { node_id: "n2", patch: { name: "logger" } }], confirm: true });
  check("patch_batch one deploy", j(pb).ok === true && deployed === 2);
  const gn4 = await tool("get_node", { node_id: "n4" });
  check("patch_batch effect", j(gn4).property === "payload.kind");

  // --- revision guard (P1): a deploy landing between read and write => 409, nothing written ---
  const armRes = await fetch("http://127.0.0.1:" + PORT + "/arm-conflict").then(r => r.text());
  check("mock conflict armed", armRes === "armed");
  const conflict = await tool("patch_node", { node_id: "n1", patch: { name: "tick3" }, confirm: true });
  check("409 on concurrent deploy", conflict.result.isError === true && text(conflict).includes("409") && deployed === 2);
  const retry = await tool("patch_node", { node_id: "n1", patch: { name: "tick3" }, confirm: true });
  check("retry after 409 succeeds", j(retry).ok === true && deployed === 3);
  const gn1b = await tool("get_node", { node_id: "n1" });
  check("retry effect", j(gn1b).name === "tick3");

  // --- upsert + wiring (free placement: no overlaps) ---
  const up = await tool("upsert_node", { node: { type: "debug", name: "dbg2" }, tab_id: "tab1", confirm: true });
  check("upsert_node creates", j(up).action === "created" && deployed === 4);
  const newId2 = j(up).node_id;
  const gnn = await tool("get_node", { node_id: newId2 });
  check("upsert_node z + position", j(gnn).z === "tab1" && typeof j(gnn).y === "number");
  const up2 = await tool("upsert_node", { node: { type: "status", name: "st" }, tab_id: "tab1", wire_from: { node_id: "n1" }, confirm: true });
  check("upsert_node wire_from", j(up2).action === "created" && deployed === 5);
  const gn1 = await tool("get_node", { node_id: "n1" });
  check("wire_from appended", JSON.stringify(j(gn1).wires).includes(j(up2).node_id));
  // two auto-placed nodes must not land on the same spot
  const p1 = j(gnn), p2 = j(await tool("get_node", { node_id: j(up2).node_id }));
  check("free placement: distinct positions", Math.abs(p1.x - p2.x) >= 40 || Math.abs(p1.y - p2.y) >= 16);

  // --- delete with splice: n3 -> n4 -> n5 becomes n3 -> n5 ---
  const dn = await tool("delete_node", { node_id: "n4", splice: true, confirm: true });
  check("delete_node splice", j(dn).ok === true && deployed === 6);
  const gn3 = await tool("get_node", { node_id: "n3" });
  check("splice rewired", JSON.stringify(j(gn3).wires) === JSON.stringify([["n5"]]));

  // --- bypass_node (P2): reroute around, keep node disabled ---
  const by = await tool("bypass_node", { node_id: "n5", confirm: true });
  check("bypass_node ok", j(by).ok === true && j(by).now_disabled === true && deployed === 7);
  const gn3b = await tool("get_node", { node_id: "n3" });
  const gn5 = await tool("get_node", { node_id: "n5" });
  check("bypass rewired n3 -> n2+m1 (n5 outputs)", JSON.stringify(j(gn3b).wires) === JSON.stringify([["n2", "m1"]]));
  check("bypass keeps node disabled on canvas", j(gn5).d === true);

  // --- set_node_enabled (P2) ---
  const se = await tool("set_node_enabled", { node_ids: ["m4"], enabled: false, confirm: true });
  check("set_node_enabled disable", j(se).ok === true && deployed === 8);
  const gm4 = await tool("get_node", { node_id: "m4" });
  check("m4 has d:true", j(gm4).d === true);
  const se2 = await tool("set_node_enabled", { node_ids: ["tab3"], enabled: false, confirm: true });
  check("set_node_enabled tab disable", j(se2).ok === true && deployed === 9);
  const gt3 = await tool("get_node", { node_id: "tab3" });
  check("tab3 disabled:true", j(gt3).disabled === true);

  // --- manage_tabs (P2) ---
  const mt = await tool("manage_tabs", { op: "create", label: "New From Test", confirm: true });
  const newTabId = j(mt).result.created.id;
  check("manage_tabs create", j(mt).ok === true && deployed === 10 && !!newTabId);
  const mtr = await tool("manage_tabs", { op: "rename", tab_id: newTabId, label: "Renamed", confirm: true });
  check("manage_tabs rename", j(mtr).result.label === "Renamed" && deployed === 11);
  const mtd = await tool("manage_tabs", { op: "delete", tab_id: "tab2", confirm: true });
  check("delete non-empty tab refused", mtd.result.isError === true && deployed === 11);
  const mtd2 = await tool("manage_tabs", { op: "delete", tab_id: newTabId, confirm: true });
  check("manage_tabs delete empty", j(mtd2).ok === true && deployed === 12);

  // --- audit_flows (P2) ---
  const au = await tool("audit_flows", {});
  const auj = j(au);
  check("audit: unused config cfg2", auj.unused_config_nodes.some(c => c.id === "cfg2"));
  check("audit: disabled node m4", auj.disabled_nodes.some(n => n.id === "m4"));
  check("audit: disabled tab3", auj.disabled_tabs.some(t => t.id === "tab3"));
  check("audit: orphan m4", auj.orphan_nodes.some(n => n.id === "m4"));
  check("audit: cross-tab wire n5->m1", auj.cross_tab_wires.some(x => x.from.includes("parse")));

  // --- layout gate (P1/P3): new group overlap is refused ---
  const gate = await tool("patch_node", { node_id: "ga", patch: { x: 430 }, confirm: true });
  check("layout gate blocks new group overlap", gate.result.isError === true && text(gate).includes("layout gate") && deployed === 12);
  const gate2 = await tool("patch_node", { node_id: "ga", patch: { x: 430 }, confirm: true, allow_overlap: true });
  check("allow_overlap overrides gate", j(gate2).ok === true && deployed === 13);
  const gate3 = await tool("patch_node", { node_id: "ga", patch: { x: 70 }, confirm: true });
  check("restoring geometry passes gate", j(gate3).ok === true && deployed === 14);

  // --- layout_audit (P1) ---
  const la = await tool("layout_audit", { tab_id: "tab2" });
  const laj = j(la);
  check("layout_audit tab2 clean", laj.summary.errors === 0 && laj.tabs[0].label === "Layout Lab" && laj.tabs[0].groups === 2);

  // --- manage_groups (P3) ---
  const mgc = await tool("manage_groups", { op: "create", tab_id: "tab2", name: "GC", node_ids: ["m4"], confirm: true });
  check("manage_groups create GC around m4", j(mgc).ok === true && deployed === 15 && j(mgc).result.created.name === "GC");
  const gcId = j(mgc).result.created.id;
  const la2 = await tool("layout_audit", { tab_id: "tab2" });
  check("GC contains m4 (no escape)", j(la2).summary.errors === 0);
  // adding a FAR member (m3 is in GB across the canvas) would span GC over GA/GB: the gate blocks it
  const mgaBlocked = await tool("manage_groups", { op: "add", group_id: gcId, node_ids: ["m3"], confirm: true });
  check("gate blocks spanning group add", mgaBlocked.result.isError === true && text(mgaBlocked).includes("layout gate") && deployed === 15);
  // place a node NEXT TO m4 and group it: no spanning, no gate issue
  const upm5 = await tool("upsert_node", { node: { type: "debug", name: "m5", x: 260, y: 400 }, tab_id: "tab2", confirm: true });
  check("upsert m5 near m4", j(upm5).action === "created" && deployed === 16);
  const m5Id = j(upm5).node_id;
  const mga = await tool("manage_groups", { op: "add", group_id: gcId, node_ids: [m5Id], confirm: true });
  check("manage_groups add member", j(mga).ok === true && j(mga).result.members === 2 && deployed === 17);
  const gm5 = j(await tool("get_node", { node_id: m5Id }));
  check("m5 now in GC", gm5.g === gcId);
  const mgd = await tool("manage_groups", { op: "remove", group_id: gcId, node_ids: [m5Id], confirm: true });
  check("manage_groups remove member", j(mgd).result.members === 1 && deployed === 18);
  const laClean = await tool("layout_audit", { tab_id: "tab2" });
  check("layout clean after remove+refit", j(laClean).summary.errors === 0);

  // --- decouple_tab (P3): m2 -> m3 crosses GA/GB ---
  const dc = await tool("decouple_tab", { tab_id: "tab2" });
  const dcj = j(dc);
  check("decouple dry-run lists crossing", Array.isArray(dcj.crossings) && dcj.crossings.length === 1 && dcj.crossings[0].src_group === "GA");
  const dca = await tool("decouple_tab", { tab_id: "tab2", apply: true, confirm: true });
  check("decouple applies", j(dca).ok === true && j(dca).decoupled === 1 && deployed === 19);
  const gm2 = await tool("get_node", { node_id: "m2" });
  const linkOutId = j(gm2).wires[0][0];
  const slo = j(await tool("get_node", { node_id: linkOutId }));
  check("m2 now wired to link out in GA", slo.type === "link out" && slo.g === "ga" && slo.links.length === 1);
  const sli = j(await tool("get_node", { node_id: slo.links[0] }));
  check("link in in GB feeds m3", sli.type === "link in" && sli.g === "gb" && JSON.stringify(sli.wires) === JSON.stringify([["m3"]]));
  const la3 = await tool("layout_audit", { tab_id: "tab2" });
  check("layout clean after decouple", j(la3).summary.errors === 0);

  // --- export/import (P2) ---
  const ex = await tool("export_nodes", { node_ids: ["n3"] });
  const exj = j(ex);
  check("export includes config ref", exj.count === 2 && exj.nodes.some(n => n.id === "cfg1"));
  const im = await tool("import_nodes", { nodes: exj.nodes, tab_id: "tab3", confirm: true });
  const imj = j(im);
  check("import remaps ids + reuses config", imj.ok === true && imj.imported === 1 && imj.reused_configs.includes("cfg1") && deployed === 20);
  const importedId = imj.id_map["n3"];
  const gi = j(await tool("get_node", { node_id: importedId }));
  check("imported node in tab3 with broker ref", gi.z === "tab3" && gi.broker === "cfg1");

  // --- arrange_tab (P3) ---
  const ar = await tool("arrange_tab", { tab_id: "tab2" });
  const arj = j(ar);
  check("arrange dry-run plan", arj.dry_run === true && arj.moves.length === 3 && arj.columns >= 1);
  const ara = await tool("arrange_tab", { tab_id: "tab2", apply: true, confirm: true });
  check("arrange applies", j(ara).ok === true && j(ara).arranged === 3 && deployed === 21);
  const gga = j(await tool("get_node", { node_id: "ga" }));
  const ggb = j(await tool("get_node", { node_id: "gb" }));
  const ggc = j(await tool("get_node", { node_id: gcId }));
  check("groups repacked (not overlapping)", !((gga.x < ggb.x + ggb.w && ggb.x < gga.x + gga.w && gga.y < ggb.y + ggb.h && ggb.y < gga.y + gga.h)));
  const la4 = await tool("layout_audit", { tab_id: "tab2" });
  check("layout clean after arrange", j(la4).summary.errors === 0);

  // --- legacy deploy/backup/restore path ---
  const dc2 = await tool("deploy_flows", { flows: [{ id: "tab9", type: "tab", label: "X" }], confirm: true });
  check("deploy with confirm ok", j(dc2).ok === true && deployed === 22);

  const backups = fs.readdirSync(BACKUP_DIR).filter(f => f.endsWith(".json"));
  check("backups created", backups.length >= 1);
  const lb = await tool("list_backups", {});
  check("list_backups", JSON.parse(text(lb)).length >= 1);
  const rb = await tool("restore_backup", { file: backups[0], confirm: true });
  check("restore_backup restores initial state", j(rb).ok === true && deployed === 23);
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
  const gc2 = await tool("get_errors", { clear: true });
  const gAfter = await tool("get_errors", {});
  check("get_errors clear empties buffer", j(gAfter).errors.length === 0);

  // --- get_errors include_last_status ---
  const gl = await tool("get_errors", { include_last_status: true });
  const glj = j(gl);
  check("get_errors nodes_in_error_now", Array.isArray(glj.nodes_in_error_now) && glj.nodes_in_error_now.some(x => x.node_id === "n1" && x.fill === "red" && x.text.includes("boom")));
  check("get_errors last_status_seen recent", Array.isArray(glj.last_status_seen) && glj.last_status_seen.some(x => x.node_id === "n2" && x.fill === "yellow") && glj.last_status_seen.every(x => x.seen_at));
} catch (e) {
  console.log("FAIL - unexpected:", e.message);
  failures++;
} finally {
  child.kill(); srv.close(); try { wss.close(); } catch {}
}
console.log(failures === 0 ? "\nALL TESTS PASSED" : "\n" + failures + " FAILURES");
process.exit(failures === 0 ? 0 : 1);
