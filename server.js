"use strict";
const http = require("node:http");
const path = require("node:path");
const fs = require("node:fs");
const WebSocket = require("ws");

const ROOT = path.join(__dirname, "public");
const DATA_DIR = path.join(__dirname, "worlds");
const PORT = Number(process.env.PORT || 3000);
const MAX_SNAPSHOT_CHARS = 1_500_000;
fs.mkdirSync(DATA_DIR, { recursive: true });

// Each room is stored as an ordinary JSON file. No native database addon is needed.
const rooms = new Map();
const clientsByRoom = new Map();
function emptyWorld() {
  return { blockChanges: {}, furnaceOrientations: {}, blockMetadata: {}, seed: null };
}
function roomFile(room) {
  // Room names have already been restricted to safe filename characters.
  return path.join(DATA_DIR, `${room}.json`);
}
function loadWorld(room) {
  if (rooms.has(room)) return rooms.get(room);
  let world = emptyWorld();
  try {
    const parsed = JSON.parse(fs.readFileSync(roomFile(room), "utf8"));
    if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
      world = {
        blockChanges: parsed.blockChanges && typeof parsed.blockChanges === "object" ? parsed.blockChanges : {},
        furnaceOrientations: parsed.furnaceOrientations && typeof parsed.furnaceOrientations === "object" ? parsed.furnaceOrientations : {},
        blockMetadata: parsed.blockMetadata && typeof parsed.blockMetadata === "object" ? parsed.blockMetadata : {},
        seed: parsed.seed ?? null
      };
    }
  } catch (err) {
    if (err.code !== "ENOENT") console.error(`World file read error (${room}):`, err.message);
  }
  rooms.set(room, world);
  return world;
}
function saveWorld(room, world) {
  const target = roomFile(room);
  const temp = `${target}.${process.pid}.tmp`;
  const data = JSON.stringify(world);
  if (data.length > MAX_SNAPSHOT_CHARS) return false;
  try {
    fs.writeFileSync(temp, data, "utf8");
    fs.renameSync(temp, target);
    rooms.set(room, world);
    return true;
  } catch (err) {
    try { fs.rmSync(temp, { force: true }); } catch {}
    console.error(`World file write error (${room}):`, err.message);
    return false;
  }
}

const server = http.createServer((req, res) => {
  let pathname;
  try { pathname = decodeURIComponent(new URL(req.url, "http://localhost").pathname); }
  catch { res.writeHead(400); return res.end("Bad request"); }
  const requested = pathname === "/" ? "/index.html" : pathname;
  const file = path.resolve(ROOT, "." + requested);
  if (!file.startsWith(ROOT + path.sep) && file !== path.join(ROOT, "index.html")) {
    res.writeHead(403); return res.end("Forbidden");
  }
  fs.readFile(file, (err, data) => {
    if (err) { res.writeHead(404); return res.end("Not found"); }
    const ext = path.extname(file);
    const types = { ".html": "text/html; charset=utf-8", ".js": "text/javascript; charset=utf-8", ".css": "text/css; charset=utf-8", ".png": "image/png", ".jpg": "image/jpeg", ".svg": "image/svg+xml" };
    res.writeHead(200, { "Content-Type": types[ext] || "application/octet-stream", "Cache-Control": "no-store" });
    res.end(data);
  });
});

const wss = new WebSocket.Server({ server, maxPayload: 2 * 1024 * 1024 });
function send(ws, event, payload) {
  if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify({ event, payload }));
}
function broadcast(room, event, payload, except) {
  for (const client of clientsByRoom.get(room) || []) if (client !== except) send(client, event, payload);
}

wss.on("connection", ws => {
  ws.room = null;
  ws.playerId = null;
  ws.on("message", raw => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch { return; }

    if (msg.type === "join") {
      const room = String(msg.room || "").replace(/[^a-zA-Z0-9_-]/g, "").slice(0, 32);
      const id = String(msg.id || "").slice(0, 80);
      if (!room || !id) { ws.close(1008, "invalid room or player id"); return; }
      // Do not allow a socket to join multiple rooms without leaving its previous one.
      if (ws.room) {
        const previous = clientsByRoom.get(ws.room);
        if (previous) previous.delete(ws);
      }
      ws.room = room;
      ws.playerId = id;
      if (!clientsByRoom.has(room)) clientsByRoom.set(room, new Set());
      const peers = clientsByRoom.get(room);
      const peerCountBeforeJoin = peers.size;
      peers.add(ws);

      const world = loadWorld(room);
      send(ws, "world_snapshot", { ...world, id: "server", target: id });
      if (peerCountBeforeJoin > 0) broadcast(room, "world_snapshot_request", { id, t: Date.now() }, ws);
      return;
    }

    if (!ws.room || msg.type !== "event" || typeof msg.event !== "string") return;
    const event = msg.event;
    const payload = msg.payload && typeof msg.payload === "object" ? msg.payload : {};

    if (event === "world_snapshot") {
      const world = {
        blockChanges: payload.blockChanges && typeof payload.blockChanges === "object" ? payload.blockChanges : {},
        furnaceOrientations: payload.furnaceOrientations && typeof payload.furnaceOrientations === "object" ? payload.furnaceOrientations : {},
        blockMetadata: payload.blockMetadata && typeof payload.blockMetadata === "object" ? payload.blockMetadata : {},
        seed: payload.seed ?? null
      };
      saveWorld(ws.room, world);
    }

    if (event === "block_change") {
      const world = loadWorld(ws.room);
      const key = [payload.x, payload.y, payload.z].join(",");
      world.blockChanges[key] = Number(payload.block) || 0;
      if (payload.furnaceDir == null) delete world.furnaceOrientations[key];
      else world.furnaceOrientations[key] = payload.furnaceDir;
      if (payload.meta == null) delete world.blockMetadata[key];
      else world.blockMetadata[key] = payload.meta;
      saveWorld(ws.room, world);
    }

    broadcast(ws.room, event, payload, ws);
  });

  ws.on("close", () => {
    if (!ws.room) return;
    const peers = clientsByRoom.get(ws.room);
    if (peers) {
      peers.delete(ws);
      if (peers.size === 0) clientsByRoom.delete(ws.room);
    }
    broadcast(ws.room, "player_leave", { id: ws.playerId });
  });
});

server.listen(PORT, "0.0.0.0", () => console.log(`Minecraft local server: http://localhost:${PORT}`));
