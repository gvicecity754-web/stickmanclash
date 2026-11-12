/* Stick Clash server: serves the game and relays messages between two seats
   of a room. The game itself runs in the browsers. */
const fs = require('fs');
const path = require('path');
const http = require('http');
const { WebSocketServer } = require('ws');

const PUBLIC = path.join(__dirname, 'public');
const TYPES = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8', '.css': 'text/css; charset=utf-8', '.png': 'image/png', '.svg': 'image/svg+xml' };

const server = http.createServer((req, res) => {
  const urlPath = decodeURIComponent((req.url || '/').split('?')[0]);
  if (urlPath === '/healthz') { res.writeHead(200, { 'Content-Type': 'text/plain' }); return res.end('ok'); }
  const rel = urlPath === '/' ? 'index.html' : urlPath.replace(/^\/+/, '');
  const file = path.normalize(path.join(PUBLIC, rel));
  if (!file.startsWith(PUBLIC + path.sep)) { res.writeHead(403); return res.end(); }
  fs.readFile(file, (err, data) => {
    if (err) { res.writeHead(404, { 'Content-Type': 'text/plain' }); return res.end('not found'); }
    res.writeHead(200, { 'Content-Type': TYPES[path.extname(file)] || 'application/octet-stream', 'Cache-Control': 'no-cache' });
    res.end(data);
  });
});
const wss = new WebSocketServer({ server, path: '/ws', maxPayload: 64 * 1024 });

const rooms = new Map(); // code -> [hostWs, guestWs]
const CHARS = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';

function newCode() {
  let c;
  do {
    c = Array.from({ length: 4 }, () => CHARS[Math.floor(Math.random() * CHARS.length)]).join('');
  } while (rooms.has(c));
  return c;
}

function send(ws, obj) {
  if (ws && ws.readyState === 1) ws.send(JSON.stringify(obj));
}

wss.on('connection', (ws) => {
  ws.room = null;
  ws.role = null;
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', (raw) => {
    let m;
    try { m = JSON.parse(raw); } catch (e) { return; }
    if (!m || typeof m.t !== 'string') return;
    if (m.t === 'ping') return;

    if (m.t === 'create') {
      if (ws.room) return;
      const code = newCode();
      rooms.set(code, [ws, null]);
      ws.room = code;
      ws.role = 0;
      return send(ws, { t: 'room', code });
    }

    if (m.t === 'join') {
      if (ws.room) return;
      const code = String(m.code || '').toUpperCase().trim();
      const room = rooms.get(code);
      if (!room) return send(ws, { t: 'error', msg: 'No room with that code.' });
      if (room[1]) return send(ws, { t: 'error', msg: 'That room is already full.' });
      room[1] = ws;
      ws.room = code;
      ws.role = 1;
      send(ws, { t: 'joined', code });
      send(room[0], { t: 'peer' });
      return;
    }

    // everything else is game traffic: pass it to the other seat unchanged
    const room = rooms.get(ws.room);
    if (!room) return;
    const other = room[ws.role === 0 ? 1 : 0];
    send(other, m);
  });

  ws.on('close', () => {
    const code = ws.room;
    if (!code) return;
    const room = rooms.get(code);
    if (!room) return;
    if (ws.role === 0) {
      rooms.delete(code);
      send(room[1], { t: 'left' });
      if (room[1]) room[1].room = null;
    } else {
      room[1] = null;
      send(room[0], { t: 'left' });
    }
  });
});

// Drop dead connections so rooms do not stay half-open.
const heartbeat = setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) { ws.terminate(); continue; }
    ws.isAlive = false;
    ws.ping();
  }
}, 30000);
wss.on('close', () => clearInterval(heartbeat));

const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => console.log(`Stick Clash listening on ${PORT}`));
