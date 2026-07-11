const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const { v4: uuidv4 } = require('uuid');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });
app.use(express.json());

const PORT = process.env.PORT || 3001;
const PEERS = (process.env.PEERS || '').split(',').filter(Boolean);
const SERVER_NAME = process.env.SERVER_NAME || 'Node-' + PORT;
const PUBLIC_URL = process.env.PUBLIC_URL || `http://localhost:${PORT}`;
const FED_SECRET = process.env.FED_SECRET || ''; // Shared secret for federation

const channels = new Map();
const users = new Map();
const history = new Map();
const knownPeers = new Set(PEERS);
const rateLimiter = new Map(); // ip -> [timestamps]
const MAX_HISTORY = 50;
const MAX_PEERS = 50;
const RATE_LIMIT = 30;

// Sanitize channel ID
function sanitizeId(str) {
  return String(str || '').toLowerCase().replace(/[^a-z0-9а-яё-]/gi, '').replace(/-+/g, '-').slice(0, 50) || 'chat';
}

// Rate limit check
function checkRate(ip) {
  const now = Date.now();
  const timestamps = rateLimiter.get(ip) || [];
  const recent = timestamps.filter(t => now - t < 60000);
  if (recent.length >= RATE_LIMIT) return false;
  recent.push(now);
  rateLimiter.set(ip, recent);
  return true;
}

// Clean rate limiter
setInterval(() => {
  const now = Date.now();
  rateLimiter.forEach((ts, ip) => {
    const recent = ts.filter(t => now - t < 60000);
    if (recent.length === 0) rateLimiter.delete(ip);
    else rateLimiter.set(ip, recent);
  });
}, 60000);

channels.set('general', { id: 'general', name: 'Общий', icon: '💬', description: 'Главный чат', created_at: Date.now(), last_activity: Date.now() });

app.use(express.static('public'));

// Security headers
app.use((req, res, next) => {
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.setHeader('X-Frame-Options', 'DENY');
  res.setHeader('X-XSS-Protection', '1; mode=block');
  res.setHeader('Referrer-Policy', 'strict-origin-when-cross-origin');
  next();
});

// ===== API =====
app.get('/api/channels', (req, res) => res.json(Array.from(channels.values())));
app.get('/api/stats', (req, res) => res.json({ users: users.size, channels: channels.size, peers: knownPeers.size }));
app.get('/api/channels/:id/history', (req, res) => {
  const page = parseInt(req.query.page) || 0;
  const limit = parseInt(req.query.limit) || 50;
  const msgs = history.get(sanitizeId(req.params.id)) || [];
  const start = Math.max(0, msgs.length - limit - (page * limit));
  const end = msgs.length - (page * limit);
  res.json(msgs.slice(Math.max(0, start), Math.max(0, end)));
});

app.post('/api/channels', (req, res) => {
  const ip = req.ip;
  if (!checkRate(ip)) return res.status(429).json({ error: 'Rate limit' });

  const { name, icon, description } = req.body;
  if (!name) return res.status(400).json({ error: 'Name required' });
  const id = sanitizeId(name);
  if (channels.has(id)) return res.status(409).json({ error: 'Channel exists' });
  const ch = { id, name: name.slice(0, 50), icon: (icon || '💬').slice(0, 2), description: (description || '').slice(0, 200), created_at: Date.now(), last_activity: Date.now() };
  channels.set(id, ch);
  relay({ type: 'channel_created', channel: ch });
  res.json(ch);
});

// ===== FEDERATION =====
function verifyFedAuth(req) {
  if (!FED_SECRET) return true;
  return req.headers['x-fed-secret'] === FED_SECRET;
}

app.get('/federation/info', (req, res) => res.json({
  name: 'TypoChat', version: '1.1.0', server: SERVER_NAME, public_url: PUBLIC_URL,
  channels: channels.size, users: users.size, peers: Array.from(knownPeers),
}));

app.get('/federation/channels', (req, res) => res.json(Array.from(channels.values())));

app.get('/federation/history/:channelId', (req, res) => res.json(history.get(sanitizeId(req.params.channelId)) || []));

app.post('/federation/register', (req, res) => {
  if (!verifyFedAuth(req)) return res.status(401).json({ error: 'Unauthorized' });
  const { url, name } = req.body;
  if (!url || typeof url !== 'string') return res.status(400).json({ error: 'url required' });
  if (!url.startsWith('http://') && !url.startsWith('https://')) return res.status(400).json({ error: 'Invalid URL' });
  knownPeers.add(url);
  console.log(`[Federation] Registered peer: ${url} (${name || '?'})`);
  res.json({ ok: true, peers: Array.from(knownPeers), channels: Array.from(channels.values()), server: SERVER_NAME });
});

app.post('/federation/relay', (req, res) => {
  if (!verifyFedAuth(req)) return res.status(401).json({ error: 'Unauthorized' });
  const { from, msg } = req.body;
  if (!msg || typeof msg !== 'object') return res.status(400).json({ error: 'No message' });

  // Sanitize channel IDs in messages
  if (msg.type === 'message' && msg.message) {
    const chId = sanitizeId(msg.message.channel);
    msg.message.channel = chId;
    msg.message.nickname = String(msg.message.nickname || '').slice(0, 20);
    msg.message.text = String(msg.message.text || '').slice(0, 2000);

    if (!channels.has(chId)) {
      channels.set(chId, { id: chId, name: chId, icon: '💬', description: '', created_at: Date.now(), last_activity: Date.now() });
    }
    if (!history.has(chId)) history.set(chId, []);
    history.get(chId).push(msg.message);
    if (history.get(chId).length > MAX_HISTORY) history.get(chId).shift();

    broadcastToChannel(chId, msg);
  }

  if (msg.type === 'channel_created' && msg.channel) {
    msg.channel.id = sanitizeId(msg.channel.id);
    channels.set(msg.channel.id, { ...msg.channel, last_activity: Date.now() });
    broadcastAllLocal(msg);
  }

  if (msg.type === 'channel_deleted' && msg.channelId) {
    msg.channelId = sanitizeId(msg.channelId);
    channels.delete(msg.channelId);
    history.delete(msg.channelId);
    broadcastAllLocal(msg);
  }

  res.json({ ok: true });
});

// ===== AUTO-DISCOVERY =====
let discovering = false;

async function discoverPeers() {
  if (discovering) return;
  discovering = true;
  try {
    for (const peer of knownPeers) {
      try {
        const headers = {};
        if (FED_SECRET) headers['x-fed-secret'] = FED_SECRET;

        const res = await fetch(`${peer}/federation/info`, { signal: AbortSignal.timeout(5000), headers });
        if (res.ok) {
          const info = await res.json();
          console.log(`[Federation] Peer: ${peer} (${info.server})`);

          await fetch(`${peer}/federation/register`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', ...headers },
            body: JSON.stringify({ url: PUBLIC_URL, name: SERVER_NAME }),
          });

          const chRes = await fetch(`${peer}/federation/channels`, { signal: AbortSignal.timeout(5000), headers });
          if (chRes.ok) {
            for (const ch of await chRes.json()) {
              ch.id = sanitizeId(ch.id);
              if (!channels.has(ch.id)) {
                channels.set(ch.id, ch);
                console.log(`[Federation] Synced channel: ${ch.id}`);
              }
            }
          }

          for (const [chId] of channels) {
            try {
              const hRes = await fetch(`${peer}/federation/history/${chId}`, { signal: AbortSignal.timeout(5000), headers });
              if (hRes.ok) {
                const msgs = await hRes.json();
                if (msgs.length > 0) {
                  if (!history.has(chId)) history.set(chId, []);
                  const existing = new Set(history.get(chId).map(m => m.id));
                  for (const msg of msgs.slice(-MAX_HISTORY)) {
                    msg.nickname = String(msg.nickname || '').slice(0, 20);
                    msg.text = String(msg.text || '').slice(0, 2000);
                    if (!existing.has(msg.id)) history.get(chId).push(msg);
                  }
                  history.get(chId).sort((a, b) => a.timestamp - b.timestamp);
                  if (history.get(chId).length > MAX_HISTORY) history.get(chId).splice(0, history.get(chId).length - MAX_HISTORY);
                }
              }
            } catch {}
          }

          if (info.peers && knownPeers.size < MAX_PEERS) {
            for (const p of info.peers) {
              if (p !== PUBLIC_URL && !knownPeers.has(p) && typeof p === 'string' && (p.startsWith('http://') || p.startsWith('https://'))) {
                if (knownPeers.size >= MAX_PEERS) break;
                knownPeers.add(p);
                console.log(`[Federation] Discovered: ${p}`);
              }
            }
          }
        }
      } catch {}
    }
  } finally {
    discovering = false;
  }
}

setInterval(discoverPeers, 60000);

// ===== WEBSOCKET =====
wss.on('connection', (ws, req) => {
  const ip = req.headers['x-forwarded-for'] || req.socket.remoteAddress;
  const userId = uuidv4();
  ws.userId = userId;
  ws.isAlive = true;
  ws.userChannel = null;

  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('error', (err) => {
    console.error(`[WS] Error for ${userId}:`, err.message);
  });

  ws.on('message', (data) => {
    if (!checkRate(ip)) return;
    try { handleMessage(ws, userId, JSON.parse(data.toString())); } catch {}
  });

  ws.on('close', () => {
    const user = users.get(userId);
    if (user) {
      broadcastToChannel(user.channel, { type: 'user_left', nickname: user.nickname });
      users.delete(userId);
      broadcastChannelUsers(user.channel);
    }
  });

  ws.send(JSON.stringify({ type: 'welcome', userId, peers: Array.from(knownPeers) }));
});

function handleMessage(ws, userId, msg) {
  switch (msg.type) {
    case 'join': {
      const nickname = String(msg.nickname || '').slice(0, 20);
      const channel = sanitizeId(msg.channel);
      if (!nickname || !channel) return;

      // Leave old channel if switching
      const oldUser = users.get(userId);
      if (oldUser && oldUser.channel !== channel) {
        broadcastToChannel(oldUser.channel, { type: 'user_left', nickname: oldUser.nickname });
        broadcastChannelUsers(oldUser.channel);
      }

      if (!channels.has(channel)) {
        channels.set(channel, { id: channel, name: channel, icon: '💬', description: '', created_at: Date.now(), last_activity: Date.now() });
      }

      const user = { id: userId, nickname, channel, color: genColor(nickname) };
      users.set(userId, user);
      ws.userChannel = channel;

      const msgs = history.get(channel) || [];
      ws.send(JSON.stringify({ type: 'history', channel, messages: msgs }));

      const channelUsers = Array.from(users.values()).filter(u => u.channel === channel).map(u => ({ nickname: u.nickname, color: u.color }));
      ws.send(JSON.stringify({ type: 'users', users: channelUsers }));

      broadcastToChannel(channel, { type: 'user_joined', nickname: user.nickname, color: user.color }, userId);
      break;
    }

    case 'message': {
      const user = users.get(userId);
      if (!user) return;
      if (!msg.text || typeof msg.text !== 'string') return;

      const ch = channels.get(user.channel);
      if (ch) ch.last_activity = Date.now();

      const chatMsg = {
        id: uuidv4(), nickname: user.nickname, channel: user.channel,
        text: msg.text.slice(0, 2000), color: user.color,
        timestamp: Date.now(),
      };

      if (!history.has(user.channel)) history.set(user.channel, []);
      history.get(user.channel).push(chatMsg);
      if (history.get(user.channel).length > MAX_HISTORY) history.get(user.channel).shift();

      broadcastToChannel(user.channel, { type: 'message', message: chatMsg }, userId);
      relay({ type: 'message', message: chatMsg });
      break;
    }

    case 'typing': {
      const user = users.get(userId);
      if (!user) return;
      broadcastToChannel(user.channel, { type: 'typing', nickname: user.nickname }, userId);
      break;
    }
  }
}

// ===== BROADCAST =====
function relay(msg) {
  broadcastAllLocal(msg);
  const headers = { 'Content-Type': 'application/json' };
  if (FED_SECRET) headers['x-fed-secret'] = FED_SECRET;
  for (const peer of knownPeers) {
    fetch(`${peer}/federation/relay`, { method: 'POST', headers, body: JSON.stringify({ from: SERVER_NAME, msg }) }).catch(() => {});
  }
}

function broadcastAllLocal(msg) {
  const data = JSON.stringify(msg);
  wss.clients.forEach(ws => { if (ws.readyState === WebSocket.OPEN) ws.send(data); });
}

function broadcastToChannel(channelId, msg, excludeUserId) {
  const data = JSON.stringify(msg);
  wss.clients.forEach(ws => {
    if (ws.readyState !== WebSocket.OPEN) return;
    const user = users.get(ws.userId);
    if (user && user.channel === channelId && ws.userId !== excludeUserId) ws.send(data);
  });
}

function broadcastChannelUsers(channelId) {
  const channelUsers = Array.from(users.values()).filter(u => u.channel === channelId).map(u => ({ nickname: u.nickname, color: u.color }));
  broadcastToChannel(channelId, { type: 'users', users: channelUsers });
}

function genColor(nick) {
  let h = 0;
  for (let i = 0; i < nick.length; i++) h = nick.charCodeAt(i) + ((h << 5) - h);
  return `hsl(${Math.abs(h % 360)}, 70%, 60%)`;
}

// Heartbeat
setInterval(() => {
  wss.clients.forEach(ws => {
    if (!ws.isAlive) return ws.terminate();
    ws.isAlive = false;
    ws.ping();
  });
}, 30000);

// Auto-delete inactive channels (24h)
setInterval(() => {
  const now = Date.now();
  channels.forEach((ch, id) => {
    if (id === 'general') return;
    if (now - ch.last_activity > 86400000) {
      channels.delete(id);
      history.delete(id);
      relay({ type: 'channel_deleted', channelId: id });
    }
  });
}, 60000);

// Health check
app.get('/health', (req, res) => res.json({ status: 'ok', uptime: process.uptime() }));

setTimeout(discoverPeers, 2000);

// Graceful shutdown
process.on('SIGTERM', () => {
  console.log('[Server] Shutting down...');
  wss.clients.forEach(ws => ws.close(1001, 'Server shutting down'));
  wss.close(() => {
    server.close(() => process.exit(0));
  });
  setTimeout(() => process.exit(1), 5000);
});

process.on('SIGINT', () => process.emit('SIGTERM'));

server.listen(PORT, () => {
  console.log(`TypoChat running on http://localhost:${PORT}`);
  console.log(`Public URL: ${PUBLIC_URL}`);
  console.log(`Federation: ${FED_SECRET ? 'enabled (secret set)' : 'open (no secret)'}`);
  console.log(`Peers: ${knownPeers.size > 0 ? Array.from(knownPeers).join(', ') : 'none'}`);
});
