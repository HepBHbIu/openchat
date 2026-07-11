const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const { v4: uuidv4 } = require('uuid');
const fs = require('fs');
const path = require('path');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

app.use(express.json());
app.use(express.urlencoded({ extended: true }));

const PORT = process.env.PORT || 3001;
const PEERS = (process.env.PEERS || '').split(',').filter(Boolean);
const SERVER_NAME = process.env.SERVER_NAME || 'Node-' + PORT;

// In-memory store — NO persistence, NO files
const channels = new Map();
const users = new Map();
// Messages live in browser only

// Default channels
channels.set('general', { id: 'general', name: 'Общий', icon: '💬', description: 'Главный чат', created_at: Date.now(), last_activity: Date.now() });

// Serve static
app.use(express.static(path.join(__dirname, 'public')));

// API
app.get('/api/channels', (req, res) => {
  const chs = Array.from(channels.values());
  res.json(chs);
});

app.get('/api/channels/:id/messages', (req, res) => {
  const limit = parseInt(req.query.limit) || 100;
  // No server storage — return empty, messages live in browser
  res.json([]);
});

app.post('/api/channels', (req, res) => {
  const { name, icon, description } = req.body;
  if (!name) return res.status(400).json({ error: 'Name required' });
  const id = name.toLowerCase().replace(/[^a-z0-9]/g, '-');
  if (channels.has(id)) return res.status(409).json({ error: 'Channel exists' });
  const ch = { id, name, icon: icon || '💬', description: description || '', created_at: Date.now(), last_activity: Date.now() };
  channels.set(id, ch);
  broadcast({ type: 'channel_created', channel: ch });
  res.json(ch);
});

// Auto-delete inactive channels (24h)
setInterval(() => {
  const now = Date.now();
  const inactive = [];
  channels.forEach((ch, id) => {
    if (id === 'general') return; // Never delete general
    if (now - ch.last_activity > 24 * 60 * 60 * 1000) {
      inactive.push(id);
    }
  });
  inactive.forEach(id => {
    channels.delete(id);
    broadcast({ type: 'channel_deleted', channelId: id });
    console.log(`[OpenChat] Deleted inactive channel: ${id}`);
  });
}, 60000); // Check every minute

app.get('/api/stats', (req, res) => {
  res.json({
    users: users.size,
    channels: channels.size,
    messages: 0,
  });
});

// Federation
app.get('/federation/info', (req, res) => {
  res.json({
    name: 'OpenChat',
    version: '1.0.0',
    channels: channels.size,
    users: users.size,
  });
});

app.get('/federation/channels', (req, res) => {
  res.json(Array.from(channels.values()));
});

// Relay messages from other servers
app.post('/federation/relay', (req, res) => {
  const { from, msg } = req.body;
  if (!msg) return res.status(400).json({ error: 'No message' });
  // Broadcast to local clients only (avoid infinite loop)
  wss.clients.forEach(ws => {
    if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(msg));
  });
  res.json({ ok: true });
});

// WebSocket
wss.on('connection', (ws) => {
  const userId = uuidv4();
  ws.userId = userId;
  ws.isAlive = true;

  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', (data) => {
    try {
      const msg = JSON.parse(data.toString());
      handleMessage(ws, userId, msg);
    } catch {}
  });

  ws.on('close', () => {
    const user = users.get(userId);
    if (user) {
      broadcast({ type: 'user_left', nickname: user.nickname, channel: user.channel });
      users.delete(userId);
    }
  });

  ws.send(JSON.stringify({ type: 'welcome', userId, channels: Array.from(channels.values()) }));
});

function handleMessage(ws, userId, msg) {
  switch (msg.type) {
    case 'join': {
      const { nickname, channel } = msg;
      if (!nickname || !channel) return;
      
      const user = {
        id: userId,
        nickname: nickname.slice(0, 20),
        channel: channel,
        color: generateColor(nickname),
        joined_at: Date.now(),
      };
      
      users.set(userId, user);
      
      // No server history — messages live in browser only
      
      // Broadcast join
      broadcast({ type: 'user_joined', nickname: user.nickname, channel, color: user.color });
      
      // Update user list
      broadcastChannelUsers(channel);
      break;
    }
    
    case 'message': {
      const user = users.get(userId);
      if (!user) return;
      
      // Update channel activity
      const ch = channels.get(user.channel);
      if (ch) ch.last_activity = Date.now();
      
      const chatMsg = {
        id: uuidv4(),
        nickname: user.nickname,
        channel: user.channel,
        text: (msg.text || '').slice(0, 2000),
        color: user.color,
        timestamp: Date.now(),
        reply_to: msg.reply_to || null,
      };
      
      // No server storage — just relay to all
      broadcast({ type: 'message', message: chatMsg });
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

function broadcast(msg) {
  const data = JSON.stringify(msg);
  // Send to local WebSocket clients
  wss.clients.forEach(ws => {
    if (ws.readyState === WebSocket.OPEN) ws.send(data);
  });
  // Send to federated servers
  PEERS.forEach(peer => {
    fetch(`${peer}/federation/relay`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ from: SERVER_NAME, msg }),
    }).catch(() => {});
  });
}

function broadcastToChannel(channelId, msg, excludeUserId) {
  const data = JSON.stringify(msg);
  wss.clients.forEach(ws => {
    if (ws.readyState === WebSocket.OPEN) {
      const user = users.get(ws.userId);
      if (user && user.channel === channelId && ws.userId !== excludeUserId) {
        ws.send(data);
      }
    }
  });
}

function broadcastChannelUsers(channelId) {
  const channelUsers = Array.from(users.values())
    .filter(u => u.channel === channelId)
    .map(u => ({ nickname: u.nickname, color: u.color }));
  
  broadcastToChannel(channelId, { type: 'users', users: channelUsers });
}

function generateColor(nickname) {
  let hash = 0;
  for (let i = 0; i < nickname.length; i++) {
    hash = nickname.charCodeAt(i) + ((hash << 5) - hash);
  }
  const hue = Math.abs(hash % 360);
  return `hsl(${hue}, 70%, 60%)`;
}

// Heartbeat
setInterval(() => {
  wss.clients.forEach(ws => {
    if (!ws.isAlive) return ws.terminate();
    ws.isAlive = false;
    ws.ping();
  });
}, 30000);

server.listen(PORT, () => {
  console.log(`OpenChat running on http://localhost:${PORT}`);
});
