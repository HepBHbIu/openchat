const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const { v4: uuidv4 } = require('uuid');
const fs = require('fs');
const path = require('path');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

const PORT = process.env.PORT || 3001;
const DATA_DIR = path.join(__dirname, 'data');

if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });

// In-memory store
const channels = new Map();
const users = new Map();
const messages = new Map();

// Default channels
const DEFAULT_CHANNELS = [
  { id: 'general', name: 'Общий', icon: '💬', description: 'Главный чат' },
  { id: 'it', name: 'IT', icon: '💻', description: 'Программирование и технологии' },
  { id: 'beer', name: 'Пиво', icon: '🍺', description: 'Отдых и развлечения' },
  { id: 'music', name: 'Музыка', icon: '🎵', description: 'Музыка и подкасты' },
  { id: 'gaming', name: 'Игры', icon: '🎮', description: 'Видеоигры' },
  { id: 'random', name: 'Рандом', icon: '🎲', description: 'Всё подряд' },
  { id: 'news', name: 'Новости', icon: '📰', description: 'Новости мира' },
  { id: 'crypto', name: 'Крипта', icon: '₿', description: 'Криптовалюты' },
];

DEFAULT_CHANNELS.forEach(ch => {
  channels.set(ch.id, { ...ch, created_at: Date.now() });
  messages.set(ch.id, []);
});

// Serve static
app.use(express.static(path.join(__dirname, 'public')));

// API
app.get('/api/channels', (req, res) => {
  const chs = Array.from(channels.values());
  res.json(chs);
});

app.get('/api/channels/:id/messages', (req, res) => {
  const msgs = messages.get(req.params.id) || [];
  const limit = parseInt(req.query.limit) || 100;
  res.json(msgs.slice(-limit));
});

app.post('/api/channels', (req, res) => {
  const { name, icon, description } = req.body;
  if (!name) return res.status(400).json({ error: 'Name required' });
  const id = name.toLowerCase().replace(/[^a-z0-9]/g, '-');
  if (channels.has(id)) return res.status(409).json({ error: 'Channel exists' });
  const ch = { id, name, icon: icon || '💬', description: description || '', created_at: Date.now() };
  channels.set(id, ch);
  messages.set(id, []);
  broadcast({ type: 'channel_created', channel: ch });
  res.json(ch);
});

app.get('/api/stats', (req, res) => {
  res.json({
    users: users.size,
    channels: channels.size,
    messages: Array.from(messages.values()).reduce((s, m) => s + m.length, 0),
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
      
      // Send recent messages
      const channelMsgs = messages.get(channel) || [];
      ws.send(JSON.stringify({ type: 'history', messages: channelMsgs.slice(-100) }));
      
      // Broadcast join
      broadcast({ type: 'user_joined', nickname: user.nickname, channel, color: user.color });
      
      // Update user list
      broadcastChannelUsers(channel);
      break;
    }
    
    case 'message': {
      const user = users.get(userId);
      if (!user) return;
      
      const chatMsg = {
        id: uuidv4(),
        nickname: user.nickname,
        channel: user.channel,
        text: (msg.text || '').slice(0, 2000),
        color: user.color,
        timestamp: Date.now(),
        reply_to: msg.reply_to || null,
      };
      
      // Store message
      if (!messages.has(user.channel)) messages.set(user.channel, []);
      messages.get(user.channel).push(chatMsg);
      
      // Keep last 1000 messages per channel
      const chMsgs = messages.get(user.channel);
      if (chMsgs.length > 1000) chMsgs.splice(0, chMsgs.length - 1000);
      
      // Broadcast to channel
      broadcastToChannel(user.channel, { type: 'message', message: chatMsg });
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
  wss.clients.forEach(ws => {
    if (ws.readyState === WebSocket.OPEN) ws.send(data);
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
