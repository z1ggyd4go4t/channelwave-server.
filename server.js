const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const TOTAL_CHANNELS = 100;
const MAX_HISTORY = 200;
const MAX_IMAGE_BYTES = 1500000; // ~1.5MB base64 cap per image

// channel state: { ownerId, private, code, members: Set<ws>, history: [], order: [] }
const channels = {};
for (let i = 1; i <= TOTAL_CHANNELS; i++) {
  channels[i] = { ownerId: null, private: false, code: null, members: new Set(), history: [], order: [] };
}

let nextMsgId = 1;

function send(ws, obj) {
  if (ws.readyState === ws.OPEN) ws.send(JSON.stringify(obj));
}

function broadcast(chNum, obj, exceptWs) {
  const ch = channels[chNum];
  if (!ch) return;
  for (const member of ch.members) {
    if (member !== exceptWs) send(member, obj);
  }
}

function pushHistory(ch, msg) {
  ch.history.push(msg);
  if (ch.history.length > MAX_HISTORY) ch.history.shift();
}

function leaveChannel(ws) {
  if (ws.channel == null) return;
  const ch = channels[ws.channel];
  if (!ch) return;
  ch.members.delete(ws);
  ch.order = ch.order.filter(w => w !== ws);

  broadcast(ws.channel, { type: 'system', text: `${ws.name} left the channel`, t: Date.now() });

  if (ch.ownerId === ws.id) {
    const next = ch.order[0];
    if (next) {
      ch.ownerId = next.id;
      send(next, { type: 'ownerChanged', isOwner: true });
      broadcast(ws.channel, { type: 'system', text: `${next.name} is now the channel owner`, t: Date.now() }, next);
    } else {
      // channel is empty: fully reset, including privacy
      ch.ownerId = null;
      ch.private = false;
      ch.code = null;
    }
  }

  broadcast(ws.channel, { type: 'presence', memberCount: ch.members.size });
  ws.channel = null;
}

function handleJoin(ws, data) {
  const chNum = Number(data.channel);
  if (!Number.isInteger(chNum) || chNum < 1 || chNum > TOTAL_CHANNELS) {
    return send(ws, { type: 'error', text: 'invalid channel' });
  }
  const ch = channels[chNum];

  if (ch.private && ch.ownerId !== null && ch.ownerId !== ws.id) {
    if (!data.code || data.code !== ch.code) {
      return send(ws, { type: 'needCode', channel: chNum });
    }
  }

  leaveChannel(ws);

  ws.name = (data.name || 'unknown').toString().slice(0, 18);
  ws.channel = chNum;
  ch.members.add(ws);
  ch.order.push(ws);

  let becameOwner = false;
  if (ch.ownerId === null) {
    ch.ownerId = ws.id;
    becameOwner = true;
  }

  send(ws, {
    type: 'joined',
    channel: chNum,
    isOwner: ch.ownerId === ws.id,
    becameOwner,
    private: ch.private,
    history: ch.history,
    memberCount: ch.members.size
  });

  broadcast(chNum, { type: 'system', text: `${ws.name} joined the channel`, t: Date.now() }, ws);
  broadcast(chNum, { type: 'presence', memberCount: ch.members.size });
}

function buildReplySnippet(data) {
  if (!data.replyTo || !data.replyTo.mid) return null;
  const name = (data.replyTo.name || '').toString().slice(0, 18);
  const text = (data.replyTo.text || '').toString().slice(0, 120);
  return { mid: data.replyTo.mid.toString(), name, text };
}

function handleMessage(ws, data) {
  if (ws.channel == null) return;
  const text = (data.text || '').toString().slice(0, 500).trim();
  const image = typeof data.image === 'string' ? data.image : null;

  if (!text && !image) return;
  if (image && image.length > MAX_IMAGE_BYTES) {
    return send(ws, { type: 'error', text: 'that gif is too big' });
  }

  const ch = channels[ws.channel];
  const msg = {
    type: 'message',
    mid: 'm' + (nextMsgId++),
    id: ws.id,
    name: ws.name,
    text,
    image,
    replyTo: buildReplySnippet(data),
    reactions: {},
    t: Date.now()
  };
  pushHistory(ch, msg);
  broadcast(ws.channel, msg);
}

function handleReact(ws, data) {
  if (ws.channel == null) return;
  const ch = channels[ws.channel];
  const mid = (data.mid || '').toString();
  const emoji = (data.emoji || '').toString().slice(0, 8);
  if (!mid || !emoji) return;
  const msg = ch.history.find(m => m.mid === mid);
  if (!msg) return;

  if (!msg.reactions[emoji]) msg.reactions[emoji] = [];
  const idx = msg.reactions[emoji].indexOf(ws.id);
  if (idx >= 0) {
    msg.reactions[emoji].splice(idx, 1);
    if (msg.reactions[emoji].length === 0) delete msg.reactions[emoji];
  } else {
    msg.reactions[emoji].push(ws.id);
  }

  broadcast(ws.channel, { type: 'reaction', mid, reactions: msg.reactions });
}

function handleSetPrivate(ws, data) {
  if (ws.channel == null) return;
  const ch = channels[ws.channel];
  if (ch.ownerId !== ws.id) {
    return send(ws, { type: 'error', text: 'only the channel owner can change privacy' });
  }
  if (data.private) {
    const code = (data.code || '').toString().slice(0, 32);
    if (!code) return send(ws, { type: 'error', text: 'set a code to make it private' });
    ch.private = true;
    ch.code = code;
  } else {
    ch.private = false;
    ch.code = null;
  }
  broadcast(ws.channel, { type: 'privacyChanged', private: ch.private });
  broadcast(ws.channel, { type: 'system', text: `channel is now ${ch.private ? 'private' : 'public'}`, t: Date.now() });
}

function handleResetChannel(ws) {
  if (ws.channel == null) return;
  const ch = channels[ws.channel];
  if (ch.ownerId !== ws.id) {
    return send(ws, { type: 'error', text: 'only the channel owner can reset this channel' });
  }
  ch.history = [];
  broadcast(ws.channel, { type: 'channelReset' });
  broadcast(ws.channel, { type: 'system', text: 'the owner reset this channel', t: Date.now() });
}

function handleRename(ws, data) {
  const newName = (data.name || '').toString().trim().slice(0, 18);
  if (!newName || newName === ws.name) return;
  const oldName = ws.name;
  ws.name = newName;
  if (ws.channel != null) {
    broadcast(ws.channel, { type: 'system', text: `${oldName} is now known as ${newName}`, t: Date.now() }, ws);
  }
}

function handleTyping(ws) {
  if (ws.channel == null) return;
  broadcast(ws.channel, { type: 'typing', id: ws.id, name: ws.name }, ws);
}

const server = http.createServer((req, res) => {
  let filePath = req.url === '/' ? '/index.html' : req.url;
  filePath = path.join(__dirname, 'public', filePath);
  fs.readFile(filePath, (err, data) => {
    if (err) {
      res.writeHead(404);
      res.end('not found');
      return;
    }
    const ext = path.extname(filePath);
    const type = ext === '.js' ? 'application/javascript' : ext === '.css' ? 'text/css' : 'text/html';
    res.writeHead(200, { 'Content-Type': type });
    res.end(data);
  });
});

// raise the socket frame size limit a bit so gif data urls fit through
const wss = new WebSocketServer({ server, maxPayload: 2000000 });
let nextId = 1;

wss.on('connection', (ws) => {
  ws.id = 'u' + (nextId++);
  ws.channel = null;
  ws.name = 'unknown';

  send(ws, { type: 'hello', id: ws.id });

  ws.on('message', (raw) => {
    let data;
    try { data = JSON.parse(raw); } catch (e) { return; }
    switch (data.type) {
      case 'join': return handleJoin(ws, data);
      case 'message': return handleMessage(ws, data);
      case 'react': return handleReact(ws, data);
      case 'setPrivate': return handleSetPrivate(ws, data);
      case 'resetChannel': return handleResetChannel(ws);
      case 'rename': return handleRename(ws, data);
      case 'typing': return handleTyping(ws);
      default: return;
    }
  });

  ws.on('close', () => leaveChannel(ws));
});

server.listen(PORT, () => {
  console.log(`channelwave server listening on port ${PORT}`);
});
