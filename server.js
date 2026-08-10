const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');
const webpush = require('web-push');

const PORT = process.env.PORT || 3000;
const TOTAL_CHANNELS = 100;
const MAX_HISTORY = 200;
const MAX_IMAGE_BYTES = 1500000; // ~1.5MB base64 cap per image

// ---- push notification setup ----
// these defaults work out of the box. for your own production keys, generate your own
// with `npx web-push generate-vapid-keys` and set VAPID_PUBLIC_KEY / VAPID_PRIVATE_KEY
// as environment variables on your host instead.
const VAPID_PUBLIC_KEY = process.env.VAPID_PUBLIC_KEY || 'BA_7Wu_Y7HuCwVi0NjqneiY0KLi9XrbH4_08m3Odq5SIlxHQrpXKnE0MknFik739n6wky9-AoYazvgcRojzcvMo';
const VAPID_PRIVATE_KEY = process.env.VAPID_PRIVATE_KEY || 'Utw-Rex58osn83KPO-OD4hB_AVnnG2YMZSzQCEfUDRw';
webpush.setVapidDetails('mailto:admin@channelwave.app', VAPID_PUBLIC_KEY, VAPID_PRIVATE_KEY);

// subscriptions keyed by push endpoint: { subscription, clientId, channel }
const subsByEndpoint = new Map();

function pushToChannel(chNum, payload, excludeClientId) {
  for (const [endpoint, rec] of subsByEndpoint) {
    if (rec.channel !== chNum) continue;
    if (excludeClientId && rec.clientId === excludeClientId) continue;
    webpush.sendNotification(rec.subscription, JSON.stringify(payload)).catch((err) => {
      if (err && (err.statusCode === 404 || err.statusCode === 410)) {
        subsByEndpoint.delete(endpoint);
      }
    });
  }
}

function pushToName(name, payload) {
  const nameLower = name.toLowerCase();
  for (const [endpoint, rec] of subsByEndpoint) {
    if (!rec.name || rec.name.toLowerCase() !== nameLower) continue;
    webpush.sendNotification(rec.subscription, JSON.stringify(payload)).catch((err) => {
      if (err && (err.statusCode === 404 || err.statusCode === 410)) {
        subsByEndpoint.delete(endpoint);
      }
    });
  }
}

function deliverPendingInvites(ws) {
  const nameLower = ws.name.toLowerCase();
  const queued = pendingInvites.get(nameLower);
  if (!queued || queued.length === 0) return;
  queued.forEach((inv) => {
    send(ws, { type: 'inviteReceived', fromName: inv.fromName, channel: inv.channel });
  });
  pendingInvites.delete(nameLower);
}

// channel state: { ownerId, private, code, members: Set<ws>, history: [], order: [], inviteBypass: Set<name> }
const channels = {};
for (let i = 1; i <= TOTAL_CHANNELS; i++) {
  channels[i] = { ownerId: null, private: false, code: null, members: new Set(), history: [], order: [], inviteBypass: new Set() };
}

// invites queued for a name that isn't connected right now: nameLower -> [{fromName, channel, t}]
const pendingInvites = new Map();

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
  ch.order = ch.order.filter((w) => w !== ws);

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

  const nameLower = (data.name || 'unknown').toString().trim().slice(0, 18).toLowerCase();
  let bypassedByInvite = false;

  if (ch.private && ch.ownerId !== null && ch.ownerId !== ws.id) {
    if (ch.inviteBypass.has(nameLower)) {
      ch.inviteBypass.delete(nameLower);
      bypassedByInvite = true;
    } else if (!data.code || data.code !== ch.code) {
      return send(ws, { type: 'needCode', channel: chNum });
    }
  }

  leaveChannel(ws);

  ws.name = (data.name || 'unknown').toString().slice(0, 18);
  ws.clientId = (data.clientId || '').toString().slice(0, 64) || null;
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
  deliverPendingInvites(ws);
}

function handleInvite(ws, data) {
  if (ws.channel == null) {
    return send(ws, { type: 'error', text: 'join a channel before inviting someone' });
  }
  const toName = (data.toName || '').toString().trim().slice(0, 18);
  if (!toName) return;
  const toNameLower = toName.toLowerCase();
  if (toNameLower === ws.name.toLowerCase()) return;

  const chNum = ws.channel;
  const ch = channels[chNum];

  // grant a one-time bypass so they can get straight in even if the channel is private
  ch.inviteBypass.add(toNameLower);

  // deliver immediately to any live connection using that name
  let deliveredLive = false;
  wss.clients.forEach((client) => {
    if (client.name && client.name.toLowerCase() === toNameLower && client !== ws) {
      send(client, { type: 'inviteReceived', fromName: ws.name, channel: chNum });
      deliveredLive = true;
    }
  });

  // queue it too, in case they're not connected right now but open the app later under that name
  if (!deliveredLive) {
    if (!pendingInvites.has(toNameLower)) pendingInvites.set(toNameLower, []);
    pendingInvites.get(toNameLower).push({ fromName: ws.name, channel: chNum, t: Date.now() });
  }

  // push a real notification in case their app/site is fully closed
  pushToName(toName, {
    title: 'Channel invite',
    body: `${ws.name} invited you to channel ${String(chNum).padStart(2, '0')}`,
    url: '/'
  });

  send(ws, { type: 'inviteSent', toName, delivered: deliveredLive });
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

  pushToChannel(
    ws.channel,
    { title: ws.name, body: text || 'sent a gif', url: '/' },
    ws.clientId
  );
}

function handleReact(ws, data) {
  if (ws.channel == null) return;
  const ch = channels[ws.channel];
  const mid = (data.mid || '').toString();
  const emoji = (data.emoji || '').toString().slice(0, 8);
  if (!mid || !emoji) return;
  const msg = ch.history.find((m) => m.mid === mid);
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
  deliverPendingInvites(ws);
}

function handleTyping(ws) {
  if (ws.channel == null) return;
  broadcast(ws.channel, { type: 'typing', id: ws.id, name: ws.name }, ws);
}

// ---- plain http body reader for the push-subscription endpoints ----
function readJsonBody(req, cb) {
  let body = '';
  req.on('data', (chunk) => {
    body += chunk;
    if (body.length > 200000) req.destroy(); // guard against absurd payloads
  });
  req.on('end', () => {
    try { cb(null, JSON.parse(body || '{}')); } catch (e) { cb(e, null); }
  });
}

const server = http.createServer((req, res) => {
  const url = req.url.split('?')[0];

  if (req.method === 'GET' && url === '/vapidPublicKey') {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    return res.end(VAPID_PUBLIC_KEY);
  }

  if (req.method === 'POST' && url === '/subscribe') {
    return readJsonBody(req, (err, data) => {
      if (err || !data || !data.subscription || !data.subscription.endpoint) {
        res.writeHead(400);
        return res.end('bad request');
      }
      const chNum = Number(data.channel);
      subsByEndpoint.set(data.subscription.endpoint, {
        subscription: data.subscription,
        clientId: (data.clientId || '').toString().slice(0, 64) || null,
        channel: Number.isInteger(chNum) ? chNum : null,
        name: (data.name || '').toString().slice(0, 18) || null
      });
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: true }));
    });
  }

  if (req.method === 'POST' && url === '/unsubscribe') {
    return readJsonBody(req, (err, data) => {
      if (!err && data && data.endpoint) subsByEndpoint.delete(data.endpoint);
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: true }));
    });
  }

  // static file serving (public/, plus manifest + service worker at root)
  let filePath = url === '/' ? '/index.html' : url;
  filePath = path.join(__dirname, 'public', filePath);
  fs.readFile(filePath, (fsErr, data) => {
    if (fsErr) {
      res.writeHead(404);
      return res.end('not found');
    }
    const ext = path.extname(filePath);
    const type =
      ext === '.js' ? 'application/javascript' :
      ext === '.css' ? 'text/css' :
      ext === '.json' ? 'application/json' :
      ext === '.svg' ? 'image/svg+xml' :
      'text/html';
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
  ws.clientId = null;

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
      case 'invite': return handleInvite(ws, data);
      default: return;
    }
  });

  ws.on('close', () => leaveChannel(ws));
});

server.listen(PORT, () => {
  console.log(`channelwave server listening on port ${PORT}`);
});
