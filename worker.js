const express = require('express');
const { WebSocketServer } = require('ws');
const { nanoid } = require('nanoid');
const { z } = require('zod');
const http = require('http');
const path = require('path');
const fs = require('fs');
const compression = require('compression');

const PORT = parseInt(process.env.PORT || 3000, 10);
const CORS_ORIGIN = process.env.CORS_ORIGIN || '*';

const app = express();
app.use(compression());

// ═══════════════════════════════════════════════════════════════════
// CORS — Netlify (frontend) se Render (backend) ko allow karta hai
// ═══════════════════════════════════════════════════════════════════
app.use((req, res, next) => {
  res.setHeader('Access-Control-Allow-Origin', CORS_ORIGIN);
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  res.setHeader('Access-Control-Max-Age', '86400');
  if (req.method === 'OPTIONS') return res.sendStatus(204);
  next();
});

// ═══════════════════════════════════════════════════════════════════
// STATIC — index.html serve (optional, agar backend ke root se bhi UI chahiye)
// ═══════════════════════════════════════════════════════════════════
app.get('/', (req, res) => {
  const idx = path.join(__dirname, 'index.html');
  if (fs.existsSync(idx)) return res.sendFile(idx);
  res.json({ ok: true, msg: 'IP Camera API', health: '/api/health' });
});
app.get('/index.html', (req, res) => {
  const idx = path.join(__dirname, 'index.html');
  if (fs.existsSync(idx)) return res.sendFile(idx);
  res.status(404).send('Not found');
});

// ═══════════════════════════════════════════════════════════════════
// HEALTH (keep-alive ping yahi hit karta hai)
// ═══════════════════════════════════════════════════════════════════
app.get('/api/health', (req, res) => {
  res.json({
    ok: true,
    pid: process.pid,
    uptime: Math.floor(process.uptime()),
    publishers: publishers.size,
    time: new Date().toISOString(),
  });
});

// ═══════════════════════════════════════════════════════════════════
// API: create stream
// ═══════════════════════════════════════════════════════════════════
const createStreamSchema = z.object({
  deviceName: z.string().min(1).max(64),
  desiredSubdomain: z
    .string()
    .min(3)
    .max(24)
    .regex(/^[a-z0-9][a-z0-9-]*[a-z0-9]$/)
    .optional(),
});

app.post('/api/streams', express.json({ limit: '32kb' }), (req, res) => {
  let parsed;
  try {
    parsed = createStreamSchema.parse(req.body);
  } catch (err) {
    return res.status(400).json({ error: err.issues || String(err) });
  }

  let streamId = parsed.desiredSubdomain
    ? parsed.desiredSubdomain
    : nanoid(10).replace(/[-_A-Z]/g, 'a');

  process.send?.({
    type: 'register-stream',
    streamId,
    deviceName: parsed.deviceName,
  });

  const onRegistered = (msg) => {
    if (msg.type === 'stream-registered' && msg.streamId === streamId) {
      process.off('message', onRegistered);
      clearTimeout(timer);
      res.json({
        streamId,
        wsPublishPath: msg.wsPublishPath,
        wsViewPath: msg.wsViewPath,
        viewUrlPath: `/view/${streamId}`,
        broadcastUrlPath: `/broadcast/${streamId}`,
      });
    }
  };
  process.on('message', onRegistered);

  const timer = setTimeout(() => {
    process.off('message', onRegistered);
    if (!res.headersSent) {
      res.json({
        streamId,
        wsPublishPath: `/ws/publish/${streamId}`,
        wsViewPath: `/ws/view/${streamId}`,
        viewUrlPath: `/view/${streamId}`,
        broadcastUrlPath: `/broadcast/${streamId}`,
      });
    }
  }, 800);
});

// ═══════════════════════════════════════════════════════════════════
// API: list cameras
// ═══════════════════════════════════════════════════════════════════
app.get('/api/cameras', (req, res) => {
  const requestId = nanoid(8);
  process.send?.({ type: 'get-cameras', requestId });

  const onList = (msg) => {
    if (msg.type === 'cameras-list' && msg.requestId === requestId) {
      process.off('message', onList);
      clearTimeout(timer);
      res.json({ cameras: msg.cameras, total: msg.cameras.length });
    }
  };
  process.on('message', onList);

  const timer = setTimeout(() => {
    process.off('message', onList);
    if (!res.headersSent) {
      const local = [];
      for (const [id, ws] of publishers) {
        if (ws.readyState === ws.OPEN) {
          local.push({
            streamId: id,
            deviceName: 'Camera',
            viewers: viewers.get(id)?.size || 0,
          });
        }
      }
      res.json({ cameras: local, total: local.length });
    }
  }, 500);
});

// ═══════════════════════════════════════════════════════════════════
// WEBSOCKET SERVERS
// ═══════════════════════════════════════════════════════════════════
const server = http.createServer(app);
const wssPublisher = new WebSocketServer({ noServer: true });
const wssViewer = new WebSocketServer({ noServer: true });

const publishers = new Map(); // streamId -> ws
const viewers = new Map();    // streamId -> Set<ws>

const MAX_VIEWER_BUFFER = 2 * 1024 * 1024;

server.on('upgrade', (req, socket, head) => {
  // Handle both /ws/publish/ID and /ws/view/ID
  const url = new URL(req.url, 'http://x');
  const pubMatch = url.pathname.match(/^\/ws\/publish\/([A-Za-z0-9]+)$/);
  const viewMatch = url.pathname.match(/^\/ws\/view\/([A-Za-z0-9]+)$/);

  if (pubMatch) {
    wssPublisher.handleUpgrade(req, socket, head, (ws) => {
      const streamId = pubMatch[1];
      publishers.set(streamId, ws);

      ws.on('message', (data, isBinary) => {
        if (!isBinary) return;
        const set = viewers.get(streamId);
        if (!set) return;
        for (const v of set) {
          if (v.readyState === v.OPEN && v.bufferedAmount < MAX_VIEWER_BUFFER) {
            v.send(data, { binary: true });
          }
        }
      });

      ws.on('close', () => {
        publishers.delete(streamId);
        process.send?.({ type: 'unregister-stream', streamId });
        const set = viewers.get(streamId);
        if (set) for (const v of set) v.close();
        viewers.delete(streamId);
      });

      ws.on('error', () => {});
    });
  } else if (viewMatch) {
    wssViewer.handleUpgrade(req, socket, head, (ws) => {
      const streamId = viewMatch[1];
      if (!viewers.has(streamId)) viewers.set(streamId, new Set());
      viewers.get(streamId).add(ws);
      process.send?.({
        type: 'viewer-count',
        streamId,
        count: viewers.get(streamId).size,
      });

      ws.on('close', () => {
        viewers.get(streamId)?.delete(ws);
        process.send?.({
          type: 'viewer-count',
          streamId,
          count: viewers.get(streamId)?.size || 0,
        });
      });

      ws.on('error', () => {});
    });
  } else {
    socket.destroy();
  }
});

server.listen(PORT, () => {
  console.log(`[Worker ${process.pid}] listening on ${PORT}`);
});