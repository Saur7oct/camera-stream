require('dotenv').config();
const cluster = require('cluster');
const os = require('os');
const http = require('http');
const https = require('https');
const { URL } = require('url');

const NUM_WORKERS = parseInt(process.env.WORKERS || os.cpus().length, 10);
const SELF_URL = process.env.SELF_URL || '';
const KEEPALIVE_INTERVAL_MS = parseInt(
  process.env.KEEPALIVE_INTERVAL_MS || '840000',
  10
); // 14 minutes = 840000 ms

// ═══════════════════════════════════════════════════════════════════
// MASTER PROCESS
// ═══════════════════════════════════════════════════════════════════
if (cluster.isPrimary) {
  console.log(`[Master ${process.pid}] spawning ${NUM_WORKERS} workers`);
  cluster.schedulingPolicy = cluster.SCHED_RR;

  // Shared registry (master owns it)
  // streamId -> { streamId, workerId, deviceName, createdAt, viewers }
  const registry = new Map();

  for (let i = 0; i < NUM_WORKERS; i++) cluster.fork();

  for (const id in cluster.workers) {
    const worker = cluster.workers[id];

    worker.on('message', (msg) => {
      if (msg.type === 'register-stream') {
        registry.set(msg.streamId, {
          streamId: msg.streamId,
          deviceName: msg.deviceName || 'Camera',
          workerId: worker.id,
          createdAt: Date.now(),
          viewers: 0,
        });
        worker.send({
          type: 'stream-registered',
          streamId: msg.streamId,
          wsPublishPath: `/ws/publish/${msg.streamId}`,
          wsViewPath: `/ws/view/${msg.streamId}`,
        });
      } else if (msg.type === 'unregister-stream') {
        registry.delete(msg.streamId);
      } else if (msg.type === 'viewer-count') {
        const s = registry.get(msg.streamId);
        if (s) s.viewers = msg.count;
      } else if (msg.type === 'get-cameras') {
        worker.send({
          type: 'cameras-list',
          requestId: msg.requestId,
          cameras: Array.from(registry.values()),
        });
      }
    });
  }

  cluster.on('exit', (worker) => {
    console.log(`[Master] Worker ${worker.id} died — respawning`);
    cluster.fork();
  });

  // ═══════════════════════════════════════════════════════════════════
  // 🔥 SELF-TRIGGER / KEEP-ALIVE — Render ko 14 min me ping karta hai
  // ═══════════════════════════════════════════════════════════════════
  if (SELF_URL) {
    console.log(`[KeepAlive] will ping ${SELF_URL}/api/health every ${KEEPALIVE_INTERVAL_MS}ms`);

    const ping = () => {
      try {
        const url = new URL('/api/health', SELF_URL);
        const client = url.protocol === 'https:' ? https : http;
        const req = client.get(
          url,
          { timeout: 20000, headers: { 'User-Agent': 'RenderKeepAlive/1.0' } },
          (res) => {
            res.resume();
            console.log(`[KeepAlive] ${new Date().toISOString()} → ${res.statusCode}`);
          }
        );
        req.on('timeout', () => { req.destroy(); console.warn('[KeepAlive] timeout'); });
        req.on('error', (err) => console.warn('[KeepAlive] error:', err.message));
      } catch (err) {
        console.warn('[KeepAlive] failed:', err.message);
      }
    };

    // Pehla ping 30 sec baad (server warm hone do)
    setTimeout(ping, 30000);
    // Uske baad har 14 minutes
    setInterval(ping, KEEPALIVE_INTERVAL_MS);
  } else {
    console.log('[KeepAlive] disabled (SELF_URL not set)');
  }
} else {
  require('./worker');
}