require('dotenv').config();
const cluster = require('cluster');
const os = require('os');
const http = require('http');
const https = require('https');
const { URL } = require('url');

const NUM_WORKERS = parseInt(process.env.WORKERS || os.cpus().length, 10);
const SELF_URL = process.env.SELF_URL || '';
const KEEPALIVE_INTERVAL_MS = parseInt(process.env.KEEPALIVE_INTERVAL_MS || '840000', 10); // 14 mins

if (cluster.isPrimary) {
  console.log(`[Master ${process.pid}] spawning ${NUM_WORKERS} workers`);
  cluster.schedulingPolicy = cluster.SCHED_RR;

  const registry = new Map(); // streamId -> { streamId, workerId, deviceName, createdAt, viewers }

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

  // 🔥 SELF-TRIGGER (Render ko active rakhne ke liye)
  if (SELF_URL) {
    const ping = () => {
      try {
        const url = new URL('/api/health', SELF_URL);
        const client = url.protocol === 'https:' ? https : http;
        const req = client.get(url, { timeout: 20000 }, (res) => {
          res.resume();
          console.log(`[KeepAlive] ping → ${res.statusCode}`);
        });
        req.on('timeout', () => req.destroy());
        req.on('error', (err) => console.warn('[KeepAlive] error:', err.message));
      } catch (err) {}
    };
    setTimeout(ping, 30000);
    setInterval(ping, KEEPALIVE_INTERVAL_MS);
  }
} else {
  require('./worker');
}
