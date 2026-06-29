const express = require('express');

function start(port, name) {
  const app = express();
  app.use(express.json());
  app.post('/internal/prepare', (_req, res) => res.json({ accepted: true, estimatedMins: 20 }));
  app.post('/internal/charge', (_req, res) => res.json({ charged: true, transactionId: `txn-${Date.now()}` }));
  app.post('/internal/dispatch', (_req, res) => res.json({ trackingId: `trk-${Date.now()}`, etaMinutes: 10 }));
  app.post('/internal/cancel', (_req, res) => res.json({ cancelled: true }));
  app.get('/health', (_req, res) => res.json({ status: 'UP' }));
  app.listen(port, () => console.log(`[MockService:${name}] Listening on :${port}`));
}

start(3001, 'kitchen');
start(3002, 'billing');
start(3003, 'delivery');
