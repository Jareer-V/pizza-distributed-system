const express = require('express');
const fs = require('fs');
const path = require('path');

const app = express();
const port = process.env.PORT || 3001;
const gatewayUrl = process.env.GATEWAY_URL || '/api/v1/order';

app.use(express.json());
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Headers', 'Content-Type, Authorization, Idempotency-Key');
  res.header('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
  if (req.method === 'OPTIONS') return res.sendStatus(204);
  next();
});

function renderFrontend(res) {
  const htmlPath = path.join(__dirname, 'frontend.html');
  let html = fs.readFileSync(htmlPath, 'utf8');
  html = html.replace('__GATEWAY_URL__', gatewayUrl);
  res.type('html').send(html);
}

function renderDashboard(res) {
  const htmlPath = path.join(__dirname, 'dashboard.html');
  let html = fs.readFileSync(htmlPath, 'utf8');
  html = html.replace('__GATEWAY_URL__', gatewayUrl);
  html = html.replace('__PORT__', String(port));
  res.type('html').send(html);
}

app.get('/', (_req, res) => renderFrontend(res));
app.get('/frontend.html', (_req, res) => renderFrontend(res));
app.get('/dashboard', (_req, res) => renderDashboard(res));
app.get('/report-arabic', (_req, res) => {
  res.sendFile(path.join(__dirname, 'report-arabic.html'));
});
app.get('/health', (_req, res) => res.json({ status: 'UP' }));

app.post('/api/v1/order', (req, res) => {
  const body = req.body || {};
  const orderId = `order-${Date.now()}`;
  res.status(201).json({
    orderId,
    status: 'CONFIRMED',
    transactionId: `txn-${Date.now()}`,
    tracking: `trk-${Date.now()}`,
    estimatedDelivery: '30 minutes',
    requestId: body.idempotencyKey || `req-${Date.now()}`,
    received: body,
  });
});

app.listen(port, '0.0.0.0', () => {
  console.log(`Frontend server listening on port ${port}`);
});
