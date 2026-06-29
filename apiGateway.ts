/**
 * ============================================================
 * COMPONENT 1: API GATEWAY — TRANSPARENCY LAYER
 * ============================================================
 * Acts as the single entry point for all client requests.
 * Provides Location Transparency (clients don't know where
 * services live) and Replication Transparency (clients don't
 * know how many instances exist behind the gateway).
 *
 * Architecture:
 *   Client → POST /api/v1/order
 *             ↓
 *         [API Gateway]
 *        ↙     ↓      ↘
 *   Kitchen  Billing  Delivery   (internal microservices)
 * ============================================================
 */

import express, { Request, Response, NextFunction } from 'express';
import { v4 as uuidv4 } from 'uuid';

const app = express();
app.use(express.json());

// ── Service Registry ──────────────────────────────────────────
// In production this would be a service-discovery system (Consul,
// Kubernetes DNS). Here we simulate it with a static map.
const SERVICE_REGISTRY: Record<string, string> = {
  kitchen:  process.env.KITCHEN_URL  || 'http://kitchen-service:3001',
  billing:  process.env.BILLING_URL  || 'http://billing-service:3002',
  delivery: process.env.DELIVERY_URL || 'http://delivery-service:3003',
};

// ── Types ─────────────────────────────────────────────────────
interface OrderRequest {
  customerId: string;
  items: Array<{ productId: string; name: string; qty: number; price: number }>;
  deliveryAddress: string;
  paymentMethod: 'card' | 'cash';
  idempotencyKey?: string;          // forwarded for downstream dedup
}

interface ServiceResponse<T = unknown> {
  ok: boolean;
  data?: T;
  error?: string;
}

// ── Correlation / Request-ID Middleware ───────────────────────
// Every request gets a unique trace ID so logs across all
// microservices can be correlated (Access Transparency).
app.use((req: Request, _res: Response, next: NextFunction) => {
  req.headers['x-request-id'] = req.headers['x-request-id'] || uuidv4();
  next();
});

// ── Health endpoint (used by load balancer) ───────────────────
app.get('/health', (_req: Request, res: Response) => {
  res.json({ status: 'UP', service: 'api-gateway', ts: new Date().toISOString() });
});

// ── Internal HTTP helper ──────────────────────────────────────
async function callService<T>(
  serviceName: string,
  path: string,
  payload: unknown,
  requestId: string,
): Promise<ServiceResponse<T>> {
  const baseUrl = SERVICE_REGISTRY[serviceName];
  if (!baseUrl) return { ok: false, error: `Unknown service: ${serviceName}` };

  try {
    const res = await fetch(`${baseUrl}${path}`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-request-id': requestId,                   // propagate trace ID
        'x-gateway-secret': process.env.GATEWAY_SECRET || 'dev-secret',
      },
      body: JSON.stringify(payload),
      signal: AbortSignal.timeout(8_000),             // 8 s timeout per call
    });

    const body = await res.json() as T;
    return { ok: res.ok, data: body };
  } catch (err) {
    return { ok: false, error: (err as Error).message };
  }
}

// ── Core Route: POST /api/v1/order ────────────────────────────
/**
 * Gateway Orchestration Flow:
 *  1. Validate request
 *  2. Call Kitchen Service  → validates ingredients & accepts order
 *  3. Call Billing Service  → charges the customer
 *  4. Call Delivery Service → dispatches a rider
 *
 * The client sees ONE endpoint. Where each service runs, how many
 * replicas exist, or which data-centre handles the request is
 * completely hidden — this is Location + Replication Transparency.
 */
app.post('/api/v1/order', async (req: Request, res: Response) => {
  const requestId = req.headers['x-request-id'] as string;
  const body = req.body as OrderRequest;

  // ── 1. Basic input validation ──────────────────────────────
  if (!body.customerId || !body.items?.length || !body.deliveryAddress) {
    return res.status(400).json({
      error: 'customerId, items, and deliveryAddress are required.',
      requestId,
    });
  }

  const orderId = uuidv4();
  const idempotencyKey = body.idempotencyKey || uuidv4();

  console.log(`[Gateway] [${requestId}] New order ${orderId} from customer ${body.customerId}`);

  // ── 2. Kitchen Service ─────────────────────────────────────
  const kitchenRes = await callService<{ accepted: boolean; estimatedMins: number }>(
    'kitchen',
    '/internal/prepare',
    { orderId, items: body.items, requestId },
    requestId,
  );

  if (!kitchenRes.ok || !kitchenRes.data?.accepted) {
    console.warn(`[Gateway] [${requestId}] Kitchen rejected order: ${kitchenRes.error}`);
    return res.status(503).json({
      error: 'Kitchen is unable to accept the order right now. Please try again.',
      requestId,
    });
  }

  // ── 3. Billing Service ─────────────────────────────────────
  const total = body.items.reduce((sum, i) => sum + i.price * i.qty, 0);

  const billingRes = await callService<{ charged: boolean; transactionId: string }>(
    'billing',
    '/internal/charge',
    {
      orderId,
      customerId: body.customerId,
      amount: total,
      paymentMethod: body.paymentMethod,
      idempotencyKey,                                 // prevents double-charge
      requestId,
    },
    requestId,
  );

  if (!billingRes.ok || !billingRes.data?.charged) {
    // Compensating transaction: tell kitchen to cancel
    await callService('kitchen', '/internal/cancel', { orderId }, requestId);
    console.warn(`[Gateway] [${requestId}] Billing failed — kitchen order cancelled`);
    return res.status(402).json({
      error: 'Payment failed. Please check your payment details.',
      requestId,
    });
  }

  // ── 4. Delivery Service ────────────────────────────────────
  const deliveryRes = await callService<{ trackingId: string; etaMinutes: number }>(
    'delivery',
    '/internal/dispatch',
    {
      orderId,
      customerId: body.customerId,
      deliveryAddress: body.deliveryAddress,
      estimatedMins: kitchenRes.data.estimatedMins,
      requestId,
    },
    requestId,
  );

  if (!deliveryRes.ok) {
    // Non-fatal: order is paid, rider assignment can be retried async.
    console.warn(`[Gateway] [${requestId}] Delivery dispatch deferred: ${deliveryRes.error}`);
  }

  // ── 5. Unified response to client ─────────────────────────
  // Client has no visibility into the 3 internal service calls.
  return res.status(201).json({
    orderId,
    status: 'CONFIRMED',
    transactionId: billingRes.data?.transactionId,
    tracking: deliveryRes.data?.trackingId ?? 'PENDING',
    estimatedDelivery: `${kitchenRes.data.estimatedMins + (deliveryRes.data?.etaMinutes ?? 10)} minutes`,
    requestId,
  });
});

// ── Global error handler ───────────────────────────────────────
app.use((err: Error, _req: Request, res: Response, _next: NextFunction) => {
  console.error('[Gateway] Unhandled error:', err.message);
  res.status(500).json({ error: 'Internal gateway error.' });
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () =>
  console.log(`[API Gateway] Listening on :${PORT} — single entry point active`),
);

export default app;
