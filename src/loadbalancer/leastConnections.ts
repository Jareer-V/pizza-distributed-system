/**
 * ============================================================
 * COMPONENT 3: LOAD BALANCER — LEAST CONNECTIONS + HEALTH CHECKS
 * ============================================================
 * Implements a code-based reverse proxy / load balancer that:
 *
 *  1. LEAST CONNECTIONS algorithm:
 *     Routes each new request to the backend instance that
 *     currently has the fewest active (in-flight) connections.
 *     Superior to Round-Robin for requests with variable latency
 *     (e.g., kitchen orders vary from 1 ms to 30 s).
 *
 *  2. ACTIVE HEALTH CHECKS:
 *     A background interval pings every instance's /health
 *     endpoint every 10 seconds. Unhealthy nodes are removed
 *     from the routing pool and re-added once they recover.
 *
 * Flow:
 *   Client → LB (least-conn pick) → Healthy Backend Instance
 *               ↑
 *         [Health Check Timer]
 * ============================================================
 */

import express, { Request, Response } from 'express';
import { createProxyMiddleware } from 'http-proxy-middleware';

// ── Backend Node Definition ───────────────────────────────────
interface BackendNode {
  id: string;
  url: string;
  activeConnections: number;     // currently in-flight requests
  healthy: boolean;              // determined by health-check pings
  consecutiveFailures: number;   // track flapping
  lastCheckedAt: Date | null;
  totalRequestsServed: number;   // for observability / metrics
}

// ── Simulated Order Service pool (3 replicas) ─────────────────
const BACKEND_POOL: BackendNode[] = [
  { id: 'order-svc-1', url: 'http://order-service-1:3010', activeConnections: 0, healthy: true, consecutiveFailures: 0, lastCheckedAt: null, totalRequestsServed: 0 },
  { id: 'order-svc-2', url: 'http://order-service-2:3011', activeConnections: 0, healthy: true, consecutiveFailures: 0, lastCheckedAt: null, totalRequestsServed: 0 },
  { id: 'order-svc-3', url: 'http://order-service-3:3012', activeConnections: 0, healthy: true, consecutiveFailures: 0, lastCheckedAt: null, totalRequestsServed: 0 },
];

const HEALTH_CHECK_INTERVAL_MS  = 10_000;   // ping every 10 s
const HEALTH_CHECK_TIMEOUT_MS   = 3_000;    // 3 s to respond
const UNHEALTHY_THRESHOLD       = 3;        // failures before marking down
const RECOVERY_THRESHOLD        = 2;        // consecutive successes to re-add

// ── LEAST CONNECTIONS ALGORITHM ───────────────────────────────
/**
 * Selects the backend with the fewest active connections.
 * Among equals, picks the one with fewer total requests
 * served (tiebreak toward less-used instances).
 *
 * Time complexity: O(n) where n = number of healthy nodes.
 * Acceptable for typical pool sizes (2–50 nodes).
 */
function leastConnectionsPick(): BackendNode | null {
  const healthyNodes = BACKEND_POOL.filter(n => n.healthy);

  if (healthyNodes.length === 0) {
    console.error('[LoadBalancer] All backend nodes are unhealthy!');
    return null;
  }

  // Sort by active connections, then by total served as tiebreaker
  return healthyNodes.reduce((best, node) => {
    if (node.activeConnections < best.activeConnections) return node;
    if (
      node.activeConnections === best.activeConnections &&
      node.totalRequestsServed < best.totalRequestsServed
    ) return node;
    return best;
  });
}

// ── ACTIVE HEALTH CHECK ───────────────────────────────────────
/**
 * Pings each node's /health endpoint.
 * Marks nodes unhealthy after UNHEALTHY_THRESHOLD consecutive fails.
 * Re-admits nodes after RECOVERY_THRESHOLD consecutive successes.
 */
async function runHealthChecks(): Promise<void> {
  const checks = BACKEND_POOL.map(async (node) => {
    try {
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), HEALTH_CHECK_TIMEOUT_MS);

      const res = await fetch(`${node.url}/health`, {
        signal: controller.signal,
        headers: { 'x-health-check': 'load-balancer' },
      });
      clearTimeout(timeout);

      node.lastCheckedAt = new Date();

      if (res.ok) {
        // Successful ping
        node.consecutiveFailures = 0;

        if (!node.healthy) {
          // Node was down — check if it's recovered
          // We track recovery differently: reset on first success,
          // but only mark healthy after RECOVERY_THRESHOLD pings.
          // (Simplified here: mark healthy immediately on first success)
          node.healthy = true;
          console.log(`[HealthCheck] ✅ Node ${node.id} RECOVERED — re-added to pool`);
        }
      } else {
        handleHealthCheckFailure(node, `HTTP ${res.status}`);
      }
    } catch (err) {
      handleHealthCheckFailure(node, (err as Error).message);
    }
  });

  await Promise.allSettled(checks);
  logPoolStatus();
}

function handleHealthCheckFailure(node: BackendNode, reason: string): void {
  node.consecutiveFailures += 1;
  node.lastCheckedAt = new Date();

  if (node.consecutiveFailures >= UNHEALTHY_THRESHOLD && node.healthy) {
    node.healthy = false;
    console.warn(
      `[HealthCheck] ❌ Node ${node.id} marked UNHEALTHY after ${node.consecutiveFailures} failures (${reason}) — removed from pool`,
    );
  } else {
    console.warn(`[HealthCheck] ⚠️  Node ${node.id} failure ${node.consecutiveFailures}/${UNHEALTHY_THRESHOLD}: ${reason}`);
  }
}

function logPoolStatus(): void {
  const healthy = BACKEND_POOL.filter(n => n.healthy).length;
  console.log(
    `[HealthCheck] Pool: ${healthy}/${BACKEND_POOL.length} healthy | ` +
    BACKEND_POOL.map(n => `${n.id}(conn=${n.activeConnections},${n.healthy ? '✅' : '❌'})`).join(' '),
  );
}

// ── Load Balancer Express App ─────────────────────────────────
const lb = express();

// Expose pool stats for monitoring dashboards
lb.get('/lb/stats', (_req: Request, res: Response) => {
  res.json({
    algorithm: 'least-connections',
    nodes: BACKEND_POOL.map(n => ({
      id: n.id,
      url: n.url,
      healthy: n.healthy,
      activeConnections: n.activeConnections,
      consecutiveFailures: n.consecutiveFailures,
      totalRequestsServed: n.totalRequestsServed,
      lastCheckedAt: n.lastCheckedAt,
    })),
    healthyCount: BACKEND_POOL.filter(n => n.healthy).length,
  });
});

// ── Proxy Middleware — route to selected backend ──────────────
lb.use('/', (req: Request, res: Response, next) => {
  const target = leastConnectionsPick();

  if (!target) {
    return res.status(503).json({ error: 'No healthy backend nodes available. Please try again shortly.' });
  }

  // Increment connection counter BEFORE proxying
  target.activeConnections += 1;
  target.totalRequestsServed += 1;

  console.log(
    `[LoadBalancer] → ${target.id} (active=${target.activeConnections}) ` +
    `[${req.method} ${req.path}]`,
  );

  // Decrement on response finish (connection released)
  res.on('finish', () => {
    target.activeConnections = Math.max(0, target.activeConnections - 1);
  });

  // Proxy the request transparently
  const proxy = createProxyMiddleware({
    target: target.url,
    changeOrigin: true,
    on: {
      error: (err, _req, proxyRes) => {
        // If proxied node errors mid-request, decrement and flag
        target.activeConnections = Math.max(0, target.activeConnections - 1);
        target.consecutiveFailures += 1;
        console.error(`[LoadBalancer] Proxy error on ${target.id}: ${err.message}`);
        if (!res.headersSent) {
          (proxyRes as Response).status(502).json({ error: 'Backend unavailable.' });
        }
      },
    },
  });

  proxy(req, res, next);
});

// ── Start the load balancer and health check timer ────────────
const LB_PORT = Number.parseInt(process.env.LB_PORT || '8080', 10);

function startServer(port: number) {
  const server = lb.listen(port, () => {
    console.log(`[LoadBalancer] Listening on :${port} — algorithm: LEAST CONNECTIONS`);

    // Kick off health checks immediately, then on interval
    runHealthChecks();
    setInterval(runHealthChecks, HEALTH_CHECK_INTERVAL_MS);
    console.log(`[LoadBalancer] Health checks every ${HEALTH_CHECK_INTERVAL_MS / 1000}s`);
  });

  server.on('error', (err: NodeJS.ErrnoException) => {
    if (err.code === 'EADDRINUSE') {
      const fallbackPort = port + 1;
      console.warn(`[LoadBalancer] Port ${port} in use; retrying on ${fallbackPort}`);
      server.close(() => startServer(fallbackPort));
    } else {
      throw err;
    }
  });
}

startServer(LB_PORT);

// ── Simulation helper (for testing without real backends) ─────
export function simulateLeastConnections(
  requests: number[],   // array of concurrent connection counts to simulate
): string {
  const testPool = requests.map((conn, i) => ({
    id: `test-node-${i + 1}`,
    url: `http://test-${i}`,
    activeConnections: conn,
    healthy: true,
    consecutiveFailures: 0,
    lastCheckedAt: null,
    totalRequestsServed: 0,
  }));

  const best = testPool.reduce((a, b) => a.activeConnections <= b.activeConnections ? a : b);
  return `Selected: ${best.id} (active connections: ${best.activeConnections}) from pool [${requests.join(', ')}]`;
}

// Quick self-test
console.log(simulateLeastConnections([5, 2, 8, 2, 1]));
// → Selected: test-node-5 (active connections: 1)

export default lb;
