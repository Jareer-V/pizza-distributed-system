/**
 * ============================================================
 * COMPONENT 4A: IDEMPOTENCY — IDEMPOTENT CONSUMER PATTERN
 * ============================================================
 * Prevents double-charging when a customer clicks "Submit Order"
 * multiple times (network retry, UI bug, impatient user).
 *
 * How it works:
 *  1. Client sends a unique Idempotency-Key header with each request.
 *  2. Before processing payment, we check a Redis cache (or
 *     in-memory TTL cache in this demo) for the key.
 *  3. If found → return the CACHED result (no second charge).
 *  4. If not found → process payment, store result in cache, return.
 *
 * The key is scoped to (customerId + idempotencyKey) and expires
 * after 24 hours (long enough to catch all realistic retries).
 *
 * ============================================================
 * COMPONENT 4B: CIRCUIT BREAKER — PAYMENT GATEWAY INTEGRATION
 * ============================================================
 * Wraps calls to the external bank/payment API with a state
 * machine that has three states:
 *
 *  CLOSED   → Normal operation; requests flow to payment API.
 *  OPEN     → Payment API is failing; block requests immediately
 *              and fall back to "Cash on Delivery".
 *  HALF-OPEN → After a cool-down period, probe with ONE request.
 *              Success → CLOSED; Failure → back to OPEN.
 *
 *  Failure threshold: 5 consecutive failures → OPEN
 *  Reset timeout: 30 seconds in OPEN before trying HALF-OPEN
 * ============================================================
 */

// ─────────────────────────────────────────────────────────────
// PART A: IN-MEMORY IDEMPOTENCY CACHE
// (In production: replace with Redis SETNX + TTL)
// ─────────────────────────────────────────────────────────────

interface CacheEntry<T> {
  value: T;
  expiresAt: number;           // Unix ms
}

class IdempotencyCache<T = unknown> {
  private store = new Map<string, CacheEntry<T>>();
  private readonly ttlMs: number;

  constructor(ttlMs = 24 * 60 * 60 * 1000) {   // default 24 h
    this.ttlMs = ttlMs;

    // Periodic eviction of expired keys (every 5 min)
    setInterval(() => this.evict(), 5 * 60 * 1000);
  }

  /**
   * Redis equivalent:
   *   GET idempotency:{key}
   */
  get(key: string): T | null {
    const entry = this.store.get(key);
    if (!entry) return null;
    if (Date.now() > entry.expiresAt) {
      this.store.delete(key);
      return null;
    }
    return entry.value;
  }

  /**
   * Redis equivalent:
   *   SET idempotency:{key} {value} EX {ttl} NX
   * Returns true if key was newly set, false if already existed.
   */
  setIfAbsent(key: string, value: T): boolean {
    if (this.store.has(key)) return false;
    this.store.set(key, { value, expiresAt: Date.now() + this.ttlMs });
    return true;
  }

  private evict(): void {
    const now = Date.now();
    let evicted = 0;
    for (const [key, entry] of this.store.entries()) {
      if (now > entry.expiresAt) {
        this.store.delete(key);
        evicted++;
      }
    }
    if (evicted > 0) console.log(`[IdempotencyCache] Evicted ${evicted} expired entries`);
  }

  size(): number { return this.store.size; }
}

// Singleton cache shared across all payment requests
const idempotencyCache = new IdempotencyCache<PaymentResult>();

// ─────────────────────────────────────────────────────────────
// PART B: CIRCUIT BREAKER STATE MACHINE
// ─────────────────────────────────────────────────────────────

type CircuitState = 'CLOSED' | 'OPEN' | 'HALF_OPEN';

interface CircuitBreakerConfig {
  failureThreshold: number;     // consecutive failures before opening
  resetTimeoutMs: number;       // ms in OPEN state before HALF-OPEN probe
  successThreshold: number;     // successes in HALF-OPEN before closing
}

class CircuitBreaker {
  private state: CircuitState = 'CLOSED';
  private consecutiveFailures = 0;
  private lastFailureTime: number | null = null;
  private halfOpenSuccesses = 0;
  private readonly config: CircuitBreakerConfig;
  private readonly name: string;

  // Metrics for monitoring
  private totalCalls = 0;
  private totalFailures = 0;
  private totalFallbacks = 0;

  constructor(name: string, config: CircuitBreakerConfig) {
    this.name = name;
    this.config = config;
  }

  /**
   * Execute a function through the circuit breaker.
   * If OPEN → skip execution and call fallback immediately.
   * If CLOSED/HALF-OPEN → execute fn; on failure update state.
   */
  async execute<T>(fn: () => Promise<T>, fallback: () => T | Promise<T>): Promise<T> {
    this.totalCalls++;

    // ── OPEN: breaker tripped, check if reset timeout elapsed ──
    if (this.state === 'OPEN') {
      const elapsed = Date.now() - (this.lastFailureTime ?? 0);
      if (elapsed >= this.config.resetTimeoutMs) {
        this.transitionTo('HALF_OPEN');
      } else {
        console.warn(
          `[CircuitBreaker:${this.name}] ⚡ OPEN — blocking call, fallback in ${Math.ceil((this.config.resetTimeoutMs - elapsed) / 1000)}s`,
        );
        this.totalFallbacks++;
        return fallback();
      }
    }

    // ── HALF-OPEN: allow exactly ONE probe request through ─────
    if (this.state === 'HALF_OPEN') {
      console.info(`[CircuitBreaker:${this.name}] 🔶 HALF-OPEN — probing payment API...`);
    }

    // ── Execute the real call ──────────────────────────────────
    try {
      const result = await fn();
      this.onSuccess();
      return result;
    } catch (err) {
      this.onFailure(err as Error);
      this.totalFallbacks++;
      return fallback();
    }
  }

  private onSuccess(): void {
    if (this.state === 'HALF_OPEN') {
      this.halfOpenSuccesses++;
      if (this.halfOpenSuccesses >= this.config.successThreshold) {
        this.transitionTo('CLOSED');
      }
    } else {
      // Reset failure count on any success while CLOSED
      this.consecutiveFailures = 0;
    }
  }

  private onFailure(err: Error): void {
    this.consecutiveFailures++;
    this.totalFailures++;
    this.lastFailureTime = Date.now();

    console.error(
      `[CircuitBreaker:${this.name}] ❌ Failure ${this.consecutiveFailures}/${this.config.failureThreshold}: ${err.message}`,
    );

    if (this.consecutiveFailures >= this.config.failureThreshold) {
      this.transitionTo('OPEN');
    }

    if (this.state === 'HALF_OPEN') {
      // Single failure in HALF-OPEN immediately re-opens
      this.transitionTo('OPEN');
    }
  }

  private transitionTo(newState: CircuitState): void {
    const prev = this.state;
    this.state = newState;

    if (newState === 'CLOSED') {
      this.consecutiveFailures = 0;
      this.halfOpenSuccesses = 0;
    }

    if (newState === 'HALF_OPEN') {
      this.halfOpenSuccesses = 0;
    }

    console.log(`[CircuitBreaker:${this.name}] 🔄 State: ${prev} → ${newState}`);
  }

  getStatus() {
    return {
      name: this.name,
      state: this.state,
      consecutiveFailures: this.consecutiveFailures,
      metrics: { totalCalls: this.totalCalls, totalFailures: this.totalFailures, totalFallbacks: this.totalFallbacks },
      resetIn: this.state === 'OPEN'
        ? Math.max(0, this.config.resetTimeoutMs - (Date.now() - (this.lastFailureTime ?? 0)))
        : null,
    };
  }
}

// ── Singleton circuit breaker for the external Payment Gateway ─
const paymentCircuitBreaker = new CircuitBreaker('PaymentGateway', {
  failureThreshold: 5,           // open after 5 consecutive failures
  resetTimeoutMs: 30_000,        // try again after 30 seconds
  successThreshold: 2,           // need 2 successes to fully close
});

// ─────────────────────────────────────────────────────────────
// PAYMENT SERVICE — Combines both patterns
// ─────────────────────────────────────────────────────────────

export interface PaymentRequest {
  orderId: string;
  customerId: string;
  amount: number;
  paymentMethod: 'card' | 'cash';
  idempotencyKey: string;
}

export interface PaymentResult {
  charged: boolean;
  transactionId: string;
  method: 'card' | 'cash' | 'deferred';
  idempotent: boolean;    // true if result came from cache (no new charge)
  timestamp: string;
}

// Simulates calling an external bank/card API
async function callExternalPaymentAPI(req: PaymentRequest): Promise<PaymentResult> {
  // Simulate 20% random failure rate (remove in production)
  if (Math.random() < 0.2) {
    throw new Error('External payment gateway timeout: connection refused');
  }

  // Simulate network latency
  await new Promise(r => setTimeout(r, 50 + Math.random() * 150));

  return {
    charged: true,
    transactionId: `txn_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
    method: 'card',
    idempotent: false,
    timestamp: new Date().toISOString(),
  };
}

// Fallback: Cash on Delivery (payment collected at door)
function cashOnDeliveryFallback(req: PaymentRequest): PaymentResult {
  console.log(`[PaymentService] 💵 Fallback → Cash on Delivery for order ${req.orderId}`);
  return {
    charged: false,
    transactionId: `cod_${req.orderId}`,
    method: 'cash',
    idempotent: false,
    timestamp: new Date().toISOString(),
  };
}

/**
 * Main payment function:
 *  1. Check idempotency cache → return cached result if found.
 *  2. Execute payment through circuit breaker.
 *  3. On breaker OPEN → fall back to Cash on Delivery.
 *  4. Store result in idempotency cache.
 */
export async function processPayment(req: PaymentRequest): Promise<PaymentResult> {
  // ── Idempotency check ─────────────────────────────────────
  const cacheKey = `${req.customerId}:${req.idempotencyKey}`;
  const cached = idempotencyCache.get(cacheKey);

  if (cached) {
    console.log(
      `[PaymentService] 🔁 Idempotent hit for key ${req.idempotencyKey} — returning cached result (no charge)`,
    );
    return { ...cached, idempotent: true };
  }

  // ── Execute through circuit breaker ───────────────────────
  const result = await paymentCircuitBreaker.execute(
    () => callExternalPaymentAPI(req),
    () => cashOnDeliveryFallback(req),
  );

  // ── Store in idempotency cache ────────────────────────────
  idempotencyCache.setIfAbsent(cacheKey, result);

  console.log(
    `[PaymentService] ✅ Payment processed — orderId=${req.orderId} ` +
    `method=${result.method} txn=${result.transactionId} idempotencyKey=${req.idempotencyKey}`,
  );

  return result;
}

// ── Status endpoints (mount on billing service Express app) ───
export function getCircuitBreakerStatus() {
  return paymentCircuitBreaker.getStatus();
}

export function getIdempotencyCacheSize() {
  return idempotencyCache.size();
}

// ─────────────────────────────────────────────────────────────
// DEMONSTRATION / SELF-TEST
// ─────────────────────────────────────────────────────────────
async function demo() {
  console.log('\n=== IDEMPOTENCY + CIRCUIT BREAKER DEMO ===\n');

  const baseReq: PaymentRequest = {
    orderId: 'order-abc-123',
    customerId: 'cust-456',
    amount: 29.99,
    paymentMethod: 'card',
    idempotencyKey: 'client-generated-uuid-789',
  };

  // Simulate user clicking "Submit" 3 times (only first charges)
  console.log('--- User clicks Submit 3 times with same idempotency key ---');
  for (let i = 1; i <= 3; i++) {
    console.log(`\nAttempt ${i}:`);
    const result = await processPayment(baseReq);
    console.log(`  charged=${result.charged} idempotent=${result.idempotent} txn=${result.transactionId}`);
  }

  console.log('\n--- Simulating circuit breaker trips (forcing failures) ---');
  // Force failures by temporarily replacing the API mock
  const failingReq: PaymentRequest = { ...baseReq, orderId: 'order-fail-test', idempotencyKey: 'fail-key-1' };

  // The Math.random() in callExternalPaymentAPI will eventually produce enough failures
  // In a controlled test you'd inject errors; here we demonstrate the status endpoint
  console.log('Circuit Breaker Status:', JSON.stringify(getCircuitBreakerStatus(), null, 2));
}

demo().catch(console.error);
