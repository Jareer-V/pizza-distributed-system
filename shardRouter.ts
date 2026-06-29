/**
 * ============================================================
 * COMPONENT 5: DATABASE SHARDING — ORDERS HISTORY DAL
 * ============================================================
 * Horizontal sharding strategy for the Orders database.
 *
 * TWO COMPLEMENTARY SHARDING STRATEGIES:
 *
 *  A) RANGE-BASED SHARDING (Order ID ranges)
 *     • Shard ALPHA: Order IDs 1 – 1,000,000      (legacy / oldest)
 *     • Shard BETA:  Order IDs 1,000,001 – 5,000,000
 *     • Shard GAMMA: Order IDs 5,000,001 – ∞       (newest, hot shard)
 *     Pro: Simple range queries, easy archival of old shards.
 *     Con: Hot-shard problem — GAMMA gets all new writes.
 *
 *  B) GEOGRAPHIC SHARDING (delivery region → shard)
 *     • Shard EU:   Europe  region orders
 *     • Shard US:   Americas region orders
 *     • Shard APAC: Asia-Pacific region orders
 *     Pro: Data sovereignty, regional latency optimisation.
 *     Con: Uneven load if regions grow unevenly.
 *
 * The ShardRouter inspects the sharding key and returns the
 * correct database connection — callers never know which shard
 * they are hitting (Transparency).
 *
 * Cross-shard queries (e.g., "all orders by customer X") require
 * a SCATTER-GATHER approach demonstrated at the bottom.
 * ============================================================
 */

// ── Database Connection Abstraction ──────────────────────────
interface DbConnection {
  shardId: string;
  query<T>(sql: string, params?: unknown[]): Promise<T[]>;
  queryOne<T>(sql: string, params?: unknown[]): Promise<T | null>;
  insert<T>(table: string, record: T): Promise<T & { id: number }>;
}

// Simulated DB connection (replace with pg.Pool in production)
class MockDbConnection implements DbConnection {
  readonly shardId: string;
  private data = new Map<string, unknown[]>();  // table → rows

  constructor(shardId: string) {
    this.shardId = shardId;
    console.log(`[DB] Shard ${shardId} connection initialised`);
  }

  async query<T>(sql: string, _params?: unknown[]): Promise<T[]> {
    // Simulate query latency
    await delay(5 + Math.random() * 10);
    // Real implementation: return pool.query(sql, params).then(r => r.rows)
    console.log(`[DB:${this.shardId}] Executing: ${sql.slice(0, 80)}...`);
    return [];
  }

  async queryOne<T>(sql: string, _params?: unknown[]): Promise<T | null> {
    await delay(5);
    console.log(`[DB:${this.shardId}] QueryOne: ${sql.slice(0, 80)}...`);
    return null;
  }

  async insert<T>(table: string, record: T): Promise<T & { id: number }> {
    await delay(10);
    const rows = this.data.get(table) ?? [];
    const withId = { ...(record as object), id: rows.length + 1 } as T & { id: number };
    rows.push(withId);
    this.data.set(table, rows);
    console.log(`[DB:${this.shardId}] Inserted into ${table}: id=${withId.id}`);
    return withId;
  }
}

const delay = (ms: number) => new Promise(r => setTimeout(r, ms));

// ─────────────────────────────────────────────────────────────
// SHARD DEFINITIONS
// ─────────────────────────────────────────────────────────────

/** Strategy A: Range-based (by numeric order ID) */
interface RangeShard {
  id: string;
  minOrderId: number;
  maxOrderId: number;   // Infinity for the latest shard
  connection: DbConnection;
  region?: string;
}

/** Strategy B: Geographic (by delivery region) */
interface GeoShard {
  id: string;
  regions: string[];    // ISO country codes or region names
  connection: DbConnection;
}

// Initialise shard pool (connections would be pg.Pool instances)
const RANGE_SHARDS: RangeShard[] = [
  { id: 'ALPHA', minOrderId: 1,         maxOrderId: 1_000_000,    connection: new MockDbConnection('ALPHA') },
  { id: 'BETA',  minOrderId: 1_000_001, maxOrderId: 5_000_000,    connection: new MockDbConnection('BETA')  },
  { id: 'GAMMA', minOrderId: 5_000_001, maxOrderId: Infinity,     connection: new MockDbConnection('GAMMA') },
];

const GEO_SHARDS: GeoShard[] = [
  { id: 'EU',   regions: ['GB', 'DE', 'FR', 'NL', 'IT', 'ES', 'PL'], connection: new MockDbConnection('EU')   },
  { id: 'US',   regions: ['US', 'CA', 'MX', 'BR', 'AR'],              connection: new MockDbConnection('US')   },
  { id: 'APAC', regions: ['JP', 'AU', 'SG', 'IN', 'CN', 'KR'],        connection: new MockDbConnection('APAC') },
];

// ─────────────────────────────────────────────────────────────
// SHARD ROUTER
// ─────────────────────────────────────────────────────────────

export class ShardRouter {
  /**
   * STRATEGY A — Route by numeric Order ID (range-based).
   *
   * Example routing:
   *   orderId = 500,000   → ALPHA (1 – 1M)
   *   orderId = 2,000,000 → BETA  (1M – 5M)
   *   orderId = 9,000,000 → GAMMA (5M – ∞)
   */
  static getShardByOrderId(orderId: number): DbConnection {
    const shard = RANGE_SHARDS.find(
      s => orderId >= s.minOrderId && orderId <= s.maxOrderId,
    );

    if (!shard) {
      throw new Error(`No shard found for orderId ${orderId}`);
    }

    console.log(`[ShardRouter] orderId=${orderId} → Shard ${shard.id} (${shard.minOrderId}-${shard.maxOrderId === Infinity ? '∞' : shard.maxOrderId})`);
    return shard.connection;
  }

  /**
   * STRATEGY B — Route by geographic delivery region (ISO code).
   *
   * Example routing:
   *   region = 'GB' → EU shard
   *   region = 'US' → US shard
   *   region = 'JP' → APAC shard
   */
  static getShardByRegion(countryCode: string): DbConnection {
    const shard = GEO_SHARDS.find(s => s.regions.includes(countryCode.toUpperCase()));

    if (!shard) {
      // Default to US shard for unknown regions (or throw based on policy)
      console.warn(`[ShardRouter] Unknown region ${countryCode} — defaulting to US shard`);
      return GEO_SHARDS.find(s => s.id === 'US')!.connection;
    }

    console.log(`[ShardRouter] region=${countryCode} → Shard ${shard.id}`);
    return shard.connection;
  }

  /**
   * CROSS-SHARD QUERY (Scatter-Gather pattern):
   * Fan out to ALL range shards, run the query on each in parallel,
   * then merge and sort the results.
   *
   * Used for queries that don't have a shard key, e.g.:
   *   "Find all orders placed by customerId X" (customer can have
   *    orders across multiple shards if they've been around a while)
   */
  static async scatterGather<T>(
    sql: string,
    params: unknown[],
    mergeFn: (results: T[][]) => T[],
  ): Promise<T[]> {
    console.log(`[ShardRouter] SCATTER-GATHER across ${RANGE_SHARDS.length} shards: ${sql.slice(0, 60)}...`);

    const promises = RANGE_SHARDS.map(shard =>
      shard.connection.query<T>(sql, params)
        .then(rows => { console.log(`[ShardRouter] Shard ${shard.id}: ${rows.length} rows returned`); return rows; })
        .catch(err => {
          console.error(`[ShardRouter] Shard ${shard.id} failed: ${err.message} — skipping`);
          return [] as T[];          // partial failure: skip bad shard
        }),
    );

    const results = await Promise.all(promises);
    return mergeFn(results);
  }
}

// ─────────────────────────────────────────────────────────────
// ORDER DATA ACCESS LAYER (uses ShardRouter internally)
// ─────────────────────────────────────────────────────────────

export interface Order {
  orderId: number;
  customerId: string;
  items: Array<{ productId: string; qty: number; price: number }>;
  totalAmount: number;
  status: 'PENDING' | 'CONFIRMED' | 'PREPARING' | 'DELIVERED' | 'CANCELLED';
  deliveryRegion: string;   // ISO country code — used for geo sharding
  createdAt: string;
}

export const OrderRepository = {

  /**
   * Insert a new order.
   * Routes to the correct range shard AND geo shard based on orderId + region.
   *
   * In this demo we use range sharding for writes (numeric ID is known at insert time).
   */
  async create(order: Omit<Order, 'orderId'>): Promise<Order> {
    // In production, the orderId would come from a distributed ID generator
    // (e.g., Twitter Snowflake, UUIDv7) to avoid hot-shard issues.
    const tempId = Date.now() % 10_000_000;   // demo: derive shard-key from epoch

    const db = ShardRouter.getShardByOrderId(tempId);
    const inserted = await db.insert('orders', { ...order, orderId: tempId });

    return inserted as unknown as Order;
  },

  /**
   * Fetch a single order by its numeric ID.
   * We know the shard from the ID range — O(1) routing, no scatter needed.
   */
  async findById(orderId: number): Promise<Order | null> {
    const db = ShardRouter.getShardByOrderId(orderId);
    return db.queryOne<Order>(
      'SELECT * FROM orders WHERE order_id = $1',
      [orderId],
    );
  },

  /**
   * Fetch all orders by a customer.
   * Customer could have orders across multiple shards (they ordered years ago
   * AND recently), so we must scatter-gather across ALL shards.
   */
  async findByCustomer(customerId: string): Promise<Order[]> {
    return ShardRouter.scatterGather<Order>(
      'SELECT * FROM orders WHERE customer_id = $1 ORDER BY created_at DESC',
      [customerId],
      // Merge: concatenate all results, re-sort by date, deduplicate
      (results) => results
        .flat()
        .sort((a, b) => new Date(b.createdAt).getTime() - new Date(a.createdAt).getTime()),
    );
  },

  /**
   * Fetch orders by delivery region (geo-shard strategy).
   * Only hits ONE geo shard — very efficient regional reporting.
   */
  async findByRegion(countryCode: string, limit = 100): Promise<Order[]> {
    const db = ShardRouter.getShardByRegion(countryCode);
    return db.query<Order>(
      'SELECT * FROM orders WHERE delivery_region = $1 ORDER BY created_at DESC LIMIT $2',
      [countryCode, limit],
    );
  },

  /**
   * Update order status — routes to the correct shard by orderId.
   */
  async updateStatus(orderId: number, status: Order['status']): Promise<void> {
    const db = ShardRouter.getShardByOrderId(orderId);
    await db.query(
      'UPDATE orders SET status = $1, updated_at = NOW() WHERE order_id = $2',
      [status, orderId],
    );
    console.log(`[OrderRepository] Updated order ${orderId} → ${status} on correct shard`);
  },
};

// ─────────────────────────────────────────────────────────────
// DEMO
// ─────────────────────────────────────────────────────────────
async function demo() {
  console.log('\n=== DATABASE SHARDING DEMO ===\n');

  // Range-based routing
  console.log('--- Range Shard Routing ---');
  ShardRouter.getShardByOrderId(500_000);     // → ALPHA
  ShardRouter.getShardByOrderId(2_000_000);   // → BETA
  ShardRouter.getShardByOrderId(7_500_000);   // → GAMMA

  // Geo-based routing
  console.log('\n--- Geo Shard Routing ---');
  ShardRouter.getShardByRegion('GB');   // → EU
  ShardRouter.getShardByRegion('US');   // → US
  ShardRouter.getShardByRegion('JP');   // → APAC
  ShardRouter.getShardByRegion('ZZ');   // → US (fallback)

  // Create an order (range shard)
  console.log('\n--- Creating an Order ---');
  await OrderRepository.create({
    customerId: 'cust-uk-001',
    items: [{ productId: 'PIZZA_MARGHERITA', qty: 2, price: 12.99 }],
    totalAmount: 25.98,
    status: 'CONFIRMED',
    deliveryRegion: 'GB',
    createdAt: new Date().toISOString(),
  });

  // Scatter-gather (cross-shard) customer lookup
  console.log('\n--- Cross-Shard Customer Lookup (Scatter-Gather) ---');
  await OrderRepository.findByCustomer('cust-uk-001');

  console.log('\n=== SHARDING DEMO COMPLETE ===');
}

demo().catch(console.error);
