# 🍕 PizzaChain — Distributed Microservices Architecture

A production-grade, highly scalable, fault-tolerant distributed e-commerce system
implementing 5 core distributed systems patterns.

---

## System Architecture Overview

```
                        ┌─────────────────────────────┐
                        │      CLIENT (Browser/App)   │
                        └──────────────┬──────────────┘
                                       │ POST /api/v1/order
                        ┌──────────────▼──────────────┐
                        │     LOAD BALANCER :8080      │◄── Health checks every 10s
                        │   Algorithm: Least-Conn      │    Active pool management
                        └───┬──────────┬──────────┬───┘
                            │          │          │
               ┌────────────▼─┐  ┌─────▼──────┐  ┌▼──────────────┐
               │  API Gateway │  │ API Gateway│  │  API Gateway  │
               │  :3000 (1)   │  │ :3000 (2)  │  │  :3000 (3)    │
               └──────┬───────┘  └─────┬──────┘  └───────┬───────┘
                      │ Orchestrates 3 internal calls (transparent to client)
            ┌─────────▼──────┬──────────────────┬────────▼──────┐
            │                │                  │               │
   ┌────────▼──────┐ ┌───────▼───────┐ ┌────────▼──────┐       │
   │Kitchen Service│ │Billing Service│ │Delivery Svc   │       │
   │ /prepare      │ │ /charge       │ │ /dispatch     │       │
   └───────┬───────┘ └───────┬───────┘ └───────────────┘       │
           │ gRPC             │ Circuit Breaker +                │
           │ (binary proto)   │ Idempotency Cache               │
   ┌───────▼───────┐ ┌───────▼───────┐ ┌──────────────────────┐│
   │  Inventory    │ │  External     │ │  Shard Router        ││
   │  Service      │ │  Payment API  │ │  ┌──────┬─────┬────┐ ││
   │  :50051 gRPC  │ │  (bank/card)  │ │  │ALPHA │BETA │ΓAMMA│││
   └───────────────┘ └───────────────┘ │  └──────┴─────┴────┘ ││
                                       │  ┌───┬────┬──────┐   ││
                                       │  │EU │ US │ APAC  │  ││
                                       │  └───┴────┴──────┘   ││
                                       └──────────────────────┘│
```

---

## Component 1: Transparency Layer — API Gateway

**File:** `src/gateway/apiGateway.ts`

### What it does
Single entry point at `POST /api/v1/order`. The client never knows:
- Where Kitchen/Billing/Delivery services run (Location Transparency)
- How many replicas exist (Replication Transparency)
- What protocol each service uses internally

### Orchestration flow
1. Validate incoming order body
2. Generate `orderId` + propagate `x-request-id` trace
3. Call **Kitchen Service** `/internal/prepare` → check ingredients
4. Call **Billing Service** `/internal/charge` → charge card (with idempotency key)
5. Call **Delivery Service** `/internal/dispatch` → assign rider
6. Return unified response: `{ orderId, status, transactionId, tracking, estimatedDelivery }`

### Key design decisions
- Compensating transactions: if billing fails → cancel kitchen order
- Delivery failure is non-fatal (async retry queue in production)
- `AbortSignal.timeout(8000)` per downstream call

---

## Component 2: gRPC Communication — Inventory Service

**Files:**
- `proto/inventory.proto` — Contract definition
- `src/inventory/grpcServer.ts` — gRPC server (Inventory)
- `src/order/grpcClient.ts` — gRPC client (Order Service)

### Why gRPC over REST?

| Metric | JSON/REST | gRPC/Protobuf |
|--------|-----------|---------------|
| Payload size | ~350 bytes | ~80 bytes (3-4×) |
| Serialisation | ~50μs | ~8μs (6×) |
| Type safety | Runtime (JSON.parse) | Compile-time |
| Streaming | Requires polling | Native support |

### Three-phase ingredient flow
```
Order Service                          Inventory Service
     │                                        │
     │── CheckIngredients (orderId, items) ──►│  (read-only, fast)
     │◄─ { all_available, statuses } ─────────│
     │                                        │
     │── ReserveIngredients (orderId, ttl) ──►│  (soft-lock stock)
     │◄─ { reserved, reservation_id } ────────│
     │                                        │
     │  [payment processed]                   │
     │                                        │
     │── CommitOrRelease (id, commit=true) ──►│  (deduct permanently)
     │◄─ { success } ─────────────────────────│
```

---

## Component 3: Load Balancer — Least Connections + Health Checks

**File:** `src/loadbalancer/leastConnections.ts`

### Least Connections Algorithm
```
Pool = [svc-1 (5 conn), svc-2 (2 conn), svc-3 (8 conn)]
         ↓
Pick svc-2 → increment to 3
         ↓
On response.finish → decrement back to 2
```

**Why Least-Connections over Round-Robin?**
Pizza order processing time varies enormously (fast read: 5ms, full order: 30s).
Round-Robin can starve slow instances. Least-Connections naturally backpressures
overloaded nodes.

### Health Check State Machine
```
[Healthy] ─── 3 consecutive failures ──► [Unhealthy / Removed from pool]
              ◄─── first success ─────── [Re-added to pool]
```

- Interval: every 10 seconds
- Timeout: 3 seconds per ping
- Threshold: 3 failures before removal

---

## Component 4: Resiliency — Circuit Breaker + Idempotency

**File:** `src/billing/paymentService.ts`

### Idempotency (Idempotent Consumer Pattern)

```
Request 1: POST /charge (idempotency-key: "abc123")
  → Key not in cache → process payment → cache result
  → Return { charged: true, txn: "txn_xyz" }

Request 2 (duplicate): POST /charge (idempotency-key: "abc123")
  → Key found in cache → return cached result
  → Return { charged: true, txn: "txn_xyz", idempotent: true }
  → NO second charge!
```

Cache key: `{customerId}:{idempotencyKey}` with 24-hour TTL.
Production: replace `IdempotencyCache` with Redis `SET key value EX 86400 NX`.

### Circuit Breaker State Machine

```
                   ┌─────────────────────────────────────────┐
                   │                                         │
              ┌────▼─────┐   ≥5 failures    ┌──────────┐    │
  Requests ──►│  CLOSED  ├────────────────►  │   OPEN   │    │
  flow through└──────────┘                  └──────────┘    │
              ▲ 2 successes                  │ After 30s     │
              │              ┌────────────┐  │               │
              └──────────────┤ HALF-OPEN  ├◄─┘               │
                             └────────────┘                  │
                                   │ 1 failure               │
                                   └─────────────────────────┘
```

**Fallback:** When OPEN → automatically return `{ method: 'cash', charged: false }`
(Cash on Delivery). The customer still gets their order; payment is collected at door.

---

## Component 5: Database Sharding

**File:** `src/database/shardRouter.ts`

### Strategy A: Range-based Sharding (by Order ID)

```
Order ID 1–1,000,000        → Shard ALPHA (legacy orders)
Order ID 1,000,001–5,000,000 → Shard BETA
Order ID 5,000,001–∞        → Shard GAMMA (hot shard, latest)
```

**Routing:** `ShardRouter.getShardByOrderId(orderId)` — O(1) lookup,
no scatter needed for single-order queries.

### Strategy B: Geographic Sharding (by Region)

```
GB, DE, FR, NL, IT, ES → Shard EU   (EU data sovereignty)
US, CA, MX, BR, AR     → Shard US
JP, AU, SG, IN, CN, KR → Shard APAC
```

**Routing:** `ShardRouter.getShardByRegion('GB')` → EU shard. One hop, no cross-shard query.

### Cross-Shard: Scatter-Gather Pattern

When the shard key is unknown (e.g., "get all orders by customer X"):
```
ShardRouter.scatterGather(sql, params, mergeFn)
  ├── Fan out to ALPHA, BETA, GAMMA in parallel (Promise.all)
  ├── Collect results (partial failure tolerant)
  └── Merge + sort by date in memory
```

**Performance:** 3 parallel queries at ~5ms each = ~5ms total (vs 15ms serial).

---

## Running the System

```bash
npm install

# Start individual services
npm run inventory-grpc    # gRPC server on :50051
npm run gateway           # API Gateway on :3000
npm run loadbalancer      # LB on :8080

# Or all at once
npm run dev:all

# Test a complete order flow
curl -X POST http://localhost:8080/api/v1/order \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: unique-client-uuid-here" \
  -d '{
    "customerId": "cust-uk-001",
    "items": [{"productId":"PIZZA_MARGHERITA","name":"Margherita","qty":2,"price":12.99}],
    "deliveryAddress": "42 Baker Street, London W1U 6RL",
    "paymentMethod": "card",
    "idempotencyKey": "unique-client-uuid-here"
  }'
```

---

## Production Hardening Checklist

- [ ] Replace `MockDbConnection` with `pg.Pool` (PostgreSQL)
- [ ] Replace `IdempotencyCache` with Redis `SETNX` + TTL
- [ ] Add TLS to gRPC (`grpc.credentials.createSsl()`)
- [ ] Add mutual TLS between gateway and internal services
- [ ] Implement Snowflake/UUIDv7 for distributed ID generation
- [ ] Add Prometheus metrics endpoint to each service
- [ ] Configure Kafka for async order event streaming
- [ ] Add Distributed Tracing (OpenTelemetry + Jaeger)
- [ ] Kubernetes Horizontal Pod Autoscaler for each microservice
- [ ] Database connection pooling + read replicas per shard
