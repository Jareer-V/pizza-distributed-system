/**
 * ============================================================
 * COMPONENT 2: gRPC SERVER — INVENTORY SERVICE
 * ============================================================
 * Implements the InventoryService defined in inventory.proto.
 * Runs on an internal port (not exposed to the public internet).
 * The Order Service calls this via gRPC before confirming any
 * pizza order to the customer.
 *
 * Key points:
 *  • Binary Protobuf serialisation (not JSON) → low latency
 *  • Unary RPC for synchronous availability checks
 *  • In-memory stock store (replace with DB in production)
 * ============================================================
 */

import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import path from 'path';

// ── Load proto at runtime (no code-gen step needed for demo) ──
const PROTO_PATH = path.resolve(__dirname, '../../proto/inventory.proto');

const packageDef = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
});

const inventoryProto = (grpc.loadPackageDefinition(packageDef) as any).inventory;

// ── In-Memory Stock Database (replace with PostgreSQL/Redis) ──
interface StockEntry {
  name: string;
  units: number;           // available units
}

const STOCK_DB: Record<string, StockEntry> = {
  PIZZA_MARGHERITA:   { name: 'Margherita Pizza',    units: 50 },
  PIZZA_PEPPERONI:    { name: 'Pepperoni Pizza',      units: 30 },
  PIZZA_BBQCHICKEN:  { name: 'BBQ Chicken Pizza',    units: 20 },
  BURGER_CLASSIC:    { name: 'Classic Burger',       units: 60 },
  BURGER_DOUBLE:     { name: 'Double Burger',        units: 40 },
  SIDE_FRIES:        { name: 'French Fries',         units: 200 },
  SIDE_RINGS:        { name: 'Onion Rings',          units: 100 },
  DRINK_COLA:        { name: 'Cola',                 units: 300 },
};

// Active reservations: reservationId → { items, expiresAt }
const RESERVATIONS = new Map<string, { items: Array<{ product_id: string; quantity: number }>; expiresAt: Date }>();

// ── RPC Handler: CheckIngredients ─────────────────────────────
function checkIngredients(
  call: grpc.ServerUnaryCall<any, any>,
  callback: grpc.sendUnaryData<any>,
) {
  const { order_id, request_id, items } = call.request;
  console.log(`[InventoryService][gRPC] CheckIngredients orderId=${order_id} reqId=${request_id}`);

  const statuses = (items as Array<{ product_id: string; quantity: number; name: string }>).map(item => {
    const stock = STOCK_DB[item.product_id];

    if (!stock) {
      return {
        product_id: item.product_id,
        available: false,
        stock_level: 0,
        reason: `Product ${item.product_id} does not exist in catalogue`,
      };
    }

    const available = stock.units >= item.quantity;
    return {
      product_id: item.product_id,
      available,
      stock_level: stock.units,
      reason: available ? '' : `Only ${stock.units} units in stock, requested ${item.quantity}`,
    };
  });

  const allAvailable = statuses.every(s => s.available);

  callback(null, {
    all_available: allAvailable,
    statuses,
    checked_at: new Date().toISOString(),
  });
}

// ── RPC Handler: ReserveIngredients ──────────────────────────
function reserveIngredients(
  call: grpc.ServerUnaryCall<any, any>,
  callback: grpc.sendUnaryData<any>,
) {
  const { order_id, items, hold_ttl_seconds } = call.request;
  console.log(`[InventoryService][gRPC] ReserveIngredients orderId=${order_id}`);

  // Verify availability one more time (race condition guard)
  for (const item of items as Array<{ product_id: string; quantity: number }>) {
    const stock = STOCK_DB[item.product_id];
    if (!stock || stock.units < item.quantity) {
      return callback(null, {
        reserved: false,
        reservation_id: '',
        expires_at: '',
      });
    }
  }

  // Temporarily deduct from stock and create reservation
  for (const item of items as Array<{ product_id: string; quantity: number }>) {
    STOCK_DB[item.product_id].units -= item.quantity;
  }

  const reservationId = `res_${order_id}_${Date.now()}`;
  const expiresAt = new Date(Date.now() + hold_ttl_seconds * 1000);
  RESERVATIONS.set(reservationId, { items, expiresAt });

  console.log(`[InventoryService][gRPC] Reserved ${reservationId} until ${expiresAt.toISOString()}`);
  callback(null, {
    reserved: true,
    reservation_id: reservationId,
    expires_at: expiresAt.toISOString(),
  });
}

// ── RPC Handler: CommitOrReleaseReservation ───────────────────
function commitOrReleaseReservation(
  call: grpc.ServerUnaryCall<any, any>,
  callback: grpc.sendUnaryData<any>,
) {
  const { reservation_id, commit } = call.request;
  const reservation = RESERVATIONS.get(reservation_id);

  if (!reservation) {
    return callback(null, { success: false, message: 'Reservation not found or already processed' });
  }

  if (!commit) {
    // Release: restore stock (payment failed / order cancelled)
    for (const item of reservation.items as Array<{ product_id: string; quantity: number }>) {
      if (STOCK_DB[item.product_id]) {
        STOCK_DB[item.product_id].units += item.quantity;
      }
    }
    console.log(`[InventoryService][gRPC] Released reservation ${reservation_id}`);
  } else {
    // Commit: stock already deducted during reserve — just clean up
    console.log(`[InventoryService][gRPC] Committed reservation ${reservation_id}`);
  }

  RESERVATIONS.delete(reservation_id);
  callback(null, { success: true, message: commit ? 'Committed' : 'Released' });
}

// ── Start gRPC Server ─────────────────────────────────────────
function startServer() {
  const server = new grpc.Server();

  server.addService(inventoryProto.InventoryService.service, {
    checkIngredients,
    reserveIngredients,
    commitOrReleaseReservation,
  });

  const GRPC_PORT = process.env.INVENTORY_GRPC_PORT || '50051';
  server.bindAsync(
    `0.0.0.0:${GRPC_PORT}`,
    grpc.ServerCredentials.createInsecure(),   // use TLS in production
    (err, port) => {
      if (err) {
        console.error('[InventoryService] Failed to bind gRPC server:', err);
        process.exit(1);
      }
      console.log(`[InventoryService][gRPC] Server listening on port ${port}`);
      server.start();
    },
  );
}

startServer();
