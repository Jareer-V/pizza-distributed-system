/**
 * ============================================================
 * COMPONENT 2: gRPC CLIENT — ORDER SERVICE → INVENTORY SERVICE
 * ============================================================
 * Used by the Order Service (or Kitchen Service) to verify
 * ingredient availability before confirming a pizza order.
 *
 * Why gRPC over REST for this internal call?
 *  • Protobuf binary payload is 3-10× smaller than equivalent JSON
 *  • HTTP/2 multiplexing = lower latency under concurrent load
 *  • Strongly typed — compile-time errors catch contract breaks
 *  • Streaming support for future use (e.g., live stock updates)
 * ============================================================
 */

import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import path from 'path';

const PROTO_PATH = path.resolve(__dirname, '../../proto/inventory.proto');

const packageDef = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
});

const inventoryProto = (grpc.loadPackageDefinition(packageDef) as any).inventory;

// ── Types mirroring the .proto definitions ────────────────────
export interface OrderItem {
  product_id: string;
  name: string;
  quantity: number;
}

export interface IngredientStatus {
  product_id: string;
  available: boolean;
  stock_level: number;
  reason: string;
}

export interface CheckIngredientResult {
  all_available: boolean;
  statuses: IngredientStatus[];
  checked_at: string;
}

export interface ReservationResult {
  reserved: boolean;
  reservation_id: string;
  expires_at: string;
}

// ── gRPC Client Factory (singleton pattern) ───────────────────
class InventoryGrpcClient {
  private client: any;
  private static instance: InventoryGrpcClient;

  private constructor() {
    const host = process.env.INVENTORY_GRPC_HOST || 'inventory-service:50051';

    // Channel options tune performance for internal service mesh
    const channelOptions = {
      'grpc.keepalive_time_ms': 10_000,
      'grpc.keepalive_timeout_ms': 5_000,
      'grpc.keepalive_permit_without_calls': 1,
      'grpc.max_receive_message_length': 1024 * 1024 * 4, // 4 MB
    };

    this.client = new inventoryProto.InventoryService(
      host,
      grpc.credentials.createInsecure(),   // use createSsl() in production
      channelOptions,
    );

    console.log(`[InventoryGrpcClient] Connected to ${host}`);
  }

  static getInstance(): InventoryGrpcClient {
    if (!InventoryGrpcClient.instance) {
      InventoryGrpcClient.instance = new InventoryGrpcClient();
    }
    return InventoryGrpcClient.instance;
  }

  // ── Promisified wrappers ─────────────────────────────────

  /**
   * Check if all items in an order are in stock.
   * Call this BEFORE confirming the order to the customer.
   * Fast read-only operation — target p99 < 5ms on LAN.
   */
  checkIngredients(
    orderId: string,
    requestId: string,
    items: OrderItem[],
  ): Promise<CheckIngredientResult> {
    return new Promise((resolve, reject) => {
      const deadline = new Date(Date.now() + 3000); // 3 s timeout

      this.client.checkIngredients(
        { order_id: orderId, request_id: requestId, items },
        { deadline },
        (err: grpc.ServiceError | null, response: CheckIngredientResult) => {
          if (err) {
            console.error(`[InventoryGrpcClient] checkIngredients failed: ${err.message}`);
            return reject(err);
          }
          resolve(response);
        },
      );
    });
  }

  /**
   * Reserve (soft-lock) ingredients for hold_ttl_seconds.
   * Call AFTER customer confirms order and BEFORE charging payment.
   */
  reserveIngredients(
    orderId: string,
    items: OrderItem[],
    holdTtlSeconds = 900,
  ): Promise<ReservationResult> {
    return new Promise((resolve, reject) => {
      const deadline = new Date(Date.now() + 5000);

      this.client.reserveIngredients(
        { order_id: orderId, items, hold_ttl_seconds: holdTtlSeconds },
        { deadline },
        (err: grpc.ServiceError | null, response: ReservationResult) => {
          if (err) return reject(err);
          resolve(response);
        },
      );
    });
  }

  /**
   * Commit or release a reservation.
   * commit=true  → deduct stock permanently (payment succeeded)
   * commit=false → restore stock (payment failed / order cancelled)
   */
  commitOrRelease(reservationId: string, commit: boolean): Promise<{ success: boolean; message: string }> {
    return new Promise((resolve, reject) => {
      const deadline = new Date(Date.now() + 3000);

      this.client.commitOrReleaseReservation(
        { reservation_id: reservationId, commit },
        { deadline },
        (err: grpc.ServiceError | null, response: { success: boolean; message: string }) => {
          if (err) return reject(err);
          resolve(response);
        },
      );
    });
  }
}

// ── Example usage in Order Service ────────────────────────────
export async function verifyAndReserveIngredients(
  orderId: string,
  requestId: string,
  items: OrderItem[],
): Promise<{ success: boolean; reservationId?: string; failedItems?: IngredientStatus[] }> {
  const client = InventoryGrpcClient.getInstance();

  // Step 1: Check availability (fast read via gRPC binary protocol)
  console.log(`[OrderService] gRPC CheckIngredients for order ${orderId}`);
  const check = await client.checkIngredients(orderId, requestId, items);

  if (!check.all_available) {
    const failedItems = check.statuses.filter(s => !s.available);
    console.warn(`[OrderService] Ingredients unavailable:`, failedItems.map(f => f.reason));
    return { success: false, failedItems };
  }

  // Step 2: Reserve (soft-lock ingredients for 15 minutes)
  console.log(`[OrderService] gRPC ReserveIngredients for order ${orderId}`);
  const reservation = await client.reserveIngredients(orderId, items, 900);

  if (!reservation.reserved) {
    return { success: false };
  }

  return { success: true, reservationId: reservation.reservation_id };
}

export { InventoryGrpcClient };
