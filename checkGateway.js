const { randomUUID } = require('crypto');

(async () => {
  const requestId = randomUUID();
  const res = await fetch('http://127.0.0.1:3001/internal/prepare', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'x-request-id': requestId,
      'x-gateway-secret': 'dev-secret',
    },
    body: JSON.stringify({
      orderId: randomUUID(),
      items: [{ productId: 'PIZZA_MARGHERITA', name: 'Margherita', qty: 1, price: 12.99 }],
      requestId,
    }),
  });

  const text = await res.text();
  let body;
  if (text) {
    try {
      body = JSON.parse(text);
    } catch {
      body = { raw: text };
    }
  }

  const result = { ok: res.ok, data: body };
  console.log(JSON.stringify(result));
  console.log('accepted?', !!result.data?.accepted);
})();
