// k6 load test for the order pipeline, fired through nginx at the 5 order instances.
//
// Open model (constant arrival rate): k6 launches as many VUs as needed to SUSTAIN
// the target req/s — so the result reflects the system's real capacity, not the
// client's. Defaults: 10k req/s for 60s.
//
//   RATE=10000 DURATION=60s ./scripts/load-test.sh
//
// Env knobs: RATE, DURATION, TARGET, VUS (preallocated), MAX_VUS, PRODUCTS.
import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

const RATE     = Number(__ENV.RATE || 10000);
const DURATION = __ENV.DURATION || '60s';
const TARGET   = __ENV.TARGET || 'http://localhost:8088/api/orders';
const PRODUCTS = Number(__ENV.PRODUCTS || 200); // spread keys so stock never runs out

// Tracks how many orders the broker accepted (HTTP 202). After the run, compare
// this against the orders.created topic count to prove zero producer-side loss.
export const accepted = new Counter('orders_accepted');

export const options = {
  discardResponseBodies: true,
  scenarios: {
    orders: {
      executor: 'constant-arrival-rate',
      rate: RATE,
      timeUnit: '1s',
      duration: DURATION,
      preAllocatedVUs: Number(__ENV.VUS || 800),
      maxVUs: Number(__ENV.MAX_VUS || 4000),
    },
  },
  thresholds: {
    // Stability gates — the run is marked failed if these are breached.
    http_req_failed:   ['rate<0.01'],   // <1% HTTP errors
    http_req_duration: ['p(95)<500', 'p(99)<1500'],
    orders_accepted:   ['count>0'],
  },
};

export default function () {
  const productId = 'p' + (1 + Math.floor(Math.random() * PRODUCTS));
  const body = JSON.stringify({
    customerId: 'c' + __VU,
    productId,
    quantity: 1,
    amount: 9.99,
  });
  const res = http.post(TARGET, body, {
    headers: { 'Content-Type': 'application/json' },
  });
  if (check(res, { 'status is 202': (r) => r.status === 202 })) {
    accepted.add(1);
  }
}
