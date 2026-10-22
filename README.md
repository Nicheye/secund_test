# Payment service

[Русская версия](README.ru.md)

Asynchronous payment processing microservice. `POST /api/v1/payments` accepts a payment and returns
`202`; a consumer later runs it through an emulated payment gateway, stores the result and notifies
the client by webhook. Stack: FastAPI + Pydantic v2, SQLAlchemy 2.0 (async), PostgreSQL, RabbitMQ
(FastStream), Alembic, Docker Compose. The assignment is in [docs/task-spec.md](docs/task-spec.md).

## Architecture

```
            POST /api/v1/payments
 client ───────────────────────────► api
                                      │  one DB transaction:
                                      │  INSERT payments (status=pending) + INSERT outbox
                                      ▼
                                  PostgreSQL ◄──────────────────────────────┐
                                      │ SELECT ... FOR UPDATE SKIP LOCKED   │
                                      ▼                                     │
                                    relay ── publish + confirm ──► RabbitMQ │
                                                    exchange `payments`     │
                                                    queue `payments.new`    │
                                                          │                 │
                                                          ▼                 │
                                                      consumer              │
                                  1. read payment (pending?)                │
                                  2. gateway emulation (2-5 s, 90% success) │
                                  3. UPDATE payments SET status ... ────────┘
                                  4. POST webhook_url (retry + backoff)
                                                          │
                       after 3 failed deliveries          ▼
                  `payments.new` ── dead-letter ──► exchange `payments.dlx`
                                                    └► queue `payments.new.dlq`
```

Processes (one compose service each): `api` (HTTP), `relay` (outbox -> RabbitMQ), `consumer`
(`payments.new` -> gateway -> DB -> webhook), `migrate` (runs `alembic upgrade head` once and
exits). Every long-running service (`postgres`, `rabbitmq`, `api`, `relay`, `consumer`) is
`restart: unless-stopped`, so the relay and consumer recover from crashes and a restarted
dependency comes back; unpublished outbox rows and unacknowledged messages are picked up again.
`migrate` is one-shot and has no restart policy.

Code layout (`src/app`): `api/` (routes, deps, error handlers), `services.py` (create/get payment),
`repositories.py`, `domain.py` (`Money`, `validate_amount`, `Currency`, status state machine),
`payment_finalization.py` (consumer use case), `gateway.py`, `webhook.py`, `timing.py`,
`messaging/` (`topology.py`, `relay.py`, `consumer.py`, `outbox_repository.py`), `models.py`,
`config.py`.

## Run

```bash
cp .env.example .env
docker compose up --build
```

Services: `postgres`, `rabbitmq`, `migrate` (applies migrations, then exits), `api`
(http://localhost:8000, OpenAPI docs at `/docs`), `relay`, `consumer`. Ports published on the host,
all bound to `127.0.0.1` only: 8000 (API), 5672 and 15672 (RabbitMQ); PostgreSQL is not published.
To reach the API from another machine, change the binding in `docker-compose.yml` on purpose; the
stack is not hardened for that.

Configuration comes from `.env` (copy of `.env.example`) through compose variable substitution, and
every variable has a default in `docker-compose.yml`, so the stack also starts without `.env`.
Compose builds `DATABASE_URL` and `RABBITMQ_URL` from `POSTGRES_*` and `RABBITMQ_*` plus the service
hostnames; the localhost URLs in `.env.example` are only for running processes on the host. The
default API key is `change-me`; `API_PORT` (default 8000) sets the host port the API is published
on. The defaults `change-me` (API key) and `guest` (RabbitMQ), as well as the PostgreSQL password
`payments`, are for the local demo only: set real values in `.env` for anything else. The app
settings are defined in `src/app/config.py` (`DATABASE_URL`, `RABBITMQ_URL`, `API_KEY` are required;
relay poll interval and batch size, `CONSUMER_PREFETCH_COUNT`, `WEBHOOK_MAX_ATTEMPTS`, webhook
backoff and timeout, gateway delay range and success rate have defaults). Startup fails on invalid
values: `RELAY_BATCH_SIZE`, `CONSUMER_PREFETCH_COUNT` and `WEBHOOK_MAX_ATTEMPTS` must be at least 1,
`GATEWAY_SUCCESS_RATE` must be between 0 and 1, and `GATEWAY_MIN_SECONDS` must not exceed
`GATEWAY_MAX_SECONDS`.

## Tests, lint, types

Tests use real PostgreSQL and RabbitMQ through testcontainers, so Docker must be running.

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e '.[dev]'

pytest                     # starts postgres:16 and a RabbitMQ container
pytest --cov=app --cov-branch --cov-report=term-missing   # same, with line and branch coverage
ruff check . && ruff format --check .
mypy                       # strict
```

To use an existing database instead of a PostgreSQL container, set `TEST_DATABASE_URL` (SQLAlchemy
asyncpg URL; migrations are applied to it and the `payments` and `outbox` tables are truncated
before each test, so never point it at data you care about):

```bash
TEST_DATABASE_URL=postgresql+asyncpg://payments:payments@localhost:5432/payments_test pytest
```

RabbitMQ is still started in a container for the consumer and relay tests.

## API

All endpoints require `X-API-Key`. Examples below assume the default `.env`.

Create a payment (`Idempotency-Key` is required). `amount` is a decimal with at most 2 places,
`currency` is `RUB`, `USD` or `EUR`:

```bash
curl -i -X POST http://localhost:8000/api/v1/payments \
  -H 'X-API-Key: change-me' \
  -H 'Idempotency-Key: order-1001' \
  -H 'Content-Type: application/json' \
  -d '{"amount": "100.50", "currency": "USD", "description": "Order 1001",
       "metadata": {"order_id": 1001}, "webhook_url": "http://host.docker.internal:9000/hook"}'
```

```
HTTP/1.1 202 Accepted
{"payment_id":"0b0e5f1e-...","status":"pending","created_at":"2026-01-01T12:00:00.123456Z"}
```

Repeat the same command: the same `payment_id` comes back (still `202`) and no second payment or
event is created. Reuse the key with a different payload (here a different amount):

```bash
curl -i -X POST http://localhost:8000/api/v1/payments \
  -H 'X-API-Key: change-me' -H 'Idempotency-Key: order-1001' -H 'Content-Type: application/json' \
  -d '{"amount": "999.00", "currency": "USD", "description": "Order 1001",
       "metadata": {"order_id": 1001}, "webhook_url": "http://host.docker.internal:9000/hook"}'
# HTTP/1.1 409 Conflict
# {"error":{"code":"idempotency_key_conflict","message":"Idempotency-Key was already used with a different request payload"}}
```

Get a payment (a few seconds after creation `status` is `succeeded` or `failed` and `processed_at`
is set):

```bash
curl -s http://localhost:8000/api/v1/payments/<payment_id> -H 'X-API-Key: change-me'
```

```json
{"payment_id": "0b0e5f1e-...", "amount": "100.50", "currency": "USD", "description": "Order 1001",
 "metadata": {"order_id": 1001}, "status": "succeeded",
 "webhook_url": "http://host.docker.internal:9000/hook",
 "created_at": "2026-01-01T12:00:00.123456Z", "processed_at": "2026-01-01T12:00:03.456789Z"}
```

Missing or wrong API key, and missing `Idempotency-Key`:

```bash
curl -i http://localhost:8000/api/v1/payments/<payment_id>
# 401 {"error":{"code":"unauthorized","message":"Missing or invalid API key"}}

curl -i -X POST http://localhost:8000/api/v1/payments -H 'X-API-Key: change-me' \
  -H 'Content-Type: application/json' \
  -d '{"amount": "1.00", "currency": "EUR", "description": "x", "webhook_url": "http://example.com/h"}'
# 400 {"error":{"code":"idempotency_key_required","message":"Idempotency-Key header is required"}}
```

### Errors

Every error has the same shape; `code` is stable, `message` is for humans. Rejected input values are
never echoed back.

```json
{"error": {"code": "validation_error", "message": "body.amount: Value error, Amount must be positive"}}
```

| Status | `code` | When |
| --- | --- | --- |
| 202 | | Payment accepted (new, or the original for a repeated key and payload) |
| 400 | `idempotency_key_required` | `Idempotency-Key` header missing or empty |
| 400 | `idempotency_key_invalid` | `Idempotency-Key` longer than 255 characters |
| 401 | `unauthorized` | `X-API-Key` missing or wrong |
| 404 | `payment_not_found` | Unknown or malformed `payment_id` |
| 404 | `not_found` | Unknown route (the message is the framework's `Not Found`) |
| 409 | `idempotency_key_conflict` | Key already used with a different payload |
| 405 | `http_error` | Wrong method for a route; any other framework HTTP error also uses `http_error` with its status and message |
| 422 | `validation_error` | Invalid body: non-positive amount, more than 2 decimals, amount above 1 000 000 000.00, unknown currency, empty description, `metadata` larger than 16 KiB (16384 bytes as compact UTF-8 JSON; message `body.metadata: Value error, Metadata must be at most 16384 bytes as compact JSON`), non-http(s) `webhook_url`, unknown fields |

Unexpected errors produce a plain `500` and are logged; details are not returned to clients.

### Webhook

The consumer sends `POST <webhook_url>` with a JSON body:

```json
{"payment_id": "0b0e5f1e-...", "status": "succeeded", "amount": "100.50",
 "currency": "USD", "processed_at": "2026-01-01T12:00:03.456789+00:00"}
```

A tiny receiver to try it (it must be reachable from the `consumer` container; on Docker Desktop
use `http://host.docker.internal:9000/hook` as `webhook_url`; on Linux that name needs an
`extra_hosts` entry, which compose does not add):

```python
# receiver.py: python receiver.py
from http.server import BaseHTTPRequestHandler, HTTPServer


class Hook(BaseHTTPRequestHandler):
    def do_POST(self):
        body = self.rfile.read(int(self.headers["Content-Length"]))
        print(body.decode(), flush=True)
        self.send_response(200)
        self.end_headers()


HTTPServer(("0.0.0.0", 9000), Hook).serve_forever()
```

### RabbitMQ UI and the DLQ

Open http://localhost:15672 (credentials are `RABBITMQ_USER` / `RABBITMQ_PASSWORD`; default `guest` / `guest`).

- Queues: `payments.new` (quorum) and `payments.new.dlq`. Exchanges: `payments` (direct) and
  `payments.dlx` (fanout).
- To see a message reach the DLQ, create a payment whose `webhook_url` always fails (for example
  `http://host.docker.internal:9999/hook` with nothing listening, or a receiver returning 500).
  After 3 deliveries (roughly 10-20 s: three webhook rounds with backoff each) the message
  appears in `payments.new.dlq`; use Get messages there to inspect it. The payment itself is
  already `succeeded` or `failed` in the DB; only the notification was not delivered.

## Design decisions and trade-offs

**Transactional outbox, at-least-once.** The payment row and its outbox row are inserted in one
transaction, so an event exists if and only if the payment does. The `relay` selects unpublished
rows with `FOR UPDATE SKIP LOCKED` (several relays can run without blocking or duplicating each
other's batches), publishes each with publisher confirms and `message_id` = outbox id, and marks it
published in the same transaction that holds the lock. A crash between the broker confirm and the
commit publishes the row again, so delivery is at-least-once and the consumer must tolerate
duplicates. If RabbitMQ is down, rows simply stay unpublished and are retried; the relay reconnects
on start. The relay publishes with `mandatory=True` on a channel with `on_return_raises`, so a
message that no queue accepts (for example a missing binding) raises instead of being dropped, and
its row stays unpublished and is retried. Rows confirmed in a batch are marked published with one
`UPDATE ... WHERE id = ANY(...)`, and rows confirmed before a failure in the same batch are still
marked.

**Poison outbox rows.** A row that can never be published would otherwise sit first in order and
raise on every poll, blocking the rows behind it. When a publish fails, the relay (in a separate
short transaction) increments `outbox.publish_attempts` and stores a truncated error in
`outbox.last_error`, and rows are taken in the order `(publish_attempts, created_at, id)`, so a
failing row falls behind healthy ones. Limitation: there is no maximum number of attempts and no
quarantine yet; such rows are retried forever, only with lower priority, and each failing poll still
raises and is logged (one new failing row is demoted per poll). Any exception raised while
publishing a row counts, not only broker errors (for example a payload that cannot be serialised),
and `run()` logs it and keeps polling, so one poison row cannot crash-loop the relay. Alert on
`publish_attempts` growing and inspect `last_error`.

**Idempotent consumer.** `payments.new` carries only `payment_id`. A finished payment skips the
gateway and only repeats the webhook, so a redelivered message does not charge twice. A message for a
payment that does not exist is logged and acknowledged (redelivery cannot help).

**Idempotency key.** `payments.idempotency_key` has a unique constraint and creation is
`INSERT ... ON CONFLICT DO NOTHING RETURNING`, so concurrent duplicates have exactly one winner and
there is no check-then-insert window. The request fingerprint (SHA-256 of the canonical payload;
`10.5` and `10.50` are equal) is stored with the payment: same key and same fingerprint returns the
original payment, same key and different fingerprint is `409`. The key is global, not per client
(there is a single static API key).

**Money.** The column is `NUMERIC(18,2)` with a positive check. The amount rules live in one
function, `validate_amount` in `domain.py`: it rejects non-positive amounts, more than 2 decimal
places (rejected, not rounded) and amounts above the maximum. The API schema calls it directly, and
the domain `Money` value object (`Decimal` plus `Currency`) calls it on construction; the ORM model
stores a plain numeric column and a currency string. The API accepts and returns amounts as decimal
strings, never floats. The scale and maximum live once, in `domain.py`.

**Delivery limit and DLQ.** `payments.new` is a quorum queue with `x-delivery-limit`, dead-letter
exchange `payments.dlx` (fanout) and `at-least-once` dead-lettering. RabbitMQ only honours
`at-least-once` together with `x-overflow: reject-publish`; without it the broker logs a warning and
silently falls back to at-most-once (checked on `rabbitmq:4.0`), so the queue sets it. The queue has
no max-length, so the overflow strategy never triggers and publishers are unaffected; a test
(`tests/test_relay.py`) checks the queue arguments on a real broker. `x-delivery-limit` counts
redeliveries, not deliveries, so it is set to 2 to get 3 deliveries in total (the constant
`QUEUE_MAX_DELIVERY_ATTEMPTS` in `messaging/topology.py`, checked against a real broker in
`tests/test_relay.py`). It is a queue argument, not a setting: it is independent of
`WEBHOOK_MAX_ATTEMPTS`, and changing it on an existing queue fails with `PRECONDITION_FAILED` (so an
existing `payments.new` created before `x-overflow` was added must be deleted once). The consumer
nacks with requeue on any exception, which makes RabbitMQ redeliver until the limit and then
dead-letter.

**Poison messages.** A message whose body is not valid JSON, has no `payment_id`, or has a
`payment_id` that is not a UUID fails validation before the handler runs. That counts as an
exception, so it is nacked and requeued like any other failure and ends in `payments.new.dlq`
after 3 deliveries, with its original body. It never loops forever and never touches a payment
(`tests/test_consumer.py`). A message for a payment that does not exist is the opposite case: it is
acknowledged and dropped, because redelivery cannot fix a missing row.

**Webhook retry.** Inside one delivery the webhook is posted up to `WEBHOOK_MAX_ATTEMPTS` (3) times
with exponential backoff plus jitter (`base * 2^(n-1)` + up to `base`, base 1 s). Retried: transport
errors (connect, timeout) and HTTP 5xx, 408, 429. Not retried: other 4xx and redirects (a permanent
rejection), which are not retried in-process but still fail the delivery. A failed delivery raises,
the message is requeued, and the next delivery skips the gateway (status is already final) and tries
the webhook again; after 3 deliveries the message goes to the DLQ. Worst case is therefore up to 9
POSTs to one URL.

**Trade-off: webhook retries run inside the handler.** The message stays unacknowledged while the
webhook is retried (at least about 3 s of backoff in the failing case), and a slow receiver holds a
consumer slot for up to `WEBHOOK_TIMEOUT_SECONDS` per attempt. This is the simplest way to satisfy
"3 attempts with exponential backoff" without extra delay queues. With real load, retries would move
to a delay queue or a separate webhook outbox.

**Consumer concurrency.** No database transaction or connection is held during the gateway sleep
(2-5 s); the payment is read, the session is closed, the gateway runs, then a new short transaction
stores the result. The consumer's concurrency is the RabbitMQ prefetch count, set explicitly with
`CONSUMER_PREFETCH_COUNT` (default 10, at least 1): at most that many messages are unacknowledged
and handled at once per consumer process. Throughput is therefore about prefetch / mean time per
message; with the default gateway (uniform 2-5 s, mean 3.5 s) that is roughly 10 / 3.5, about 3
payments per second (about 170 per minute) per consumer process, if webhooks answer quickly. Slow
webhook receivers lower it, because their retries hold a slot. Scale by raising the prefetch count
or running more consumer containers (`docker compose up --scale consumer=N`); a message that waits
in a prefetch buffer is not available to other consumers.

**State machine.** `pending -> succeeded | failed`, defined in one place (`domain.py`); both final
states are terminal. The result is written with a conditional `UPDATE ... WHERE status = 'pending'`,
so of several workers racing on one payment only the first write wins; the others adopt the stored
result instead of overwriting it.

**Known limitation: duplicate gateway call under true concurrency.** There is no `processing`
state or lock. If the same message is delivered to two workers at the same moment (for example a
relay duplicate consumed in parallel, or redelivery after a consumer crash mid-call), both may call
the gateway. The database stays correct (first write wins, one final status), but a real gateway
would need its own idempotency key to avoid a double charge. Webhook receivers can also get the same
notification more than once (at-least-once), so they should deduplicate by `payment_id`.

**No healthchecks for `relay` and `consumer`.** They serve no port, and a check that only proves the
process is alive adds nothing over `restart: unless-stopped`. A real signal (last successful poll,
consumer lag) needs the metrics listed below.

**Other.** Timestamps are UTC and timezone-aware. Payment IDs are random UUIDs. The webhook URL's
query string is never logged because it may carry tokens. The API key is compared in constant time.
Configuration is read in one place (`config.py`).

## Deliberately left out / next steps

- Per-client authentication and per-client idempotency scopes (today: one static API key, global
  idempotency keys).
- A real payment gateway client (with its own idempotency key and timeouts).
- HMAC signature on webhooks so receivers can verify the sender.
- SSRF hardening: `webhook_url` is only checked to be http(s); private and loopback address
  blocking, allow-lists and DNS pinning are not implemented.
- Metrics and tracing (queue depth, outbox lag, DLQ size, correlation ids); only logs exist.
- Outbox quarantine: a maximum number of publish attempts, after which a row is parked for manual
  review, and an alert on rows with many `publish_attempts`.
- Outbox cleanup: published rows are kept forever; add retention or partitioning.
- A `processing` state (or an advisory lock) to close the duplicate-gateway-call window, and a
  delay-queue based webhook retry that does not hold the message.
- A tool to inspect and replay messages from the DLQ.
