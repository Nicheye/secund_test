# Task spec: asynchronous payment processing service

Source: `tz_tech.pdf` (English translation of the original Russian spec). The task is a skills
check and is unpaid.

## Description

A microservice for asynchronous payment processing. It accepts payment requests, processes them via
an external payment gateway (emulated) and notifies the client of the result through a webhook.

## Entity: Payment

- Payment ID (unique identifier)
- Amount (decimal)
- Currency (RUB, USD, EUR)
- Description (string)
- Metadata (JSON field for extra information)
- Status (pending, succeeded, failed)
- Idempotency key (unique key protecting against duplicates)
- Webhook URL (to notify about the result)
- Created and processed dates

## API

All endpoints require a static API key in the `X-API-Key` header.

### 1. Create payment: `POST /api/v1/payments`

- Header: `Idempotency-Key` (required)
- Body: amount, currency, description, metadata, webhook_url
- Response: `202 Accepted`, payment_id, status, created_at

### 2. Get payment: `GET /api/v1/payments/{payment_id}`

- Response: detailed payment information

## Technical requirements

### Message broker

- On payment creation an event is published to the `payments.new` queue.
- One consumer:
  - receives the message from the queue
  - emulates payment processing (2-5 s, 90% success, 10% failure)
  - updates the status in the DB
  - sends a webhook notification to the given URL
  - retries on webhook delivery errors

### Delivery guarantees

- Outbox pattern for guaranteed event publication
- Idempotency key protects against duplicates
- Dead Letter Queue for messages not processed after 3 attempts

### Stack

- FastAPI + Pydantic v2
- SQLAlchemy 2.0 (async mode)
- PostgreSQL
- RabbitMQ (FastStream)
- Alembic (migrations)
- Docker + docker-compose

## Deliverables

1. Models and migrations: `payments` and `outbox` tables
2. API endpoints: create and get payment
3. Consumer: one handler doing everything
4. Outbox pattern: guaranteed event delivery
5. Retry: 3 attempts with exponential backoff
6. Dead Letter Queue: for finally failed messages
7. Docker: compose file with postgres, rabbitmq, api, consumer
8. Documentation: README with run instructions and examples

## Evaluation criteria

- Architecture and code cleanliness
- Correct implementation of the Outbox pattern
- Working with RabbitMQ (queues, exchanges, DLQ)
- Idempotency
- Error handling and retry logic
- Working Docker environment
