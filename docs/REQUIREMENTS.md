# Telemetry Ingestion Pipeline: Requirements

## 1. Functional Requirements (FR)
- **FR-1 (Ingestion):** Expose `POST /v1/telemetry`. Validate `X-API-Key` header; reject unauthorized with `401`. Push to buffered queue asynchronously. Return `202 Accepted` on success, `503 Service Unavailable` when queue is full.
- **FR-2 (Transformation):** Efficiently transform flat indexed keys (`dim_rkX`/`rvX`/`rtX`, `met_rkX`/`rvX`/`rtX`) into nested JSON (`dimensions`, `metrics`, `client_metadata`). Filter out malformed/incomplete data.
- **FR-3 (Downstream Delivery):** Forward transformed events via HTTP POST to webhook destination with strict timeout (5s).
- **FR-4 (Retry & DLQ):** Exponential backoff retry on failure (up to 3 attempts). Route exhausted failures to Dead Letter Queue file (`dlq_failed_events.jsonl`).
- **FR-5 (Observability):** Expose `GET /metrics` reporting atomic counters: received, processed, dlq count, active workers.

## 2. Non-Functional Requirements (NFR)
- **NFR-1 (Throughput & Latency):** Support ≥ 15,000 req/sec ingress. Sub-10ms p99 ingress response time.
- **NFR-2 (Allocation Efficiency):** Minimal heap allocations on the hot path (transformation and queuing).
- **NFR-3 (Bounded Workers):** Managed pool of $N$ worker goroutines reading from a single shared channel to prevent goroutine leaks.
- **NFR-4 (Backpressure):** Bounded queue capacity; non-blocking enqueue via `select/default`.
- **NFR-5 (Graceful Shutdown):** Intercept SIGINT/SIGTERM, stop HTTP intake, drain buffered queue, and cleanly stop workers within a timeout.

## 3. Key Edge Cases & Assumptions
- **Malformed Triplets:** Incomplete dimension/metric triplets (e.g. key without value) are discarded without dropping the entire event.
- **Downstream Outage:** Retries exhaust quickly and fall back to DLQ; worker must not block other queued events.
- **Queue Drain on Exit:** Workers must finish processing in-flight events before exit.
