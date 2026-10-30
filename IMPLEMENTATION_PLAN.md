# Telemetry Ingestion Pipeline: 4-Day Implementation Plan

## Day 1: Foundation
- Set up the project, folders and config (port, webhook URL, worker count, queue size)
- Build the `/v1/telemetry` endpoint with API key checks and basic validation
- Add the queue with 202/503 responses
- Write the transformation logic and test it against the sample input and output

## Day 2: Processing and delivery
- Build the worker pool that pulls from the queue
- Connect transformation and the downstream push
- Build a small mock downstream server that fails randomly
- Add the timeout, retries with backoff, and the DLQ file

## Day 3: Monitoring and stability
- Add the `/metrics` endpoint and counters
- Add graceful shutdown so queued events are finished before exit
- Handle edge cases like bad types, missing fields, and wrong tenant
- Write tests for the main paths and failure paths

## Day 4: Polish and delivery
- Run a simple load test to check backpressure and throughput
- Clean up code and logging
- Write the README: how to run it, design choices, assumptions, possible improvements
- Optional: add a Dockerfile, a Makefile, or a DLQ replay command

> **Note:** Day 4 is your buffer. Have days 1–3 working before you add any extras.
