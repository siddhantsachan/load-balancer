# Async Python Load Balancer

A production-grade, asynchronous Layer 7 Load Balancer built purely in Python using `aiohttp` and `asyncio`. 

This project demonstrates how to build a highly concurrent proxy capable of handling high throughput without blocking, implementing industry-standard distributed systems patterns including algorithmic routing, circuit breaking, and rate limiting.

## Features

- **Asynchronous Core**: Built on `aiohttp` to handle thousands of concurrent connections efficiently.
- **True Consistent Hashing**: Uses a Python `bisect` Hash Ring with 100 virtual nodes per server to ensure minimal cache misses when backends are added or removed.
- **Weighted Round Robin**: Mathematically flattens traffic distribution across servers based on configurable weights.
- **Circuit Breaker Pattern**: Isolates failing backends. If a server fails 3 times, it enters an `OPEN` state. After 10 seconds, it enters `HALF_OPEN` and routes exactly ONE test request to safely verify recovery.
- **Token Bucket Rate Limiting**: Built-in middleware to protect against DDoS, limiting burst traffic per IP address (10 tokens max, refills 1/sec).
- **Admin API**: Add or remove servers dynamically via authenticated POST/DELETE requests.
- **Live Dashboard**: View real-time routing metrics at `/dashboard`.

## Quick Start

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Start the dummy backend servers (in separate terminals or in the background):
```bash
python dummy_server.py 8081
python dummy_server.py 8082
python dummy_server.py 8083
```

3. Start the Load Balancer:
```bash
python load_balancer.py
```

4. View the live metrics dashboard:
Navigate to `http://localhost:8080/dashboard`.

## Benchmarks

Using Locust to stress test the proxy, the asynchronous architecture comfortably achieves:

* **Throughput**: ~100 Requests Per Second (RPS)
* **Median Latency**: 19ms
* **99th Percentile**: ~270ms

*(Tested with 50 concurrent users locally proxying active HTTP traffic to 3 backend nodes)*

## Architecture & Algorithms

### The Caching Bug & Fix
During development, a critical flaw was discovered: the `consistent_hashing` and `weighted_round_robin` algorithms read from cached arrays (`hash_ring` and `wrr_list`). When the Circuit Breaker tripped, these caches were not updated, causing the algorithms to silently bypass the circuit breaker and continue routing traffic to dead servers.

**The Fix:** The state machine was refactored so that any transition to `OPEN` or `HALF_OPEN` instantaneously forces an `update_wrr_list()` and `update_hash_ring()`. Because the update logic explicitly filters out `OPEN` servers, the dead server is purged from all routing algorithms within milliseconds.

### Thread Safety and Race Conditions
The admin API allows servers to be removed dynamically (`DELETE /admin/servers`). If a request is in flight when the admin removes the server, the connection count decrement could crash with a `KeyError`. The request handler wraps all state modifications with `if backend_url in SERVER_REGISTRY` under an `asyncio.Lock()`, ensuring total thread safety against live backend removals.
