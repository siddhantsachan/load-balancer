import pytest
from aiohttp import web
from load_balancer import (
    rate_limiter, api_key_auth, metrics, add_server, 
    handle_request, on_startup, on_cleanup, 
    SERVER_REGISTRY, RATE_LIMIT_DB,
    MAX_FAILURES, RATE_LIMIT_CAPACITY, ADMIN_API_KEY,
    reset_registry
)
import time

import pytest_asyncio

@pytest_asyncio.fixture
async def cli(aiohttp_client):
    app = web.Application(middlewares=[rate_limiter, api_key_auth])
    app.router.add_get('/metrics', metrics)
    app.router.add_post('/admin/servers', add_server)
    app.router.add_route('*', '/{tail:.*}', handle_request)
    
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    
    RATE_LIMIT_DB.clear()
    reset_registry()
        
    return await aiohttp_client(app)

@pytest.mark.asyncio
async def test_api_key_auth(cli):
    # Missing key
    resp = await cli.post('/admin/servers', json={"url": "http://test"})
    assert resp.status == 401
    
    # Wrong key
    resp = await cli.post('/admin/servers', headers={"X-API-Key": "wrong"}, json={"url": "http://test"})
    assert resp.status == 401

    # Correct key
    resp = await cli.post('/admin/servers', headers={"X-API-Key": ADMIN_API_KEY}, json={"url": "http://test"})
    assert resp.status == 200

@pytest.mark.asyncio
async def test_rate_limiter(cli):
    # The bucket holds 10 tokens. Refills 1 per sec.
    # Send 20 requests rapidly to guarantee 429 against a NORMAL path.
    statuses = []
    for _ in range(RATE_LIMIT_CAPACITY * 2):
        resp = await cli.get('/some_normal_path')
        statuses.append(resp.status)
        
    assert 429 in statuses

@pytest.mark.asyncio
async def test_circuit_breaker_integration(cli):
    # Change ports to non-existent ones so we guarantee 502 failures even if dummy servers are running
    SERVER_REGISTRY.clear()
    SERVER_REGISTRY["http://localhost:9991"] = {"weight": 1, "active_connections": 0, "healthy": True, "failures": 0, "state": "CLOSED", "retry_at": 0, "is_testing": False}
    SERVER_REGISTRY["http://localhost:9992"] = {"weight": 1, "active_connections": 0, "healthy": True, "failures": 0, "state": "CLOSED", "retry_at": 0, "is_testing": False}
    SERVER_REGISTRY["http://localhost:9993"] = {"weight": 1, "active_connections": 0, "healthy": True, "failures": 0, "state": "CLOSED", "retry_at": 0, "is_testing": False}
    from load_balancer import router
    router.update_wrr_list()
    router.update_hash_ring()
    
    # We have 3 servers. Each needs MAX_FAILURES to open.
    for _ in range(3 * MAX_FAILURES):
        resp = await cli.get('/test_path')
        assert resp.status in (502, 503)
        
    # Now all circuits should be OPEN or marked unhealthy.
    # The load balancer should fast-fail with 503 No healthy servers.
    resp = await cli.get('/test_path')
    assert resp.status == 503
    
@pytest.mark.asyncio
async def test_half_open_isolation(cli):
    # Force 9991 to be OPEN but ready for a test request (retry_at in the past)
    SERVER_REGISTRY.clear()
    SERVER_REGISTRY["http://localhost:9991"] = {"weight": 1, "active_connections": 0, "healthy": True, "failures": MAX_FAILURES, "state": "OPEN", "retry_at": time.time() - 10, "is_testing": False}
    SERVER_REGISTRY["http://localhost:9992"] = {"weight": 1, "active_connections": 0, "healthy": True, "failures": MAX_FAILURES, "state": "OPEN", "retry_at": time.time() + 100, "is_testing": False}
    SERVER_REGISTRY["http://localhost:9993"] = {"weight": 1, "active_connections": 0, "healthy": True, "failures": MAX_FAILURES, "state": "OPEN", "retry_at": time.time() + 100, "is_testing": False}
    from load_balancer import router
    router.update_wrr_list()
    router.update_hash_ring()
    
    # This request should map to 9991. It will test it, fail, and re-open the circuit.
    resp = await cli.get('/test_path')
    assert resp.status == 502
    
    # Verify 9991 is back to OPEN state with a future retry time
    assert SERVER_REGISTRY["http://localhost:9991"]["state"] == "OPEN"
    assert SERVER_REGISTRY["http://localhost:9991"]["retry_at"] > time.time()
