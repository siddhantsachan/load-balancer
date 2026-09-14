import asyncio
from aiohttp import web
import aiohttp

# REFACTOR: Deleted hardcoded SERVERS list. Replaced with complex registry tracking state.
SERVER_REGISTRY = {
    "http://localhost:8081": {"weight": 1, "active_connections": 0, "healthy": True},
    "http://localhost:8082": {"weight": 2, "active_connections": 0, "healthy": True},
    "http://localhost:8083": {"weight": 1, "active_connections": 0, "healthy": True}
}
state_lock = asyncio.Lock()

import hashlib
class RoutingAlgorithms:
    def consistent_hashing(self, available_servers, client_ip):
        if not client_ip: client_ip = "unknown"
        hash_val = int(hashlib.md5(client_ip.encode()).hexdigest(), 16)
        return available_servers[hash_val % len(available_servers)]

    def __init__(self):
        self.rr_index = 0

    def round_robin(self, available_servers):
        server = available_servers[self.rr_index % len(available_servers)]
        self.rr_index += 1
        return server

    def least_connections(self, available_servers):
        return min(available_servers, key=lambda s: SERVER_REGISTRY[s]["active_connections"])

router = RoutingAlgorithms()
CURRENT_ALGO = "consistent_hashing"

async def health_check_loop(app):
    session = app['session']
    while True:
        await asyncio.sleep(5)
        for server in list(SERVER_REGISTRY.keys()):
            try:
                async with session.get(server, timeout=2) as response:
                    async with state_lock:
                        if response.status == 200 and not SERVER_REGISTRY[server]["healthy"]:
                            SERVER_REGISTRY[server]["healthy"] = True
            except (aiohttp.ClientError, asyncio.TimeoutError):
                async with state_lock:
                    if SERVER_REGISTRY[server]["healthy"]:
                        SERVER_REGISTRY[server]["healthy"] = False

async def get_next_server(client_ip):
    async with state_lock:
        available = [s for s, data in SERVER_REGISTRY.items() if data["healthy"]]
        if not available: return None
        if CURRENT_ALGO == "least_connections": return router.least_connections(available)
        if CURRENT_ALGO == "consistent_hashing": return router.consistent_hashing(available, client_ip)
        return router.round_robin(available)

async def handle_request(request):
    backend_url = await get_next_server(request.remote)
    if not backend_url:
        return web.Response(status=503, text="No healthy backend servers available")
        
    # Track connection
    async with state_lock:
        SERVER_REGISTRY[backend_url]["active_connections"] += 1
        
    session = request.app['session']
    try:
        async with session.get(f"{backend_url}{request.path}", timeout=3) as backend_resp:
            body = await backend_resp.read()
            return web.Response(body=body, status=backend_resp.status)
    except (aiohttp.ClientError, asyncio.TimeoutError):
        return web.Response(status=502, text="Bad Gateway")
    finally:
        # BUG FIXED: Decrement connection count NO MATTER WHAT
        async with state_lock:
            SERVER_REGISTRY[backend_url]["active_connections"] -= 1

async def on_startup(app):
    app['session'] = aiohttp.ClientSession()
    app['health_check'] = asyncio.create_task(health_check_loop(app))

async def on_cleanup(app):
    app['health_check'].cancel()
    await app['session'].close()

if __name__ == "__main__":
    app = web.Application(middlewares=[rate_limiter, api_key_auth])
    app.router.add_post('/admin/servers', add_server)
    app.router.add_delete('/admin/servers', remove_server)
    app.router.add_route('*', '/{tail:.*}', handle_request)
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    web.run_app(app, port=8080)

# --- ADMIN API ---
async def add_server(request):
    data = await request.json()
    url = data.get("url")
    if not url: return web.Response(status=400, text="url required")
    async with state_lock:
        if url not in SERVER_REGISTRY:
            SERVER_REGISTRY[url] = {"weight": 1, "active_connections": 0, "healthy": True}
    return web.json_response({"status": "added", "url": url})

async def remove_server(request):
    url = request.query.get("url")
    async with state_lock:
        if url in SERVER_REGISTRY:
            del SERVER_REGISTRY[url]
    return web.json_response({"status": "removed", "url": url})

# --- RATE LIMITING MIDDLEWARE ---
import time
RATE_LIMIT_DB = {}

@web.middleware
async def api_key_auth(request, handler):
    if request.path.startswith("/admin"):
        if request.headers.get("X-API-Key") != "secret-key":
            return web.Response(status=401, text="Unauthorized")
    return await handler(request)
 # IP -> [tokens, last_refill]

@web.middleware
async def rate_limiter(request, handler):
    if request.path.startswith("/admin"): return await handler(request)
    
    ip = request.remote or "unknown"
    now = time.time()
    
    if ip not in RATE_LIMIT_DB:
        RATE_LIMIT_DB[ip] = [10, now] # 10 tokens max
    else:
        tokens, last_refill = RATE_LIMIT_DB[ip]
        elapsed = now - last_refill
        RATE_LIMIT_DB[ip][0] = min(10, tokens + elapsed)
        RATE_LIMIT_DB[ip][1] = now
        
    if RATE_LIMIT_DB[ip][0] < 1:
        return web.Response(status=429, text="Too Many Requests")
        
    RATE_LIMIT_DB[ip][0] -= 1
    return await handler(request)
