import asyncio
from aiohttp import web
import aiohttp
import hashlib
import time
import bisect

SERVER_REGISTRY = {
    "http://localhost:8081": {"weight": 3, "active_connections": 0, "healthy": True, "failures": 0, "state": "CLOSED", "retry_at": 0, "is_testing": False},
    "http://localhost:8082": {"weight": 2, "active_connections": 0, "healthy": True, "failures": 0, "state": "CLOSED", "retry_at": 0, "is_testing": False},
    "http://localhost:8083": {"weight": 1, "active_connections": 0, "healthy": True, "failures": 0, "state": "CLOSED", "retry_at": 0, "is_testing": False}
}
state_lock = asyncio.Lock()

class RoutingAlgorithms:
    def __init__(self):
        self.rr_index = 0
        self.wrr_list = []
        self.hash_ring = []
        self.ring_nodes = {}
        self.update_wrr_list()
        self.update_hash_ring()

    def update_wrr_list(self):
        seq = []
        for server, data in SERVER_REGISTRY.items():
            if data["healthy"] and data["state"] != "OPEN":
                seq.extend([server] * data.get("weight", 1))
        self.wrr_list = seq

    def update_hash_ring(self):
        self.hash_ring = []
        self.ring_nodes = {}
        for server, data in SERVER_REGISTRY.items():
            if data["healthy"] and data["state"] != "OPEN":
                for i in range(100):
                    vnode = f"{server}-vnode-{i}"
                    h = int(hashlib.md5(vnode.encode()).hexdigest(), 16)
                    self.hash_ring.append(h)
                    self.ring_nodes[h] = server
        self.hash_ring.sort()

    def round_robin(self, available_servers):
        if not self.wrr_list:
            return None
        server = self.wrr_list[self.rr_index % len(self.wrr_list)]
        self.rr_index += 1
        return server

    def least_connections(self, available_servers):
        return min(available_servers, key=lambda s: SERVER_REGISTRY[s]["active_connections"])

    def consistent_hashing(self, available_servers, client_ip):
        if not self.hash_ring:
            return None
        if not client_ip:
            client_ip = "unknown"
        h = int(hashlib.md5(client_ip.encode()).hexdigest(), 16)
        idx = bisect.bisect(self.hash_ring, h)
        if idx == len(self.hash_ring):
            idx = 0
        return self.ring_nodes[self.hash_ring[idx]]

router = RoutingAlgorithms()
CURRENT_ALGO = "consistent_hashing"

async def health_check_loop(app):
    session = app['session']
    while True:
        await asyncio.sleep(5)
        for server in list(SERVER_REGISTRY.keys()):
            try:
                async with session.get(f"{server}/health", timeout=2) as response:
                    async with state_lock:
                        if response.status == 200 and not SERVER_REGISTRY[server]["healthy"]:
                            SERVER_REGISTRY[server]["healthy"] = True
                            router.update_wrr_list()
                            router.update_hash_ring()
            except (aiohttp.ClientError, asyncio.TimeoutError):
                async with state_lock:
                    if SERVER_REGISTRY[server]["healthy"]:
                        SERVER_REGISTRY[server]["healthy"] = False
                        router.update_wrr_list()
                        router.update_hash_ring()

async def get_next_server(client_ip):
    async with state_lock:
        available = []
        now = time.time()
        for s, data in SERVER_REGISTRY.items():
            if not data["healthy"]: 
                continue
            
            if data["state"] == "OPEN":
                if now > data["retry_at"]:
                    data["state"] = "HALF_OPEN"
                    data["is_testing"] = False
                else:
                    continue
                    
            if data["state"] == "HALF_OPEN":
                if data["is_testing"]:
                    continue
                data["is_testing"] = True
                return s

            available.append(s)
            
        if not available: 
            return None
            
        if CURRENT_ALGO == "least_connections": 
            return router.least_connections(available)
        elif CURRENT_ALGO == "consistent_hashing": 
            return router.consistent_hashing(available, client_ip)
        else:
            return router.round_robin(available)

async def handle_request(request):
    client_ip = request.remote
    backend_url = await get_next_server(client_ip)
    
    if not backend_url:
        return web.Response(status=503, text="No healthy backend servers available")
        
    async with state_lock:
        SERVER_REGISTRY[backend_url]["active_connections"] += 1
        
    session = request.app['session']
    try:
        async with session.get(f"{backend_url}{request.path}", timeout=3) as backend_resp:
            body = await backend_resp.read()
            async with state_lock:
                reg = SERVER_REGISTRY[backend_url]
                if reg["state"] == "HALF_OPEN":
                    reg["state"] = "CLOSED"
                    reg["failures"] = 0
                    reg["is_testing"] = False
            return web.Response(body=body, status=backend_resp.status)
    except (aiohttp.ClientError, asyncio.TimeoutError):
        async with state_lock:
            reg = SERVER_REGISTRY[backend_url]
            reg["failures"] += 1
            if reg["failures"] >= 3 and reg["state"] == "CLOSED":
                reg["state"] = "OPEN"
                reg["retry_at"] = time.time() + 10
            elif reg["state"] == "HALF_OPEN":
                reg["state"] = "OPEN"
                reg["retry_at"] = time.time() + 10
                reg["is_testing"] = False
        return web.Response(status=502, text="Bad Gateway")
    finally:
        async with state_lock:
            SERVER_REGISTRY[backend_url]["active_connections"] -= 1

# --- RATE LIMITING MIDDLEWARE ---
RATE_LIMIT_DB = {}

@web.middleware
async def rate_limiter(request, handler):
    if request.path.startswith("/admin") or request.path.startswith("/metrics") or request.path.startswith("/dashboard"): 
        return await handler(request)
    
    ip = request.remote or "unknown"
    now = time.time()
    
    if ip not in RATE_LIMIT_DB:
        RATE_LIMIT_DB[ip] = [10, now]
    else:
        tokens, last_refill = RATE_LIMIT_DB[ip]
        elapsed = now - last_refill
        RATE_LIMIT_DB[ip][0] = min(10, tokens + elapsed)
        RATE_LIMIT_DB[ip][1] = now
        
    if RATE_LIMIT_DB[ip][0] < 1:
        return web.Response(status=429, text="Too Many Requests")
        
    RATE_LIMIT_DB[ip][0] -= 1
    return await handler(request)

# --- API KEY AUTH MIDDLEWARE ---
@web.middleware
async def api_key_auth(request, handler):
    if request.path.startswith("/admin"):
        if request.headers.get("X-API-Key") != "secret-key":
            return web.Response(status=401, text="Unauthorized")
    return await handler(request)

# --- ENDPOINTS ---
async def metrics(request):
    return web.json_response(SERVER_REGISTRY)

async def dashboard(request):
    with open("dashboard.html", "r") as f:
        return web.Response(text=f.read(), content_type="text/html")

async def add_server(request):
    data = await request.json()
    url = data.get("url")
    weight = data.get("weight", 1)
    if not url: return web.Response(status=400, text="url required")
    async with state_lock:
        if url not in SERVER_REGISTRY:
            SERVER_REGISTRY[url] = {"weight": weight, "active_connections": 0, "healthy": True, "failures": 0, "state": "CLOSED", "retry_at": 0, "is_testing": False}
            router.update_wrr_list()
            router.update_hash_ring()
    return web.json_response({"status": "added", "url": url})

async def remove_server(request):
    url = request.query.get("url")
    async with state_lock:
        if url in SERVER_REGISTRY:
            del SERVER_REGISTRY[url]
            router.update_wrr_list()
            router.update_hash_ring()
    return web.json_response({"status": "removed", "url": url})

async def on_startup(app):
    app['session'] = aiohttp.ClientSession()
    app['health_check'] = asyncio.create_task(health_check_loop(app))

async def on_cleanup(app):
    app['health_check'].cancel()
    await app['session'].close()

if __name__ == "__main__":
    app = web.Application(middlewares=[rate_limiter, api_key_auth])
    
    app.router.add_get('/metrics', metrics)
    app.router.add_get('/dashboard', dashboard)
    app.router.add_post('/admin/servers', add_server)
    app.router.add_delete('/admin/servers', remove_server)
    app.router.add_route('*', '/{tail:.*}', handle_request)
    
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    
    print("Starting async load balancer on port 8080...")
    web.run_app(app, port=8080)
