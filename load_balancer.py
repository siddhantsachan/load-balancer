import asyncio
from aiohttp import web
import aiohttp
import hashlib
import time
import bisect
import os

# --- CONFIGURATION ---
MAX_FAILURES = 3
RETRY_TIMEOUT_SEC = 10
VIRTUAL_NODES = 100
RATE_LIMIT_CAPACITY = 10
RATE_LIMIT_REFILL_RATE = 1.0
ADMIN_API_KEY = os.environ.get("ADMIN_API_KEY", "secret-key")
LISTEN_PORT = 8080
HEALTH_CHECK_INTERVAL_SEC = 5
HEALTH_CHECK_TIMEOUT_SEC = 2
PROXY_REQUEST_TIMEOUT_SEC = 3
# ---------------------
SERVER_REGISTRY = {
    "http://localhost:8081": {"weight": 3, "active_connections": 0, "healthy": True, "failures": 0, "state": "CLOSED", "retry_at": 0, "is_testing": False},
    "http://localhost:8082": {"weight": 2, "active_connections": 0, "healthy": True, "failures": 0, "state": "CLOSED", "retry_at": 0, "is_testing": False},
    "http://localhost:8083": {"weight": 1, "active_connections": 0, "healthy": True, "failures": 0, "state": "CLOSED", "retry_at": 0, "is_testing": False}
}

def reset_registry():
    SERVER_REGISTRY.clear()
    SERVER_REGISTRY["http://localhost:8081"] = {"weight": 3, "active_connections": 0, "healthy": True, "failures": 0, "state": "CLOSED", "retry_at": 0, "is_testing": False}
    SERVER_REGISTRY["http://localhost:8082"] = {"weight": 2, "active_connections": 0, "healthy": True, "failures": 0, "state": "CLOSED", "retry_at": 0, "is_testing": False}
    SERVER_REGISTRY["http://localhost:8083"] = {"weight": 1, "active_connections": 0, "healthy": True, "failures": 0, "state": "CLOSED", "retry_at": 0, "is_testing": False}
    if 'router' in globals():
        router.sync_routing()

def set_server_status(url, healthy=None, state=None, failures=None, retry_at=None, is_testing=None):
    if url not in SERVER_REGISTRY: return
    reg = SERVER_REGISTRY[url]
    
    needs_rebuild = False
    if healthy is not None and reg["healthy"] != healthy:
        reg["healthy"] = healthy
        needs_rebuild = True
    if state is not None and reg["state"] != state:
        reg["state"] = state
        needs_rebuild = True
        
    if failures is not None: reg["failures"] = failures
    if retry_at is not None: reg["retry_at"] = retry_at
    if is_testing is not None: reg["is_testing"] = is_testing
    
    # Sync routing tables if server availability changed
    if needs_rebuild and 'router' in globals():
        router.sync_routing()

state_lock = asyncio.Lock()

class RoutingAlgorithms:
    def __init__(self):
        self.rr_index = 0
        self.wrr_list = []
        self.hash_ring = []
        self.ring_nodes = {}
        self.sync_routing()

    def update_wrr_list(self):
        # Flatten weighted servers into a simple list
        seq = []
        for server, data in SERVER_REGISTRY.items():
            if data["healthy"] and data["state"] == "CLOSED":
                seq.extend([server] * data.get("weight", 1))
        self.wrr_list = seq

    def update_hash_ring(self):
        # Build virtual nodes for even distribution
        self.hash_ring = []
        self.ring_nodes = {}
        for server, data in SERVER_REGISTRY.items():
            if data["healthy"] and data["state"] == "CLOSED":
                for i in range(VIRTUAL_NODES):
                    vnode = f"{server}-vnode-{i}"
                    h = int(hashlib.md5(vnode.encode()).hexdigest(), 16)
                    self.hash_ring.append(h)
                    self.ring_nodes[h] = server
        self.hash_ring.sort()
    def sync_routing(self):
        self.update_wrr_list()
        self.update_hash_ring()
    def round_robin(self, available_servers):
        if not self.wrr_list:
            return None
        server = self.wrr_list[self.rr_index % len(self.wrr_list)]
        self.rr_index += 1
        return server

    def least_connections(self, available_servers):
        # Pick the server currently handling the fewest requests
        return min(available_servers, key=lambda s: SERVER_REGISTRY[s]["active_connections"])

    def consistent_hashing(self, available_servers, client_ip):
        # Map IP address to the closest virtual node on the ring
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
    # Continuously poll backend health endpoints in the background
    session = app['session']
    while True:
        await asyncio.sleep(HEALTH_CHECK_INTERVAL_SEC)
        for server in list(SERVER_REGISTRY.keys()):
            try:
                async with session.get(f"{server}/health", timeout=HEALTH_CHECK_TIMEOUT_SEC) as response:
                    async with state_lock:
                        if response.status == 200 and not SERVER_REGISTRY[server]["healthy"]:
                            set_server_status(server, healthy=True)
                        elif response.status != 200 and SERVER_REGISTRY[server]["healthy"]:
                            set_server_status(server, healthy=False)
            except Exception:
                async with state_lock:
                    if SERVER_REGISTRY[server]["healthy"]:
                        set_server_status(server, healthy=False)

async def get_next_server(client_ip):
    async with state_lock:
        now = time.time()
        
        # Test recovering servers one request at a time
        for s, data in SERVER_REGISTRY.items():
            if data["state"] == "OPEN" and now > data["retry_at"]:
                set_server_status(s, state="HALF_OPEN", is_testing=False)
                
            if data["state"] == "HALF_OPEN":
                if data["is_testing"]:
                    continue
                data["is_testing"] = True
                return s

        # Route normal traffic to fully healthy servers
        available = [s for s, data in SERVER_REGISTRY.items() if data["healthy"] and data["state"] == "CLOSED"]
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
        async with session.get(f"{backend_url}{request.path}", timeout=PROXY_REQUEST_TIMEOUT_SEC) as backend_resp:
            body = await backend_resp.read()
            async with state_lock:
                if backend_url in SERVER_REGISTRY:
                    reg = SERVER_REGISTRY[backend_url]
                    if reg["state"] == "HALF_OPEN":
                        reg["state"] = "CLOSED"
                        reg["failures"] = 0
                        reg["is_testing"] = False
                        router.update_wrr_list()
                        router.update_hash_ring()
            return web.Response(body=body, status=backend_resp.status)
    except (aiohttp.ClientError, asyncio.TimeoutError):
        async with state_lock:
            if backend_url in SERVER_REGISTRY:
                reg = SERVER_REGISTRY[backend_url]
                reg["failures"] += 1
                if reg["failures"] >= MAX_FAILURES and reg["state"] == "CLOSED":
                    set_server_status(backend_url, state="OPEN", retry_at=time.time() + RETRY_TIMEOUT_SEC)
                elif reg["state"] == "HALF_OPEN":
                    set_server_status(backend_url, state="OPEN", retry_at=time.time() + RETRY_TIMEOUT_SEC, is_testing=False)
        return web.Response(status=502, text="Bad Gateway")
    finally:
        async with state_lock:
            if backend_url in SERVER_REGISTRY:
                SERVER_REGISTRY[backend_url]["active_connections"] -= 1

# --- RATE LIMITING MIDDLEWARE ---
RATE_LIMIT_DB = {}

@web.middleware
async def rate_limiter(request, handler):
    if request.path.startswith("/admin") or request.path.startswith("/metrics") or request.path.startswith("/dashboard"): 
        return await handler(request)
        
    if os.environ.get("ALLOW_LOAD_TEST_BYPASS") == "true" and request.headers.get("X-Load-Test") == "true":
        return await handler(request)
    
    ip = request.remote or "unknown"
    now = time.time()
    
    if ip not in RATE_LIMIT_DB:
        RATE_LIMIT_DB[ip] = [RATE_LIMIT_CAPACITY, now]
    else:
        tokens, last_refill = RATE_LIMIT_DB[ip]
        elapsed = now - last_refill
        RATE_LIMIT_DB[ip][0] = min(RATE_LIMIT_CAPACITY, tokens + (elapsed * RATE_LIMIT_REFILL_RATE))
        RATE_LIMIT_DB[ip][1] = now
        
    if RATE_LIMIT_DB[ip][0] < 1:
        return web.Response(status=429, text="Too Many Requests")
        
    RATE_LIMIT_DB[ip][0] -= 1
    return await handler(request)

# --- API KEY AUTH MIDDLEWARE ---
@web.middleware
async def api_key_auth(request, handler):
    if request.path.startswith("/admin"):
        if request.headers.get("X-API-Key") != ADMIN_API_KEY:
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
            SERVER_REGISTRY[url] = {
            "weight": weight,
            "active_connections": 0,
            "healthy": True,
            "failures": 0,
            "state": "CLOSED",
            "retry_at": 0,
            "is_testing": False
        }
        router.sync_routing()
    return web.json_response({"status": "added", "url": url})

async def remove_server(request):
    url = request.query.get("url")
    if not url:
        return web.json_response({"error": "Missing url parameter"}, status=400)
        
    async with state_lock:
        if url in SERVER_REGISTRY:
            del SERVER_REGISTRY[url]
            router.sync_routing()
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
    
    print(f"Starting async load balancer on port {LISTEN_PORT}...")
    web.run_app(app, port=LISTEN_PORT)
