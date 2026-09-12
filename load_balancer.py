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

class RoutingAlgorithms:
    def __init__(self):
        self.rr_index = 0

    def round_robin(self, available_servers):
        server = available_servers[self.rr_index % len(available_servers)]
        self.rr_index += 1
        return server

    def least_connections(self, available_servers):
        return min(available_servers, key=lambda s: SERVER_REGISTRY[s]["active_connections"])

router = RoutingAlgorithms()
CURRENT_ALGO = "least_connections"

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

async def get_next_server():
    async with state_lock:
        available = [s for s, data in SERVER_REGISTRY.items() if data["healthy"]]
        if not available: return None
        if CURRENT_ALGO == "least_connections": return router.least_connections(available)
        return router.round_robin(available)

async def handle_request(request):
    backend_url = await get_next_server()
    if not backend_url:
        return web.Response(status=503, text="No healthy backend servers available")
        
    # Track connection
    async with state_lock:
        SERVER_REGISTRY[backend_url]["active_connections"] += 1
        
    session = request.app['session']
    try:
        async with session.get(f"{backend_url}{request.path}", timeout=3) as backend_resp:
            body = await backend_resp.read()
            # INTENTIONAL BUG: Decrement only happens on success!
            async with state_lock:
                SERVER_REGISTRY[backend_url]["active_connections"] -= 1
            return web.Response(body=body, status=backend_resp.status)
    except (aiohttp.ClientError, asyncio.TimeoutError):
        # We forgot to decrement active_connections on error!
        return web.Response(status=502, text="Bad Gateway")

async def on_startup(app):
    app['session'] = aiohttp.ClientSession()
    app['health_check'] = asyncio.create_task(health_check_loop(app))

async def on_cleanup(app):
    app['health_check'].cancel()
    await app['session'].close()

if __name__ == "__main__":
    app = web.Application()
    app.router.add_route('*', '/{tail:.*}', handle_request)
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    web.run_app(app, port=8080)
