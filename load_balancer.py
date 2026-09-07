import asyncio
from aiohttp import web
import aiohttp

SERVERS = [
    "http://localhost:8081",
    "http://localhost:8082",
    "http://localhost:8083"
]

# Using asyncio.Lock to protect shared state in the event loop
state_lock = asyncio.Lock()
healthy_servers = {server: True for server in SERVERS}
iterator_index = 0

async def health_check_loop(app):
    """Background task to ping backend servers."""
    while True:
        await asyncio.sleep(5)
        
        # We need a new session just for health checks
        async with aiohttp.ClientSession() as session:
            for server in SERVERS:
                try:
                    async with session.get(server, timeout=2) as response:
                        if response.status == 200:
                            async with state_lock:
                                if server not in healthy_servers:
                                    healthy_servers[server] = True
                                    print(f"[Health] {server} is back UP")
                except (aiohttp.ClientError, asyncio.TimeoutError):
                    async with state_lock:
                        if server in healthy_servers:
                            del healthy_servers[server]
                            print(f"[Health] {server} went DOWN")

async def get_next_server():
    global iterator_index
    async with state_lock:
        available_servers = [s for s in healthy_servers.keys()]
        if not available_servers:
            return None
        iterator_index = (iterator_index + 1) % len(available_servers)
        return available_servers[iterator_index]

async def handle_request(request):
    backend_url = await get_next_server()
    if not backend_url:
        return web.Response(status=503, text="No healthy backend servers available")
        
    print(f"Routing request to: {backend_url}")
    
    # INTENTIONAL BUG for tomorrow: Creating a new session per request leaks memory!
    session = aiohttp.ClientSession()
    
    try:
        async with session.get(f"{backend_url}{request.path}", timeout=3) as backend_resp:
            body = await backend_resp.read()
            # We don't close the session here! Memory leak!
            return web.Response(body=body, status=backend_resp.status)
    except (aiohttp.ClientError, asyncio.TimeoutError):
        return web.Response(status=502, text="Bad Gateway")

async def start_background_tasks(app):
    app['health_check'] = asyncio.create_task(health_check_loop(app))

async def cleanup_background_tasks(app):
    app['health_check'].cancel()
    await app['health_check']

if __name__ == "__main__":
    app = web.Application()
    app.router.add_route('*', '/{tail:.*}', handle_request)
    
    # Register background tasks
    app.on_startup.append(start_background_tasks)
    app.on_cleanup.append(cleanup_background_tasks)
    
    print("Starting asyncio load balancer on port 8080...")
    web.run_app(app, port=8080)
