import asyncio
from aiohttp import web
import aiohttp

SERVERS = [
    "http://localhost:8081",
    "http://localhost:8082",
    "http://localhost:8083"
]

state_lock = asyncio.Lock()
healthy_servers = {server: True for server in SERVERS}
iterator_index = 0

async def health_check_loop(app):
    session = app['session']
    while True:
        await asyncio.sleep(5)
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
    max_retries = 3
    
    # FIX: Use the single global session instead of creating one per request
    session = request.app['session']
    
    for attempt in range(max_retries):
        backend_url = await get_next_server()
        if not backend_url:
            return web.Response(status=503, text="No healthy backend servers available")
            
        print(f"Routing request to: {backend_url} (Attempt {attempt + 1})")
        
        try:
            async with session.get(f"{backend_url}{request.path}", timeout=3) as backend_resp:
                body = await backend_resp.read()
                return web.Response(body=body, status=backend_resp.status)
        except (aiohttp.ClientError, asyncio.TimeoutError):
            print(f"Failed to connect to {backend_url}. Retrying...")
            await asyncio.sleep(2 ** attempt)
            
    return web.Response(status=502, text="Bad Gateway")

async def on_startup(app):
    # Create ONE session for the entire application lifecycle
    app['session'] = aiohttp.ClientSession()
    app['health_check'] = asyncio.create_task(health_check_loop(app))

async def on_cleanup(app):
    app['health_check'].cancel()
    await app['health_check']
    # Cleanly close the session on shutdown
    await app['session'].close()

if __name__ == "__main__":
    app = web.Application()
    app.router.add_route('*', '/{tail:.*}', handle_request)
    
    # Register lifecycle hooks
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    
    web.run_app(app, port=8080)
