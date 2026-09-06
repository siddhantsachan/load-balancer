import time
import threading
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
import requests

SERVERS = [
    "http://localhost:8081",
    "http://localhost:8082",
    "http://localhost:8083"
]

healthy_servers = {server: True for server in SERVERS}
iterator_index = 0

def health_check_loop():
    while True:
        time.sleep(5)
        for server in SERVERS:
            try:
                response = requests.get(server, timeout=2)
                if response.status_code == 200:
                    if server not in healthy_servers:
                        healthy_servers[server] = True
                        print(f"[Health] {server} is back UP")
            except requests.exceptions.RequestException:
                if server in healthy_servers:
                    del healthy_servers[server]
                    print(f"[Health] {server} went DOWN")

health_thread = threading.Thread(target=health_check_loop, daemon=True)
health_thread.start()


class LoadBalancerHandler(BaseHTTPRequestHandler):
    def get_next_server(self):
        global iterator_index
        
        available_servers = [s for s in healthy_servers.keys()]
        
        if not available_servers:
            return None
            
        iterator_index = (iterator_index + 1) % len(available_servers)
        return available_servers[iterator_index]

    def do_GET(self):
        backend_url = self.get_next_server()
        if not backend_url:
            self.send_error(503, "No healthy backend servers available")
            return
            
        print(f"Routing request to: {backend_url}")
        
        try:
            response = requests.get(f"{backend_url}{self.path}", timeout=3)
            self.send_response(response.status_code)
            for key, value in response.headers.items():
                if key.lower() not in ['server', 'date', 'transfer-encoding', 'connection']:
                    self.send_header(key, value)
            self.end_headers()
            self.wfile.write(response.content)
        except requests.exceptions.RequestException:
            self.send_error(502, "Bad Gateway")

if __name__ == "__main__":
    port = 8080
    server = ThreadingHTTPServer(("localhost", port), LoadBalancerHandler)
    print(f"Starting multithreaded load balancer on port {port}...")
    server.serve_forever()
