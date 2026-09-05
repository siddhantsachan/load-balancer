from http.server import HTTPServer, BaseHTTPRequestHandler
import requests
import itertools

SERVERS = [
    "http://localhost:8081",
    "http://localhost:8082",
    "http://localhost:8083"
]

server_iterator = itertools.cycle(SERVERS)

class LoadBalancerHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        backend_url = next(server_iterator)
        print(f"Routing request to: {backend_url}")
        
        try:
            response = requests.get(f"{backend_url}{self.path}")
            
            self.send_response(response.status_code)
            for key, value in response.headers.items():
                if key.lower() not in ['server', 'date', 'transfer-encoding', 'connection']:
                    self.send_header(key, value)
            self.end_headers()
            self.wfile.write(response.content)
            
        except requests.exceptions.RequestException as e:
            print(f"Error connecting to {backend_url}: {e}")
            self.send_response(502)
            self.send_header("Content-type", "text/plain")
            self.end_headers()
            self.wfile.write(b"502 Bad Gateway: Backend server unreachable.\n")

if __name__ == "__main__":
    port = 8080
    server = HTTPServer(("localhost", port), LoadBalancerHandler)
    print(f"Starting simple load balancer on port {port}...")
    print(f"Routing traffic to: {SERVERS}")
    server.serve_forever()
