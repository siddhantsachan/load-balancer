import sys
from http.server import HTTPServer, BaseHTTPRequestHandler

class DummyServer(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/plain")
        self.end_headers()
        port = self.server.server_port
        self.wfile.write(f"Response from dummy backend server running on port {port}\n".encode("utf-8"))

if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8081
    server = HTTPServer(("localhost", port), DummyServer)
    print(f"Starting dummy server on port {port}...")
    server.serve_forever()
