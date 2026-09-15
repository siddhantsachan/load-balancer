import subprocess
import time

def main():
    print("Starting Dummy Servers...")
    p1 = subprocess.Popen(["python", "dummy_server.py", "8081"])
    p2 = subprocess.Popen(["python", "dummy_server.py", "8082"])
    p3 = subprocess.Popen(["python", "dummy_server.py", "8083"])
    
    print("Starting Load Balancer...")
    lb = subprocess.Popen(["python", "load_balancer.py"])
    
    time.sleep(3)
    
    print("Running Locust Benchmark...")
    subprocess.run([
        "python", "-m", "locust", "-f", "locustfile.py", 
        "--headless", "-u", "50", "-r", "10", 
        "--run-time", "15s", "--host", "http://localhost:8080"
    ])
    
    print("Cleaning up...")
    p1.terminate()
    p2.terminate()
    p3.terminate()
    lb.terminate()

if __name__ == "__main__":
    main()
