import pytest
import time
from load_balancer import RoutingAlgorithms, SERVER_REGISTRY

@pytest.fixture(autouse=True)
def reset_state():
    for server in SERVER_REGISTRY:
        SERVER_REGISTRY[server]["healthy"] = True
        SERVER_REGISTRY[server]["state"] = "CLOSED"
        SERVER_REGISTRY[server]["failures"] = 0
        SERVER_REGISTRY[server]["active_connections"] = 0
        SERVER_REGISTRY[server]["is_testing"] = False

def test_weighted_round_robin():
    router = RoutingAlgorithms()
    counts = {"http://localhost:8081": 0, "http://localhost:8082": 0, "http://localhost:8083": 0}
    servers = list(SERVER_REGISTRY.keys())
    
    for _ in range(6):
        res = router.round_robin(servers)
        counts[res] += 1
        
    assert counts["http://localhost:8081"] == 3
    assert counts["http://localhost:8082"] == 2
    assert counts["http://localhost:8083"] == 1

def test_consistent_hashing():
    router = RoutingAlgorithms()
    servers = list(SERVER_REGISTRY.keys())
    s1 = router.consistent_hashing(servers, "192.168.1.1")
    s2 = router.consistent_hashing(servers, "192.168.1.1")
    s3 = router.consistent_hashing(servers, "10.0.0.5")
    assert s1 == s2
    assert s1 is not None

def test_circuit_breaker_isolation():
    SERVER_REGISTRY["http://localhost:8081"]["failures"] = 3
    SERVER_REGISTRY["http://localhost:8081"]["state"] = "OPEN"
    SERVER_REGISTRY["http://localhost:8081"]["retry_at"] = time.time() + 10
    
    router = RoutingAlgorithms()
    router.update_wrr_list()
    
    assert "http://localhost:8081" not in router.wrr_list
