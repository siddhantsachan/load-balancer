import pytest
from load_balancer import RoutingAlgorithms

def test_round_robin():
    router = RoutingAlgorithms()
    servers = ["A", "B", "C"]
    assert router.round_robin(servers) == "A"
    assert router.round_robin(servers) == "B"
    assert router.round_robin(servers) == "C"
    assert router.round_robin(servers) == "A"
