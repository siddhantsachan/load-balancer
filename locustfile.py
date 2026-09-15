from locust import HttpUser, task, between

class LoadBalancerUser(HttpUser):
    wait_time = between(0.1, 0.5)

    @task
    def hit_proxy(self):
        self.client.get("/metrics")
