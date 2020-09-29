import ray
from ray import serve
import time

ray.init(_system_config={"enable_multi_tenancy": True})
serve.start(detached=True)
while (True):
    time.sleep(0.1)
