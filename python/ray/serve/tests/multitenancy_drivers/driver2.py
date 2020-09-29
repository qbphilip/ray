import ray
from ray import serve
import os

import tensorflow

def tf_version(request):
    return  "This backend is using tensorflow version " + tensorflow.__version__ + "with env" + str(os.environ)
ray.init(address="auto")
client = serve.connect()
client.create_backend("tf2", tf_version)
client.create_endpoint("tf2", backend="tf2", route="/tf2")