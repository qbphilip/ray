import ray
from ray import serve
import os


def tf_version(request):
    import tensorflow
    return ("This backend is using tensorflow version " +
            tensorflow.__version__ + "with env" + str(os.environ))


ray.init(address="auto")
client = serve.connect()
client.create_backend("tf1", tf_version)
client.create_endpoint("tf1", backend="tf1", route="/tf1")
