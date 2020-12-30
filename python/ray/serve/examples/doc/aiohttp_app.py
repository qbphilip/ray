# File name: aiohttp_app.py
from aiohttp import web

import ray
from ray import serve

# Connect to the running Ray cluster.
ray.init(address="auto")

# Connect to the running Ray Serve instance.
client = serve.connect()

my_handle = client.get_handle("my_endpoint")  # Returns a ServeHandle object.


# Define our AIOHTTP request handler.
async def handle_request(request):
    # Offload the computation to our Ray Serve backend.
    result = await my_handle.remote("dummy input")
    return web.Response(text=result)


app = web.Application()
app.add_routes([web.get('/dummy-model',
                        handle_request)])  # Sets up an HTTP endpoint.

if __name__ == '__main__':
    web.run_app(app)
