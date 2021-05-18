import ray
import sys

ray.init()
ray_version = ray.__version__
python_version = f"{sys.version_info.major}{sys.version_info.minor}"
os_strings = {
    "darwin": "macosx_10_13_intel",
    "linux": "manylinux2014_x86_64",
    "win32": "win_amd64"
}

nightly_url = (f"https://s3-us-west-2.amazonaws.com/ray-wheels/latest/"
                f"ray-{ray_version}-cp{python_version}-"
                f"cp{python_version}{'m' if python_version != '38' else ''}"
                f"-{os_strings[sys.platform]}.whl")
runtime_env = {
    "conda": {
        "dependencies": [{
            "pip": [nightly_url, "pip-install-test==0.5"]
        }]
    }
}
@ray.remote
def f():
    import pip_install_test
# with pytest.raises(ModuleNotFoundError):
#     # Ensure pip-install-test is not installed on the test
#     import pip_install_test
# with pytest.raises(ray.exceptions.RayTaskError):
#     ray.get(f.remote())
ray.get(f.options(runtime_env=runtime_env).remote())