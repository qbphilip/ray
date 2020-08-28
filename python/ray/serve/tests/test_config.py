import pytest

from ray import serve
from ray.serve.config import BackendConfig, ReplicaConfig
from pydantic import ValidationError


def test_backend_config_validation():
    # Test unknown key.
    with pytest.raises(ValidationError):
        BackendConfig(unknown_key=-1)

    # Test num_replicas validation.
    BackendConfig(num_replicas=1)
    with pytest.raises(ValidationError, match="type_error"):
        BackendConfig(num_replicas="hello")
    with pytest.raises(ValidationError, match="value_error"):
        BackendConfig(num_replicas=-1)

    # Test max_batch_size validation.
    BackendConfig(max_batch_size=10, accepts_batches=True)._validate_complete()
    with pytest.raises(ValueError):
        BackendConfig(
            max_batch_size=10, accepts_batches=False)._validate_complete()
    with pytest.raises(ValidationError, match="type_error"):
        BackendConfig(max_batch_size="hello")
    with pytest.raises(ValidationError, match="value_error"):
        BackendConfig(max_batch_size=0)
    with pytest.raises(ValidationError, match="value_error"):
        BackendConfig(max_batch_size=-1)


def test_backend_config_update():
    b = BackendConfig(num_replicas=1, max_batch_size=1)

    # Test updating a key works.
    b.num_replicas = 2
    assert b.num_replicas == 2
    # Check that not specifying a key doesn't update it.
    assert b.max_batch_size == 1

    # Check that input is validated.
    with pytest.raises(ValidationError):
        b.num_replicas = "Hello"
    with pytest.raises(ValidationError):
        b.num_replicas = -1

    # Test batch validation.
    b = BackendConfig(accepts_batches=False)
    b.max_batch_size = 1
    with pytest.raises(ValueError):
        b.max_batch_size = 2
        b._validate_complete()

    b = BackendConfig(accepts_batches=True)
    b.max_batch_size = 2


def test_replica_config_validation():
    class Class:
        pass

    class BatchClass:
        @serve.accept_batch
        def __call__(self):
            pass

    def function():
        pass

    @serve.accept_batch
    def batch_function():
        pass

    ReplicaConfig(Class)
    ReplicaConfig(function)
    with pytest.raises(TypeError):
        ReplicaConfig(Class())

    # Check max_batch_size validation.
    assert not ReplicaConfig(function).accepts_batches
    assert not ReplicaConfig(Class).accepts_batches
    assert ReplicaConfig(batch_function).accepts_batches
    assert ReplicaConfig(BatchClass).accepts_batches

    # Check ray_actor_options validation.
    ReplicaConfig(
        Class,
        ray_actor_options={
            "num_cpus": 1.0,
            "num_gpus": 10,
            "resources": {
                "abc": 1.0
            },
            "memory": 1000000.0,
            "object_store_memory": 1000000,
        })
    with pytest.raises(TypeError):
        ReplicaConfig(Class, ray_actor_options=1.0)
    with pytest.raises(TypeError):
        ReplicaConfig(Class, ray_actor_options=False)
    with pytest.raises(TypeError):
        ReplicaConfig(Class, ray_actor_options={"num_cpus": "hello"})
    with pytest.raises(ValueError):
        ReplicaConfig(Class, ray_actor_options={"num_cpus": -1})
    with pytest.raises(TypeError):
        ReplicaConfig(Class, ray_actor_options={"num_gpus": "hello"})
    with pytest.raises(ValueError):
        ReplicaConfig(Class, ray_actor_options={"num_gpus": -1})
    with pytest.raises(TypeError):
        ReplicaConfig(Class, ray_actor_options={"memory": "hello"})
    with pytest.raises(ValueError):
        ReplicaConfig(Class, ray_actor_options={"memory": -1})
    with pytest.raises(TypeError):
        ReplicaConfig(
            Class, ray_actor_options={"object_store_memory": "hello"})
    with pytest.raises(ValueError):
        ReplicaConfig(Class, ray_actor_options={"object_store_memory": -1})
    with pytest.raises(TypeError):
        ReplicaConfig(Class, ray_actor_options={"resources": None})
    with pytest.raises(ValueError):
        ReplicaConfig(Class, ray_actor_options={"name": None})
    with pytest.raises(ValueError):
        ReplicaConfig(Class, ray_actor_options={"lifetime": None})
    with pytest.raises(ValueError):
        ReplicaConfig(Class, ray_actor_options={"max_restarts": None})


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", "-s", __file__]))
