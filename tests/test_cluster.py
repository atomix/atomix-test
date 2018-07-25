from atomixtest import create_cluster
import uuid

def _test_value(node):
    value = node.value(str(uuid.uuid4()))
    value.set("Hello world!")
    assert value.get() == "Hello world!"

def test_setup_shutdown_cluster():
    with create_cluster('test_setup_shutdown_cluster', profiles=['data-grid']) as cluster:
        pass
