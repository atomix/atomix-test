from atomixtest import with_cluster, create_cluster
import uuid

def _test_value(node):
    value = node.value(str(uuid.uuid4()))
    value.set("Hello world!")
    assert value.get() == "Hello world!"

@with_cluster(nodes=3, core_partitions=3, data_partitions=3)
def test_cluster_add_server_node(cluster):
    node = cluster.add_node(type='server')
    _test_value(node)

@with_cluster(nodes=3, core_partitions=3, data_partitions=3)
def test_cluster_add_client_node(cluster):
    node = cluster.add_node(type='client')
    _test_value(node)

@with_cluster(nodes=3, core_partitions=3, data_partitions=3)
def test_cluster_restart(cluster):
    with cluster.add_node(type='client') as node:
        _test_value(node)
    cluster.restart()
    with cluster.add_node(type='client') as node:
        _test_value(node)

@with_cluster(nodes=1)
def test_cluster_add_node_and_restart(cluster):
    cluster.add_node()
    assert len(cluster.nodes()) == 2
    cluster.restart()
    _test_value(cluster.node(1))

@with_cluster(nodes=1)
def test_cluster_scale_up_down(cluster):
    node1 = cluster.node(1)
    assert len(cluster.nodes()) == 1
    _test_value(node1)
    node2 = cluster.add_node()
    assert len(cluster.nodes()) == 2
    _test_value(node2)
    node2.remove()
    assert len(cluster.nodes()) == 1
    _test_value(node1)
    node1.stop()
    node1.start()
    _test_value(node1)
