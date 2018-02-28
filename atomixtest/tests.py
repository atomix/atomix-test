from atomixtest import with_cluster
from cluster import Cluster
import uuid

@with_cluster(name=lambda: str(uuid.uuid4()), nodes=3)
def test_cluster(cluster):
    assert len(cluster.nodes()) == 3
    assert cluster.node(1).id == 1
    assert cluster.node(1).name == '{}-{}'.format(cluster.name, 1)
    assert cluster.node(1).status == 'running'
    assert cluster.node('{}-{}'.format(cluster.name, 1)).id == 1
    assert cluster.node(2).id == 2
    assert cluster.node(2).name == '{}-{}'.format(cluster.name, 2)
    assert cluster.node(2).status == 'running'
    assert cluster.node('{}-{}'.format(cluster.name, 2)).id == 2
    assert cluster.node(3).id == 3
    assert cluster.node(3).name == '{}-{}'.format(cluster.name, 3)
    assert cluster.node(3).status == 'running'
    assert cluster.node('{}-{}'.format(cluster.name, 3)).id == 3

    cluster = Cluster(cluster.name)
    assert len(cluster.nodes()) == 3
    assert cluster.node(1).id == 1
    assert cluster.node(1).name == '{}-{}'.format(cluster.name, 1)
    assert cluster.node('{}-{}'.format(cluster.name, 1)).id == 1
    assert cluster.node(2).id == 2
    assert cluster.node(2).name == '{}-{}'.format(cluster.name, 2)
    assert cluster.node('{}-{}'.format(cluster.name, 2)).id == 2
    assert cluster.node(3).id == 3
    assert cluster.node(3).name == '{}-{}'.format(cluster.name, 3)
    assert cluster.node('{}-{}'.format(cluster.name, 3)).id == 3


@with_cluster(name=lambda: str(uuid.uuid4()), nodes=3)
def test_node(cluster):
    node = cluster.node(1)
    node.kill()
    assert node.status == 'exited'

    cluster = Cluster(cluster.name)
    node = cluster.node(1)
    assert node.status == 'exited'

    node.recover()
    assert node.status == 'running'

    cluster = Cluster(cluster.name)
    node = cluster.node(1)
    assert node.status == 'running'

    cluster = Cluster(cluster.name)
    node = cluster.node(1)
    node.stop()
    assert node.status == 'exited'
    node.start()
    assert node.status == 'running'
