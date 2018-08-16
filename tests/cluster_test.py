from atomixtest import create_cluster

def test_setup_teardown_consensus_cluster():
    """Tests setting up and tearing down a consensus cluster"""
    with create_cluster('consensus', nodes=3):
        pass

def test_setup_teardown_strong_data_grid():
    """Tests setting up and tearing down a strongly consistent data grid cluster"""
    with create_cluster('strong-data-grid', nodes=3):
        pass

def test_setup_teardown_weak_data_grid():
    """Tests setting up and tearing down a weakly consistent data grid cluster"""
    with create_cluster('weak-data-grid', nodes=3):
        pass

def test_add_remove_consensus_client():
    """Tests adding and removing a consensus client"""
    with create_cluster('consensus', nodes=3) as cluster:
        node = cluster.add_node('client')
        node.remove()

def test_add_remove_strong_data_grid_client():
    """Tests adding and removing a strong data grid client"""
    with create_cluster('strong-data-grid', nodes=3) as cluster:
        node = cluster.add_node('client')
        node.remove()

def test_add_remove_weak_data_grid_client():
    """Tests adding and removing a weak data grid client"""
    with create_cluster('weak-data-grid', nodes=3) as cluster:
        node = cluster.add_node('client')
        node.remove()

def test_consensus_cluster_membership():
    """Tests reading cluster membership from a consensus cluster."""
    with create_cluster('consensus', nodes=3) as cluster:
        node = cluster.node(1)
        assert len(node.client.cluster.nodes()) == 3, "number of nodes is not equal to 3"
        assert node.client.cluster.node().id == node.name, "node identifier is not " + node.name
