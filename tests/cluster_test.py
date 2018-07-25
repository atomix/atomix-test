from atomixtest import create_cluster, logger

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

def test_setup_shutdown_cluster():
    logger.debug('This is a debug message')
    logger.info('This is an info message')
    logger.warn('This is a warn message')
    logger.error('This is an error message')

def test_add_node():
    logger.debug('This is another debug message')
    logger.info('This is another info message')
    logger.warn('This is another warn message')
    logger.error('This is another error message')
    raise ValueError("Something went wrong!")
