from atomixtest.logger import log
from atomixtest.cluster import cluster, node, Node
import time

def test_test():
    """Test test"""
    client1 = cluster.add_node(Node.Type.CLIENT)
    log.message("Attempting to isolate node 1")
    node(1).isolate()
    log.message("Attempting to heal node 1")
    node(1).unisolate()
    log.message("Killing node 2")
    node(2).kill()
    log.message("Restarting node 2")
    node(2).start()
    log.message("Getting key")
    client1.map('foo').get('bar')
    log.message("Putting key")
    client1.map('foo').put('bar', 'baz')
    log.message("Getting key")
    assert client1.map('foo').get('bar') == 'baz'
    log.message("Removing node")
    client1.teardown()
    log.message("All done!")
