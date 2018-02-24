from atomixtest import with_cluster, log

@with_cluster(nodes=3)
def test_test(cluster):
    """Test test"""
    client1 = cluster.add_client()
    log.message("Attempting to isolate node 1")
    cluster.node(1).isolate()
    log.message("Attempting to heal node 1")
    cluster.node(1).unisolate()
    log.message("Killing node 2")
    cluster.node(2).kill()
    log.message("Restarting node 2")
    cluster.node(2).start()
    log.message("Getting key")
    client1.map('foo').get('bar')
    log.message("Putting key")
    client1.map('foo').put('bar', 'baz')
    log.message("Getting key")
    assert client1.map('foo').get('bar')['value'] == 'baz'
    log.message("Removing node")
    client1.teardown()
    log.message("All done!")
