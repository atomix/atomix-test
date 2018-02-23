from atomixtest.test import test, log
from atomixtest.cluster import node

@test('test-test')
def test_test():
    """Test test"""
    log.message("Attempting to isolate node 1")
    node(1).isolate()
    log.message("Attempting to heal node 1")
    node(1).unisolate()
    log.message("Killing node 2")
    node(2).kill()
    log.message("Restarting node 2")
    node(2).start()
    log.message("Getting key")
    node(3).client.map('foo').get('bar')
    log.message("Putting key")
    node(3).client.map('foo').put('bar', 'baz')
    log.message("Getting key")
    assert node(3).client.map('foo').get('bar') == 'baz'
    log.message("All done!")
