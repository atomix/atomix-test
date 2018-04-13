from cluster import Cluster, Node, create_cluster
from datetime import datetime

def with_cluster(name=None, nodes=3, clients=0, **kwargs):
    """Decorator for passing a cluster into a function."""
    def get_name(f):
        if name is None:
            return '{}-{}'.format(f.__name__, datetime.now().strftime('%Y%m%d%H%M%S'))
        elif callable(name):
            return '{}-{}'.format(name(), datetime.now().strftime('%Y%m%d%H%M%S'))
        return name

    def wrap(f):
        cluster = Cluster(get_name(f))
        def new_func():
            try:
                cluster.setup(nodes, **kwargs)
                for _ in range(clients):
                    cluster.add_node(Node.Type.CLIENT)
                f(cluster)
            except:
                cluster.teardown()
                raise
            else:
                cluster.teardown()
                cluster.cleanup()
        new_func.__name__ = f.__name__
        return new_func
    return wrap
