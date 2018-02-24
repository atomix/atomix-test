from cluster import Cluster, Node
from logger import log

def with_cluster(name=None, nodes=3, clients=0, supernet='172.18.0.0/16', subnet=None, gateway=None):
    """Decorator for passing a cluster into a function."""
    def wrap(f):
        cluster = Cluster(name if name is not None else f.__name__)
        def new_func():
            try:
                cluster.setup(nodes, supernet, subnet, gateway)
                for _ in range(clients):
                    cluster.add_node(Node.Type.CLIENT)
                f(cluster)
            finally:
                cluster.teardown()
        new_func.__name__ = f.__name__
        return new_func
    return wrap
