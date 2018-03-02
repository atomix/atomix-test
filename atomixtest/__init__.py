from cluster import Cluster, Node, create_cluster
from logger import log

def with_cluster(name=None, nodes=3, clients=0, **kwargs):
    """Decorator for passing a cluster into a function."""
    def get_name(f):
        if name is None:
            return f.__name__
        elif callable(name):
            return name()
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
                cluster.shutdown()
                raise
            else:
                cluster.teardown()
        new_func.__name__ = f.__name__
        return new_func
    return wrap
