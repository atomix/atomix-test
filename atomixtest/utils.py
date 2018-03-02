from terminaltables import AsciiTable

def _create_table(data):
    """Creates a table from the given data."""
    table = AsciiTable(data)
    table.inner_column_border = False
    table.inner_row_border = False
    table.outer_border = False
    table.inner_heading_row_border = False
    table.padding_right = 4
    return str(table.table)

def clusters_to_str(clusters):
    """Returns a string table for the given clusters."""
    data = [['NAME', 'NODES', 'PATH', 'NETWORK', 'SUBNET', 'GATEWAY', 'CPUS', 'MEMORY', 'PROFILING'],]
    for cluster in clusters:
        data.append([
            cluster.name,
            len(cluster.nodes()),
            cluster.path,
            cluster.network.name,
            cluster.network.subnet,
            cluster.network.gateway,
            cluster.cpus,
            cluster.memory,
            'enabled' if cluster.profiling else 'disabled'
        ])
    return _create_table(data)

def cluster_to_str(cluster):
    """Returns a string table for the given cluster."""
    data = [['ID', 'NAME', 'STATUS', 'PATH', 'IP', 'LOCAL PORT', 'PROFILER PORT'],]
    for node in cluster.nodes():
        profiler_port = node.profiler_port
        data.append([node.id, node.name, node.status, node.path, node.ip, node.local_port, profiler_port if profiler_port is not None else ''])
    return _create_table(data)

class Context(object):
    def __init__(self, *contexts):
        self._contexts = contexts

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        self()

    def __call__(self):
        for context in self._contexts:
            context()

def with_context(*contexts):
    return Context(*contexts)
