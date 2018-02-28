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
    data = [['NAME', 'NODES', 'NETWORK', 'SUBNET', 'GATEWAY'],]
    for cluster in clusters:
        data.append([cluster.name, len(cluster.nodes()), cluster.network.name, cluster.network.subnet, cluster.network.gateway])
    return _create_table(data)

def cluster_to_str(cluster):
    """Returns a string table for the given cluster."""
    data = [['ID', 'NAME', 'STATUS', 'IP', 'LOCAL PORT'],]
    for node in cluster.nodes():
        data.append([node.id, node.name, node.status, node.ip, node.local_port])
    return _create_table(data)