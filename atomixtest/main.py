from cluster import get_cluster, get_clusters, Cluster
from utils import clusters_to_str, cluster_to_str
from errors import TestError
import sys

def setup(args):
    Cluster(args.cluster).setup(args.nodes, subnet=args.subnet, gateway=args.gateway)

def teardown(args):
    cluster = get_cluster(args.cluster)
    cluster.teardown()
    cluster.cleanup()

def add_node(args):
    get_cluster(args.cluster).add_node(args.type)

def remove_node(args):
    get_cluster(args.cluster).remove_node(args.node)

def list_clusters(args):
    print clusters_to_str(get_clusters())

def cluster_info(args):
    if args.cluster is not None:
        print get_cluster(args.cluster)
    else:
        print '\n'.join([str(cluster) for cluster in get_clusters()])

def cluster_nodes(args):
    print cluster_to_str(get_cluster(args.cluster))

def stop(args):
    get_cluster(args.cluster).node(args.node).stop()

def start(args):
    get_cluster(args.cluster).node(args.node).start()

def kill(args):
    get_cluster(args.cluster).node(args.node).kill()

def recover(args):
    get_cluster(args.cluster).node(args.node).recover()

def restart(args):
    get_cluster(args.cluster).node(args.node).restart()

def logs(args):
    print get_cluster(args.cluster).node(args.node).logs()

def partition(args):
    get_cluster(args.cluster).network.partition(args.local, args.remote)

def partition_halves(args):
    get_cluster(args.cluster).network.partition_halves()

def partition_random(args):
    get_cluster(args.cluster).network.partition_random()

def partition_bridge(args):
    get_cluster(args.cluster).network.partition_bridge(args.node)

def partition_isolate(args):
    get_cluster(args.cluster).network.partition_isolate(args.node)

def delay(args):
    get_cluster(args.cluster).network.delay(args.node, args.latency, args.jitter, args.correlation, args.distribution)

def drop(args):
    get_cluster(args.cluster).network.drop(args.node, args.probability, args.correlation)

def reorder(args):
    get_cluster(args.cluster).network.reorder(args.node, args.probability, args.correlation)

def duplicate(args):
    get_cluster(args.cluster).network.duplicate(args.node, args.probability, args.correlation)

def corrupt(args):
    get_cluster(args.cluster).network.corrupt(args.node, args.probability)

def restore(args):
    get_cluster(args.cluster).network.restore(args.node)

def run(args):
    from pytest import main
    sys.exit(main(args.args))

def _create_parser():
    import argparse

    def percentage(value):
        if value.endswith('%'):
            return float(value[:-1]) / 100
        return float(value)

    def milliseconds(value):
        if value.lower().endswith('ms'):
            return int(value[:-2])
        return int(value)

    def name_or_id(value):
        try:
            return int(value)
        except ValueError:
            return value

    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()

    cluster_parser = subparsers.add_parser('cluster', help="Cluster commands")
    cluster_parser.add_argument('cluster', nargs='?', help="The cluster on which to operate")

    cluster_subparsers = cluster_parser.add_subparsers(dest='action', help="The action to execute")

    setup_parser = cluster_subparsers.add_parser('setup', help="Setup a test cluster")
    setup_parser.add_argument('-n', '--nodes', type=int, default=3, help="The number of nodes in the cluster")
    setup_parser.add_argument('-s', '--subnet', help="The subnet in which to create the cluster")
    setup_parser.add_argument('-g', '--gateway', help="The IPv4 gateway for the master subnet")
    setup_parser.set_defaults(func=setup)

    teardown_parser = cluster_subparsers.add_parser('teardown', help="Tear down a test cluster")
    teardown_parser.set_defaults(func=teardown)

    add_node_parser = cluster_subparsers.add_parser('add-node', help="Add a node to a test cluster")
    add_node_parser.add_argument('-t', '--type', choices=['server', 'client'], default='server', help="The type of node to add")
    add_node_parser.set_defaults(func=add_node)

    remove_node_parser = cluster_subparsers.add_parser('remove-node', help="Remove a node from a test cluster")
    remove_node_parser.add_argument('node', type=name_or_id, help="The node to remove from the cluster")
    remove_node_parser.set_defaults(func=remove_node)

    list_clusters_parser = cluster_subparsers.add_parser('list', help="Get a list of test clusters")
    list_clusters_parser.set_defaults(func=list_clusters)

    cluster_info_parser = cluster_subparsers.add_parser('info', help="Get information about a test cluster")
    cluster_info_parser.set_defaults(func=cluster_info)

    cluster_nodes_parser = cluster_subparsers.add_parser('nodes', help="List information about all nodes in the cluster")
    cluster_nodes_parser.set_defaults(func=cluster_nodes)

    kill_parser = cluster_subparsers.add_parser('stop', help="Stop a node")
    kill_parser.add_argument('node', type=name_or_id, help="The node to stop")
    kill_parser.set_defaults(func=stop)

    kill_parser = cluster_subparsers.add_parser('start', help="Start a node")
    kill_parser.add_argument('node', type=name_or_id, help="The node to start")
    kill_parser.set_defaults(func=start)

    kill_parser = cluster_subparsers.add_parser('kill', help="Kill a node")
    kill_parser.add_argument('node', type=name_or_id, help="The node to kill")
    kill_parser.set_defaults(func=kill)

    recover_parser = cluster_subparsers.add_parser('recover', help="Recover a node")
    recover_parser.add_argument('node', type=name_or_id, help="The node to recover")
    recover_parser.set_defaults(func=recover)

    restart_parser = cluster_subparsers.add_parser('restart', help="Restart a node")
    restart_parser.add_argument('node', type=name_or_id, help="The node to restart")
    restart_parser.set_defaults(func=restart)

    partition_parser = cluster_subparsers.add_parser('partition', help="Partition a node")
    partition_parser.add_argument('local', type=name_or_id, help="The node to partition")
    partition_parser.add_argument('remote', type=name_or_id, nargs='?', help="The remote to partition")
    partition_parser.set_defaults(func=partition)

    partition_halves_parser = cluster_subparsers.add_parser('partition-halves', help="Partition the cluster into two halves")
    partition_halves_parser.set_defaults(func=partition_halves)

    partition_random_parser = cluster_subparsers.add_parser('partition-random', help="Partition a random node")
    partition_random_parser.set_defaults(func=partition_random)

    partition_bridge_parser = cluster_subparsers.add_parser('partition-bridge', help="Partition the cluster with a bridge node")
    partition_bridge_parser.add_argument('node', type=name_or_id, nargs='?', help="The bridge node")
    partition_bridge_parser.set_defaults(func=partition_bridge)

    partition_isolate_parser = cluster_subparsers.add_parser('partition-isolate', help="Isolate a node in the cluster")
    partition_isolate_parser.add_argument('node', type=name_or_id, nargs='?', help="The node to isolate")
    partition_isolate_parser.set_defaults(func=partition_isolate)

    delay_parser = cluster_subparsers.add_parser('delay', help="Delay packets to a node")
    delay_parser.add_argument('node', nargs='?', type=name_or_id, help="The node to disrupt")
    delay_parser.add_argument('-l', '--latency', default='50ms', type=milliseconds, help="The latency in milliseconds")
    delay_parser.add_argument('-j', '--jitter', default='10ms', type=milliseconds, help="The jitter in milliseconds")
    delay_parser.add_argument('-c', '--correlation', default='75%', type=percentage, help="The correlation")
    delay_parser.add_argument('-d', '--distribution', default='normal', choices=['normal', 'pareto', 'paretonormal'], help="The distribution")
    delay_parser.set_defaults(func=delay)

    drop_parser = cluster_subparsers.add_parser('drop', help="Drop packets to a node")
    drop_parser.add_argument('node', nargs='?', type=name_or_id, help="The node to disrupt")
    drop_parser.add_argument('-p', '--probability', default='2%', type=percentage, help="The probability")
    drop_parser.add_argument('-c', '--correlation', default='25%', type=percentage, help="The correlation")
    drop_parser.set_defaults(func=drop)

    reorder_parser = cluster_subparsers.add_parser('reorder', help="Reorder packets to a node")
    reorder_parser.add_argument('node', nargs='?', type=name_or_id, help="The node to disrupt")
    reorder_parser.add_argument('-p', '--probability', default='2%', type=percentage, help="The probability")
    reorder_parser.add_argument('-c', '--correlation', default='50%', type=percentage, help="The correlation")
    reorder_parser.set_defaults(func=reorder)

    duplicate_parser = cluster_subparsers.add_parser('duplicate', help="Duplicate packets to a node")
    duplicate_parser.add_argument('node', nargs='?', type=name_or_id, help="The node to disrupt")
    duplicate_parser.add_argument('-p', '--probability', default='.5%', type=percentage, help="The probability")
    duplicate_parser.add_argument('-c', '--correlation', default='5%', type=percentage, help="The correlation")
    duplicate_parser.set_defaults(func=duplicate)

    corrupt_parser = cluster_subparsers.add_parser('corrupt', help="Corrupt packets to a node")
    corrupt_parser.add_argument('node', nargs='?', type=name_or_id, help="The node to disrupt")
    corrupt_parser.add_argument('-p', '--probability', default='2%', type=percentage, help="The probability")
    corrupt_parser.set_defaults(func=corrupt)

    restore_parser = cluster_subparsers.add_parser('restore', help="Restore packets to a node")
    restore_parser.add_argument('node', nargs='?', type=name_or_id, help="The node to disrupt")
    restore_parser.set_defaults(func=restore)

    logs_parser = cluster_subparsers.add_parser('logs', help="Get logs for a specific node")
    logs_parser.add_argument('node', type=name_or_id, help="The node for which to retrieve logs")
    logs_parser.set_defaults(func=logs)

    run_parser = subparsers.add_parser('run', help="Run a test")
    run_parser.add_argument('args', nargs='*', help="The tests to run")
    run_parser.set_defaults(func=run)

    return parser

def main():
    """Runs the test framework."""
    from colorama import init
    init()

    args = _create_parser().parse_args()
    try:
        args.func(args)
    except TestError, e:
        print str(e)
        sys.exit(1)
    else:
        sys.exit(0)
