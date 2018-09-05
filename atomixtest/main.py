from cluster import get_cluster, get_clusters, Cluster
from network import get_networks
from utils import clusters_to_str, cluster_to_str
from errors import TestError
import sys
import os
import shutil

def setup(args):
    Cluster(args.name).setup(
        *args.config,
        nodes=args.nodes,
        version=args.version,
        subnet=args.subnet,
        gateway=args.gateway,
        cpus=args.cpu,
        memory=args.memory_limit,
        profiler=args.profiler,
        debug=args.debug,
        trace=args.trace
    )

def teardown(args):
    cluster = get_cluster(args.name)
    cluster.teardown()
    if args.delete:
        cluster.cleanup()

def cleanup(args):
    clusters = get_clusters()
    for cluster in clusters:
        cluster.teardown()
    for network in get_networks():
        network.teardown()
    if args.delete:
        path = os.path.join(os.getcwd(), '.data')
        if os.path.exists(path):
            shutil.rmtree(path)

def upgrade(args):
    get_cluster(args.name).upgrade(version=args.version)

def add_node(args):
    get_cluster(args.name).add_node(*args.config, version=args.version)

def remove_node(args):
    get_cluster(args.name).remove_node(args.node)

def upgrade_node(args):
    get_cluster(args.name).upgrade_node(args.node, version=args.version)

def list_clusters(args):
    print clusters_to_str(get_clusters())

def cluster_info(args):
    if args.name is not None:
        print get_cluster(args.name)
    else:
        print '\n'.join([str(cluster) for cluster in get_clusters()])

def cluster_nodes(args):
    print cluster_to_str(get_cluster(args.name))

def stop(args):
    get_cluster(args.name).node(args.node).stop()

def start(args):
    get_cluster(args.name).node(args.node).start()

def kill(args):
    get_cluster(args.name).node(args.node).kill()

def recover(args):
    get_cluster(args.name).node(args.node).recover()

def restart(args):
    get_cluster(args.name).node(args.node).restart()

def attach(args):
    try:
        for line in get_cluster(args.name).node(args.node).attach():
            sys.stdout.write(line)
            sys.stdout.flush()
    except KeyboardInterrupt:
        pass

def logs(args):
    output = get_cluster(args.name).node(args.node).logs(stream=args.stream)
    if isinstance(output, basestring):
        print output
    else:
        try:
            for line in output:
                sys.stdout.write(line)
                sys.stdout.flush()
        except KeyboardInterrupt:
            pass

def partition(args):
    get_cluster(args.name).network.partition(args.local, args.remote)

def partition_halves(args):
    get_cluster(args.name).network.partition_halves()

def partition_random(args):
    get_cluster(args.name).network.partition_random()

def partition_bridge(args):
    get_cluster(args.name).network.partition_bridge(args.node)

def partition_isolate(args):
    get_cluster(args.name).network.partition_isolate(args.node)

def heal(args):
    get_cluster(args.name).network.heal(args.local, args.remote)

def delay(args):
    get_cluster(args.name).network.delay(args.node, args.latency, args.jitter, args.correlation, args.distribution)

def drop(args):
    get_cluster(args.name).network.drop(args.node, args.probability, args.correlation)

def reorder(args):
    get_cluster(args.name).network.reorder(args.node, args.probability, args.correlation)

def duplicate(args):
    get_cluster(args.name).network.duplicate(args.node, args.probability, args.correlation)

def corrupt(args):
    get_cluster(args.name).network.corrupt(args.node, args.probability)

def restore(args):
    get_cluster(args.name).network.restore(args.node)

def stress(args):
    get_cluster(args.name).stress(args.node, args.timeout, args.cpu, args.io, args.memory, args.hdd)

def destress(args):
    get_cluster(args.name).destress(args.node)

def run(args):
    from test import run
    sys.exit(run(args.paths, fail_fast=args.fail_fast))

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
    cluster_parser.add_argument('-i', '--name', required=False, default='test', help="The cluster on which to operate")

    cluster_subparsers = cluster_parser.add_subparsers(dest='action', help="The action to execute")

    setup_parser = cluster_subparsers.add_parser('setup', help="Setup a test cluster")
    setup_parser.add_argument('--config', '-c', nargs='+', help="The configuration(s) to apply to the cluster")
    setup_parser.add_argument('--nodes', '-n', type=int, default=3, help="The number of nodes in the cluster")
    setup_parser.add_argument('--version', '-v', type=str, default='latest', help="The version to setup")
    setup_parser.add_argument('--subnet', help="The subnet in which to create the cluster")
    setup_parser.add_argument('--gateway', help="The IPv4 gateway for the master subnet")
    setup_parser.add_argument('--cpu', help="CPUs in which to allow execution (0-3, 0,1)")
    setup_parser.add_argument('--memory-limit', help="The per-container memory limit")
    setup_parser.add_argument('--debug', '-d', action='store_true', default=False, help="Enable debug logging")
    setup_parser.add_argument('--trace', '-t', action='store_true', default=False, help="Enable trace logging")
    setup_parser.add_argument('--profiler', choices=['yourkit'], help="Enable profiling")
    setup_parser.set_defaults(func=setup)

    teardown_parser = cluster_subparsers.add_parser('teardown', help="Tear down a test cluster")
    teardown_parser.add_argument('-d', '--delete', action='store_true', default=False, help="Whether to delete the cluster logs")
    teardown_parser.set_defaults(func=teardown)

    cleanup_parser = cluster_subparsers.add_parser('cleanup', help="Cleans up all test clusters")
    cleanup_parser.add_argument('-d', '--delete', action='store_true', default=False, help="Whether to delete the cluster logs")
    cleanup_parser.set_defaults(func=cleanup)

    upgrade_parser = cluster_subparsers.add_parser('upgrade', help="Upgrades a test cluster")
    upgrade_parser.add_argument('--version', '-v', type=str, default='latest', help="The version to which to upgrade")
    upgrade_parser.set_defaults(func=upgrade)

    add_node_parser = cluster_subparsers.add_parser('add-node', help="Add a node to a test cluster")
    add_node_parser.add_argument('-c', '--config', nargs='+', help="The configuration(s) to apply to the node")
    add_node_parser.add_argument('-v', '--version', type=str, default='latest', help="The version to setup")
    add_node_parser.set_defaults(func=add_node)

    remove_node_parser = cluster_subparsers.add_parser('remove-node', help="Remove a node from a test cluster")
    remove_node_parser.add_argument('node', type=name_or_id, help="The node to remove from the cluster")
    remove_node_parser.set_defaults(func=remove_node)

    upgrade_node_parser = cluster_subparsers.add_parser('upgrade-node', help="Upgrade a node")
    upgrade_node_parser.add_argument('node', type=name_or_id, help="The node to upgrade")
    upgrade_node_parser.add_argument('-v', '--version', type=str, default='latest', help="The version to which to upgrade")
    upgrade_node_parser.set_defaults(func=upgrade_node)

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

    heal_parser = cluster_subparsers.add_parser('heal', help="Heal a partition")
    heal_parser.add_argument('local', type=name_or_id, nargs='?', help="The node to heal")
    heal_parser.add_argument('remote', type=name_or_id, nargs='?', help="The remote to heal")
    heal_parser.set_defaults(func=heal)

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

    stress_parser = cluster_subparsers.add_parser('stress', help="Stress a node")
    stress_parser.add_argument('node', nargs='?', type=name_or_id, help="The node to stress")
    stress_parser.add_argument('-t', '--timeout', type=str, help="Timeout after N seconds")
    stress_parser.add_argument('-c', '--cpu', type=int, help="Spawn N workers spinning on sqrt()")
    stress_parser.add_argument('-i', '--io', type=int, help="Spawn N workers spinning on sync()")
    stress_parser.add_argument('-m', '--memory', type=int, help="Spawn N workers spinning on malloc()/free()")
    stress_parser.add_argument('-mb', '--memory-bytes', type=str, help="malloc() bytes per worker")
    stress_parser.add_argument('-d', '--hdd', type=int, help="Spawn N workers spinning on write()/unlink()")
    stress_parser.set_defaults(func=stress)

    destress_parser = cluster_subparsers.add_parser('destress', help="Desress a node")
    destress_parser.add_argument('node', nargs='?', type=name_or_id, help="The node to destress")
    destress_parser.set_defaults(func=destress)

    attach_parser = cluster_subparsers.add_parser('attach', help="Attaches to a specific node")
    attach_parser.add_argument('node', type=name_or_id, help="The node to which to attach")
    attach_parser.set_defaults(func=attach)

    logs_parser = cluster_subparsers.add_parser('logs', help="Get logs for a specific node")
    logs_parser.add_argument('node', type=name_or_id, help="The node for which to retrieve logs")
    logs_parser.add_argument('-s', '--stream', action='store_true', default=False, help="Whether to stream the logs")
    logs_parser.set_defaults(func=logs)

    run_parser = subparsers.add_parser('run', help="Run a test")
    run_parser.add_argument('paths', nargs='+', help="The modules or packages containing the test(s) to run")
    run_parser.add_argument('-ff', '--fail-fast', action='store_true', default=False, help="Whether to fail the test run on the first individual test failure")
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
