from cluster import get_cluster, get_clusters, Cluster
from network import get_networks
from utils import clusters_to_str, cluster_to_str
from errors import TestError
import re
import sys
import os
import shutil
from argparse import ArgumentParser, Namespace, ArgumentTypeError
from datetime import timedelta

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

def _entropy_functions():
    return {
        'crash': _parse_crash_args,
        'partition': _parse_partition_args,
        'restart': _parse_restart_args,
        'stress': _parse_stress_args
    }

def entropy_test(args):
    from entropy import run

    functions = []
    def add_function(name, delay, *kwargs):
        functions.append((name, delay, list(kwargs)))

    if args.function_delay is not None:
        function_delay = (
            args.function_delay[0] if len(args.function_delay) > 0 else 15,
            args.function_delay[1] if len(args.function_delay) > 1 else 30
        )
    else:
        function_delay = (15, 30)

    def get_or_default(l, i, d=None):
        if len(l) > i:
            return l[i]
        return d

    def get_delay(delay_args):
        if delay_args.start is not None and delay_args.end is not None:
            return delay_args.start, delay_args.end
        elif delay_args.start is not None:
            return delay_args.start, function_delay[1]
        else:
            return function_delay

    if 'crash' in args:
        crash = args.crash
        add_function(
            'crash_random',
            get_delay(crash),
            ('start', get_or_default(crash.random, 0, 15.0)),
            ('end', get_or_default(crash.random, 1, 30.0))
        )
    if 'partition' in args:
        partition = args.partition
        if partition.random is not None:
            add_function(
                'partition_random',
                get_delay(partition),
                ('start', get_or_default(partition.random, 0, 15.0)),
                ('end', get_or_default(partition.random, 1, 30.0))
            )
        if partition.isolate is not None:
            add_function(
                'isolate_random',
                get_delay(partition),
                ('start', get_or_default(partition.isolate, 0, 15.0)),
                ('end', get_or_default(partition.isolate, 1, 30.0))
            )
        if partition.bridge is not None:
            add_function(
                'partition_bridge',
                get_delay(partition),
                ('start', get_or_default(partition.bridge, 0, 15.0)),
                ('end', get_or_default(partition.bridge, 1, 30.0))
            )
        if partition.halves is not None:
            add_function(
                'partition_halves',
                get_delay(partition),
                ('start', get_or_default(partition.halves, 0, 15.0)),
                ('end', get_or_default(partition.halves, 1, 30.0))
            )
    if 'restart' in args:
        add_function('restart', (15.0, 30.0))
    if 'stress' in args:
        stress = args.stress
        if stress.network is not None:
            if stress.all is not None:
                add_function(
                    'delay',
                    get_delay(stress),
                    ('latency', stress.network),
                    ('start', get_or_default(stress.all, 0, 15.0)),
                    ('end', get_or_default(stress.all, 1, 30.0))
                )
            if stress.random is not None:
                add_function(
                    'delay_random',
                    get_delay(stress),
                    ('latency', stress.network),
                    ('start', get_or_default(stress.random, 0, 15.0)),
                    ('end', get_or_default(stress.random, 1, 30.0))
                )
        if stress.cpu is not None:
            if stress.all is not None:
                add_function(
                    'stress_cpu',
                    get_delay(stress),
                    ('processes', stress.cpu),
                    ('start', get_or_default(stress.all, 0, 15.0)),
                    ('end', get_or_default(stress.all, 1, 30.0))
                )
            if stress.random is not None:
                add_function(
                    'stress_cpu_random',
                    get_delay(stress),
                    ('processes', stress.cpu),
                    ('start', get_or_default(stress.random, 0, 15.0)),
                    ('end', get_or_default(stress.random, 1, 30.0))
                )
        if stress.io is not None:
            if stress.all is not None:
                add_function(
                    'stress_io',
                    get_delay(stress),
                    ('processes', stress.io),
                    ('start', get_or_default(stress.all, 0, 15.0)),
                    ('end', get_or_default(stress.all, 1, 30.0))
                )
            if stress.random is not None:
                add_function(
                    'stress_io_random',
                    get_delay(stress),
                    ('processes', stress.io),
                    ('start', get_or_default(stress.random, 0, 15.0)),
                    ('end', get_or_default(stress.random, 1, 30.0))
                )
        if stress.memory is not None:
            if stress.all is not None:
                add_function(
                    'stress_memory',
                    get_delay(stress),
                    ('processes', stress.memory),
                    ('start', get_or_default(stress.all, 0, 15.0)),
                    ('end', get_or_default(stress.all, 1, 30.0))
                )
            if stress.random is not None:
                add_function(
                    'stress_memory_random',
                    get_delay(stress),
                    ('processes', stress.memory),
                    ('start', get_or_default(stress.random, 0, 15.0)),
                    ('end', get_or_default(stress.random, 1, 30.0))
                )

    if args.debug:
        for function in functions:
            print function[0] + '(' + ', '.join([str(arg[0]) + '=' + str(arg[1]) for arg in function[2]]) + ')'

    sys.exit(run(
        nodes=args.nodes,
        configs=args.config,
        version=args.version,
        dry_run=args.dry_run,
        processes=args.parallelism,
        scale=args.scale,
        prime=args.prime,
        ops=args.ops,
        run_time=args.run_time,
        functions=functions
    ))

def run(args):
    from test import run
    sys.exit(run(args.paths, fail_fast=args.fail_fast))

def percentage(value):
    if value.endswith('%'):
        return float(value[:-1]) / 100
    return float(value)

def _parse_milliseconds(time):
    regex = re.compile(r'((?P<hours>\d+?)h)?((?P<minutes>\d+?)m(?!s))?((?P<seconds>\d+?)s)?((?P<milliseconds>\d+?)ms)?')
    parts = regex.match(time.lower())
    if not parts:
        return None
    parts = parts.groupdict()
    time_params = {}
    for (name, param) in parts.iteritems():
        if param:
            time_params[name] = int(param)
    if not time_params:
        return None
    return int(timedelta(**time_params).total_seconds() * 1000)

def milliseconds(time):
    millis = _parse_milliseconds(time)
    if millis is None:
        return float(time)
    return time

def seconds(time):
    millis = _parse_milliseconds(time)
    if millis is None:
        return float(time)
    return millis / 1000.0

def _parse_cluster_args(args):

    def percentage(value):
        if value.endswith('%'):
            return float(value[:-1]) / 100
        return float(value)

    def _parse_milliseconds(time):
        regex = re.compile(r'((?P<hours>\d+?)h)?((?P<minutes>\d+?)m(?!s))?((?P<seconds>\d+?)s)?((?P<milliseconds>\d+?)ms)?')
        parts = regex.match(time.lower())
        if not parts:
            return None
        parts = parts.groupdict()
        time_params = {}
        for (name, param) in parts.iteritems():
            if param:
                time_params[name] = int(param)
        if not time_params:
            return None
        return int(timedelta(**time_params).total_seconds() * 1000)

    def milliseconds(time):
        millis = _parse_milliseconds(time)
        if millis is None:
            return float(time)
        return time

    def seconds(time):
        millis = _parse_milliseconds(time)
        if millis is None:
            return float(time)
        return millis / 1000.0

    def milliseconds_plus_seconds_range():
        args = []
        def func(arg):
            try:
                if len(args) == 0:
                    return milliseconds(arg)
                elif len(args) == 1:
                    return seconds(arg)
                else:
                    raise ArgumentTypeError("Too many arguments")
            finally:
                args.append(arg)
        return func

    def seconds_range():
        args = []
        def func(arg):
            try:
                if len(args) < 2:
                    return seconds(arg)
                else:
                    raise ArgumentTypeError("Too many arguments")
            finally:
                args.append(arg)
        return func

    def int_plus_seconds_range():
        args = []
        def func(arg):
            try:
                if len(args) == 0:
                    try:
                        return int(arg)
                    except ValueError, e:
                        raise ArgumentTypeError(e)
                elif len(args) < 3:
                    return seconds(arg)
                else:
                    raise ArgumentTypeError("Too many arguments")
            finally:
                args.append(arg)
        return func

    def name_or_id(value):
        try:
            return int(value)
        except ValueError:
            return value

    parser = ArgumentParser()
    subparsers = parser.add_subparsers()

    cluster_parser = subparsers.add_parser('cluster', help="Cluster commands")
    cluster_parser.add_argument('name', nargs='?', default='test', help="The cluster on which to operate")

    cluster_subparsers = cluster_parser.add_subparsers(dest='action', help="The action to execute")

    setup_parser = cluster_subparsers.add_parser('setup', help="Setup a test cluster")
    setup_parser.add_argument(
        '--config',
        '-c',
        nargs='+',
        help="The configuration(s) to apply to the cluster"
    )
    setup_parser.add_argument(
        '--nodes',
        '-n',
        type=int,
        default=3,
        help="The number of nodes in the cluster"
    )
    setup_parser.add_argument(
        '--version',
        '-v',
        type=str,
        default='latest',
        help="The version to setup"
    )
    setup_parser.add_argument(
        '--subnet',
        help="The subnet in which to create the cluster"
    )
    setup_parser.add_argument(
        '--gateway',
        help="The IPv4 gateway for the master subnet"
    )
    setup_parser.add_argument(
        '--cpu',
        help="CPUs in which to allow execution (0-3, 0,1)"
    )
    setup_parser.add_argument(
        '--memory-limit',
        help="The per-container memory limit"
    )
    setup_parser.add_argument(
        '--debug',
        '-d',
        action='store_true',
        default=False,
        help="Enable debug logging"
    )
    setup_parser.add_argument(
        '--trace',
        '-t',
        action='store_true',
        default=False,
        help="Enable trace logging"
    )
    setup_parser.add_argument(
        '--profiler',
        choices=['yourkit'],
        help="Enable profiling"
    )
    setup_parser.set_defaults(func=setup)

    teardown_parser = cluster_subparsers.add_parser('teardown', help="Tear down a test cluster")
    teardown_parser.add_argument(
        '-d',
        '--delete',
        action='store_true',
        default=False,
        help="Whether to delete the cluster logs"
    )
    teardown_parser.set_defaults(func=teardown)

    cleanup_parser = cluster_subparsers.add_parser('cleanup', help="Cleans up all test clusters")
    cleanup_parser.add_argument(
        '-d',
        '--delete',
        action='store_true',
        default=False,
        help="Whether to delete the cluster logs"
    )
    cleanup_parser.set_defaults(func=cleanup)

    upgrade_parser = cluster_subparsers.add_parser('upgrade', help="Upgrades a test cluster")
    upgrade_parser.add_argument(
        '--version',
        '-v',
        type=str,
        default='latest',
        help="The version to which to upgrade"
    )
    upgrade_parser.set_defaults(func=upgrade)

    add_node_parser = cluster_subparsers.add_parser('add-node', help="Add a node to a test cluster")
    add_node_parser.add_argument(
        '-c',
        '--config',
        nargs='+',
        help="The configuration(s) to apply to the node"
    )
    add_node_parser.add_argument(
        '-v',
        '--version',
        type=str,
        default='latest',
        help="The version to setup"
    )
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
    delay_parser.add_argument(
        'node',
        nargs='?',
        type=name_or_id,
        help="The node to disrupt"
    )
    delay_parser.add_argument(
        '-l',
        '--latency',
        default='50ms',
        type=milliseconds,
        metavar='DELTA',
        help="The latency in the format [<hours>h][<minutes>m][<seconds>s][<milliseconds>ms]"
    )
    delay_parser.add_argument(
        '-j',
        '--jitter',
        default='10ms',
        type=milliseconds,
        metavar='DELTA',
        help="The jitter in the format [<hours>h][<minutes>m][<seconds>s][<milliseconds>ms]"
    )
    delay_parser.add_argument(
        '-c',
        '--correlation',
        default='75%',
        type=percentage,
        help="The correlation"
    )
    delay_parser.add_argument(
        '-d',
        '--distribution',
        default='normal',
        choices=['normal', 'pareto', 'paretonormal'],
        help="The distribution"
    )
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

    entropy_parser = subparsers.add_parser('entropy', help="Run an entropy test")

    run_parser = subparsers.add_parser('run', help="Run a test")
    run_parser.add_argument('paths', nargs='+', help="The modules or packages containing the test(s) to run")
    run_parser.add_argument('-ff', '--fail-fast', action='store_true', default=False, help="Whether to fail the test run on the first individual test failure")
    run_parser.set_defaults(func=run)

    return parser.parse_args(args)

def _parse_root_args(args):
    functions = _entropy_functions().keys()
    parser = ArgumentParser(prog='atomix-test entropy')
    parser.add_argument(
        '--nodes',
        '-n',
        type=int,
        default=3,
        metavar='NUM',
        help="The number of nodes in the cluster. Each node will be configured with the provided configurations."
    )
    parser.add_argument(
        '--config',
        '-c',
        nargs='+',
        metavar='FILE',
        help="The configuration(s) to apply to the cluster."
    )
    parser.add_argument(
        '--version',
        '-v',
        type=str,
        default='latest',
        help="The version of Atomix to setup. Must be a full semver version string, e.g. 3.0.6"
    )
    parser.add_argument(
        '--debug',
        action='store_true',
        default=False,
        help="Prints debug output to display the functions that will be run."
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        default=False,
        help="Run entropy test without live containers."
    )
    parser.add_argument(
        '-p',
        '--parallelism',
        type=int,
        default=8,
        metavar='COUNT',
        help="""Number of parallel processes with which to test. Each parallel process will perform a subset of the 
        operations. Defaults to 8."""
    )
    parser.add_argument(
        '-s',
        '--scale',
        type=int,
        default=1000,
        metavar='COUNT',
        help="""Number of unique keys to write to the cluster. The scale is directly proportional to the maximum
        amount of memory consumed by the test run."""
    )
    parser.add_argument(
        '--prime',
        type=int,
        default=0,
        metavar='COUNT',
        help="""Number of write operations with which to prime the cluster. These operations will be performed 
        prior to the test time beginning."""
    )
    parser.add_argument(
        '--ops',
        type=int,
        default=1,
        metavar='COUNT',
        help="""Number of operations to execute per second. The operations will be randomly distributed among all the
        specified processes such that the total operations per second from all processes matches this input."""
    )
    parser.add_argument(
        '-t',
        '--run-time',
        type=seconds,
        default='1m',
        metavar='DURATION',
        help="The amount of time for which to run the test in the format [<hours>h][<minutes>m][<seconds>s][<milliseconds>ms]"
    )
    parser.add_argument(
        '-d',
        '--function-delay',
        type=seconds,
        nargs='*',
        metavar='DURATION',
        help="""Uniform random delay to wait between entropy functions. Defaults to 15s-30s between functions
        The delay period is specified in the format [<hours>h][<minutes>m][<seconds>s][<milliseconds>ms]
        """
    )

    subparsers = parser.add_subparsers()
    for function in functions:
        subparsers.add_parser(function)
    return parser.parse_args(args)

def _parse_crash_args(args):
    parser = ArgumentParser(prog='atomix-test entropy crash')
    parser.add_argument(
        'start',
        nargs='?',
        type=seconds,
        help="The beginning of the crash period."
    )
    parser.add_argument(
        'end',
        nargs='?',
        type=seconds,
        help="The end of the crash period."
    )
    parser.add_argument('--random', '-r', nargs='*', type=seconds, metavar='DURATION', help="Crash a random node")

    namespace = parser.parse_args(args)
    if namespace.random is not None and len(namespace.random) > 2:
        parser.error("argument --random/-r: expected 0-2 argument(s)")
    return namespace

def _parse_partition_args(args):
    parser = ArgumentParser(prog='atomix-test entropy partition')
    parser.add_argument(
        'start',
        nargs='?',
        type=seconds,
        metavar='DURATION',
        help="The beginning of the partition period."
    )
    parser.add_argument(
        'end',
        nargs='?',
        type=seconds,
        metavar='DURATION',
        help="The end of the partition period."
    )
    parser.add_argument(
        '-r',
        '--random',
        nargs='*',
        type=seconds,
        metavar='DURATION',
        help="Partition a random set of nodes"
    )
    parser.add_argument(
        '-i',
        '--isolate',
        nargs='*',
        type=seconds,
        metavar='DURATION',
        help="Isolate a random node"
    )
    parser.add_argument(
        '-b',
        '--bridge',
        nargs='*',
        type=seconds,
        metavar='DURATION',
        help="Partition the cluster with a bridge node."
    )
    parser.add_argument(
        '-v',
        '--halves',
        nargs='*',
        type=seconds,
        metavar='DURATION',
        help="Partition the cluster into two halves."
    )

    namespace = parser.parse_args(args)
    if namespace.random is not None and len(namespace.random) > 2:
        parser.error("argument --random/-r: expected 0-2 argument(s)")
    if namespace.isolate is not None and len(namespace.isolate) > 2:
        parser.error("argument --isolate/-i: expected 0-2 argument(s)")
    if namespace.bridge is not None and len(namespace.bridge) > 2:
        parser.error("argument --bridge/-b: expected 0-2 argument(s)")
    if namespace.halves is not None and len(namespace.halves) > 2:
        parser.error("argument --halves/-v: expected 0-2 argument(s)")
    return namespace

def _parse_restart_args(args):
    parser = ArgumentParser(prog='atomix-test entropy restart')
    return parser.parse_args(args)

def _parse_stress_args(args):
    parser = ArgumentParser(prog='atomix-test entropy stress')
    parser.add_argument(
        'start',
        nargs='?',
        type=seconds,
        metavar='DURATION',
        help="The beginning of the stress period."
    )
    parser.add_argument(
        'end',
        nargs='?',
        type=seconds,
        metavar='DURATION',
        help="The end of the stress period."
    )
    parser.add_argument(
        '-r',
        '--random',
        nargs='*',
        type=seconds,
        metavar='DURATION',
        help="Stress a random node"
    )
    parser.add_argument(
        '-a',
        '--all',
        nargs='*',
        type=seconds,
        metavar='DURATION',
        help="Stress all nodes"
    )
    parser.add_argument(
        '-n',
        '--network',
        nargs='?',
        type=milliseconds,
        const=1,
        metavar='LATENCY',
        help="Stress the network"
    )
    parser.add_argument(
        '-c',
        '--cpu',
        nargs='?',
        type=int,
        const=1,
        metavar='PROCESSES',
        help="Stress the CPU"
    )
    parser.add_argument(
        '-i',
        '--io',
        nargs='?',
        type=int,
        const=1,
        metavar='PROCESSES',
        help="Stress the I/O"
    )
    parser.add_argument(
        '-m',
        '--memory',
        nargs='?',
        type=int,
        const=1,
        metavar='PROCESSES',
        help="Stress the memory"
    )

    namespace = parser.parse_args(args)
    if namespace.random is not None and len(namespace.random) > 2:
        parser.error("argument --random/-r: expected 0-2 argument(s)")
    if namespace.all is not None and len(namespace.all) > 2:
        parser.error("argument --all/-a: expected 0-2 argument(s)")
    return namespace

def _parse_entropy_args(args):
    functions = _entropy_functions()

    results = {'func': entropy_test}
    root_args = []
    root_parsed = False

    # Iterate through the arguments and apply them to subparsers.
    i = 0
    while i < len(args):
        arg = args[i]

        # If this is a help argument, apply the help to the current subparser if one is specified, otherwise
        # apply the help argument to the root parser.
        if arg in ('--help', '-h'):
            return _parse_root_args(['-h'])
        # If the argument is a function name, parse the root arguments, create a subparser, then parse
        # the subparser arguments.
        elif arg in functions:
            # If no parser has yet been defined, create a root parser and parse root arguments.
            if not root_parsed:
                results.update(vars(_parse_root_args(root_args + [arg])))
                root_parsed = True

            # Iterate through remaining arguments until the next function name is located and populate function arguments.
            parser_args = None
            for j in range(i + 1, len(args)):
                if args[j] in ('--help', '-h'):
                    return functions[arg](['-h'])
                elif args[j] in functions:
                    parser_args = args[i+1:j-1]
                    break

            # If no next function has been found, parse the remaining arguments.
            if parser_args is None:
                parser_args = args[i+1:]

            # Parse the function arguments then update the result dict.
            results[arg] = functions[arg](parser_args)

            # Increment the next argument index by the length of the parser arguments plus 1 for the function name.
            i += 1 + len(parser_args)
        # If this is a root argument, just append the argument and increment the next argument index.
        else:
            root_args.append(arg)
            i += 1
    return Namespace(**results)


def parse_args():
    args = sys.argv
    if args[1] == 'entropy':
        return _parse_entropy_args(args[2:])
    else:
        return _parse_cluster_args(args[1:])


def main():
    """Runs the test framework."""
    from colorama import init
    init()

    args = parse_args()
    try:
        args.func(args)
    except TestError, e:
        print str(e)
        sys.exit(1)
    else:
        sys.exit(0)
