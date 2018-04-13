from network import Network
from atomix import AtomixClient
from errors import UnknownClusterError, UnknownNetworkError, UnknownNodeError
from utils import logger, with_context
from six.moves import shlex_quote
import shutil
import os
import docker
import socket
import time
import uuid
from docker.api.client import APIClient
from docker.utils import kwargs_from_env

class Cluster(object):
    """Atomix test cluster."""
    def __init__(self, name):
        self.log = logger(name)
        self.name = name
        self.network = Network(name)
        self._docker_client = docker.from_env()
        self._docker_api_client = APIClient(kwargs_from_env())
        self._nodes = self._load_nodes()

    @property
    def path(self):
        """Returns the cluster data path."""
        return os.path.join(os.getcwd(), '.data', self.name)

    @property
    def cpus(self):
        return self._docker_api_client.inspect_container(self.node(1).name)['HostConfig']['CpusetCpus']

    @property
    def memory(self):
        return self._docker_api_client.inspect_container(self.node(1).name)['HostConfig']['Memory']

    @property
    def profiling(self):
        envs = self._docker_api_client.inspect_container(self.node(1).name)['Config']['Env']
        return _find_env(envs, 'profile') == 'true'

    def node(self, id):
        """Returns the node with the given ID."""
        if isinstance(id, int):
            return self.nodes()[id-1]
        else:
            return [node for node in self.nodes() if node.name == id].pop()

    def nodes(self, *types):
        """Returns a list of nodes in the cluster."""
        return [node for node in self._nodes if len(types) == 0 or node.type in types]

    def _load_nodes(self):
        """Returns a list of nodes in the cluster."""
        # Sort the containers by name and then extract the IP address from the container info.
        labels = ['atomix-test=true', 'atomix-cluster={}'.format(self.name),]
        containers = sorted(self._docker_client.containers.list(all=True, filters={'label': labels}), key=lambda c: c.name)
        nodes = []
        for container in containers:
            container_info = self._docker_api_client.inspect_container(container.name)
            nodes.append(Node(container.name, container_info['NetworkSettings']['Networks'][self.network.name]['IPAddress'], container_info['Config']['Labels']['atomix-type'], self))
        return nodes

    def core_nodes(self):
        return self.nodes(Node.Type.CORE)

    def data_nodes(self):
        return self.nodes(Node.Type.DATA)

    def client_nodes(self):
        return self.nodes(Node.Type.CLIENT)

    def _node_name(self, id):
        return '{}-{}'.format(self.name, id)

    def setup(
            self,
            nodes=3,
            type='core',
            core_partitions=7,
            data_partitions=71,
            supernet='172.18.0.0/16',
            subnet=None,
            gateway=None,
            cpu=None,
            memory=None,
            profiling=False,
            log_level='trace',
            console_log_level='info',
            file_log_level='info'):
        """Sets up the cluster."""
        self.log.info("Setting up cluster")

        # Set up the test network.
        self.network.setup(supernet, subnet, gateway)

        # Iterate through nodes and setup containers.
        setup_nodes = []
        for n in range(1, nodes + 1):
            node = Node(self._node_name(n), next(self.network.hosts), type, self)
            self._nodes.append(node)
            setup_nodes.append(node)

        for node in setup_nodes:
            node.setup(
                core_partitions,
                data_partitions,
                cpu,
                memory,
                profiling,
                log_level,
                console_log_level,
                file_log_level
            )

        self.log.info("Waiting for cluster bootstrap")
        self.wait_for_start()
        return self

    def add_node(self, type='data'):
        """Adds a new node to the cluster."""
        self.log.info("Adding a node to the cluster")

        # Look up the number of configured core/data partitions on the first core node.
        first_node = self.core_nodes()[0]
        core_partitions, data_partitions = first_node.core_partitions, first_node.data_partitions

        # Create a new node instance and setup the node.
        node = Node(self._node_name(len(self.nodes())+1), next(self.network.hosts), type, self)
        node.setup(
            cpus=self.cpus,
            memory=self.memory,
            profiling=self.profiling,
            core_partitions=core_partitions,
            data_partitions=data_partitions
        )

        # Wait for the node to finish startup before returning.
        node.wait_for_start()
        return node

    def remove_node(self, id):
        """Removes a node from the cluster."""
        self.log.info("Removing a node from the cluster")
        self.node(id).teardown()

    def wait_for_start(self):
        """Waits for a cluster to finish startup."""
        for node in self.nodes():
            node.wait_for_start()

    def wait_for_stop(self):
        """Waits for a cluster to finish shutdown."""
        for node in self.nodes():
            node.wait_for_stop()

    def shutdown(self):
        """Shuts down the cluster."""
        for node in self.nodes():
            node.kill()

    def startup(self):
        """Starts up the cluster."""
        for node in self.nodes():
            node.start()
        for node in self.nodes():
            node.wait_for_start()

    def restart(self):
        """Restarts the cluster."""
        self.shutdown()
        self.startup()

    def teardown(self):
        """Tears down the cluster."""
        self.log.info("Tearing down cluster")
        for node in self.nodes():
            try:
                node.teardown()
            except UnknownNodeError, e:
                self.log.error(str(e))
        try:
            self.network.teardown()
        except UnknownNetworkError, e:
            self.log.error(str(e))

    def cleanup(self):
        """Cleans up the cluster data."""
        self.log.info("Cleaning up cluster state")
        if os.path.exists(self.path):
            shutil.rmtree(self.path)

    def stress(self, node=None, timeout=None, cpu=None, io=None, memory=None, hdd=None):
        """Creates stress on nodes in the cluster."""
        if node is not None:
            return self.node(node).stress(timeout, cpu, io, memory, hdd)
        else:
            contexts = []
            for node in self.nodes():
                contexts.append(node.stress(timeout, cpu, io, memory, hdd))
            return with_context(*contexts)

    def destress(self, node=None):
        """Stops stress on node in the cluster."""
        if node is not None:
            self.node(node).destress()
        else:
            for node in self.nodes():
                node.destress()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.teardown()

    def __str__(self):
        lines = []
        lines.append('cluster: {}'.format(self.name))
        lines.append('network:')
        lines.append('  name: {}'.format(self.network.name))
        lines.append('  subnet: {}'.format(self.network.subnet))
        lines.append('  gateway: {}'.format(self.network.gateway))
        lines.append('nodes:')
        for node in self.nodes():
            lines.append('  {}:'.format(node.name))
            lines.append('    state: {}'.format(node.docker_container.status))
            lines.append('    type: {}'.format(node.type))
            lines.append('    ip: {}'.format(node.ip))
            lines.append('    host port: {}'.format(node.local_port))
        return '\n'.join(lines)


class _ConfiguredCluster(Cluster):
    def __init__(self, name, **kwargs):
        super(_ConfiguredCluster, self).__init__(name)
        self._kwargs = kwargs

    def setup(self):
        super(_ConfiguredCluster, self).setup(**self._kwargs)

    def __enter__(self):
        self.setup()
        return self


class Node(object):
    """Atomix test node."""
    class Type(object):
        CORE = 'core'
        DATA = 'data'
        CLIENT = 'client'

    def __init__(self, name, ip, type, cluster):
        self.log = logger(cluster.name)
        self.name = name
        self.ip = ip
        self.type = type
        self.http_port = 5678
        self.tcp_port = 5679
        self.cluster = cluster
        self.path = os.path.join(self.cluster.path, self.name)
        self._docker_client = docker.from_env()
        self._docker_api_client = APIClient(kwargs_from_env())
        try:
            self.client = AtomixClient(port=self.local_port)
        except UnknownNodeError:
            self.client = None

    def __getattr__(self, name):
        try:
            return super(Node, self).__getattr__(name)
        except AttributeError, e:
            try:
                return getattr(self.client, name)
            except AttributeError:
                raise e

    def _find_open_port(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(("", 0))
        s.listen(1)
        port = s.getsockname()[1]
        s.close()
        return port

    @property
    def id(self):
        return int(self.name.split('-')[-1])

    @property
    def status(self):
        return self.docker_container.status

    @property
    def local_port(self):
        port_bindings = self._docker_api_client.inspect_container(self.docker_container.name)['HostConfig']['PortBindings']
        return port_bindings['{}/tcp'.format(self.http_port)][0]['HostPort']

    @property
    def profiler_port(self):
        if self.cluster.profiling:
            port_bindings = self._docker_api_client.inspect_container(self.docker_container.name)['HostConfig']['PortBindings']
            return port_bindings['10001/tcp'.format(self.http_port)][0]['HostPort']

    @property
    def core_partitions(self):
        envs = self._docker_api_client.inspect_container(self.docker_container.name)['Config']['Env']
        core_partitions = _find_env(envs, 'core_partitions')
        return int(core_partitions) if core_partitions is not None else 7

    @property
    def data_partitions(self):
        envs = self._docker_api_client.inspect_container(self.docker_container.name)['Config']['Env']
        data_partitions = _find_env(envs, 'data_partitions')
        return int(data_partitions) if data_partitions is not None else 71

    def attach(self):
        """Watches output on the node."""
        return self.docker_container.attach(stream=True, logs=False)

    def logs(self, stream=False):
        """Returns the logs for the node."""
        return self.docker_container.logs(stream=stream)

    @property
    def docker_container(self):
        try:
            return self._docker_client.containers.get(self.name)
        except docker.errors.NotFound:
            raise UnknownNodeError(self.name)

    def setup(self, core_partitions=7, data_partitions=71, cpus=None, memory=None, profiling=False, log_level='trace', console_log_level='info', file_log_level='info'):
        """Sets up the node."""
        args = []
        args.append('%s@%s:%d' % (self.name, self.ip, self.tcp_port))

        args.append('--type')
        args.append(self.type)

        config = ""
        config += "cluster:"
        config += "  name: {}".format(self.cluster.name)
        config += "partition-groups:"

        core_nodes = self.cluster.nodes(Node.Type.CORE)
        if len(core_nodes) > 0:
            args.append('--core-nodes')
            for node in core_nodes:
                args.append('%s@%s:%d' % (node.name, node.ip, node.tcp_port))
            config += "  - type: raft"
            config += "    name: core"
            config += "    partitions: {}".format(core_partitions)
        else:
            data_nodes = self.cluster.nodes(Node.Type.DATA)
            if len(data_nodes) > 0:
                args.append('--bootstrap-nodes')
                for node in data_nodes:
                    args.append('%s@%s:%d' % (node.name, node.ip, node.tcp_port))
            else:
                client_nodes = self.cluster.nodes(Node.Type.CLIENT)
                if len(client_nodes) > 0:
                    args.append('--bootstrap-nodes')
                    for node in client_nodes:
                        args.append('%s@%s:%d' % (node.name, node.ip, node.tcp_port))

        config += "  - type: multi-primary"
        config += "    name: data"
        config += "    partitions: {}".format(data_partitions)

        args.append('--config')
        args.append(config)

        ports = {self.http_port: self._find_open_port()}
        if profiling:
            ports[10001] = self._find_open_port()

        log_levels = ('trace', 'debug', 'info', 'warn', 'error')
        def find_index(level):
            for i in range(len(log_levels)):
                if log_levels[i] == level:
                    return i

        if find_index(console_log_level) < find_index(log_level):
            log_level = console_log_level
        if find_index(file_log_level) < find_index(log_level):
            log_level = file_log_level

        self.log.info("Running container %s", self.name)
        self._docker_client.containers.run(
            'atomix',
            ' '.join([shlex_quote(str(arg)) for arg in args]),
            name=self.name,
            labels={'atomix-test': 'true', 'atomix-cluster': self.cluster.name, 'atomix-type': self.type},
            cap_add=['NET_ADMIN'],
            network=self.cluster.network.name,
            ports=ports,
            detach=True,
            volumes={self.path: {'bind': '/data', 'mode': 'rw'}},
            cpuset_cpus=cpus,
            mem_limit=memory,
            environment={
                'profile': 'true' if profiling else 'false',
                'log_level': log_level.upper(),
                'console_log_level': console_log_level.upper(),
                'file_log_level': file_log_level.upper(),
                'cluster_name': self.cluster.name,
                'core_partitions': core_partitions,
                'data_partitions': data_partitions
            })
        self.client = AtomixClient(port=self.local_port)
        return self

    def run(self, *command):
        """Runs the given command in the container."""
        if len(command) > 1:
            command = ' '.join([shlex_quote(str(arg)) for arg in command])
        else:
            command = command[0]
        self.log.info("Executing command '%s' on %s", command, self.name)
        return self.docker_container.exec_run(command)

    def execute(self, *command):
        """Runs the given command in the container."""
        if len(command) > 1:
            command = ' '.join([shlex_quote(str(arg)) for arg in command])
        else:
            command = command[0]
        self.log.info("Executing command '%s' on %s", command, self.name)
        return self.docker_container.exec_run(command, detach=True)

    def stop(self):
        """Stops the node."""
        self.log.info("Stopping node %s", self.name)
        self.docker_container.stop()

    def start(self):
        """Starts the node."""
        self.log.info("Starting node %s", self.name)
        self.docker_container.start()
        self.wait_for_start()

    def kill(self):
        """Kills the node."""
        self.log.info("Killing node %s", self.name)
        self.docker_container.kill()

    def recover(self):
        """Recovers a killed node."""
        self.log.info("Recovering node %s", self.name)
        self.docker_container.start()
        self.wait_for_start()

    def restart(self):
        """Restarts the node."""
        self.log.info("Restarting node %s", self.name)
        self.docker_container.restart()
        self.wait_for_start()

    def partition(self, node):
        """Partitions this node from the given node."""
        return self.cluster.network.partition(self.name, node.name)

    def heal(self, node):
        """Heals a partition between this node and the given node."""
        return self.cluster.network.heal(self.name, node.name)

    def isolate(self):
        """Isolates this node from all other nodes in the cluster."""
        return self.cluster.network.partition(self.name)

    def unisolate(self):
        """Unisolates this node from all other nodes in the cluster."""
        return self.cluster.network.heal(self.name)

    def delay(self, latency=50, jitter=10, correlation=.75, distribution='normal'):
        """Delays packets to this node."""
        return self.cluster.network.delay(self.name, latency, jitter, correlation, distribution)

    def drop(self, probability=.02, correlation=.25):
        """Drops packets to this node."""
        return self.cluster.network.drop(self.name, probability, correlation)

    def reorder(self, probability=.02, correlation=.5):
        """Reorders packets to this node."""
        return self.cluster.network.reorder(self.name, probability, correlation)

    def duplicate(self, probability=.005, correlation=.05):
        """Duplicates packets to this node."""
        return self.cluster.network.duplicate(self.name, probability, correlation)

    def corrupt(self, probability=.02):
        """Duplicates packets to this node."""
        return self.cluster.network.corrupt(self.name, probability)

    def restore(self):
        """Restores packets to this node to normal order."""
        return self.cluster.network.restore(self.name)

    def stress(self, timeout=None, cpu=None, io=None, memory=None, hdd=None):
        """Creates stress on the node."""
        command = ['stress']

        def maybe_append(name, arg):
            if arg is not None:
                command.append('--{}'.format(name))
                command.append(arg)

        maybe_append('timeout', timeout)
        maybe_append('cpu', cpu)
        maybe_append('io', io)
        maybe_append('vm', memory)
        maybe_append('hdd', hdd)
        self.execute(*command)
        return with_context(lambda: self.destress())

    def destress(self):
        """Stops stress on a node."""
        self.run("pkill -f stress")

    def remove(self):
        """Removes the node from the cluster."""
        self.teardown()

    def teardown(self):
        """Tears down the node."""
        container = self.docker_container
        self.log.info("Stopping container %s", self.name)
        container.stop()
        self.log.info("Removing container %s", self.name)
        container.remove()

    def wait_for_start(self, timeout=60):
        """Waits for the node to finish startup."""
        for _ in range(timeout):
            if not self.client.status():
                time.sleep(1)
            else:
                return
        raise AssertionError("Failed to start node {}".format(self.name))

    def wait_for_stop(self):
        """Waits for the node to exit."""
        self.docker_container.wait()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.teardown()


def create_cluster(name=None, **kwargs):
    if name is None:
        name = str(uuid.uuid4())
    return _ConfiguredCluster(name, **kwargs)


def _find_cluster():
    docker_client = docker.from_env()
    docker_api_client = APIClient(kwargs_from_env())
    containers = docker_client.containers.list(filters={'label': 'atomix-test=true'})
    if len(containers) > 0:
        container = containers[0]
        cluster_name = docker_api_client.inspect_container(container.name)['Config']['Labels']['atomix-cluster']
        return Cluster(cluster_name)
    raise UnknownClusterError(None)


def get_cluster(name=None):
    return Cluster(name) if name is not None else _find_cluster()


def get_clusters():
    docker_client = docker.from_env()
    docker_api_client = APIClient(kwargs_from_env())
    containers = docker_client.containers.list(filters={'label': 'atomix-test=true'})
    clusters = set()
    for container in containers:
        cluster_name = docker_api_client.inspect_container(container.name)['Config']['Labels']['atomix-cluster']
        clusters.add(cluster_name)
    return [Cluster(name) for name in clusters]


def _find_env(envs, name):
    prefix = name + '='
    for env in envs:
        if env.startswith(prefix):
            return env[len(prefix):]
    return False
