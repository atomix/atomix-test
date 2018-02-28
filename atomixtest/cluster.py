from network import Network
from atomix import AtomixClient
from logger import Logger
from errors import UnknownClusterError, UnknownNetworkError, UnknownNodeError
from six.moves import shlex_quote
import shutil
import os
import docker
import socket
import time
from docker.api.client import APIClient
from docker.utils import kwargs_from_env

class Cluster(object):
    """Atomix test cluster."""
    def __init__(self, name):
        self.log = Logger(name, Logger.Type.FRAMEWORK)
        self.name = name
        self.network = Network(name)
        self._docker_client = docker.from_env()
        self._docker_api_client = APIClient(kwargs_from_env())

    @property
    def path(self):
        """Returns the cluster data path."""
        return os.path.join(os.getcwd(), '.data', self.name)

    def node(self, id):
        """Returns the node with the given ID."""
        if isinstance(id, int):
            return self.nodes()[id-1]
        else:
            return [node for node in self.nodes() if node.name == id].pop()

    def nodes(self, type=None):
        """Returns a list of nodes in the cluster."""
        # Sort the containers by name and then extract the IP address from the container info.
        if type is not None:
            labels = ['atomix-test=true', 'atomix-cluster={}'.format(self.name), 'atomix-type={}'.format(type)]
        else:
            labels = ['atomix-test=true', 'atomix-cluster={}'.format(self.name),]
        containers = sorted(self._docker_client.containers.list(all=True, filters={'label': labels}), key=lambda c: c.name)
        nodes = []
        for container in containers:
            container_info = self._docker_api_client.inspect_container(container.name)
            nodes.append(Node(container.name, container_info['NetworkSettings']['Networks'][self.network.name]['IPAddress'], container_info['Config']['Labels']['atomix-type'], self))
        return nodes

    def servers(self):
        return self.nodes(Node.Type.SERVER)

    def clients(self):
        return self.nodes(Node.Type.CLIENT)

    def _node_name(self, id):
        return '{}-{}'.format(self.name, id)

    def setup(self, nodes=3, supernet='172.18.0.0/16', subnet=None, gateway=None, cpu=None, memory=None):
        """Sets up the cluster."""
        self.log.message("Setting up cluster")

        # Set up the test network.
        self.network.setup(supernet, subnet, gateway)

        # Iterate through nodes and setup containers.
        for n in range(1, nodes + 1):
            Node(self._node_name(n), next(self.network.hosts), Node.Type.SERVER, self).setup(cpu, memory)

        self.log.message("Waiting for cluster bootstrap")
        self.wait_for_start()
        return self.nodes()

    def add_node(self, type='server'):
        """Adds a new node to the cluster."""
        self.log.message("Adding a node to the cluster")
        node = Node(self._node_name(len(self.nodes())+1), next(self.network.hosts), type, self)
        node.setup()
        node.wait_for_start()
        return node

    def remove_node(self, id):
        """Removes a node from the cluster."""
        self.log.message("Removing a node from the cluster")
        self.node(id).teardown()

    def wait_for_start(self):
        """Waits for a cluster to finish startup."""
        for node in self.nodes():
            node.wait_for_start()

    def wait_for_stop(self):
        """Waits for a cluster to finish shutdown."""
        for node in self.nodes():
            node.wait_for_stop()

    def teardown(self):
        """Tears down the cluster."""
        self.log.message("Tearing down cluster")
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
        self.log.message("Cleaning up cluster state")
        if os.path.exists(self.path):
            shutil.rmtree(self.path)

    def stress(self, node=None, timeout=None, cpu=None, io=None, memory=None, hdd=None):
        """Creates stress on nodes in the cluster."""
        if node is not None:
            self.node(node).stress(timeout, cpu, io, memory, hdd)
        else:
            for node in self.nodes():
                node.stress(timeout, cpu, io, memory, hdd)

    def destress(self, node=None):
        """Stops stress on node in the cluster."""
        if node is not None:
            self.node(node).destress()
        else:
            for node in self.nodes():
                node.destress()

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


class Node(object):
    """Atomix test node."""
    class Type(object):
        SERVER = 'server'
        CLIENT = 'client'

    def __init__(self, name, ip, type, cluster):
        self.log = Logger(cluster.name, Logger.Type.FRAMEWORK)
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

    def logs(self):
        """Returns the logs for the node."""
        return self.docker_container.logs()

    @property
    def docker_container(self):
        try:
            return self._docker_client.containers.get(self.name)
        except docker.errors.NotFound:
            raise UnknownNodeError(self.name)

    def setup(self, cpu=None, memory=None):
        """Sets up the node."""
        args = []
        args.append('%s:%s:%d' % (self.name, self.ip, self.tcp_port))
        args.append('--bootstrap')
        for node in self.cluster.nodes():
            args.append('%s:%s:%d' % (node.name, node.ip, node.tcp_port))

        self.log.message("Running container {}", self.name)
        self._docker_client.containers.run(
            'atomix',
            ' '.join(args),
            name=self.name,
            labels={'atomix-test': 'true', 'atomix-cluster': self.cluster.name, 'atomix-type': self.type},
            network=self.cluster.network.name,
            ports={self.http_port: self._find_open_port()},
            detach=True,
            volumes={self.path: {'bind': '/data', 'mode': 'rw'}},
            cpuset_cpus=cpu,
            mem_limit=memory)
        self.client = AtomixClient(port=self.local_port)

    def run(self, *command):
        """Runs the given command in the container."""
        if len(command) > 1:
            command = ' '.join([shlex_quote(str(arg)) for arg in command])
        else:
            command = command[0]
        self.log.message("Executing command '{}' on {}", command, self.name)
        return self.docker_container.exec_run(command)

    def execute(self, *command):
        """Runs the given command in the container."""
        if len(command) > 1:
            command = ' '.join([shlex_quote(str(arg)) for arg in command])
        else:
            command = command[0]
        self.log.message("Executing command '{}' on {}", command, self.name)
        return self.docker_container.exec_run(command, detach=True)

    def stop(self):
        """Stops the node."""
        self.log.message("Stopping node {}", self.name)
        self.docker_container.stop()

    def start(self):
        """Starts the node."""
        self.log.message("Starting node {}", self.name)
        self.docker_container.start()
        self.wait_for_start()

    def kill(self):
        """Kills the node."""
        self.log.message("Killing node {}", self.name)
        self.docker_container.kill()

    def recover(self):
        """Recovers a killed node."""
        self.log.message("Recovering node {}", self.name)
        self.docker_container.start()
        self.wait_for_start()

    def restart(self):
        """Restarts the node."""
        self.log.message("Restarting node {}", self.name)
        self.docker_container.restart()
        self.wait_for_start()

    def partition(self, node):
        """Partitions this node from the given node."""
        self.cluster.network.partition(self.name, node.name)

    def heal(self, node):
        """Heals a partition between this node and the given node."""
        self.cluster.network.heal(self.name, node.name)

    def isolate(self):
        """Isolates this node from all other nodes in the cluster."""
        self.cluster.network.isolate(self.name)

    def unisolate(self):
        """Unisolates this node from all other nodes in the cluster."""
        self.cluster.network.unisolate(self.name)

    def delay(self, latency=50, jitter=10, correlation=.75, distribution='normal'):
        """Delays packets to this node."""
        self.cluster.network.delay(self.name, latency, jitter, correlation, distribution)

    def drop(self, probability=.02, correlation=.25):
        """Drops packets to this node."""
        self.cluster.network.drop(self.name, probability, correlation)

    def reorder(self, probability=.02, correlation=.5):
        """Reorders packets to this node."""
        self.cluster.network.reorder(self.name, probability, correlation)

    def duplicate(self, probability=.005, correlation=.05):
        """Duplicates packets to this node."""
        self.cluster.network.duplicate(self.name, probability, correlation)

    def corrupt(self, probability=.02):
        """Duplicates packets to this node."""
        self.cluster.network.corrupt(self.name, probability)

    def restore(self):
        """Restores packets to this node to normal order."""
        self.cluster.network.restore(self.name)

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

    def destress(self):
        """Stops stress on a node."""
        self.run("pkill -f stress")

    def teardown(self):
        """Tears down the node."""
        container = self.docker_container
        self.log.message("Stopping container {}", self.name)
        container.stop()
        self.log.message("Removing container {}", self.name)
        container.remove()

    def wait_for_start(self):
        """Waits for the node to finish startup."""
        for _ in range(30):
            if not self.client.status():
                time.sleep(1)
            else:
                return
        raise AssertionError("Failed to start node {}".format(self.name))

    def wait_for_stop(self):
        """Waits for the node to exit."""
        self.docker_container.wait()

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
