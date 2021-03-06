import docker
import os
import shutil
import socket
import time
from atomix import AtomixClient
from docker.api.client import APIClient
from docker.utils import kwargs_from_env
from six.moves import shlex_quote
from threading import Thread

from errors import UnknownClusterError, UnknownNetworkError, UnknownNodeError
from logging import logger
from network import Network
from utils import with_context


class Cluster(object):
    """Atomix test cluster."""
    def __init__(self, name, process_id=None):
        self.name = name
        self.process_id = process_id
        self.network = Network(name, process_id)
        self._docker_client = docker.from_env()
        self._docker_api_client = APIClient(kwargs_from_env())
        self._nodes = self._load_nodes()

        if not os.path.exists(self.path):
            os.makedirs(self.path)

    @property
    def path(self):
        """Returns the cluster data path."""
        return os.path.join(os.getcwd(), 'data', self.name)

    @property
    def cpus(self):
        return self._docker_api_client.inspect_container(self.node(1).name)['HostConfig']['CpusetCpus']

    @property
    def memory(self):
        return self._docker_api_client.inspect_container(self.node(1).name)['HostConfig']['Memory']

    @property
    def profile(self):
        container_info = self._docker_api_client.inspect_container(self.node(1).name)
        return container_info['Config']['Labels']['atomix-profile'] != ''

    def node(self, id):
        """Returns the node with the given ID."""
        if isinstance(id, int):
            return self.nodes()[id-1]
        else:
            nodes = [node for node in self.nodes() if node.name == id]
            return nodes.pop() if len(nodes) > 0 else None

    def nodes(self, bootstrap=False):
        """Returns a list of nodes in the cluster."""
        return [node for node in self._nodes if bootstrap is False or node.bootstrap]

    def _load_nodes(self):
        """Returns a list of nodes in the cluster."""
        # Sort the containers by name and then extract the IP address from the container info.
        labels = ['atomix-test=true', 'atomix-cluster={}'.format(self.name),]
        containers = sorted(self._docker_client.containers.list(all=True, filters={'label': labels}), key=lambda c: c.name)
        nodes = []
        for container in containers:
            container_info = self._docker_api_client.inspect_container(container.name)
            nodes.append(Node(
                container.name,
                container_info['NetworkSettings']['Networks'][self.network.name]['IPAddress'],
                self,
                container_info['Config']['Labels']['atomix-version'],
                container_info['Config']['Labels']['atomix-bootstrap'] == 'true',
                container_info['Config']['Labels']['atomix-process']
            ))
        return nodes

    def _node_name(self, id):
        return '{}-{}'.format(self.name, id)

    def setup(self, *args, **kwargs):
        """Sets up the cluster."""
        logger.info("Setting up cluster")

        defaults = {
            'nodes': 3,
            'supernet': '172.18.0.0/16',
            'subnet': None,
            'gateway': None,
            'version': 'latest',
            'profile': False
        }

        def kwarg(name):
            return kwargs.get(name, defaults[name])

        # Set up the test network.
        self.network.setup(kwarg('supernet'), kwarg('subnet'), kwarg('gateway'))

        # Iterate through nodes and setup containers.
        setup_nodes = []
        for n in range(1, kwarg('nodes') + 1):
            node = Node(
                self._node_name(n),
                next(self.network.hosts),
                self,
                kwarg('version'),
                bootstrap=True,
                process_id=self.process_id
            )
            self._nodes.append(node)
            setup_nodes.append(node)

        profiler_port = 10001 if kwarg('profile') else None
        for node in setup_nodes:
            kwargs['profile'] = profiler_port
            node.setup(*args, **kwargs)
            profiler_port += 1

        logger.info("Waiting for cluster bootstrap")
        self.wait_for_start()
        return self

    def upgrade(self, version='latest'):
        """Upgrades the cluster to the given version."""
        for node in self.nodes():
            self.upgrade_node(node.name, version)

    def add_node(self, *args, **kwargs):
        """Adds a new node to the cluster."""
        logger.info("Adding a node to the cluster")

        # Create a new node instance and setup the node.
        node = Node(
            self._node_name(len(self.nodes())+1),
            next(self.network.hosts),
            self,
            kwargs.get('version', 'latest'),
            bootstrap=False,
            process_id=self.process_id
        )
        self._nodes.append(node)

        node.setup(
            *args,
            cpus=self.cpus,
            memory=self.memory,
            profile=10000 + len(self._nodes),
            debug=kwargs.get('debug', False),
            trace=kwargs.get('trace', False)
        )

        # Wait for the node to finish startup before returning.
        node.wait_for_start()
        return node

    def remove_node(self, id):
        """Removes a node from the cluster."""
        logger.info("Removing a node from the cluster")
        node = self.node(id)
        node.teardown()
        self._nodes.remove(node)

    def upgrade_node(self, id, version='latest'):
        """Upgrades the node to the given version."""
        logger.info("Upgrading node {} to version '{}'".format(id, version))
        node = self.node(id)
        node.upgrade(version)
        node.wait_for_start()
        return node

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
        logger.info("Tearing down cluster")

        threads = []
        for node in self.nodes():
            try:
                thread = Thread(target=lambda: node.teardown())
                threads.append(thread)
                thread.start()
            except UnknownNodeError, e:
                logger.error(str(e))

        for thread in threads:
            thread.join()

        try:
            self.network.teardown()
        except UnknownNetworkError, e:
            logger.error(str(e))

    def cleanup(self):
        """Cleans up the cluster data."""
        logger.info("Cleaning up cluster state")
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
        self.cleanup()

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
            lines.append('    state: {}'.format(node.status))
            lines.append('    type: {}'.format(node.type))
            lines.append('    ip: {}'.format(node.ip))
            lines.append('    host port: {}'.format(node.local_port))
        return '\n'.join(lines)


class TestClient(AtomixClient):
    """Atomix test client."""
    def __init__(self, host='127.0.0.1', port=5678):
        super(TestClient, self).__init__(host, port)

    def get(self, path, headers=None, *args, **kwargs):
        logger.debug('GET {}'.format(path.format(*args, **kwargs)))
        try:
            return super(TestClient, self).get(path, headers, *args, **kwargs)
        except:
            logger.error('GET {}'.format(path.format(*args, **kwargs)))
            raise

    def post(self, path, data=None, headers=None, *args, **kwargs):
        logger.debug('POST {}'.format(path.format(*args, **kwargs)))
        try:
            return super(TestClient, self).post(path, data, headers, *args, **kwargs)
        except:
            logger.error('POST {}'.format(path.format(*args, **kwargs)))
            raise

    def put(self, path, data=None, headers=None, *args, **kwargs):
        logger.debug('PUT {}'.format(path.format(*args, **kwargs)))
        try:
            return super(TestClient, self).put(path, data, headers, *args, **kwargs)
        except:
            logger.error('PUT {}'.format(path.format(*args, **kwargs)))
            raise

    def delete(self, path, headers=None, *args, **kwargs):
        logger.debug('DELETE {}'.format(path.format(*args, **kwargs)))
        try:
            return super(TestClient, self).delete(path, headers, *args, **kwargs)
        except:
            logger.error('DELETE {}'.format(path.format(*args, **kwargs)))
            raise


class Node(object):
    """Atomix test node."""
    def __init__(self, name, ip, cluster, version, bootstrap, process_id=None):
        self.name = name
        self.ip = ip
        self.version = version
        self.bootstrap = bootstrap
        self.process_id = process_id
        self.http_port = 5678
        self.tcp_port = 5679
        self.cluster = cluster
        self.path = os.path.join(self.cluster.path, self.name)
        self._docker_client = docker.from_env()
        self._docker_api_client = APIClient(kwargs_from_env())
        try:
            self.client = TestClient(port=self.local_port)
        except UnknownNodeError:
            self.client = None

        if not os.path.exists(self.path):
            os.makedirs(self.path)

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
    def address(self):
        return '{}:{}'.format(self.name, self.tcp_port)

    @property
    def status(self):
        return self.docker_container.status

    @property
    def local_port(self):
        port_bindings = self._docker_api_client.inspect_container(self.docker_container.name)['HostConfig']['PortBindings']
        return port_bindings['{}/tcp'.format(self.http_port)][0]['HostPort']

    @property
    def profiler_port(self):
        container_info = self._docker_api_client.inspect_container(self.docker_container.name)
        return int(container_info['Config']['Labels']['atomix-profile']) if container_info['Config']['Labels']['atomix-profile'] != '' else None

    @property
    def profiles(self):
        envs = self._docker_api_client.inspect_container(self.docker_container.name)['Config']['Env']
        profiles = _find_env(envs, 'profiles')
        return tuple(profiles.split(',')) if profiles is not None else ('client',)

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

    def setup(self, *configs, **kwargs):
        """Sets up the node."""

        defaults = {
            'cpus': None,
            'memory': None,
            'profile': None,
            'debug': False,
            'trace': False
        }

        def kwarg(name):
            return kwargs.get(name, defaults[name])

        args = []

        # Create a membership discovery configuration
        discovery_config = []
        discovery_config.append('type: bootstrap')

        # Create a members list variable to use for substitution
        members_config = []

        # Populate the discovery and members list configurations from bootstrap members
        i = 0
        for node in self.cluster.nodes(bootstrap=True):
            i += 1
            discovery_config.append('nodes.{} {{'.format(i))
            discovery_config.append('  id: {}'.format(node.name))
            discovery_config.append('  address: "{}"'.format(node.address))
            discovery_config.append('}')
            members_config.append(node.name)

        if len(configs) == 0:
            args.append('--config')
            for filename in os.listdir(self.path):
                if filename.endswith('.conf'):
                    args.append(os.path.join('/data', os.path.basename(filename)))
        else:
            args.append('--config')
            for config in configs:
                config_file = None
                for dirpath, dirnames, filenames in os.walk(os.path.join(os.path.dirname(__file__), '../config')):
                    for filename in filenames:
                        if filename.endswith('.conf') and filename[0:-5] == config:
                            config_file = os.path.join(dirpath, filename)
                            break
                    if config_file is not None:
                        break

                if config_file is None:
                    config_file = config

                if os.path.exists(config_file):
                    with open(config_file, 'r') as f:
                        config_text = f.read()
                else:
                    raise UnknownNodeError("Failed to locate configuration file: '{}'".format(config_file))

                def replace(lines, property, values):
                    complete = False
                    while not complete:
                        complete = True
                        for i in range(len(lines)):
                            line = lines[i]
                            if line.strip() == property:
                                spaces = 0
                                for c in line:
                                    if c == ' ':
                                        spaces += 1
                                    else:
                                        break
                                lines = lines[:i] + [''.join([' ' for _ in range(spaces)]) + value for value in values] + lines[i+1:]
                                complete = False
                    return lines

                # Substitute the discovery and members list configurations in the provided configuration file
                lines = config_text.split('\n')
                lines = replace(lines, '${DISCOVERY}', discovery_config)
                lines = replace(lines, '${MEMBERS}', members_config)
                config_text = '\n'.join(lines)

                # Create a named temporary file to pass in to the Atomix agent process
                with open(os.path.join(self.path, os.path.basename(config_file)), 'w+') as f:
                    f.truncate(0)
                    f.write(config_text)
                    args.append(os.path.join('/data', os.path.basename(f.name)))

        environment = {
            'CLUSTER_ID': self.cluster.name,
            'NODE_ID': self.name,
            'NODE_ADDRESS': self.address,
            'DATA_DIR': '/data'
        }

        # Find an open HTTP port and if profiling is enabled a profiler port to which to bind
        ports = {self.http_port: self._find_open_port()}
        profiler_port = kwarg('profile')
        if profiler_port is not None:
            ports[10001] = profiler_port
            environment['JAVA_OPTS'] = '-agentpath:/root/atomix/lib/libyjpagent.so=listen=all'

        args.append('--log-dir')
        args.append('/data/log')
        if kwarg('trace'):
            args.append('--log-level')
            args.append('TRACE')
            args.append('--file-log-level')
            args.append('TRACE')
        elif kwarg('debug'):
            args.append('--log-level')
            args.append('DEBUG')
            args.append('--file-log-level')
            args.append('DEBUG')
        args.append('--ignore-resources')

        logger.info("Running container %s", self.name)
        self._docker_client.containers.run(
            'atomix/atomix-test:{}'.format(self.version),
            ' '.join([shlex_quote(str(arg)) for arg in args]),
            name=self.name,
            labels={
                'atomix-test': 'true',
                'atomix-process': self.process_id or '',
                'atomix-cluster': self.cluster.name,
                'atomix-bootstrap': 'true' if self.bootstrap else 'false',
                'atomix-version': self.version,
                'atomix-profile': str(profiler_port) or ''
            },
            cap_add=['NET_ADMIN'],
            network=self.cluster.network.name,
            ports=ports,
            detach=True,
            links=[(node.name, node.name) for node in self.cluster.nodes()],
            volumes={self.path: {'bind': '/data', 'mode': 'rw'}},
            cpuset_cpus=kwarg('cpus'),
            mem_limit=kwarg('memory'),
            environment=environment)
        self.client = TestClient(port=self.local_port)
        return self

    def upgrade(self, version):
        """Upgrades the node to the given version."""
        self.teardown()
        self.version = version
        self.setup()

    def run(self, *command):
        """Runs the given command in the container."""
        if len(command) > 1:
            command = ' '.join([shlex_quote(str(arg)) for arg in command])
        else:
            command = command[0]
        logger.info("Executing command '%s' on %s", command, self.name)
        return self.docker_container.exec_run(command)

    def execute(self, *command):
        """Runs the given command in the container."""
        if len(command) > 1:
            command = ' '.join([shlex_quote(str(arg)) for arg in command])
        else:
            command = command[0]
        logger.info("Executing command '%s' on %s", command, self.name)
        return self.docker_container.exec_run(command, detach=True)

    def stop(self):
        """Stops the node."""
        logger.info("Stopping node %s", self.name)
        self.docker_container.stop()

    def start(self):
        """Starts the node."""
        logger.info("Starting node %s", self.name)
        self.docker_container.start()

    def kill(self):
        """Kills the node."""
        logger.info("Killing node %s", self.name)
        self.docker_container.kill()

    def recover(self):
        """Recovers a killed node."""
        logger.info("Recovering node %s", self.name)
        self.docker_container.start()
        self.wait_for_start()

    def restart(self):
        """Restarts the node."""
        logger.info("Restarting node %s", self.name)
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
        try:
            logger.info("Stopping container %s", self.name)
            container.stop()
        except:
            pass
        try:
            logger.info("Removing container %s", self.name)
            container.remove()
        except:
            pass

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

    def __str__(self):
        return self.name

    def __eq__(self, other):
        return isinstance(other, Node) and self.name == other.name

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.teardown()


def _find_cluster():
    docker_client = docker.from_env()
    docker_api_client = APIClient(kwargs_from_env())
    containers = docker_client.containers.list(all=True, filters={'label': 'atomix-test=true'})
    if len(containers) > 0:
        container = containers[0]
        cluster_name = docker_api_client.inspect_container(container.name)['Config']['Labels']['atomix-cluster']
        return Cluster(cluster_name)
    raise UnknownClusterError(None)


def get_cluster(name=None):
    return Cluster(name) if name is not None else _find_cluster()


def get_clusters(process_id=None):
    docker_client = docker.from_env()
    docker_api_client = APIClient(kwargs_from_env())
    containers = docker_client.containers.list(all=True, filters={'label': 'atomix-test=true'})
    clusters = set()
    for container in containers:
        if process_id is None or process_id == docker_api_client.inspect_container(container.name)['Config']['Labels']['atomix-process']:
            cluster_name = docker_api_client.inspect_container(container.name)['Config']['Labels']['atomix-cluster']
            clusters.add(cluster_name)
    return [Cluster(name) for name in clusters]


def get_configs():
    """Returns a list of available configurations."""
    for filename in os.listdir(os.path.join(os.path.dirname(__file__), '../config')):
        name, extension = os.path.splitext(filename)
        if extension == '.conf':
            yield name


def _find_env(envs, name):
    prefix = name + '='
    for env in envs:
        if env.startswith(prefix):
            return env[len(prefix):]
    return False
