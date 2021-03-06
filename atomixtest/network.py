from errors import UnknownNetworkError
from utils import with_context
from ipaddress import IPv4Network, IPv4Interface
from logging import logger
import random
import docker
from docker.api.client import APIClient
from docker.utils import kwargs_from_env
from six.moves import shlex_quote

class Network(object):
    """Atomix test network."""
    def __init__(self, name, process_id=None):
        self.name = name
        self.process_id = process_id
        self._docker_client = docker.from_env()
        self._docker_api_client = APIClient(kwargs_from_env())
        self._hosts = None

    @property
    def docker_network(self):
        try:
            return self._docker_client.networks.get(self.name)
        except docker.errors.NotFound:
            raise UnknownNetworkError(self.name)

    @property
    def subnet(self):
        return str(self._docker_api_client.inspect_network(self.name)['IPAM']['Config'][0]['Subnet'])

    @property
    def gateway(self):
        return str(self._docker_api_client.inspect_network(self.name)['IPAM']['Config'][0]['Gateway'])

    @property
    def hosts(self):
        if self._hosts is None:
            self._hosts = self._create_hosts_iterator()
        return self._hosts

    def _create_hosts_iterator(self):
        """Creates a host iterator from available hosts by inspecting existing containers attached to the network."""
        ips = set([self.gateway] + [str(IPv4Interface(container['IPv4Address']).ip) for container in self._docker_api_client.inspect_network(self.name)['Containers'].values()])
        for host in IPv4Network(unicode(self.subnet)).hosts():
            host = str(host)
            if host not in ips:
                yield host

    def setup(self, supernet='172.18.0.0/16', subnet=None, gateway=None):
        """Sets up the network."""
        def find_subnet():
            docker_subnets = []
            for network in self._docker_client.networks.list():
                network_info = self._docker_api_client.inspect_network(network.name)
                if len(network_info['IPAM']['Config']) > 0:
                    docker_subnets.append(str(network_info['IPAM']['Config'][0]['Subnet']))
            for subnet in IPv4Network(unicode(supernet)).subnets(new_prefix=24):
                if str(subnet) not in docker_subnets:
                    return str(subnet)
            raise UnknownNetworkError("Cannot find available subnet from supernet {}".format(supernet))

        if subnet is None:
            subnet = find_subnet()

        self._hosts = iter([str(host) for host in IPv4Network(unicode(subnet)).hosts()])
        if gateway is None:
            gateway = str(next(self._hosts))

        ipam_pool = docker.types.IPAMPool(
            subnet=subnet,
            gateway=gateway
        )
        ipam_config = docker.types.IPAMConfig(
            pool_configs=[ipam_pool]
        )
        logger.info("Creating network")
        labels = {'atomix-test': 'true', 'atomix-process': self.process_id or '', 'atomix-cluster': self.name}
        self._docker_client.networks.create(self.name, driver='bridge', ipam=ipam_config, labels=labels)

    def teardown(self):
        """Tears down the network."""
        logger.info("Removing network")
        try:
            self.docker_network.remove()
        except:
            pass

    def partition(self, local, remote=None):
        """Partitions the given local from the given remote using a bi-directional partition."""
        local, remote = self._get_node(local), self._get_node(remote)
        return self.bipartition(local, remote)

    def unipartition(self, local, remote=None):
        """Partitions the given local from the given remote."""
        local, remote = self._get_node(local), self._get_node(remote)
        if remote is not None:
            return self._partition(local, remote)
        else:
            disruptions = []
            for name, ip in self._interfaces():
                if name != local:
                    disruptions.append(self._partition(local, name))
            return with_context(*disruptions)

    def bipartition(self, node1, node2=None):
        """Creates a bi-directional partition between the two nodes."""
        node1, node2 = self._get_node(node1), self._get_node(node2)
        if node2 is not None:
            return with_context(self._partition(node1, node2), self._partition(node2, node1))
        else:
            disruptions = []
            for name, ip in self._interfaces():
                if name != node1:
                    disruptions.append(self.bipartition(node1, name))
            return with_context(*disruptions)

    def heal(self, local=None, remote=None):
        """Heals partitions."""
        local, remote = self._get_node(local), self._get_node(remote)
        if local is not None and remote is not None:
            self._heal(local, remote)
            self._heal(remote, local)
        elif local is not None:
            for name, ip in self._interfaces():
                if name != local:
                    self._heal(local, name)
                    self._heal(name, local)
        else:
            for name1, ip1 in self._interfaces():
                for name2, ip2 in self._interfaces():
                    if name1 != name2:
                        self._heal(name1, name2)
                        self._heal(name2, name1)

    def partition_halves(self):
        """Partitions the network into two halves."""
        disruptions = []
        ips = self._interfaces()
        for i in range(len(ips)):
            if i % 2 == 0:
                for j in range(len(ips)):
                    if i != j and j % 2 == 1:
                        disruptions.append(self.bipartition(ips[i][0], ips[j][0]))
        return with_context(*disruptions)

    def partition_random(self):
        """Partitions a random node."""
        node1 = self._random_interface()[0]
        node2 = self._random_interface()[0]
        while node1 == node2:
            node2 = self._random_interface()
        return with_context(self.bipartition(node1, node2))

    def partition_bridge(self, node=None):
        """Partitions a node as a bridge to two sides of a cluster."""
        if node is None:
            node = self._random_interface()[0]
        else:
            node = self._get_node(node)

        interfaces = self._interfaces()
        disruptions = []
        for i in range(len(interfaces)):
            if i % 2 == 0 and interfaces[i][0] != node:
                for j in range(len(interfaces)):
                    if i != j and j % 2 == 1 and interfaces[j][0] != node:
                        disruptions.append(self.bipartition(interfaces[i][0], interfaces[j][0]))
        return with_context(*disruptions)

    def partition_isolate(self, node=None):
        """Isolates the given node from all its peers."""
        if node is None:
            node = self._random_interface()[0]
        else:
            node = self._get_node(node)

        disruptions = []
        for name, ip in self._interfaces():
            if name != node:
                disruptions.append(self.bipartition(node, name))
        return with_context(*disruptions)

    def delay(self, node=None, latency=50, jitter=10, correlation=.75, distribution='normal'):
        """Delays packets to the given node."""
        if node is None:
            return with_context(*[self._delay(name, latency, jitter, correlation, distribution) for name, ip in self._interfaces()])
        return self._delay(self._get_node(node), latency, jitter, correlation, distribution)

    def drop(self, node=None, probability=.02, correlation=.25):
        """Drops packets to the given node."""
        if node is None:
            return with_context(*[self._drop(name, probability, correlation) for name, ip in self._interfaces()])
        return self._drop(self._get_node(node), probability, correlation)

    def reorder(self, node=None, probability=.02, correlation=.5):
        """Reorders packets to the given node."""
        if node is None:
            return with_context(*[self._reorder(name, probability, correlation) for name, ip in self._interfaces()])
        return self._reorder(self._get_node(node), probability, correlation)

    def duplicate(self, node=None, probability=.005, correlation=.05):
        """Duplicates packets to the given node."""
        if node is None:
            return with_context(*[self._duplicate(name, probability, correlation) for name, ip in self._interfaces()])
        return self._duplicate(self._get_node(node), probability, correlation)

    def corrupt(self, node=None, probability=.02):
        """Duplicates packets to the given node."""
        if node is None:
            return with_context(*[self._corrupt(name, probability) for name, ip in self._interfaces()])
        return self._corrupt(self._get_node(node), probability)

    def restore(self, node=None):
        """Restores packets to the given node to normal order."""
        if node is None:
            for name, ip in self._interfaces():
                self._restore(name)
        else:
            self._restore(self._get_node(node))

    def _partition(self, local, remote):
        """Partitions the given local from the given remote."""
        logger.info("Cutting off link %s->%s", local, remote)
        self._run_in_container(local, 'iptables', '-A', 'INPUT', '-s', self._get_ip(remote), '-j', 'DROP', '-w')
        return with_context(lambda: self.heal(local, remote))

    def _heal(self, local, remote):
        """Heals a partition from the given local to the given remote."""
        logger.info("Restoring link %s->%s", local, remote)
        self._run_in_container(local, 'iptables', '-D', 'INPUT', '-s', self._get_ip(remote), '-j', 'DROP', '-w')

    def _delay(self, node, latency=50, jitter=10, correlation=.75, distribution='normal'):
        """Delays packets to the given node."""
        latency, jitter, correlation = self._millize(latency), self._millize(jitter), self._percentize(correlation)
        logger.info("Delaying packets to %s (latency=%s, jitter=%s, correlation=%s, distribution=%s)", node, latency, jitter, correlation, distribution)
        self._run_in_container(node, 'tc', 'qdisc', 'add', 'dev', 'eth0', 'root', 'netem', 'delay', latency, jitter, correlation, 'distribution', distribution)
        return with_context(lambda: self.restore(node))

    def _drop(self, node, probability=.02, correlation=.25):
        """Drops packets to the given node."""
        probability, correlation = self._percentize(probability), self._percentize(correlation)
        logger.info("Dropping packets to %s (probability=%s, correlation=%s)", node, probability, correlation)
        self._run_in_container(node, 'tc', 'qdisc', 'add', 'dev', 'eth0', 'root', 'netem', 'loss', probability, correlation)
        return with_context(lambda: self.restore(node))

    def _reorder(self, node, probability=.02, correlation=.5):
        """Reorders packets to the given node."""
        probability, correlation = self._percentize(probability), self._percentize(correlation)
        logger.info("Reordering packets to %s (probability=%s, correlation=%s)", node, probability, correlation)
        self._run_in_container(node, 'tc', 'qdisc', 'add', 'dev', 'eth0', 'root', 'netem', 'reorder', probability, correlation)
        return with_context(lambda: self.restore(node))

    def _duplicate(self, node, probability=.005, correlation=.05):
        """Duplicates packets to the given node."""
        probability, correlation = self._percentize(probability), self._percentize(correlation)
        logger.info("Duplicating packets to %s (probability=%s, correlation=%s)", node, probability, correlation)
        self._run_in_container(node, 'tc', 'qdisc', 'add', 'dev', 'eth0', 'root', 'netem', 'duplicate', probability, correlation)
        return with_context(lambda: self.restore(node))

    def _corrupt(self, node, probability=.02):
        """Duplicates packets to the given node."""
        probability = self._percentize(probability)
        logger.info("Corrupting packets to %s (probability=%s)", node, probability)
        self._run_in_container(node, 'tc', 'qdisc', 'add', 'dev', 'eth0', 'root', 'netem', 'corrupt', probability)
        return with_context(lambda: self.restore(node))

    def _restore(self, node):
        """Restores packets to the given node to normal order."""
        logger.info("Restoring packets to %s", node)
        self._run_in_container(node, 'tc', 'qdisc', 'del', 'dev', 'eth0', 'root')

    def _run_in_container(self, node, *command):
        command = ' '.join([shlex_quote(str(arg)) for arg in command])
        self._get_container(node).exec_run(command)

    def _percentize(self, d, digits=2):
        return '{}%'.format(round(d * 100, digits))

    def _millize(self, ms):
        return '{}ms'.format(ms)

    def _interfaces(self):
        """Lists the IPs used in the network."""
        containers = self._docker_api_client.inspect_network(self.name)['Containers']
        return sorted([(container['Name'], str(IPv4Interface(container['IPv4Address']).ip)) for container in containers.values()], key=lambda x: x[0])

    def _random_interface(self):
        """Returns a random interface name, ip pair."""
        return random.choice(self._interfaces())

    def _get_container(self, name):
        return self._docker_client.containers.get(name)

    def _get_ip(self, name):
        return dict(self._interfaces())[name]

    def _get_node(self, name):
        if name is None:
            return None
        try:
            return '{}-{}'.format(self.name, int(name))
        except ValueError:
            return name

def get_networks(process_id=None):
    docker_client = docker.from_env()
    docker_api_client = APIClient(kwargs_from_env())
    networks = docker_client.networks.list(filters={'label': 'atomix-test=true'})
    nets = set()
    for network in networks:
        if process_id is None or process_id == docker_api_client.inspect_network(network.name)['Labels']['atomix-process']:
            network_name = docker_api_client.inspect_network(network.name)['Labels']['atomix-cluster']
            nets.add(network_name)
    return [Network(name) for name in nets]
