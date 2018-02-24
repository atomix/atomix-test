from logger import Logger
from errors import UnknownNetworkError
from ipaddress import IPv4Network, IPv4Interface
import docker
from docker.api.client import APIClient
from docker.utils import kwargs_from_env

class Network(object):
    """Atomix test network."""
    def __init__(self, name):
        self.log = Logger(name, Logger.Type.FRAMEWORK)
        self.name = name
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
        self.log.message("Creating network")
        self._docker_client.networks.create(self.name, driver='bridge', ipam=ipam_config)

    def teardown(self):
        """Tears down the network."""
        self.log.message("Removing network")
        self.docker_network.remove()

    def _percentize(self, d, digits=2):
        return round(d * 100, digits) + '%'

    def _millize(self, ms):
        return ms + 'ms'

    def partition(self, local, remote):
        """Partitions the given local from the given remote."""
        self.log.message("Cutting off link {}->{}", local.name, remote.name)
        local.run('/bin/bash', 'sudo', 'iptables', '-A', 'INPUT', '-s', remote.ip, '-j', 'DROP', '-w')

    def heal(self, local, remote):
        """Heals a partition from the given local to the given remote."""
        self.log.message("Restoring link {}->{}", local.name, remote.name)
        local.run('/bin/bash', 'sudo', 'iptables', '-D', 'INPUT', '-s', remote.ip, '-j', 'DROP', '-w')

    def isolate(self, node):
        """Isolates the given node from all its peers."""
        self.log.message("Isolating node {}", node.name)
        for n in node.cluster.servers():
            if n.name != node.name:
                self.partition(node, n)
                self.partition(n, node)

    def unisolate(self, node):
        """Unisolates the given node from all its peers."""
        self.log.message("Healing node {}", node.name)
        for n in node.cluster.servers():
            if n.name != node.name:
                self.heal(node, n)
                self.heal(n, node)

    def delay(self, node, latency=50, jitter=10, correlation=.75, distribution='normal'):
        """Delays packets to the given node."""
        correlation = self._percentize(correlation)
        self.log.message("Delaying packets to {} (latency={}, jitter={}, correlation={}, distribution={})", node.name, self._millize(latency), self._millize(jitter), correlation, distribution)
        node.run('/bin/bash', 'sudo', 'tc', 'qdisc', 'add', 'dev', 'eth0', 'root', 'netem', 'delay', latency, jitter, correlation, 'distribution', distribution)

    def drop(self, node, probability=.02, correlation=.25):
        """Drops packets to the given node."""
        probability, correlation = self._percentize(probability), self._percentize(correlation)
        self.log.message("Dropping packets to {} (probability={}, correlation={})", node.name, probability, correlation)
        node.run('/bin/bash', 'sudo', 'tc', 'qdisc', 'add', 'dev', 'eth0', 'root', 'netem', 'loss', probability, correlation)

    def reorder(self, node, probability=.02, correlation=.5):
        """Reorders packets to the given node."""
        probability, correlation = self._percentize(probability), self._percentize(correlation)
        self.log.message("Reordering packets to {} (probability={}, correlation={})", node.name, probability, correlation)
        node.run('/bin/bash', 'sudo', 'tc', 'qdisc', 'add', 'dev', 'eth0', 'root', 'netem', 'reorder', probability, correlation)

    def duplicate(self, node, probability=.005, correlation=.05):
        """Duplicates packets to the given node."""
        probability, correlation = self._percentize(probability), self._percentize(correlation)
        self.log.message("Duplicating packets to {} (probability={}, correlation={})", node.name, probability, correlation)
        node.run('/bin/bash', 'sudo', 'tc', 'qdisc', 'add', 'dev', 'eth0', 'root', 'netem', 'duplicate', probability, correlation)

    def corrupt(self, node, probability=.02):
        """Duplicates packets to the given node."""
        probability = self._percentize(probability)
        self.log.message("Corrupting packets to {} (probability={})", node.name, probability)
        node.run('/bin/bash', 'sudo', 'tc', 'qdisc', 'add', 'dev', 'eth0', 'root', 'netem', 'corrupt', probability)

    def restore(self, node):
        """Restores packets to the given node to normal order."""
        self.log.message("Restoring packets to {}", node.name)
        node.run('/bin/bash', 'sudo', 'tc', 'qdisc', 'del', 'dev', 'eth0', 'root')
