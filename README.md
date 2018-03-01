# Atomix test framework

This project is a framework for deploying Docker-based [Atomix](http://github.com/atomix/atomix)
clusters for systems/fault injection/load testing. The test framework supports:
* Setup Docker-based test clusters
* Teardown Docker-based test clusters
* Add client/server nodes
* Remove nodes
* Kill/restart nodes
* Create network partitions
* Inject network latency
* Generate load on nodes (CPU, I/O, memory, etc)

### Requirements

The test framework requires that the Docker engine be running. It uses
[docker-py](http://github.com/docker/docker-py) to communicate with the Docker engine
to create networks and containers for test clusters.

Additionally, you should build the Atomix Docker container with `docker build -t atomix .`

### Setup

To install the test framework, run `python setup.py install`

To run tests, run `python setup.py test`

## Managing test clusters

The `atomix-test` script can be used to manage Atomix clusters for use in testing.
To view a list of cluster management commands, run `atomix-test cluster -h`

### Create a new test cluster

To create a new test cluster, use the `setup` command:

```
> atomix-test cluster my-cluster setup
my-cluster Setting up cluster
my-cluster Creating network
my-cluster Running container my-cluster-1
my-cluster Running container my-cluster-2
my-cluster Running container my-cluster-3
my-cluster Waiting for cluster bootstrap
```

To configure the cluster with more than three nodes, pass a `--nodes` or `-n` argument:

```
> atomix-test cluster my-cluster setup -n 5
```

Running the `setup` command will set up a new Docker network and a set of containers.
The network will have the same name as the cluster and the container names will be
prefixed with the cluster name as visible via the Docker engine:

```
> docker network ls
NETWORK ID          NAME                DRIVER              SCOPE
dd03f9e3a669        my-cluster          bridge              local
```

```
> docker ps
CONTAINER ID        IMAGE               COMMAND                  CREATED              STATUS              PORTS                               NAMES
719a14f535a3        atomix              "java -jar atomix-..."   About a minute ago   Up 2 minutes        5679/tcp, 0.0.0.0:50219->5678/tcp   my-cluster-3
55d6f3188a88        atomix              "java -jar atomix-..."   About a minute ago   Up 2 minutes        5679/tcp, 0.0.0.0:50218->5678/tcp   my-cluster-2
6b2d6569c503        atomix              "java -jar atomix-..."   About a minute ago   Up 2 minutes        5679/tcp, 0.0.0.0:50217->5678/tcp   my-cluster-1
```

### List cluster info

To get a list of the currently running test clusters, use the `list` command:

```
> atomix-test cluster list
my-cluster
my-other-cluster
```

To list the nodes in a cluster, use the `nodes` command:

```
> atomix-test cluster my-cluster nodes
ID     NAME             STATUS      IP             LOCAL PORT
1      my-cluster-1     running     172.18.0.2     61170
2      my-cluster-2     running     172.18.0.3     61171
3      my-cluster-3     running     172.18.0.4     61172
```

The port listed under `LOCAL PORT` is the `localhost` port on which the Atomix HTTP
server is running.

The Docker engine is treated as the source of truth for test cluster info. The `atomix-test`
script inspects running Docker containers and networks to determine which test clusters are
running.

### Resize a cluster

To add a node to a cluster, use the `add-node` command:

```
> atomix-test cluster my-cluster add-node
```

To remove a node from a cluster, use the `remove-node` command, passing the node ID as the last argument:

```
> atomix-test cluster my-cluster remove-node 4
```

### Shutdown a cluster

To shutdown a cluster, use the `teardown` command:

```
> atomix-test cluster my-cluster teardown
my-cluster Tearing down cluster
my-cluster Stopping container my-cluster-1
my-cluster Removing container my-cluster-1
my-cluster Stopping container my-cluster-2
my-cluster Removing container my-cluster-2
my-cluster Stopping container my-cluster-3
my-cluster Removing container my-cluster-3
my-cluster Removing network
my-cluster Cleaning up cluster state
```

## Writing tests

Tests are written and run using [pytest](https://docs.pytest.org/en/latest/), which relies
primarily on simple `assert` statements.

To create a cluster in a test, use the `create_cluster` function:

```python
from atomixtest.cluster import create_cluster

with create_cluster('test', nodes=3) as cluster:
  node = cluster.node(1)
  ...
```

Alternatively, the test framework provides a `@with_cluster` decorator which can be used
to inject per-test clusters:

```python
from atomixtest import with_cluster

@with_cluster(nodes=3)
def test_map(cluster):
  node = cluster.node(1)
  ...
```

By default, a cluster with the same name as the test function - e.g. `test_map` - will be
started prior to running the test method and will be torn down afterwards. The `Cluster`
object provided to tests has APIs for managing and accessing the test network, cluster,
and nodes.

### Managing clusters

The same test cluster management functions are available via the `Cluster` object:

```python
@with_cluster(nodes=3)
def test_resize(cluster):
  # Add a node to the cluster
  cluster.add_node()

  # Add a client node to the cluster
  cluster.add_node(type='client')

  # Remove a node from the cluster
  cluster.remove_node(4)
```

### Accessing primitives

Nodes can be accessed via the `Cluster` API:

```python
node = cluster.node(1)
```

Each node is also an Atomix REST client from which primitive instances can be created:

```python
@with_cluster(nodes=3)
def test_map(cluster):
  node = cluster.add_node(type='client')
  map = node.map('test-map')
  assert map.get('foo') is None
  assert map.put('foo', 'Hello world!') is None
  assert map.get('foo')['value'] == 'Hello world!'
```

### Fault injection

The `Cluster`, `Network` and `Node` objects have various methods for injecting network
and node failures in tests.

```python
@with_cluster(nodes=3)
def test_map(cluster):
  # Kill a node
  cluster.node(1).kill()

  # Restart the node
  cluster.node(1).start()

  # Partition a node from all other nodes
  cluster.node(1).isolate()

  # Heal the isolated node
  cluster.node(1).unisolate()

  # Partition one node from another node
  cluster.node(1).partition(cluster.node(2))

  # Inject latency in the network and attempt to write to a map
  with cluster.network.delay(latency=100):
    cluster.node(2).map('test-map').put('bar', 'baz')
```

Fault injection methods also support a context manager that can be used to encapsulate
blocks of code to be executed during the failure.

```python
@with_cluster(nodes=3)
def test_map(cluster):
  node1 = cluster.node(1)
  node2 = cluster.node(2)

  # Test writing to a map with the cluster under load.
  with cluster.stress(cpu=4):
    node1.map('test-map').put('foo', 'bar')
  
  # Test that a map can be read from node 2 while node 1 is partitioned.
  with node1.isolate():
    assert node2.map('test-map').get('foo')['value'] == 'bar'
```

### Cluster

The `Cluster` object supports the following properties and methods:
* `path` - the host path in which Atomix data is persisted
* `network` - the `Network` used to communicate across nodes in the cluster
* `node(id)` - returns a `Node` object by `int` ID or `str` name
* `nodes(type=None)` - returns a list of all nodes in the cluster of the given `type`
* `servers()` - returns a list of all `server` type nodes in the cluster
* `clients()` - returns a list of all `client` type nodes in the cluster
* `setup(nodes=3, supernet='172.18.0.0/16', subnet=None, gateway=None, cpu=None, memory=None)` - sets up a new cluster
* `add_node(type='server')` - adds a new node to the cluster
* `remove_node(id)` - removes a node by `int` ID or `str` name
* `teardown()` - tears down the cluster
* `stress(node=None, timeout=None, cpu=None, io=None, memory=None, hdd=None)` - stresses all nodes in the cluster
* `destress()` - kills stressors on all nodes in the cluster

### Network

The `Network` object can be accessed via `Cluster.network` and supports the following
properties and methods:
* `setup(supernet='172.18.0.0/16', subnet=None, gateway=None)` - sets up the network,
creating a new Docker network with a subnet in the given `supernet`
* `teardown()` - tears down the network, removing the Docker network
* `partition(local, remote=None)` - partitions the given `local` node from the given `remote`
node or from all nodes in the network if `remote` is `None`. `local` and `remote` must be
node names or IDs.
* `unipartition(local, remote=None)` - creates a uni-directional partition from the given
`local` node to the given `remote` node or to all nodes in the network if `remote`
is `None`. `local` and `remote` must be node names or IDs.
* `bipartition(local, remote=None)` - partitions the given `local` node from the given `remote`
node or from all nodes in the network if `remote` is `None`. `local` and `remote` must be
node names or IDs.
* `heal(local=None, remote=None)` - heals a partition from the given `local` node to the given
`remote` node. If the `remote` node is `None`, all partitions from the given `local` are removed.
If `local` is `None` then all partitions in the cluster are healed. `local` and `remote` must be
node names or IDs.
* `partition_halves()` - partitions the cluster into two halves using bi-directional partitions
* `partition_random()` - partitions a random node from all other nodes in the cluster
* `partition_bridge(node=None)` - partitions the cluster into two halves with a single "bridge"
node able to see each side of the partition. If the given `node` is `None` then the bridge
node will be selected randomly.
* `partition_isolate(node=None)` - partitions a node from all other nodes in the cluster.
If the given node is `None` then a random node will be selected.
* `delay(node=None, latency=50, jitter=10, correlation=.75, distribution='normal')` - delays
packets to the given node or to all nodes if `node` is `None`
* `drop(node=None, probability=.02, correlation=.25)` - drops packets to the given node
or to all nodes if `node` is `None`
* `reorder(node=None, probability=.02, correlation=.5)` - reorders packets to the given node
or to all nodes if `node` is `None`
* `duplicate(node=None, probability=.005, correlation=.05)` - delays packets to the given
node or to all nodes if `node` is `None`
* `corrupt(node=None, probability=.02)` - delays packets to the given node or to all nodes
if `node` is `None`
* `restore(node=None)` - restores all traffic to the given node or to all nodes if
`node` is `None`

### Node

The `Node` object can be accessed via `Cluster.node(id)` and supports the following
properties and methods:
* `id` - the `int` ID of the node
* `status` - the `str` status of the node provided by the underlying Docker container
* `local_port` - the `localhost` port on which the Atomix HTTP server is listening
* `logs()` - returns the Atomix logs for the node
* `setup(cpu=None, memory=None)` - sets up the node, passing the `cpu` and `memory`
to Docker at setup
* `teardown()` - tears down the node, removing the Docker container
* `run(*command)` - runs a command in the Docker container
* `execute(*command)` - runs a command in the Docker container, detaching from the shell
* `stop()` - stops the Docker container using `docker stop`
* `start()` - starts the Docker container using `docker start`
* `kill()` - kills the Docker container using `docker kill`
* `recover()` - restarts a dead Docker container using `docker start`
* `restart()` - restarts the Docker container using `docker restart`
* `partition(node)` - partitions the node from the given `node` using a bi-directional partition
* `heal(node)` - heals a partition from this node to the given `node`
* `isolate()` - isolates this node from all other nodes in the cluster
* `unisolate()` - heals an isolation from all other nodes in the cluster
* `delay(latency=50, jitter=10, correlation=.75, distribution='normal')` - delays packets
to the node with the given `latency` in millseconds
* `drop(probability=.02, correlation=.25)` - drops packets to the node with the given
`probability`
* `reorder(probability=.02, correlation=.5)` - reorders packets to the node with the given
`probability`
* `duplicate(probability=.005, correlation=.05)` - duplicates packets to the node with
the given `probability`
* `corrupt(probability=.02)` - corrupts packets to the node with the given `probability`
* `restore(self)`
* `stress(timeout=None, cpu=None, io=None, memory=None, hdd=None)`
* `destress()`

### Running tests

To run tests, use the `run` command:

```
> atomix-test run
```

The `run` command can take any arguments supported by [pytest](https://docs.pytest.org/en/latest/)
