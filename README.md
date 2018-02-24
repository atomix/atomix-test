# Atomix test framework

This project is a framework for deploying Docker-based [Atomix](http://github.com/atomix/atomix)
clusters for testing and running systems tests. The test framework supports node/network
fault injection, deploying and scaling Atomix clusters, and operating on all types of
primitives.

### Requirements

The test framework requires that the Docker engine be running. It uses
[docker-py](http://github.com/docker/docker-py) to communicate with the Docker engine
to create networks and containers for test clusters.

Additionally, you should build the Atomix Docker container with `docker build -t atomix .`

### Setup

To install the test framework, run `python setup.py install`

## Managing test clusters

The `atomix-test` script can be used to manage Atomix clusters for use in testing.

### Create a new test cluster

To create a new test cluster, use the `setup` command:

```
> atomix-test setup my-cluster
my-cluster Setting up cluster
my-cluster Creating network
my-cluster Running container my-cluster-1
my-cluster Running container my-cluster-2
my-cluster Running container my-cluster-3
my-cluster Waiting for cluster bootstrap
```

To configure the cluster with more than three nodes, pass a `--nodes` or `-n` argument:

```
> atomix-test setup my-cluster -n 5
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

To get a list of the currently running test clusters, use the `clusters` command:

```
> atomix-test clusters
my-cluster
my-other-cluster
```

To list detailed information about a cluster, use the `cluster-info` command:

```
> atomix-test cluster-info my-cluster
cluster: my-cluster
network:
  name: my-cluster
  subnet: 172.18.0.0/24
  gateway: 172.18.0.1
nodes:
  my-cluster-1:
    state: running
    type: server
    ip: 172.18.0.2
    host port: 50217
  my-cluster-2:
    state: running
    type: server
    ip: 172.18.0.3
    host port: 50218
  my-cluster-3:
    state: running
    type: server
    ip: 172.18.0.4
    host port: 50219
```

The Docker engine is treated as the source of truth for test cluster info. The `atomix-test`
script inspects running Docker containers and networks to determine which test clusters are
running.

### Resize a cluster

To add a node to a cluster, use the `add-node` command:

```
> atomix-test add-node my-cluster
```

To remove a node from a cluster, use the `remove-node` command, passing the node ID as the last argument:

```
> atomix-test remove-node my-cluster-4
```

### Shutdown a cluster

To shutdown a cluster, use the `teardown` command:

```
> atomix-test teardown my-cluster
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
primarily on simple `assert` statements. Additionally, the test framework provides a
`@with_cluster` decorator which can be used to instantiate per-test clusters:

```python
from atomixtest import with_cluster

@with_cluster(nodes=3)
def test_map(cluster):
  pass
```

The `Cluster` object provided to tests has APIs for managing and accessing the test
network, cluster, and nodes.

### Managing clusters

The same test cluster management functions are available via the `Cluster` object:

```python
@with_cluster(nodes=3)
def test_resize(cluster):
  # Add a node to the cluster
  cluster.add_node()

  # Add a client node to the cluster
  cluster.add_node(type='client')

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

The `Network` and `Node` objects have various methods for injecting network and node
failures in tests.

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

  # Inject latency on a specific node
  cluster.node(1).delay(latency=100)
```

### Accessing primitives

### Running tests

To run tests, use the `run` command:

```
> atomix-test run
```

The `run` command can take any arguments supported by [pytest](https://docs.pytest.org/en/latest/)
