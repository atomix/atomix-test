from cluster import Cluster
import random
import uuid
import re
import sys
import threading
import time
import json
from logging import logger
from datetime import timedelta
from collections import OrderedDict
from abc import ABCMeta, abstractmethod

def _generate_test_name():
    """Generates a unique test name."""
    return "entropy-test-" + str(uuid.uuid4())


def _parse_time(run_time):
    regex = re.compile(r'((?P<hours>\d+?)h)?((?P<minutes>\d+?)m)?((?P<seconds>\d+?)s)?((?P<milliseconds>\d+?)ms)?')
    parts = regex.match(run_time.lower())
    if not parts:
        return
    parts = parts.groupdict()
    time_params = {}
    for (name, param) in parts.iteritems():
        if param:
            time_params[name] = int(param)
    return timedelta(**time_params).total_seconds()

def run(
        name=None,
        nodes=3,
        configs=(),
        version='latest',
        processes=8,
        scale=1000,
        prime=0,
        operation_count=0,
        operation_delay=(1, 5),
        run_time='1m',
        functions=(),
        function_delay=(15, 30)
):
    """Runs the linearizability test."""

    if name is None:
        name = _generate_test_name()

    # Initialize the test cluster.
    cluster = _init_test_cluster(name, nodes, configs, version)

    # Create a history object with which to track history
    history = History()
    controller = Controller(cluster, functions, function_delay, history)
    primer = Primer(0, name, operation_count, operation_delay, scale, history, cluster, prime)
    processes = [Process(i+1, name, operation_count, operation_delay, scale, history, _parse_time(run_time), random.choice(cluster.nodes())) for i in range(processes)]

    # Start the test.
    _start_test(primer, controller, processes)

    # Run the controller and processes until complete.
    _block_until_complete(controller, processes)

    # Shuts down the test cluster.
    _teardown_test_cluster(cluster, history)


def _init_test_cluster(name, nodes=3, configs=(), version='latest'):
    """Initializes a test cluster."""
    cluster = Cluster(name)
    cluster.setup(*configs, nodes=nodes, version=version)
    return cluster


def _teardown_test_cluster(cluster, history):
    """Shuts down the test cluster."""
    if history.count('fail') > 0:
        cluster.shutdown()
    else:
        cluster.teardown()


def _start_test(primer, controller, processes):
    """Starts the test threads."""
    primer.run()
    for process in processes:
        process.start()
    controller.start()


def _block_until_complete(controller, processes):
    """Runs the given controller and processes until complete."""
    while True:
        # If any process is still running, sleep and then continue to the next iteration of the loop.
        if len([process for process in processes if process.is_running()]) == 0:
            # Once all processes have completed, stop the controller.
            controller.stop()

        # Wait for the controller thread to complete to ensure partitions are healed and crashed nodes are recovered.
        if not controller.is_running():
            break

        # If we haven't broken out of the loop by now, sleep and then check again.
        time.sleep(1)


class History(object):
    """Records and logs the history of operations.

    This object directly mimics the format expected by the Knossos linearizability checker. Events are logged in
    edn format, and str(history) will return the full history in edn format.
    """
    def __init__(self):
        self.entries = []

    def record(self, entry):
        """Records an entry in the history."""
        self.entries.append(entry)
        logger.info('{} {} {} ({})'.format(entry.process, entry.action, entry.operation, ', '.join([str(value) for value in entry.values])))

    def count(self, action):
        """Returns the number of entries for the given action."""
        return len([entry for entry in self.entries if entry.action == action])

    def __str__(self):
        return json.dumps([entry.format() for entry in self.entries])


class HistoryEntry(object):
    """History entry."""
    __metaclass__ = ABCMeta

    def format(self):
        return OrderedDict([
            ('process', self.process),
            ('type', self.action),
            ('function', self.operation),
            ('value', list(self.values))
        ])

    def __str__(self):
        return json.dumps(self.format())


class ProcessEntry(HistoryEntry):
    """Process entry."""
    def __init__(self, process, action, operation, *values):
        self.process = process
        self.action = action
        self.operation = operation
        self.values = values


class ControllerEntry(HistoryEntry):
    """Controller history entry."""
    def __init__(self, event, message):
        self.process = 'controller'
        self.action = 'info'
        self.operation = event
        self.values = (message,)
        self.event = event
        self.message = message


class Runnable(object):
    """Base class for managing the lifecycle of a threaded test process."""
    __metaclass__ = ABCMeta

    def __init__(self):
        self.thread = None
        self.running = False

    def start(self):
        """Starts the runnable thread."""
        self.thread = threading.Thread(target=self.run)
        self.thread.daemon = True
        self.running = True
        self.thread.start()

    @abstractmethod
    def run(self):
        """Runs the thread. This method should be overridden by implementors."""

    def is_running(self):
        """Returns a boolean indicating whether the runnable is running."""
        return self.running or self.thread.is_alive()

    def stop(self):
        """Stops the runnable thread.

        Calling this method will not immediately stop the thread. Instead, a flag will be set, and the run() method
        is expected to exit according to the 'running' flag. Use 'is_running()' to determine whether the thread is
        stopped and has exited.
        """
        self.running = False


class Operator(Runnable):
    """Base class for runnables that operate on the cluster state."""
    def __init__(self, id, name, operation_count, delay, scale, history):
        super(Operator, self).__init__()
        self.id = id
        self.name = name
        self.operation_count = operation_count
        self.delay = delay
        self._keys = [str(uuid.uuid4()) for _ in range(scale)]
        self.history = history
        self.operations = (self.read, self.write, self.delete)

    def _run(self):
        """Runs a random operation."""
        return random.choice(self.operations)()

    def _random_key(self):
        """Returns a random key to get or set."""
        return random.choice(self._keys)

    def _random_value(self):
        """Returns the next random value to set."""
        return random.randint(1, 10)

    def _log(self, action, operation, *values):
        """Logs an operation."""
        self.history.record(ProcessEntry(self.id, action, operation, *values))

    def _invoke(self, operation, *values):
        """Logs an operation invocation event in the process history."""
        self._log('invoke', operation, *values)

    def _ok(self, operation, *values):
        """Logs an operation success event in the process history."""
        self._log('ok', operation, *values)
        return True

    def _fail(self, operation, *values):
        """Logs an operation failure event in the process history."""
        self._log('fail', operation, *values)
        return True

    def _info(self, operation, *values):
        """Logs an operation info event in the process history and stops the process."""
        self._log('info', operation, *values)
        self.stop()
        return False

    def read(self):
        """Executes a read operation."""
        key = self._random_key()
        self._invoke('read', key)
        try:
            return self._ok('read', key, self.node.map(self.name).get(key))
        except:
            return self._info('read', key)

    def write(self):
        """Executes a write operation."""
        key, value = self._random_key(), self._random_value()
        self._invoke('write', key, value)
        try:
            self.node.map(self.name).put(key, value)
            return self._ok('write', key, value)
        except:
            return self._info('write', key, value)

    def delete(self):
        """Executes a delete operation."""
        key = self._random_key()
        self._invoke('delete', key)
        try:
            self.node.map(self.name).remove(key)
            return self._ok('delete', key)
        except:
            return self._info('delete', key)


class Primer(Operator):
    def __init__(self, id, name, operation_count, delay, scale, history, cluster, prime=0):
        super(Primer, self).__init__(id, name, operation_count, delay, scale, history)
        self.cluster = cluster
        self.prime = prime

    def _random_node(self):
        """Returns a random node on which to perform an operation."""
        return random.choice(self.cluster.nodes())

    def _invoke(self, operation, *values):
        """Logs an operation invocation event in the process history."""

    def _ok(self, operation, *values):
        """Logs an operation success event in the process history."""
        return True

    def _fail(self, operation, *values):
        """Logs an operation failure event in the process history."""
        return True

    def run(self):
        """Runs the primer."""
        self._info('prime', self.prime)
        for i in range(self.prime):
            key, value = self._random_key(), self._random_value()
            self.node.map(self.name).put(key, value)


class Process(Operator):
    """Test runner for a single process.

    A process simulates operations from a single actor in the cluster. When the process is started, it will begin
    performing random read, write, or cas operations, sleeping for random intervals between operations. Each operation
    performed by the process will be logged in the History object provided to the constructor. The process runs for a
    predefined number of operations or until an operation fails with an unknown error (e.g. a timeout).
    """
    def __init__(self, id, name, operation_count, delay, scale, history, run_time, node):
        super(Process, self).__init__(id, name, operation_count, delay, scale, history)
        self.id = id
        self.name = name
        self.operation_count = operation_count
        self.delay = delay
        self._keys = [str(uuid.uuid4()) for _ in range(scale)]
        self.run_time = run_time
        self.node = node
        self.history = history
        self.operations = (self.read, self.write, self.delete)
        self.start_time = None

    def run(self):
        """Runs the process."""
        self.start_time = time.time()
        for _ in range(self.operation_count):
            self._wait()
            self._run()
            self._check_time()
            if not self.running:
                break
        while True:
            self._wait()
            if self.operation_count <= 0:
                self._run()
            self._check_time()
            if not self.running:
                break

    def _check_time(self):
        """Checks whether the run time has completed."""
        if time.time() - self.start_time > self.run_time:
            self.stop()

    def _wait(self):
        """Blocks for a uniform random delay according to the process configuration."""
        time.sleep(random.uniform(self.delay[0], self.delay[1]))


class Controller(Runnable):
    """Cluster controller.

    The controller periodically disrupts the cluster using a random disruptor function to e.g. partition the network,
    crash a node, or slow communication within the network. The disruptor guarantees that only one disruptor function
    will run at any given time and the previous disruptor will be healed prior to the next disruptor beginning.
    The disruptor sleeps for a uniform random interval between disruptor functions.
    """
    def __init__(self, cluster, functions, delay, history):
        super(Controller, self).__init__()
        self.cluster = cluster
        self.delay = delay
        self.history = history
        self.functions = []
        for func in functions:
            try:
                self.functions.append((getattr(self, func[0]), func[1:]))
            except AttributeError:
                print "Unknown entropy function %s" % (func[0],)
                sys.exit(1)

    def run(self):
        """Runs the controller until stopped."""
        while self.running:
            self._wait()
            if self.running:
                self._run()

    def _run(self):
        """Runs a random function."""
        function, args = random.choice(self.functions)
        function(*args)

    def _wait(self):
        """Waits for a uniform random delay."""
        time.sleep(random.uniform(self.delay[0], self.delay[1]))

    def _random_node(self):
        """Returns a random node on which to perform an operation."""
        return random.choice(self.cluster.nodes())

    def _log(self, event, message):
        """Logs an event in the function history."""
        self.history.record(ControllerEntry(event, message))

    def _start(self, message):
        """Logs a start event in the function history."""
        self._log('start', message)

    def _stop(self, message):
        """Logs a stop event in the function history."""
        self._log('stop', message)

    def _partition(self, node1, node2):
        """Partitions node1 from node2."""
        node1.partition(node2)
        node2.partition(node1)

    def _partition_halves(self):
        """Partitions the cluster into two halves."""
        nodes = self.cluster.nodes()
        for i in range(len(nodes)):
            for j in range(len(nodes)):
                if i != j and i % 2 == 0 and j % 2 == 1:
                    nodes[i].partition(nodes[j])
                    nodes[j].partition(nodes[i])

    def _partition_bridge(self, node):
        """Partitions the cluster with the given node as a bridge between two halves."""
        nodes = self.cluster.nodes()
        for i in range(len(nodes)):
            for j in range(len(nodes)):
                if i != j and nodes[i].name != node.name and nodes[j].name != node.name and i % 2 == 0 and j % 2 == 1:
                    nodes[i].partition(nodes[j])
                    nodes[j].partition(nodes[i])

    def _heal(self, node1=None, node2=None):
        """Heals a partition between two nodes or between all nodes if the given nodes are None."""
        if node1 is not None and node2 is not None:
            node1.heal(node2)
            node2.heal(node1)
        else:
            for node1 in self.cluster.nodes():
                for node2 in self.cluster.nodes():
                    if node1.name != node2.name:
                        node1.heal(node2)

    def _crash(self, node):
        """Crashes the given node."""
        node.kill()

    def _recover(self, node):
        """Recovers the given node from a crash."""
        node.recover()

    def _delay(self, node=None, latency=100):
        """Delays communication from all nodes or from the given node if specified."""
        if node is not None:
            node.delay(latency=latency)
        else:
            for node in self.cluster.nodes():
                node.delay(latency=latency)

    def _restore(self, node=None):
        """Restores communication on all nodes or on the given node if specified."""
        if node is not None:
            node.restore()
        else:
            for node in self.cluster.nodes():
                node.restore()

    def _shutdown(self):
        """Shuts down the entire cluster."""
        for node in self.cluster.nodes():
            node.kill()

    def _startup(self):
        """Starts up the entire cluster."""
        for node in self.cluster.nodes():
            node.start()
        for node in self.cluster.nodes():
            node.wait_for_start()

    def _stress_cpu(self, node=None, processes=1):
        if node is not None:
            node.stress(cpu=processes)
        else:
            for node in self.cluster.nodes():
                node.stress(cpu=processes)

    def _stress_io(self, node=None, processes=1):
        if node is not None:
            node.stress(io=processes)
        else:
            for node in self.cluster.nodes():
                node.stress(io=processes)

    def _stress_memory(self, node=None, processes=1):
        if node is not None:
            node.stress(memory=processes)
        else:
            for node in self.cluster.nodes():
                node.stress(memory=processes)

    def _destress(self, node=None):
        if node is not None:
            node.destress()
        else:
            for node in self.cluster.nodes():
                node.destress()

    def partition_random(self):
        """Partitions two random nodes from each other."""
        node1 = self._random_node()
        node2 = node1
        while node2 == node1:
            node2 = self._random_node()
        self._start("Cut off %s->%s" % (node1, node2))
        self._partition(node1, node2)
        self._wait()
        self._heal(node1, node2)
        self._stop("Fully connected")

    def partition_halves(self):
        """Partitions the cluster into two halves."""
        self._start("Partitioning network into two halves")
        self._partition_halves()
        self._wait()
        self._heal()
        self._stop("Fully connected")

    def partition_bridge(self):
        """Partitions the cluster into two halves with a bridge between them."""
        node = self._random_node()
        self._start("Partitioning network with bridge %s" % (node,))
        self._partition_bridge(node)
        self._wait()
        self._heal()
        self._stop("Fully connected")

    def crash_random(self):
        """Crashes a random node."""
        node = self._random_node()
        self._start("Crashing %s" % (node,))
        self._crash(node)
        self._wait()
        self._recover(node)
        self._stop("Recovered %s" % (node,))

    def delay(self, latency=100):
        """Delays messages on all nodes."""
        self._start("Delay communication on all nodes")
        self._delay(latency=latency)
        self._wait()
        self._restore()
        self._stop("Communication restored")

    def delay_random(self, latency='100ms'):
        """Delays communication on a random node."""
        node = self._random_node()
        self._start("Delay communication on %s" % (node,))
        self._delay(node, latency=_parse_time(latency))
        self._wait()
        self._restore(node)
        self._stop("Communication restored on %s" % (node,))

    def restart(self):
        """Restarts the entire cluster."""
        self._start("Restarting cluster")
        self._shutdown()
        self._wait()
        self._startup()
        self._stop("Cluster restarted")

    def stress_cpu(self, processes=1):
        self._start("Increase CPU usage on all nodes")
        self._stress_cpu(processes=processes)
        self._wait()
        self._destress()
        self._stop("CPU usage reduced on all nodes")

    def stress_io(self, processes=1):
        self._start("Increase I/O on all nodes")
        self._stress_io(processes=processes)
        self._wait()
        self._destress()
        self._stop("I/O reduced on all nodes")

    def stress_memory(self, processes=1):
        self._start("Increase memory usage on all nodes")
        self._stress_memory(processes=processes)
        self._wait()
        self._destress()
        self._stop("Memory usage reduced on all nodes")

    def stress_cpu_random(self, processes=1):
        node = self._random_node()
        self._start("Increase CPU usage on %s" % (node,))
        self._stress_cpu(node, processes)
        self._wait()
        self._destress(node)
        self._stop("CPU usage reduced on %s" % (node,))

    def stress_io_random(self, processes=1):
        node = self._random_node()
        self._start("Increase I/O on %s" % (node,))
        self._stress_io(node, processes)
        self._wait()
        self._destress(node)
        self._stop("I/O reduced on %s" % (node,))

    def stress_memory_random(self, processes=1):
        node = self._random_node()
        self._start("Increase memory usage on %s" % (node,))
        self._stress_memory(node, processes)
        self._wait()
        self._destress(node)
        self._stop("Memory usage reduced on %s" % (node,))
