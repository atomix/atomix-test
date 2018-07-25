from imp import find_module, load_module
from inspect import isfunction
import os, sys, traceback
from logging import set_logger, reset_logger
from cluster import Cluster, get_clusters
from network import get_networks
from errors import TestError
import time
from datetime import datetime
import uuid
from colorama import Fore, init, Style

init()

_test_runner = None


def _set_test_runner(runner):
    """Sets the current test runner."""
    global _test_runner
    _test_runner = runner


def _get_test_runner():
    return _test_runner


class _ConfiguredCluster(Cluster):
    def __init__(self, name, process_id=None, *args, **kwargs):
        super(_ConfiguredCluster, self).__init__(name, process_id)
        self._args = args
        self._kwargs = kwargs

    def setup(self):
        super(_ConfiguredCluster, self).setup(*self._args, **self._kwargs)

    def __enter__(self):
        self.setup()
        return self


def create_cluster(*args, **kwargs):
    """Creates a cluster within the context of the currently running test."""
    runner = _get_test_runner()
    if runner is None or not runner.is_running():
        raise TestError("No test is currently in progress")
    name = '{}-{}'.format(runner.current_test, datetime.now().strftime('%Y%m%d%H%M%S'))
    return _ConfiguredCluster(name, runner.test_id, *args, **kwargs)


class TestRunner(object):
    """Test runner."""
    def __init__(self):
        self.test_id = str(uuid.uuid4())
        self.current_test = None

    def is_running(self):
        """Returns a boolean indicating whether the test runner is currently running."""
        return self.current_test is not None

    def run(self, paths, fail_fast=False):
        """Runs the tests at the given paths."""
        _set_test_runner(self)
        try:
            return self._run_paths(paths, fail_fast)
        finally:
            _set_test_runner(None)

    def _run_paths(self, paths, fail_fast=False):
        return_code = 0
        for path in paths:
            path_code = self._run_path(path, fail_fast)
            if path_code != 0:
                if return_code == 0:
                    return_code = path_code
                if fail_fast:
                    return return_code
        return return_code

    def _run_path(self, path, fail_fast=False):
        return_code = 0
        for test in self._find_tests(path):
            start = time.time()
            self.current_test = test.__name__
            self._print_test(self.current_test)
            try:
                set_logger(self.current_test)
                test()
            except KeyboardInterrupt, e:
                self._print_failure("{} cancelled".format(self.current_test))
                raise e
            except:
                end = time.time()
                self._print_failure("{} failed in {} seconds".format(self.current_test, round(end - start, 4)))
                traceback.print_exc(file=sys.stdout)
                if fail_fast:
                    return 1
                else:
                    return_code = 1
            else:
                end = time.time()
                self._print_success("{} passed in {} seconds".format(self.current_test, round(end - start, 4)))
            finally:
                reset_logger()
                self.current_test = None
        return return_code

    def _print_test(self, name):
        text = "Running {}".format(name)
        print ''.join(['-' for _ in range(len(text))])
        print text
        print ''.join(['-' for _ in range(len(text))])

    def _print_success(self, message):
        print Fore.GREEN + message + Style.RESET_ALL

    def _print_failure(self, message):
        print Fore.RED + message + Style.RESET_ALL

    def cleanup(self):
        """Cleans up the test run."""
        for cluster in get_clusters(self.test_id):
            try:
                cluster.teardown()
                cluster.cleanup()
            except:
                pass

        for network in get_networks(self.test_id):
            try:
                network.teardown()
            except:
                pass

    def _find_tests(self, path):
        path_names = path.split('.')
        module, parent = None, None
        tests = None
        for path_name in path_names:
            if module is not None and not hasattr(module, '__path__'):
                tests = self._find_functions(module, path_name)
            else:
                parent = module.__path__ if module is not None else None
                file, pathname, description = find_module(path_name, parent)
                module = load_module(path_name, file, pathname, description)

        if tests is None:
            tests = self._scan_functions(module)
        return tests

    def _find_functions(self, module, name=None):
        funcs = []
        if name is not None:
            funcs.append(module.__dict__[name])
        else:
            for func in module.__dict__.values():
                if isfunction(func) and func.__name__.startswith('test_'):
                    funcs.append(func)
        return funcs

    def _scan_functions(self, module):
        funcs = []
        if hasattr(module, '__path__'):
            modulepath = module.__path__[0]
            for filename in os.listdir(modulepath):
                filepath = os.path.join(modulepath, filename)
                if not filename.startswith('__') and (os.path.isdir(filepath) or (os.path.isfile(filepath) and filename.endswith('.py'))):
                    modulename = filename[:-3]
                    file, pathname, description = find_module(modulename, module.__path__)
                    submodule = load_module(modulename, file, pathname, description)
                    funcs += self._scan_functions(submodule)
        funcs += self._find_functions(module)
        return funcs

def run(paths, fail_fast=False):
    """Runs tests."""
    runner = TestRunner()
    try:
        return runner.run(paths, fail_fast)
    finally:
        runner.cleanup()
