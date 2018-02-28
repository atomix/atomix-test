from setuptools import setup, find_packages
from setuptools.command.test import test as TestCommand
import sys

class PyTest(TestCommand):
    user_options = [('pytest-args=', 'a', 'atomixtest/tests.py')]

    def initialize_options(self):
        TestCommand.initialize_options(self)
        self.pytest_args = 'atomixtest/tests.py'

    def run_tests(self):
        import shlex
        import pytest
        errno = pytest.main(shlex.split(self.pytest_args))
        sys.exit(errno)

setup(
    name='atomixtest',
    version='1.0',
    description='Systems test framework for Atomix 2.1',
    author='Jordan Halterman',
    author_email='jordan.halterman@gmail.com',
    url='http://github.com/atomix/atomix-test',
    packages=['atomixtest'],
    install_requires=['atomix', 'docker', 'pytest', 'six', 'terminaltables>=3.1.0'],
    tests_require=['pytest'],
    cmdclass = {'test': PyTest},
    extras_require={
        ':python_version=="2.6"': [
            'argparse>=1.1',
        ]
    },
    scripts=['bin/atomix-test'],
    license="Apache License 2.0",
    classifiers=(
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'Natural Language :: English',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7'
    ),
)
