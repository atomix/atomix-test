from setuptools import setup, find_packages

setup(
    name='atomixtest',
    version='1.0',
    description='Systems test framework for Atomix 2.1',
    author='Jordan Halterman',
    author_email='jordan.halterman@gmail.com',
    url='http://github.com/atomix/atomix-test',
    packages=find_packages(),
    install_requires=['atomix', 'docker', 'pytest'],
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
