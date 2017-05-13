from __future__ import print_function
from __future__ import unicode_literals

import os
from setuptools import setup, find_packages


def __read_requirements():
    with open(os.path.join(os.path.split(__file__)[0], 'requirements.txt'), 'r') as f:
        return [s.strip() for s in f.readlines()]


setup(
    name='pycebes',
    version='0.1.0.dev0',
    packages=find_packages(exclude=['tests']),
    description='Python client for Cebes HTTP server.',
    author='Vu Pham',
    author_email='vu.phoai@gmail.com',
    license='Apache 2.0',
    long_description=open('README.md', 'rb').read().decode('utf8'),
    install_requires=__read_requirements(),
)
