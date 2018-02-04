# Copyright 2016 The Cebes Authors. All Rights Reserved.
#
# Licensed under the Apache License, version 2.0 (the "License").
# You may not use this work except in compliance with the License,
# which is available at www.apache.org/licenses/LICENSE-2.0
#
# This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied, as more fully set forth in the License.
#
# See the NOTICE file distributed with this work for information regarding copyright ownership.

from __future__ import print_function
from __future__ import unicode_literals

import os
from setuptools import setup, find_packages


def __read_requirements():
    with open(os.path.join(os.path.split(__file__)[0], 'requirements.txt'), 'r') as f:
        return [s.strip() for s in f.readlines()]


setup(
    name='pycebes',
    version='0.10.0',
    packages=find_packages(exclude=['tests']),
    description='Python client for Cebes HTTP server.',
    author='Vu Pham',
    author_email='vuph@cebes.io',
    license='Apache 2.0',
    long_description=open('README.md', 'rb').read().decode('utf8'),
    install_requires=__read_requirements(),
)
