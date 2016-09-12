from __future__ import print_function

from setuptools import setup, find_packages

setup(
    cmdclass={},
    ext_modules=[],
    name='pycebes',
    version='0.1.0.dev0',
    packages=find_packages(),
    description='Python client for Cebes HTTP server.',
    license='Apache 2.0',
    long_description=open('README.rst', 'rb').read().decode('utf8'),
    install_requires=[],
)
