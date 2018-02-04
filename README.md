# cebes-python
Python client for Cebes - an integrated framework for Data Science. 

See [Cebes website](https://cebes.github.io) and 
[cebes-server](https://github.com/cebes/cebes-server) for more information.

## Installation

`pycebes` is a pure Python package and can be installed from PyPI:

```bash
$ pip install pycebes
```

## Development guide

### Unit tests

```bash
$ pip install nose coverage
$ nosetests --with-coverage --cover-package=pycebes --cover-html --cover-html-dir=tests/report tests/test_*
```

