#!/usr/bin/env bash

set -e

rm dist/* || true
python setup.py sdist
python setup.py bdist_wheel --universal

# test the distributions
twine upload dist/*
