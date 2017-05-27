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

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import six

from pycebes.core.sample import DataSample
from pycebes.core.schema import Schema
from pycebes.internal.implicits import get_default_session
from pycebes.core.column import Column
from pycebes.core.expressions import SparkPrimitiveExpression


@six.python_2_unicode_compatible
class Dataframe(object):
    def __init__(self, _id, _schema):
        self._id = _id
        self._schema = _schema

    """
    Helpers
    """

    @property
    def _client(self):
        """
        Default client, taken from the current default session
        :rtype: pycebes.core.client.Client
        """
        return get_default_session().client

    @classmethod
    def from_json(cls, js_data):
        """
        Return a ``Dataframe`` instance from its JSON representation
        :param js_data: a dict with ``id`` and ``schema``
        :rtype: Dataframe
        """
        if 'id' not in js_data or 'schema' not in js_data:
            raise ValueError('Invalid Dataframe JSON: {!r}'.format(js_data))

        return Dataframe(_id=js_data['id'], _schema=Schema.from_json(js_data['schema']))

    """
    Public properties and Python magics
    """

    @property
    def id(self):
        return self._id

    @property
    def schema(self):
        """
        The Schema of this data frame
        :rtype: Schema
        """
        return self._schema

    @property
    def shape(self):
        """
        Return a 2-tuple with number of rows and columns
        """
        return len(self), len(self.columns)

    @property
    def columns(self):
        """
        Return a list of column names in this ``Dataframe``
        """
        return self.schema.columns

    def __len__(self):
        """
        Number of rows in this ``Dataframe``
        """
        return self._client.post_and_wait('df/count', data={'df': self.id})

    def __repr__(self):
        return '{}(id={!r})'.format(self.__class__.__name__, self.id)

    def __getattr__(self, item):
        try:
            return self[item]
        except KeyError:
            raise AttributeError('Attribute not found: {!r}'.format(item))

    def __getitem__(self, item):
        if item in self.columns:
            return Column(SparkPrimitiveExpression(self._id, item))
        raise KeyError('Column not found: {!r}'.format(item))

    """
    Sampling functions
    """

    def take(self, n=10):
        """
        Take a sample of this ``Dataframe``
        :param n: maximum number of rows to be taken
        :rtype: DataSample
        """
        r = self._client.post_and_wait('df/take', {'df': self.id, 'n': n})
        return DataSample.from_json(r)

    def sample(self, fraction=0.1, replacement=True, seed=42):
        """
        Take a sample from this ``Dataframe``
        :param fraction: 
        :param replacement: 
        :param seed: 
        :return: 
        :rtype: Dataframe
        """
        r = self._client.post_and_wait(
            'df/sample', {'df': self.id, 'fraction': fraction,
                          'withReplacement': replacement, 'seed': seed})
        return Dataframe.from_json(r)

    def show(self, n=5):
        """
        Convenient function to show basic information and sample rows from this Dataframe
        :return: nothing
        """
        print('ID: {}\nShape: {}\nSample {} rows:\n{!r}'.format(
            self.id, self.shape, n, self.take(n).to_pandas()))

    """
    SQL API
    """

    def select(self, *columns):
        """
        Selects a set of columns based on expressions.
        :param columns: list of columns
        :rtype: Dataframe
        """
        if not all(isinstance(c, Column) for c in columns):
            raise ValueError('Expect a list of Column objects')
        r = self._client.post_and_wait('df/select', {'df': self.id, 'cols': [col.to_json() for col in columns]})
        return Dataframe.from_json(r)

    def where(self, condition):
        """
        Filters rows using the given condition.
        :param condition: the condition as a Column
        :type condition: Column
        :rtype: Dataframe
        """
        if not isinstance(condition, Column):
            raise ValueError('condition: expect a Column object')
        r = self._client.post_and_wait('df/where', {'df': self.id, 'cols': [condition.to_json()]})
        return Dataframe.from_json(r)
