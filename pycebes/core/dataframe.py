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

from pycebes.core.column import Column
from pycebes.core.expressions import SparkPrimitiveExpression
from pycebes.core.sample import DataSample
from pycebes.core.schema import Schema
from pycebes.internal.implicits import get_default_session


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

    def _df_command(self, cmd='', **kwargs):
        """
        Helper to send a POST request to server, and parse the result as a Dataframe

        :rtype: Dataframe
        """
        r = self._client.post_and_wait('df/{}'.format(cmd), kwargs)
        return Dataframe.from_json(r)

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

    def __dir__(self):
        return dir(type(self)) + list(self.__dict__.keys()) + self.columns

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

    def sample(self, prob=0.1, replacement=True, seed=42):
        """
        Take a sample from this ``Dataframe`` with or without replacement at the given probability.

        Note that this function is probabilistic: it samples each row with a probability of ``prob``,
        therefore if the original ``Dataframe`` has ``N`` rows, the result of this function
        will *NOT* have exactly ``N * prob`` rows.

        :param prob: The probability to sample each row in the ``Dataframe``
        :param replacement: Whether to sample with replacement
        :param seed: random seed
        :rtype: Dataframe
        """
        return self._df_command('sample', df=self.id, fraction=prob,
                                withReplacement=replacement, seed=seed)

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
        return self._df_command('select', df=self.id, cols=[col.to_json() for col in columns])

    def where(self, condition):
        """
        Filters rows using the given condition.

        :param condition: the condition as a Column
        :type condition: Column
        :rtype: Dataframe
        """
        if not isinstance(condition, Column):
            raise ValueError('condition: expect a Column object')
        return self._df_command('where', df=self.id, cols=[condition.to_json()])

    def limit(self, n=100):
        """
        Returns a new ``Dataframe`` by taking the first ``n`` rows.
        """
        return self._df_command('limit', df=self.id, n=n)

    def intersect(self, other):
        """
        Returns a new Dataframe containing rows only in both this Dataframe and another Dataframe.

        :param other: another Dataframe to compute the intersection
        :type other: Dataframe
        """
        return self._df_command('intersect', df=self.id, otherDf=other.id)

    def union(self, other):
        """
        Returns a new Dataframe containing union of rows in this Dataframe and another Dataframe
        (without deduplication)

        :param other: another Dataframe to compute the union
        :type other: Dataframe
        """
        return self._df_command('union', df=self.id, otherDf=other.id)

    def subtract(self, other):
        """
        Returns a new Dataframe containing rows in this Dataframe but not in another Dataframe.
        This is equivalent to `EXCEPT` in SQL.

        :param other: another Dataframe to compute the except
        :type other: Dataframe
        """
        return self._df_command('except', df=self.id, otherDf=other.id)

    def join(self, other, expr, join_type='inner'):
        """
        Join with another ``Dataframe``, using the given join expression.

        :Example:

        .. code-block:: python

           df1.join(df2, df1.df1Key == df2.df2Key, join_type='outer')

        :param other: right side of the join
        :type other: Dataframe
        :param expr: Join expression, as a ``Column``
        :type expr: Column
        :param join_type: One of: `inner`, `outer`, `left_outer`, `right_outer`, `leftsemi`, `leftanti`, `cross`
        :rtype: Dataframe
        """
        join_types = ('inner', 'outer', 'left_outer', 'right_outer', 'leftsemi', 'leftanti', 'cross')
        join_type = join_type.lower()
        if join_type not in join_types:
            raise ValueError('Invalid join type: {}. Valid values are: {}'.format(join_type, ', '.join(join_types)))

        return self._df_command('join', leftDf=self.id, rightDf=other.id,
                                joinExprs=expr.to_json(), joinType=join_type)

    @property
    def broadcast(self):
        """
        Marks a Dataframe as small enough for use in broadcast joins.
        """
        return self._df_command('broadcast', df=self.id)

    def alias(self, alias='new_name'):
        """
        Returns a new Dataframe with an alias set
        """
        return self._df_command('alias', df=self.id, alias=alias)

    def with_column(self, col_name, col):
        """
        Returns a new ``Dataframe`` by adding a column or replacing
        the existing column that has the same name (case-insensitive).

        :param col_name: new column name
        :type col_name: six.text_type
        :param col: ``Column`` object describing the new column
        :type col: Column
        """
        return self._df_command('withcolumn', df=self.id, colName=col_name, col=col.to_json())

    def with_column_renamed(self, existing_name, new_name):
        """
        Returns a new ``Dataframe`` with a column renamed.

        :type existing_name: six.text_type
        :type new_name: six.text_type
        """
        return self._df_command('withcolumnrenamed', df=self.id, existingName=existing_name, newName=new_name)

    """
    Exploratory functions
    """

    def sort(self, *args):
        """
        Sort this ``Dataframe`` based on the given list of expressions.

        :param args: a list of arguments to specify how to sort the Dataframe, where each element
            is either a ``Column`` object or a column name.
            When the element is a column name, it will be sorted in ascending order.
        :return: a new ``Dataframe``
        :Example:

        .. code-block:: python

            df2 = df1.sort()
            df2 = df1.sort(df1['timestamp'].asc, df1['customer'].desc)
            df2 = df1.sort('timestamp', df1.customer.desc)

            # raise ValueError
            df1.sort('non_exist')
        """
        cols = []
        for c in args:
            if isinstance(c, six.text_type):
                if c not in self.columns:
                    raise ValueError('Column not found: {}'.format(c))
                cols.append(self[c].asc.to_json())
            elif isinstance(c, Column):
                cols.append(c.to_json())
            else:
                raise ValueError('Expect a column or a column name, got {!r}'.format(c))

        return self._df_command('sort', df=self.id, cols=cols)
