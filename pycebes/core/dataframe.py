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

import types

import six

from pycebes.core import functions
from pycebes.core.column import Column
from pycebes.core.expressions import SparkPrimitiveExpression, UnresolvedColumnName
from pycebes.core.sample import DataSample
from pycebes.core.schema import Schema
from pycebes.internal.helpers import require
from pycebes.internal.implicits import get_default_session
from pycebes.internal.serializer import to_json


def _parse_column_names(df, *columns):
    """
    Parse the list of column names (as strings) or ``Column`` objects which belong to this Dataframe

    :param df: The ``Dataframe`` to parse the column names
    :type df: Dataframe
    :return: the list of column names, ready to be sent to the server
    """
    result = []
    col_names = set(df.columns)
    for c in columns:
        if isinstance(c, six.text_type):
            require(c in col_names, 'Column not found in this Dataframe: {}'.format(c))
            result.append(c)
        else:
            require(isinstance(c, Column),
                    'Only column names (strings) or `Column` objects are allowed. Got {!r}'.format(c))
            expr = c.expr

            if isinstance(expr, SparkPrimitiveExpression):
                require(expr.df_id == df.id, 'Column from a different Dataframe is not allowed: {!r}'.format(c))
                require(expr.col_name in col_names, 'Column not found in this Dataframe: {}'.format(c))
                result.append(expr.col_name)
            elif isinstance(expr, UnresolvedColumnName):
                require(expr.col_name in col_names, 'Column not found in this Dataframe: {}'.format(c))
                result.append(expr.col_name)
            else:
                raise ValueError('Invalid column: {!r}. Only column objects constructed from a Dataframe is allowed. '
                                 'Use Dataframe.<column_name> or functions.col(<column_name>) to get one'.format(c))
    return result


def _parse_columns(df, *columns):
    """
    Parse the list of column names (as strings) or ``Column`` objects which belong to this Dataframe
    This is similar to :func:`_parse_column_names`, but returns a list of ``Column`` object instead.

    :return: the list of ``Column`` objects
    """
    cols = []
    if len(columns) == 1 and isinstance(columns[0], types.GeneratorType):
        columns = list(columns[0])

    for c in columns:
        if isinstance(c, six.text_type):
            cols.append(df[c])
        else:
            require(isinstance(c, Column), 'Expect a column or a column name, got {!r}'.format(c))
            cols.append(c)
    return cols


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
        require('id' in js_data and 'schema' in js_data, 'Invalid Dataframe JSON: {!r}'.format(js_data))
        return Dataframe(_id=js_data['id'], _schema=Schema.from_json(js_data['schema']))

    """
    Public properties and Python magics
    """

    @property
    def id(self):
        """
        Return the unique ID of this :class:`Dataframe`

        :rtype: six.text_type
        """
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

        :Example:

        .. code-block:: python

            df.shape
                (540, 40)
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
        if item in self.columns:
            return Column(SparkPrimitiveExpression(self._id, item))
        raise AttributeError('Attribute not found: {!r}'.format(item))

    def __getitem__(self, item):
        try:
            return self.__getattr__(item)
        except AttributeError:

            if isinstance(item, six.text_type):
                # this might seem non-sense at first, but it will help in complicated queries, e.g.
                # df.alias('left').join(...).select(df['left.*'])
                # but this is only allowed in __getitem__(), not __getattr__()
                return functions.col(item)

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

        :param n: Number of rows to show
        :Example:

        .. code-block:: python

            df.groupby(df.customer, 'cylinder_number').count().show()

                ID: 4ae73a7c-27e5-43b9-b69f-43c606e38ee0
                Shape: (490, 3)
                Sample 5 rows:
                    customer cylinder_number  count
                0     MODMAT            F108      1
                1     MODMAT              M4      1
                2  CASLIVING             M74      1
                3    ECKERDS            X381      1
                4     TARGET             R43      1
        """
        print('ID: {}\nShape: {}\nSample {} rows:\n{!r}'.format(
            self.id, self.shape, n, self.take(n).to_pandas()))

    """
    SQL API
    """

    def select(self, *columns):
        """
        Selects a set of columns based on expressions.

        :param columns: list of columns, or column names
        :rtype: Dataframe

        :Example:

        .. code-block:: python

            import pycebes as cb

            # various ways to provide arguments to `select`
            df.select(df.customer, cb.substring('cylinder_number', 0, 1).alias('cylinder_t'),
                      cb.col('hardener'), 'wax').show()
                ...
                  customer cylinder_t  hardener  wax
                0  TVGUIDE          X       1.0  2.5
                1  TVGUIDE          X       0.7  2.5
                2   MODMAT          B       0.9  2.8
                3   MASSEY          T       1.3  2.5
                4    KMART          J       0.6  2.3

            # Select all columns in a dataframe can be done in several ways
            df.select('*')
            df.select(df['*'])
            df.select(cb.col('*'))
            df.alias('my_name').select('my_name.*')

        """
        columns = _parse_columns(self, *columns)
        return self._df_command('select', df=self.id, cols=[col.to_json() for col in columns])

    def where(self, condition):
        """
        Filters rows using the given condition.

        :param condition: the condition as a Column
        :type condition: Column
        :rtype: Dataframe
        :Example:

        .. code-block:: python

            df.where((df.hardener >= 1) & (df.wax < 2.8)).select('wax', df.hardener, df.customer).show()
                ...
                   wax  hardener     customer
                0  2.5       1.0      TVGUIDE
                1  2.5       1.3       MASSEY
                2  2.5       1.1        ROSES
                3  2.5       1.0  HANOVRHOUSE
                4  2.0       1.0   GUIDEPOSTS
        """
        require(isinstance(condition, Column), 'condition: expect a Column object')
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
        :Example:

        .. code-block:: python

            df.where(df.wax > 2).intersect(df.where(df.wax < 2.8)).select('customer', 'wax').show()
                ...
                        customer  wax
                0      SERVMERCH  2.7
                1        TVGUIDE  2.5
                2         MODMAT  2.7
                3           AMES  2.4
                4  colorfulimage  2.5
        """
        return self._df_command('intersect', df=self.id, otherDf=other.id)

    def union(self, other):
        """
        Returns a new Dataframe containing union of rows in this Dataframe and another Dataframe
        (without deduplication)

        :param other: another Dataframe to compute the union
        :type other: Dataframe
        :Example:

        .. code-block:: python

            df.where(df.wax < 2).union(df.where(df.wax > 2.8)).select('customer', 'wax').show()
                ...
                    customer  wax
                0      USCAV  1.1
                1   COLORTIL  1.7
                2   COLORTIL  1.0
                3  ABBYPRESS  1.0
                4  ABBYPRESS  1.0
        """
        return self._df_command('union', df=self.id, otherDf=other.id)

    def subtract(self, other):
        """
        Returns a new Dataframe containing rows in this Dataframe but not in another Dataframe.
        This is equivalent to `EXCEPT` in SQL.

        :param other: another Dataframe to compute the except
        :type other: Dataframe
        :Example:

        .. code-block:: python

            df.where(df.wax < 2.8).subtract(df.where(df.wax < 2)).select('customer', 'wax').show()
                ...
                        customer  wax
                0      SERVMERCH  2.7
                1        TVGUIDE  2.5
                2         MODMAT  2.7
                3           AMES  2.4
                4  colorfulimage  2.5

        """
        return self._df_command('except', df=self.id, otherDf=other.id)

    def join(self, other, expr, join_type='inner'):
        """
        Join with another ``Dataframe``, using the given join expression.

        :Example:

        .. code-block:: python

            df1.join(df2, df1.df1Key == df2.df2Key, join_type='outer')

            # for self-joins, you should rename the columns so that they are accessible after the join
            df1 = df.where(df.wax > 2).select(df[c].alias('df1_{}'.format(c)) for c in df.columns)
            df2 = df.where(df.wax < 2.2).select(df[c].alias('df2_{}'.format(c)) for c in df.columns)

            df_join = df1.join(df2, df1.df1_customer == df2.df2_customer)

        :param other: right side of the join
        :type other: Dataframe
        :param expr: Join expression, as a ``Column``
        :type expr: Column
        :param join_type: One of: `inner`, `outer`, `left_outer`, `right_outer`, `leftsemi`, `leftanti`, `cross`
        :rtype: Dataframe
        """
        join_types = ('inner', 'outer', 'left_outer', 'right_outer', 'leftsemi', 'leftanti', 'cross')
        join_type = join_type.lower()
        require(join_type in join_types,
                'Invalid join type: {}. Valid values are: {}'.format(join_type, ', '.join(join_types)))

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

    def groupby(self, *columns):
        """
        Groups the ``Dataframe`` using the specified columns, so that we can run aggregation on them.

        See :class:`GroupedDataframe` for all the available aggregate functions.

        :param columns: list of column names or ``Column`` objects
        :return: ``GroupedDataframe`` object
        :rtype: GroupedDataframe
        :Example:

        .. code-block:: python

            df.groupby(df.customer, 'cylinder_number').count().show()
                ...
                    customer cylinder_number  count
                0     MODMAT            F108      1
                1     MODMAT              M4      1
                2  CASLIVING             M74      1
                3    ECKERDS            X381      1
                4     TARGET             R43      1

            df.groupby().count().show()
                ...
                   count
                0    540

        See :class:`GroupedDataframe` for more examples on different aggregate functions

        """
        return GroupedDataframe(df=self, agg_columns=_parse_columns(self, *columns),
                                agg_type=GroupedDataframe.GROUPBY)

    def rollup(self, *columns):
        """
        Create a multi-dimensional rollup for the current ``Dataframe`` using the specified columns,
        so we can run aggregation on them.

        See :class:`GroupedDataframe` for all the available aggregate functions.

        :param columns: list of column names or ``Column`` objects
        :return: ``GroupedDataframe`` object
        :rtype: GroupedDataframe
        :Example:

        .. code-block:: python

            df.rollup(df.customer, 'proof_on_ctd_ink').count().show()
                ...
                      customer proof_on_ctd_ink  count
                0  NTLWILDLIFE              YES      1
                1     HANHOUSE              YES      2
                2   YIELDHOUSE              YES      1
                3      toysrus                       3
                4          CVS                       2

            df.rollup(df.customer, 'proof_on_ctd_ink').agg({'hardener': 'max', 'wax': 'avg'}).show()
                ...
                      customer proof_on_ctd_ink  max(hardener)  avg(wax)
                0  NTLWILDLIFE              YES            0.6      3.00
                1     HANHOUSE              YES            1.0      2.25
                2   YIELDHOUSE              YES            0.5      3.00
                3      toysrus                             2.1      2.40
                4          CVS                             1.0      2.30

        See :class:`GroupedDataframe` for more examples on different aggregate functions
        """
        return GroupedDataframe(df=self, agg_columns=_parse_columns(self, *columns),
                                agg_type=GroupedDataframe.ROLLUP)

    def cube(self, *columns):
        """
        Create a multi-dimensional cube for the current ``Dataframe`` using the specified columns,
        so we can run aggregation on them.

        See :class:`GroupedDataframe` for all the available aggregate functions.

        :param columns: list of column names or ``Column`` objects
        :return: ``GroupedDataframe`` object
        :rtype: GroupedDataframe
        :Example:

        .. code-block:: python

            df.cube(df.customer, 'proof_on_ctd_ink').count().show()
                ...
                      customer proof_on_ctd_ink  count
                0  NTLWILDLIFE              YES      1
                1     HANHOUSE              YES      2
                2   YIELDHOUSE              YES      1
                3      toysrus                       3
                4          CVS                       2

            df.cube(df.customer, 'proof_on_ctd_ink').agg({'hardener': 'max', 'wax': 'avg'}).show()
                ...
                      customer proof_on_ctd_ink  max(hardener)  avg(wax)
                0  NTLWILDLIFE              YES            0.6      3.00
                1     HANHOUSE              YES            1.0      2.25
                2   YIELDHOUSE              YES            0.5      3.00
                3      toysrus                             2.1      2.40
                4          CVS                             1.0      2.30

        See :class:`GroupedDataframe` for more examples on different aggregate functions
        """
        return GroupedDataframe(df=self, agg_columns=_parse_columns(self, *columns),
                                agg_type=GroupedDataframe.CUBE)

    def agg(self, *exprs):
        """
        Compute aggregates and returns the result as a DataFrame.

        This is a convenient method in which the aggregations are computed on
        all rows. In other words, ``self.agg(*exprs)`` is equivalent to ``self.groupby().agg(*exprs)``

        If exprs is a single dict mapping from string to string,
        then the key is the column to perform aggregation on, and the value is the aggregate function.

        The available aggregate functions are `avg`, `max`, `min`, `sum`, `count`.

        Alternatively, exprs can also be a list of aggregate ``Column`` expressions.

        :param exprs: a dict mapping from column name (string) to aggregate functions (string),
            or a list of :class:`Column`
        :rtype: Dataframe

        :Example:

        .. code-block:: python

            import pycebes as cb

            # count number of non-NA values in column `hardener`
            df.agg(cb.count(df.hardener)).show()
                    count(hardener)
                0              533

            # count number of non-NA values in all columns:
            df.agg(*[cb.count(c).alias(c) for c in df.columns])

        See :class:`GroupedDataframe` for more examples

        """
        return self.groupby().agg(*exprs)

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
        col_names = set(self.columns)
        for c in args:
            if isinstance(c, six.text_type):
                require(c in col_names, 'Column not found: {}'.format(c))
                cols.append(self[c].asc.to_json())
            else:
                require(isinstance(c, Column), 'Expect a column or a column name, got {!r}'.format(c))
                cols.append(c.to_json())

        return self._df_command('sort', df=self.id, cols=cols)

    def drop(self, *columns):
        """
        Drop columns in this ``Dataframe``

        :param columns: column names
        :return: a new ``Dataframe`` with the given columns dropped.
            If ``columns`` is empty, this ``Dataframe`` is returned
        :Example:

        .. code-block:: python

            df1.show()
                ...
                  customer cylinder_number  hardener
                0  TVGUIDE            X126       1.0
                1  TVGUIDE            X266       0.7
                2   MODMAT              B7       0.9
                3   MASSEY            T133       1.3
                4    KMART             J34       0.6

            df1.drop(df1.customer, 'hardener').show()
                ...
                  cylinder_number
                0            X126
                1            X266
                2              B7
                3            T133
                4             J34
        """
        col_names = _parse_column_names(self, *columns)
        if len(col_names) == 0:
            return self
        return self._df_command('dropcolumns', df=self.id, colNames=col_names)

    def drop_duplicates(self, *columns):
        """
        Returns a new Dataframe that contains only the unique rows from this Dataframe, only considered
        the given list of columns.

        If no columns are given (the default), all columns will be considered

        :return: a new ``Dataframe`` with duplicated rows removed
        :Example:

        .. code-block:: python

            # original Dataframe has 540 rows
            df.shape
                (540, 40)

            # No duplicated rows in this Dataframe, therefore the shape stays the same
            df.drop_duplicates().shape
                (540, 40)

            # only consider the `customer` column
            df.drop_duplicates(df.customer).shape
                (83, 40)

            # we can check by computing the number of distinct values in column `customer`
            df.select(cb.count_distinct(df.customer)).take().to_pandas()
                   count(DISTINCT customer)
                0                        83

            # only consider 2 columns
            df.drop_duplicates(df.customer, 'cylinder_number').shape
                (490, 40)

        """
        col_names = _parse_column_names(self, *columns)
        if len(col_names) == 0:
            col_names = self.columns
        return self._df_command('dropduplicates', df=self.id, colNames=col_names)

    def dropna(self, how='any', thresh=None, columns=None):
        """
        Return object with labels on given axis omitted where some of the data are missing

        :param how: strategy to drop rows. Accepted values are:
            * ``any``: if any NA values are present, drop that label
            * ``all``: if all values are NA, drop that label
        :param thresh: drop rows containing less than ``thresh`` non-null and non-NaN values
            If ``thresh`` is specified, ``how`` will be ignored.
        :type thresh: int
        :param columns: a list of columns to include. Default is None, meaning all columns are included
        :return: a new ``Dataframe`` with NA values dropped
        :Example:

        .. code-block:: python

        df.dropna()
        df.dropna(how='all')
        df.dropna(thresh=38)
        df.dropna(thresh=2, columns=[df.wax, 'proof_on_ctd_ink', df.hardener])
        """
        if columns is None:
            columns = self.columns
        columns = _parse_column_names(self, *columns)

        min_non_null = thresh
        if min_non_null is None:
            require(how in ('any', 'all'), 'how={} is not supported. Only "any" or "all" are allowed'.format(how))
            if how == 'any':
                min_non_null = len(columns)
            else:
                min_non_null = 1
        return self._df_command('dropna', df=self.id, minNonNulls=min_non_null, colNames=columns)

    def fillna(self, value=None, columns=None):
        """
        Fill NA/NaN values using the specified value.

        :param value: the value used to fill the holes. Can be a ``float``, ``string`` or a ``dict``.
            When ``value`` is a dict, it is a map from column names to a value used to fill the holes in that column.
            In this case the value can only be of type ``int``, ``float``, ``string`` or ``bool``.
            The values will be casted to the column data type.
        :param columns: List of columns to consider. If None, all columns are considered
        :return: a new Dataframe with holes filled
        :rtype: Dataframe
        """
        if columns is None:
            columns = self.columns
        columns = _parse_column_names(self, *columns)
        if isinstance(value, int):
            value = float(value)
        if isinstance(value, (six.text_type, float)):
            return self._df_command('fillna', df=self.id, value=value, colNames=columns)
        if isinstance(value, dict):
            return self._df_command('fillnawithmap', df=self.id, valueMap=to_json(value))
        raise ValueError('Unsupported value: {!r}'.format(value))


@six.python_2_unicode_compatible
class GroupedDataframe(object):
    GROUPBY = 'GroupBy'
    ROLLUP = 'RollUp'
    CUBE = 'Cube'

    def __init__(self, df, agg_columns=(), agg_type=GROUPBY, pivot_column=None, pivot_values=()):
        """

        :type df: Dataframe
        :param agg_columns: list of aggregate columns
        :param agg_type: aggregation type
        :param pivot_column: column name of pivot
        :param pivot_values: pivot values
        """
        self.df = df
        self.agg_type = agg_type
        self.agg_columns = agg_columns
        self.pivot_column = pivot_column
        self.pivot_values = pivot_values

    def __repr__(self):
        return ('{}(df_id={!r}, agg_columns={!r}, agg_type={!r}, pivot_column={!r}, '
                'pivot_values={!r})'.format(self.__class__.__name__,
                                            self.df.id, self.agg_columns,
                                            self.agg_type, self.pivot_column, self.pivot_values))

    def _send_request(self, generic_agg_exprs=None, agg_func=None, agg_col_names=None):
        client = get_default_session().client

        data = {
            'df': self.df.id,
            'cols': [c.to_json() for c in self.agg_columns],
            'aggType': self.agg_type,
            'pivotValues': None,
            'aggColNames': []
        }
        if self.pivot_column is not None:
            data['pivotColName'] = self.pivot_column
        if self.pivot_values is not None:
            data['pivotValues'] = [to_json(v) for v in self.pivot_values]
        if generic_agg_exprs is not None:
            data['genericAggExprs'] = [c.to_json() for c in generic_agg_exprs]
        if agg_func is not None:
            data['aggFunc'] = agg_func
        if agg_col_names is not None:
            data['aggColNames'] = _parse_column_names(self.df, *agg_col_names)

        return Dataframe.from_json(client.post_and_wait('df/aggregate', data))

    def agg(self, *exprs):
        """
        Compute aggregates and returns the result as a DataFrame.

        If exprs is a single dict mapping from string to string,
        then the key is the column to perform aggregation on, and the value is the aggregate function.

        The available aggregate functions are `avg`, `max`, `min`, `sum`, `count`.

        Alternatively, exprs can also be a list of aggregate ``Column`` expressions.

        :param exprs: a dict mapping from column name (string) to aggregate functions (string),
            or a list of :class:`Column`

        :Example:

        .. code-block:: python

            import pycebes as cb

            df.groupby(df.customer).agg(cb.max(df.timestamp), cb.min('job_number'),
                                        cb.count_distinct('job_number').alias('cnt')).show()
                ...
                       customer  max(timestamp)  min(job_number)  cnt
                      WOOLWORTH        19920405            34227    5
                       HOMESHOP        19910129            38064    1
                   homeshopping        19920712            36568    1
                         GLOBAL        19900621            36846    1
                            JCP        19910322            34781    2

            df.groupby(df.customer).agg({'timestamp': 'max', 'job_number': 'min', 'job_number': 'count'}).show()
                ...
                       customer  max(timestamp)  count(job_number)
                      WOOLWORTH        19920405                  9
                       HOMESHOP        19910129                  2
                   homeshopping        19920712                  1
                         GLOBAL        19900621                  1
                            JCP        19910322                  4
        """
        agg_cols = []

        if len(exprs) == 1 and isinstance(exprs[0], dict):
            funcs = {'avg': functions.avg, 'max': functions.max,
                     'min': functions.min, 'sum': functions.sum,
                     'count': functions.count}

            for col, func in exprs[0].items():
                require(func in funcs, 'Unsupported aggregation function: {!r}'.format(func))
                agg_cols.append(funcs[func](functions.col(col)))
        else:
            for expr in exprs:
                require(isinstance(expr, Column), 'Expected a Column expression, got {!r}'.format(expr))
                agg_cols.append(expr)

        return self._send_request(generic_agg_exprs=agg_cols)

    def count(self):
        """
        Counts the number of records for each group.

        :Example:

        .. code-block:: python

            df.groupby(df.customer).count().show()
                ...
                      customer  count
                     WOOLWORTH      9
                      HOMESHOP      2
                  homeshopping      1
                        GLOBAL      1
                           JCP      4

            df.groupby(df.customer, 'proof_on_ctd_ink').count().show()
                ...
                       customer proof_on_ctd_ink  count
                     GUIDEPOSTS              YES      5
                       homeshop                       4
                  colorfulimage                       1
                      CASLIVING              YES      2
                          ABBEY              YES      4

        """
        return self._send_request(agg_func='count')

    def min(self, *columns):
        """
        Computes the min value for each numeric column for each group.

        :param columns: list of column names (string). Non-numeric columns are ignored.
        :Example:

        .. code-block:: python

            df.groupby(df.customer).min('hardener', 'wax').show()
                ...
                      customer  min(hardener)  min(wax)
                     WOOLWORTH            0.0      0.00
                      HOMESHOP            0.8      2.50
                  homeshopping            NaN      2.50
                        GLOBAL            1.1      3.00
                           JCP            0.6      1.75

        """
        return self._send_request(agg_func='min', agg_col_names=columns)

    def max(self, *columns):
        """
        Computes the max value for each numeric column for each group.

        :param columns: list of column names (string). Non-numeric columns are ignored.
        :Example:

        .. code-block:: python

            df.groupby(df.customer).max('hardener', 'wax').show()
                ...
                       customer  max(hardener)  max(wax)
                      WOOLWORTH            1.3       2.6
                       HOMESHOP            1.8       2.5
                   homeshopping            NaN       2.5
                         GLOBAL            1.1       3.0
                            JCP            1.7       3.0
        """
        return self._send_request(agg_func='max', agg_col_names=columns)

    def mean(self, *columns):
        """
        Computes the average value for each numeric column for each group.

        :param columns: list of column names (string). Non-numeric columns are ignored.
        :Example:

        .. code-block:: python

            df.groupby(df.customer).mean('hardener', 'wax').show()
                ...
                       customer  avg(hardener)  avg(wax)
                      WOOLWORTH       0.811111  1.744444
                       HOMESHOP       1.300000  2.500000
                   homeshopping            NaN  2.500000
                         GLOBAL       1.100000  3.000000
                            JCP       0.975000  2.437500
        """
        return self._send_request(agg_func='mean', agg_col_names=columns)

    avg = mean

    def sum(self, *columns):
        """
        Computes the sum for each numeric column for each group.

        :param columns: list of column names (string). Non-numeric columns are ignored.
        :Example:

        .. code-block:: python

            df.groupby(df.customer).sum('hardener', 'wax').show()
                ...
                       customer  sum(hardener)  sum(wax)
                      WOOLWORTH            7.3     15.70
                       HOMESHOP            2.6      5.00
                   homeshopping            NaN      2.50
                         GLOBAL            1.1      3.00
                            JCP            3.9      9.75
        """
        return self._send_request(agg_func='sum', agg_col_names=columns)

    def pivot(self, column, values=None):
        """
        Pivots a column of the ``Dataframe`` and perform the specified aggregation.

        :param column: Name of the column to pivot.
        :type column: six.text_type
        :param values: List of values that will be translated to columns in the output ``Dataframe``
            If unspecified, Cebes will need to compute the unique values of the given column before
            the pivot, therefore it will be less efficient.
        :rtype: GroupedDataframe
        :Example:

        .. code-block:: python

            # for each pair of (`customer`, `proof_on_ctd_ink`), count the number of entries
            df.groupby(df.customer).pivot('proof_on_ctd_ink').count().show()
                ...
                       customer        NO  YES
                      WOOLWORTH  1.0  1.0  7.0
                       HOMESHOP  NaN  NaN  2.0
                   homeshopping  1.0  NaN  NaN
                         GLOBAL  NaN  NaN  1.0
                            JCP  NaN  NaN  4.0

            # for each pair of (`customer`, `proof_on_ctd_ink`), get the max value of `hardener`
            df.groupby(df.customer).pivot('proof_on_ctd_ink').max('hardener').show()
                ...
                      customer        NO  YES
                     WOOLWORTH  1.0  0.0  1.3
                      HOMESHOP  NaN  NaN  1.8
                  homeshopping  NaN  NaN  NaN
                        GLOBAL  NaN  NaN  1.1
                           JCP  NaN  NaN  1.7

            # specify the pivot values, more efficient
            df.groupby(df.customer).pivot('proof_on_ctd_ink', values=['YES', 'NO']).count().show()
                ...
                       customer  YES   NO
                      WOOLWORTH  7.0  1.0
                       HOMESHOP  2.0  NaN
                   homeshopping  NaN  NaN
                         GLOBAL  1.0  NaN
                            JCP  4.0  NaN

        """
        return GroupedDataframe(df=self.df, agg_columns=self.agg_columns,
                                agg_type=self.agg_type, pivot_column=column,
                                pivot_values=values)
