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
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import unittest

import pandas as pd
import six

from pycebes.core import functions
from pycebes.core.column import Column
from pycebes.core.dataframe import Dataframe
from pycebes.core.exceptions import ServerException
from pycebes.core.sample import DataSample
from pycebes.core.schema import Schema, SchemaField
from pycebes.internal.responses import TaggedDataframeResponse
from tests import test_base


class TestDataframe(test_base.TestBase):
    def test_properties_and_magics(self):
        df = self.cylinder_bands
        self.assertIsInstance(df.id, six.text_type)
        self.assertIsInstance(df.schema, Schema)
        self.assertGreater(len(df), 10)
        self.assertEqual(len(df.columns), 40)
        self.assertEqual(df.shape, (len(df), 40))
        self.assertIsInstance(df.columns, list)
        self.assertTrue(all(isinstance(x, six.text_type) for x in df.columns))

        # column names are in dir(df)
        members = set(dir(df))
        for c in df.columns:
            self.assertIn(c, members)

        # getattr works
        col_name = df.columns[0]
        self.assertIsInstance(getattr(df, col_name), Column)

        # get schema field using column name
        self.assertIsInstance(df.schema[col_name], SchemaField)
        self.assertEqual(df.schema[col_name].name, col_name)
        with self.assertRaises(KeyError):
            _ = df.schema['non_exists']

        # failed getattr
        with self.assertRaises(AttributeError):
            getattr(df, 'non_existed_column')

        # getitem works
        self.assertIsInstance(df[col_name], Column)

        # getitem with weird column name is still allowed
        self.assertIsInstance(df['non_existed_column'], Column)

        # getitem won't work with a key that is not a string (i.e. column name)
        with self.assertRaises(KeyError):
            _ = df[{'a': 100}]

    def test_get_tags(self):
        tags = self.session.dataframe.list(max_count=10)
        self.assertIsInstance(tags, TaggedDataframeResponse)

    def test_take(self):
        s1 = self.cylinder_bands.take(n=10)
        self.assertIsInstance(s1, DataSample)

        df = s1.to_pandas(raise_if_error=False)
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df.columns), len(s1.columns))
        self.assertEqual(len(df), 10)

        with self.assertRaises(ServerException) as cm:
            self.cylinder_bands.take(-100)
            self.assertTrue('The limit expression must be equal to '
                            'or greater than 0, but got -100' in cm.exception.message)

    def test_sample(self):
        df1 = self.cylinder_bands.sample(prob=0.5)
        self.assertIsInstance(df1, Dataframe)
        self.assertListEqual(df1.columns, self.cylinder_bands.columns)
        ratio = float(len(df1)) / len(self.cylinder_bands)
        self.assertTrue(0.45 < ratio)
        self.assertTrue(0.55 >= ratio)

        with self.assertRaises(ServerException) as cm:
            self.cylinder_bands.sample(prob=-0.6)
            self.assertTrue('Fraction must be nonnegative, but got -0.6' in cm.exception.message)

    """
    SQL APIs
    """

    def test_select(self):
        df = self.cylinder_bands

        df1 = df.select(df.customer, functions.substring('cylinder_number', 0, 1).alias('cylinder_t'),
                        functions.col('hardener'), 'wax')
        self.assertEqual(len(df), len(df1))
        self.assertListEqual(df1.columns, ['customer', 'cylinder_t', 'hardener', 'wax'])

        df1 = df.select('*')
        self.assertEqual(len(df), len(df1))
        self.assertListEqual(df.columns, df1.columns)

        df1 = df.select(df['*'])
        self.assertEqual(len(df), len(df1))
        self.assertListEqual(df.columns, df1.columns)

        df1 = df.select(functions.col('*'))
        self.assertEqual(len(df), len(df1))
        self.assertListEqual(df.columns, df1.columns)

        df1 = df.alias('abcd').select('abcd.*')
        self.assertEqual(len(df), len(df1))
        self.assertListEqual(df.columns, df1.columns)

    def test_where(self):
        df = self.cylinder_bands

        df1 = df.where(df.hardener >= 1)
        self.assertTrue(10 <= len(df1) < len(df))
        self.assertListEqual(df1.columns, df.columns)

        df1 = df.where((df.hardener >= 1) & (df.wax < 2.8))
        self.assertTrue(10 <= len(df1) < len(df))
        self.assertListEqual(df1.columns, df.columns)

    def test_limit(self):
        df = self.cylinder_bands
        df1 = df.limit(100)
        self.assertEqual(len(df1), 100)
        self.assertListEqual(df1.columns, df.columns)

    def test_intersect(self):
        df = self.cylinder_bands

        df1 = df.where(df.wax > 2).intersect(df.where(df.wax < 2.8))
        df2 = df.where((df.wax > 2) & (df.wax < 2.8))
        n1 = len(df1)
        self.assertGreater(n1, 10)
        self.assertEqual(n1, len(df2))
        self.assertListEqual(df1.columns, df.columns)

    def test_union(self):
        df = self.cylinder_bands

        df1 = df.where(df.wax < 2).union(df.where(df.wax > 2.8))
        df2 = df.where((df.wax < 2) | (df.wax > 2.8))
        n1 = len(df1)
        self.assertGreater(n1, 10)
        self.assertEqual(n1, len(df2))
        self.assertListEqual(df1.columns, df.columns)

    def test_subtract(self):
        df = self.cylinder_bands

        df1 = df.where(df.wax < 2.8).subtract(df.where(df.wax < 2))
        df2 = df.where((df.wax >= 2) & (df.wax < 2.8))
        n1 = len(df1)
        self.assertGreater(n1, 10)
        self.assertEqual(n1, len(df2))
        self.assertListEqual(df1.columns, df.columns)

    def test_join(self):
        df = self.cylinder_bands

        df1 = df.where(df.wax > 2).select(df[c].alias('df1_{}'.format(c)) for c in df.columns)
        df2 = df.where(df.wax < 2.2).select(df[c].alias('df2_{}'.format(c)) for c in df.columns)

        df_join = df1.join(df2, df1.df1_customer == df2.df2_customer)
        self.assertListEqual(df_join.columns, df1.columns + df2.columns)
        self.assertGreater(len(df_join), 10)

        df_join = df1.join(df2.broadcast.alias('df2'), df1.df1_customer == functions.col('df2.df2_customer'))
        self.assertListEqual(df_join.columns, df1.columns + df2.columns)
        self.assertGreater(len(df_join), 10)

    def test_with_column(self):
        df = self.cylinder_bands

        df1 = df.with_column('customer2', functions.substring(df.customer, 0, 4))
        self.assertListEqual(df1.columns, df.columns + ['customer2'])
        df1 = df.with_column('customer', df.timestamp / 2)
        self.assertListEqual(df1.columns, df.columns)

        df2 = df.with_column_renamed('customer', 'customer_new')
        self.assertEqual(len(df2.columns), len(df.columns))
        self.assertTrue('customer' not in df2.columns)
        self.assertTrue('customer_new' in df2.columns)

    def test_groupby(self):
        df = self.cylinder_bands

        # with agg
        df1 = df.groupby(df.customer).agg({'timestamp': 'max', 'job_number': 'avg'})
        self.assertGreater(len(df1), 10)
        self.assertEqual(len(df1.columns), 3)

        df1 = df.groupby(df.customer).agg(functions.max(df.timestamp), functions.min('job_number'))
        self.assertGreater(len(df1), 10)
        self.assertEqual(len(df1.columns), 3)

        with self.assertRaises(ValueError) as ex:
            df.groupby(df.customer).agg({'timestamp': 'max', 'job_number': 'wrong_function'})
            self.assertIn('Unsupported aggregation function', ex.exception)

        with self.assertRaises(ServerException) as ex:
            df.groupby(df.customer).agg({'timestamp': 'max', 'job_number_wrong': 'count'})
            self.assertIn('Spark query analysis exception', ex.exception)

        # with count
        df1 = df.groupby(df.customer).count()
        self.assertGreater(len(df1), 10)
        self.assertEqual(len(df1.columns), 2)

        # with min
        df1 = df.groupby(df.customer).min()
        self.assertGreater(len(df1), 10)
        self.assertEqual(len(df1.columns), 25)
        df1 = df.groupby(df.customer).min('job_number', 'hardener')
        self.assertGreater(len(df1), 10)
        self.assertEqual(len(df1.columns), 3)

        with self.assertRaises(ValueError) as ex:
            df.groupby(df.customer).min('job_number_wrong')
            self.assertIn('Column not found', ex.exception)

        with self.assertRaises(ServerException) as ex:
            df.groupby(df.customer).min(df.proof_on_ctd_ink)
            self.assertIn('"proof_on_ctd_ink" is not a numeric column', ex.exception)

        # with max
        df1 = df.groupby(df.customer).max()
        self.assertGreater(len(df1), 10)
        self.assertEqual(len(df1.columns), 25)
        df1 = df.groupby(df.customer).max('job_number', 'hardener')
        self.assertGreater(len(df1), 10)
        self.assertEqual(len(df1.columns), 3)

        # with avg
        df1 = df.groupby(df.customer).avg()
        self.assertGreater(len(df1), 10)
        self.assertEqual(len(df1.columns), 25)
        df1 = df.groupby(df.customer).mean('job_number', 'hardener')
        self.assertGreater(len(df1), 10)
        self.assertEqual(len(df1.columns), 3)

        # with sum
        df1 = df.groupby(df.customer).sum()
        self.assertGreater(len(df1), 10)
        self.assertEqual(len(df1.columns), 25)
        df1 = df.groupby(df.customer).sum('job_number', 'hardener')
        self.assertGreater(len(df1), 10)
        self.assertEqual(len(df1.columns), 3)

        # pivot
        df1 = df.groupby(df.customer).pivot('proof_on_ctd_ink').count()
        self.assertGreater(len(df1), 10)
        self.assertEqual(len(df1.columns), 4)
        df1 = df.groupby(df.customer).pivot('proof_on_ctd_ink', values=['YES', 'NO']).count()
        self.assertGreater(len(df1), 10)
        self.assertEqual(len(df1.columns), 3)

    def test_rollup(self):
        df = self.cylinder_bands

        df1 = df.rollup(df.customer, 'proof_on_ctd_ink').count()
        self.assertGreater(len(df1), 10)
        self.assertEqual(len(df1.columns), 3)

        df1 = df.rollup(df.customer, 'proof_on_ctd_ink').agg({'hardener': 'max', 'wax': 'avg'})
        self.assertGreater(len(df1), 10)
        self.assertEqual(len(df1.columns), 4)

    def test_cube(self):
        df = self.cylinder_bands

        df1 = df.cube(df.customer, 'proof_on_ctd_ink').count()
        self.assertGreater(len(df1), 10)
        self.assertEqual(len(df1.columns), 3)

        df1 = df.cube(df.customer, 'proof_on_ctd_ink').agg({'hardener': 'max', 'wax': 'avg'})
        self.assertGreater(len(df1), 10)
        self.assertEqual(len(df1.columns), 4)

    def test_agg(self):
        df = self.cylinder_bands

        df1 = df.agg(*[functions.count(c).alias(c) for c in df.columns])
        self.assertEqual(len(df1), 1)
        self.assertListEqual(df1.columns, df.columns)

    """
    Data exploration
    """

    def test_sort(self):
        df1 = self.cylinder_bands

        df2 = df1.sort()
        self.assertNotEqual(df1.id, df2.id)
        self.assertEqual(len(df1), len(df2))

        df2 = df1.sort(df1['timestamp'].asc, df1['customer'].desc)
        self.assertNotEqual(df1.id, df2.id)
        self.assertEqual(len(df1), len(df2))
        sample2 = df2.take(50).to_pandas()
        self.assertTrue(all(sample2.loc[i, 'timestamp'] <= sample2.loc[i + 1, 'timestamp']
                            for i in range(len(sample2) - 1)))

        df2 = df1.sort('timestamp', df1['customer'].desc)
        self.assertNotEqual(df1.id, df2.id)
        self.assertEqual(len(df1), len(df2))

        with self.assertRaises(ValueError) as ex:
            df1.sort('non_exist')
            self.assertTrue('Column not found' in '{}'.format(ex.exception))

    def test_drop(self):
        df = self.cylinder_bands

        df1 = df.drop(df.customer, 'cylinder_number', functions.col('wax'))
        self.assertEqual(len(df1), len(df))
        self.assertEqual(len(df1.columns) + 3, len(df.columns))

    def test_drop_duplicates(self):
        df = self.cylinder_bands
        n = len(df)

        df1 = df.drop_duplicates()
        self.assertEqual(len(df1), n)

        # only consider the `customer` column
        df1 = df.drop_duplicates(df.customer)
        self.assertTrue(10 < len(df1) < n)

        df1 = df.drop_duplicates(df.customer, 'cylinder_number')
        self.assertTrue(10 < len(df1) < n)

    def test_dropna(self):
        df = self.cylinder_bands
        n = len(df)

        df1 = df.dropna()
        self.assertTrue(10 < len(df1) < n)
        df1 = df.dropna(how='all')
        self.assertEqual(len(df1), n)
        df1 = df.dropna(thresh=38)
        self.assertTrue(10 < len(df1) < n)
        df1 = df.dropna(thresh=2, columns=[df.wax, 'proof_on_ctd_ink', df.hardener])
        self.assertTrue(10 < len(df1) < n)

    def test_fillna(self):
        df = self.cylinder_bands
        n = len(df)

        df1 = df.fillna(value=0, columns=[df.current_density])
        self.assertEqual(len(df1), n)
        self.assertListEqual(df1.columns, df.columns)

        df1 = df.fillna(value={'current_density': 0.0, 'chrome_content': 'empty'})
        self.assertEqual(len(df1), n)
        self.assertListEqual(df1.columns, df.columns)

        with self.assertRaises(ValueError):
            df.fillna(value=[2, 3, 4])


if __name__ == '__main__':
    unittest.main()
