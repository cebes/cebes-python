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
from pycebes.core.schema import Schema
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

        # getitem works
        self.assertIsInstance(df[col_name], Column)

    def test_get_tags(self):
        tags = self.session.tags(max_count=10)
        self.assertIsInstance(tags, list)

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
        df1 = self.cylinder_bands.sample(fraction=0.5)
        self.assertIsInstance(df1, Dataframe)
        self.assertListEqual(df1.columns, self.cylinder_bands.columns)
        ratio = float(len(df1)) / len(self.cylinder_bands)
        self.assertTrue(0.45 < ratio)
        self.assertTrue(0.55 >= ratio)

        with self.assertRaises(ServerException) as cm:
            self.cylinder_bands.sample(fraction=-0.6)
            self.assertTrue('Fraction must be nonnegative, but got -0.6' in cm.exception.message)

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
        self.assertEqual(len(df1.columns), 4)

if __name__ == '__main__':
    unittest.main()
