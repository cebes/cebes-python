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

if __name__ == '__main__':
    unittest.main()
