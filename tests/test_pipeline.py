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
import unittest

from tests import test_base
from pycebes.core.pipeline import Pipeline
from pycebes.core import pipeline_api as pl
from pycebes.core.dataframe import Dataframe


class TestPipeline(test_base.TestBase):

    def test_drop(self):
        df = self.cylinder_bands
        with Pipeline() as ppl:
            d = pl.drop(df, ['hardener', 'customer'])

        print(ppl.to_json())
        df2 = ppl.run(d.output_df)
        self.assertIsInstance(df2, Dataframe)
        self.assertEqual(len(df2.columns) + 2, len(df.columns))
        self.assertTrue('hardener' not in df2.columns)
        self.assertTrue('customer' not in df2.columns)

if __name__ == '__main__':
    unittest.main()
