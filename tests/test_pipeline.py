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

from pycebes.core import pipeline_api as pl
from pycebes.core.dataframe import Dataframe
from pycebes.core.exceptions import ServerException
from pycebes.core.pipeline import Pipeline, Model
from tests import test_base


class TestPipeline(test_base.TestBase):

    def test_stage_general(self):
        df = self.cylinder_bands
        with Pipeline():
            s = pl.drop(df, ['hardener', 'customer'])
            name = s.get_name()
            self.assertIsNotNone(name)

            with self.assertRaises(ValueError):
                pl.drop(df, ['customer'], name=name)

    def test_drop(self):
        df = self.cylinder_bands
        with Pipeline() as ppl:
            d = pl.drop(df, ['hardener', 'customer'], name='drop_stage')

        df2 = ppl.run(d.output_df)
        self.assertIsInstance(df2, Dataframe)
        self.assertEqual(len(df2.columns) + 2, len(df.columns))
        self.assertTrue('hardener' not in df2.columns)
        self.assertTrue('customer' not in df2.columns)

        # magic methods
        self.assertTrue(d in ppl)
        self.assertTrue('drop_stage' in ppl)
        self.assertEqual(d, ppl['drop_stage'])

    def test_placeholder(self):
        with Pipeline() as ppl:
            data = pl.placeholder(pl.PlaceholderTypes.DATAFRAME)
            d = pl.drop(df=data, col_names=['hardener', 'customer'])

        with self.assertRaises(ServerException) as ex:
            ppl.run(d.output_df)
        self.assertTrue('Input slot inputVal is undefined' in '{}'.format(ex.exception))

        df = self.cylinder_bands
        df2 = ppl.run(d.output_df, feeds={data: df})
        self.assertIsInstance(df2, Dataframe)
        self.assertEqual(len(df2.columns) + 2, len(df.columns))
        self.assertTrue('hardener' not in df2.columns)
        self.assertTrue('customer' not in df2.columns)

    def test_value_placeholder(self):
        with Pipeline() as ppl:
            data = pl.placeholder(pl.PlaceholderTypes.DATAFRAME)
            cols = pl.placeholder(pl.PlaceholderTypes.VALUE, value_type='array')
            d = pl.drop(df=data, col_names=cols)

        with self.assertRaises(ServerException) as ex:
            ppl.run(d.output_df)
        self.assertTrue('Input slot inputVal is undefined' in '{}'.format(ex.exception))

        df = self.cylinder_bands
        df2 = ppl.run(d.output_df, feeds={data: df, cols: ['hardener', 'customer']})
        self.assertIsInstance(df2, Dataframe)
        self.assertEqual(len(df2.columns) + 2, len(df.columns))
        self.assertTrue('hardener' not in df2.columns)
        self.assertTrue('customer' not in df2.columns)

    def test_linear_regression_with_vector_assembler(self):
        df = self.cylinder_bands
        self.assertGreater(len(df), 10)
        df = df.dropna(columns=['viscosity', 'proof_cut', 'caliper'])
        self.assertGreater(len(df), 10)

        with Pipeline() as ppl:
            assembler = pl.vector_assembler(df, ['viscosity', 'proof_cut'], 'features')
            s = pl.linear_regression(assembler.output_df, features_col='features',
                                     label_col='caliper', prediction_col='caliper_predict', reg_param=0.001)

        r = ppl.run([s.output_df, s.model, assembler.output_df])

        self.assertEqual(len(r), 3)
        df1 = r[0]
        self.assertIsInstance(df1, Dataframe)
        self.assertEqual(len(df1), len(df))
        self.assertEqual(len(df1.columns), len(df.columns) + 2)
        self.assertTrue('features' in df1.columns)
        self.assertTrue('caliper_predict' in df1.columns)

        m = r[1]
        self.assertIsInstance(m, Model)
        self.assertEqual(m.inputs['reg_param'], 0.001)
        self.assertIsInstance(m.metadata, dict)

        df2 = r[2]
        self.assertIsInstance(df2, Dataframe)
        self.assertEqual(len(df2), len(df))
        self.assertEqual(len(df2.columns), len(df.columns) + 1)
        self.assertTrue('features' in df2.columns)

    def test_linear_regression_with_vector_assembler_with_placeholder(self):

        # define the pipeline
        with Pipeline() as ppl:
            inp = pl.placeholder(pl.PlaceholderTypes.DATAFRAME)
            assembler = pl.vector_assembler(inp, ['viscosity', 'proof_cut'], 'features')
            s = pl.linear_regression(assembler.output_df, features_col='features',
                                     label_col='caliper', prediction_col='caliper_predict', reg_param=0.001)

        # fail because placeholder is not filled
        with self.assertRaises(ServerException) as ex:
            ppl.run([s.output_df, s.model, assembler.output_df])
        self.assertTrue('Input slot inputVal is undefined' in '{}'.format(ex.exception))

        # run again with feeds into the placeholder
        df = self.cylinder_bands.dropna(columns=['viscosity', 'proof_cut', 'caliper'])
        self.assertGreater(len(df), 10)
        r = ppl.run([s.output_df, s.model, assembler.output_df], feeds={inp: df})

        self.assertEqual(len(r), 3)
        df1 = r[0]
        self.assertIsInstance(df1, Dataframe)
        self.assertEqual(len(df1), len(df))
        self.assertEqual(len(df1.columns), len(df.columns) + 2)
        self.assertTrue('features' in df1.columns)
        self.assertTrue('caliper_predict' in df1.columns)
        pandas_df = df1.take(5)
        self.assertEqual(len(pandas_df), 5)

        m = r[1]
        self.assertIsInstance(m, Model)
        self.assertEqual(m.inputs['reg_param'], 0.001)
        self.assertIsInstance(m.metadata, dict)

        df2 = r[2]
        self.assertIsInstance(df2, Dataframe)
        self.assertEqual(len(df2), len(df))
        self.assertEqual(len(df2.columns), len(df.columns) + 1)
        self.assertTrue('features' in df2.columns)

    def test_linear_regression_with_vector_assembler_with_placeholders(self):

        # define the pipeline
        with Pipeline() as ppl:
            inp_df = pl.placeholder(pl.PlaceholderTypes.DATAFRAME)
            inp_col = pl.placeholder(pl.PlaceholderTypes.VALUE)
            assembler = pl.vector_assembler(inp_df, [''], inp_col)
            s = pl.linear_regression(assembler.output_df, features_col='features',
                                     label_col='caliper', prediction_col='caliper_predict', reg_param=0.001)

        df = self.cylinder_bands.dropna(columns=['viscosity', 'proof_cut', 'caliper'])
        self.assertGreater(len(df), 10)

        r = ppl.run([s.output_df, s.model, assembler.output_df],
                    feeds={inp_df: df, inp_col: 'features', assembler.input_cols: ['viscosity', 'proof_cut']})

        self.assertEqual(len(r), 3)
        df1 = r[0]
        self.assertIsInstance(df1, Dataframe)
        self.assertEqual(len(df1), len(df))
        self.assertEqual(len(df1.columns), len(df.columns) + 2)
        self.assertTrue('features' in df1.columns)
        self.assertTrue('caliper_predict' in df1.columns)

        m = r[1]
        self.assertIsInstance(m, Model)
        self.assertEqual(m.inputs['reg_param'], 0.001)
        self.assertIsInstance(m.metadata, dict)

        df2 = r[2]
        self.assertIsInstance(df2, Dataframe)
        self.assertEqual(len(df2), len(df))
        self.assertEqual(len(df2.columns), len(df.columns) + 1)
        self.assertTrue('features' in df2.columns)

        # assemble some other columns
        df = self.cylinder_bands.dropna(columns=['viscosity', 'proof_cut', 'ink_temperature', 'caliper'])
        self.assertGreater(len(df), 10)
        r = ppl.run([s.output_df, s.model, assembler.output_df],
                    feeds={inp_df: df, inp_col: 'new_features',
                           assembler.input_cols: ['viscosity', 'proof_cut', 'ink_temperature'],
                           s.features_col: 'new_features'})

        self.assertEqual(len(r), 3)
        df1 = r[0]
        self.assertIsInstance(df1, Dataframe)
        self.assertEqual(len(df1), len(df))
        self.assertEqual(len(df1.columns), len(df.columns) + 2)
        self.assertTrue('new_features' in df1.columns)
        self.assertTrue('caliper_predict' in df1.columns)

        m = r[1]
        self.assertIsInstance(m, Model)
        self.assertEqual(m.inputs['reg_param'], 0.001)
        self.assertIsInstance(m.metadata, dict)

        df2 = r[2]
        self.assertIsInstance(df2, Dataframe)
        self.assertEqual(len(df2), len(df))
        self.assertEqual(len(df2.columns), len(df.columns) + 1)
        self.assertTrue('new_features' in df2.columns)


if __name__ == '__main__':
    unittest.main()
