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

import os
import unittest

import six

from pycebes.core import pipeline_api as pl
from pycebes.core.dataframe import Dataframe
from pycebes.core.exceptions import ServerException
from pycebes.core.pipeline import Pipeline
from pycebes.core.schema import StorageTypes
from pycebes.core.session import CsvReadOptions, JsonReadOptions
from tests import test_base


class TestSession(test_base.TestBase):
    def test_tag_dataframes(self):
        df = self.cylinder_bands

        try:
            self.session.dataframe.untag('tag1')
        except ServerException:
            pass

        self.session.dataframe.tag(df, 'tag1')

        # get tags
        response = self.session.dataframe.list()
        self.assertGreaterEqual(len(response.tagged_objects), 1)
        tag1_entry = next(entry for entry in response.tagged_objects if entry.tag == 'tag1:latest')
        self.assertEqual(tag1_entry.id, df.id)
        self.assertIsInstance(repr(response), six.text_type)

        # get dataframe by tag
        df1 = self.session.dataframe.get('tag1')
        self.assertEqual(df1.id, df.id)

        # get by ID
        df1 = self.session.dataframe.get(df.id)
        self.assertEqual(df1.id, df.id)

        # get random name
        with self.assertRaises(ServerException) as ex:
            self.session.dataframe.get('non-existed')
        self.assertTrue('Tag not found' in '{}'.format(ex.exception))

        # untag
        df1 = self.session.dataframe.untag('tag1')
        self.assertEqual(df1.id, df.id)
        with self.assertRaises(ServerException) as ex:
            self.session.dataframe.untag('tag1')
        self.assertTrue('Tag not found' in '{}'.format(ex.exception))

        # list again, 'tag1' should not exist
        response = self.session.dataframe.list()
        self.assertIsNone(next((entry for entry in response.tagged_objects if entry.tag == 'tag1:latest'), None))

    def test_tag_pipelines_and_models(self):

        with Pipeline() as ppl:
            inp = pl.placeholder(pl.PlaceholderTypes.DATAFRAME)
            assembler = pl.vector_assembler(inp, ['viscosity', 'proof_cut'], 'features')
            s = pl.linear_regression(assembler.output_df, features_col='features',
                                     label_col='caliper', prediction_col='caliper_predict', reg_param=0.001)

        df = self.cylinder_bands.dropna(columns=['viscosity', 'proof_cut', 'caliper'])
        self.assertGreater(len(df), 10)
        _, model, _ = ppl.run([s.output_df, s.model, assembler.output_df], feeds={inp: df})
        self.assertIsNotNone(ppl.id)

        # tagged pipelines
        try:
            self.session.pipeline.untag('tag1')
        except ServerException:
            pass

        self.session.pipeline.tag(ppl, 'tag1')

        # get tags
        response = self.session.pipeline.list()
        self.assertGreaterEqual(len(response.tagged_objects), 1)
        tag1_entry = next(entry for entry in response.tagged_objects if entry.tag == 'tag1:latest')
        self.assertEqual(tag1_entry.id, ppl.id)
        self.assertIsInstance(repr(response), six.text_type)

        # get pipeline by tag
        ppl1 = self.session.pipeline.get('tag1')
        self.assertEqual(ppl1.id, ppl.id)
        self.assertEqual(ppl1.to_json(), ppl.to_json())
        self.assertEqual(ppl1[inp.get_name()].get_name(), inp.get_name())
        self.assertEqual(ppl1[assembler.get_name()].get_name(), assembler.get_name())
        regressor = ppl1[s.get_name()]
        self.assertEqual(regressor.get_input(regressor.prediction_col), 'caliper_predict')

        # get by ID
        ppl1 = self.session.pipeline.get(ppl.id)
        self.assertEqual(ppl1.id, ppl.id)

        # get random name
        with self.assertRaises(ServerException) as ex:
            self.session.pipeline.get('non-existed')
        self.assertTrue('Tag not found' in '{}'.format(ex.exception))

        # untag
        ppl1 = self.session.pipeline.untag('tag1')
        self.assertEqual(ppl1.id, ppl.id)
        with self.assertRaises(ServerException) as ex:
            self.session.pipeline.untag('tag1')
        self.assertTrue('Tag not found' in '{}'.format(ex.exception))

        # list again, 'tag1' should not exist
        response = self.session.pipeline.list()
        self.assertIsNone(next((entry for entry in response.tagged_objects if entry.tag == 'tag1:latest'), None))

        # tagged models
        try:
            self.session.model.untag('tag1')
        except ServerException:
            pass

        self.session.model.tag(model, 'tag1')

        # get tags
        response = self.session.model.list()
        self.assertGreaterEqual(len(response.tagged_objects), 1)
        tag1_entry = next(entry for entry in response.tagged_objects if entry.tag == 'tag1:latest')
        self.assertEqual(tag1_entry.id, model.id)
        self.assertIsInstance(repr(response), six.text_type)

        # get pipeline by tag
        model1 = self.session.model.get('tag1')
        self.assertEqual(model1.id, model.id)

        # get by ID
        model1 = self.session.model.get(model.id)
        self.assertEqual(model1.id, model.id)

        # get random name
        with self.assertRaises(ServerException) as ex:
            self.session.model.get('non-existed')
        self.assertTrue('Tag not found' in '{}'.format(ex.exception))

        # untag
        model1 = self.session.model.untag('tag1')
        self.assertEqual(model1.id, model.id)
        with self.assertRaises(ServerException) as ex:
            self.session.model.untag('tag1')
        self.assertTrue('Tag not found' in '{}'.format(ex.exception))

        # list again, 'tag1' should not exist
        response = self.session.model.list()
        self.assertIsNone(next((entry for entry in response.tagged_objects if entry.tag == 'tag1:latest'), None))

    def test_read_local_csv(self):
        csv_path = os.path.join(os.path.split(__file__)[0], 'data', 'cylinder_bands.csv')
        df = self.session.read_csv(path=csv_path, options=CsvReadOptions())
        self.assertIsInstance(df, Dataframe)
        self.assertEqual(len(df.columns), 40)
        self.assertTrue(all(f.storage_type == StorageTypes.STRING for f in df.schema.fields))

        # with infer schema
        df = self.session.read_csv(path=csv_path, options=CsvReadOptions(infer_schema=True))
        self.assertIsInstance(df, Dataframe)
        self.assertEqual(len(df.columns), 40)
        self.assertFalse(all(f.storage_type == StorageTypes.STRING for f in df.schema.fields))

    def test_read_local_json(self):
        json_path = os.path.join(os.path.split(__file__)[0], 'data', 'cylinder_bands.json')
        with self.assertRaises(ValueError):
            self.session.read_json(path=json_path, options=CsvReadOptions())

        df = self.session.read_json(path=json_path, options=JsonReadOptions())
        self.assertIsInstance(df, Dataframe)
        self.assertEqual(len(df.columns), 40)
        self.assertFalse(all(f.storage_type == StorageTypes.STRING for f in df.schema.fields))

if __name__ == '__main__':
    unittest.main()
