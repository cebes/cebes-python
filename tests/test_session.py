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

import pandas as pd
import six

from pycebes.core import pipeline_api as pl
from pycebes.core.dataframe import Dataframe
from pycebes.core.exceptions import ServerException
from pycebes.core.pipeline import Pipeline
from pycebes.core.schema import StorageTypes
from pycebes.core.session import Session, CsvReadOptions, JsonReadOptions
from tests import config as test_config
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

    def test_from_pandas(self):
        pandas_df = pd.read_csv(os.path.join(os.path.split(__file__)[0], 'data', 'cylinder_bands.csv'),
                                header=None,
                                names=['timestamp', 'cylinder_number', 'customer', 'job_number', 'grain_screened',
                                       'ink_color', 'proof_on_ctd_ink', 'blade_mfg', 'cylinder_division',
                                       'paper_type', 'ink_type', 'direct_steam', 'solvent_type', 'type_on_cylinder',
                                       'press_type', 'press', 'unit_number', 'cylinder_size', 'paper_mill_location',
                                       'plating_tank', 'proof_cut', 'viscosity', 'caliper', 'ink_temperature',
                                       'humifity', 'roughness', 'blade_pressure', 'varnish_pct', 'press_speed FLOAT',
                                       'ink_pct', 'solvent_pct', 'esa_voltage', 'esa_amperage', 'wax',
                                       'hardener', 'roller_durometer', 'current_density', 'anode_space_ratio',
                                       'chrome_content', 'band_type'])
        cebes_df = self.session.from_pandas(pandas_df)
        self.assertEqual(len(pandas_df), len(cebes_df))
        self.assertListEqual(list(pandas_df.columns), cebes_df.columns)
        self.assertEqual(cebes_df.schema['timestamp'].storage_type, StorageTypes.INTEGER)
        self.assertEqual(cebes_df.schema['anode_space_ratio'].storage_type, StorageTypes.DOUBLE)

    @unittest.skipUnless(test_config.ENABLE_DOCKER_TESTS, 'Docker tests are disabled')
    def test_docker_container(self):
        """
        Tests for automatically start Cebes containers
        """
        s1 = Session(host=None)
        df = self.load_data(s1)[0]
        s1.dataframe.tag(df, 'test_df')
        s1_dfs = s1.dataframe.list()
        self.assertGreater(len(s1_dfs), 0)

        # s2 should be talking to the same Cebes server with s1
        s2 = Session(host=None)
        self.assertEqual(repr(s1), repr(s2))
        s2_dfs = s2.dataframe.list()
        self.assertEqual(len(s1_dfs), len(s2_dfs))
        self.assertTrue(all(x.id == y.id for x, y in zip(s1_dfs.tagged_objects, s2_dfs.tagged_objects)))

        s1.dataframe.untag('test_df')
        s1.close()

        # any call on s2 should fail
        with self.assertRaises(ConnectionError):
            s2.dataframe.list()

    @unittest.skipUnless(test_config.ENABLE_DOCKER_TESTS, 'Docker tests are disabled')
    def test_local_pipeline_repository(self):
        s1 = Session(host=None)
        s1.start_repository_container()

        s1.pipeline.login()

        with Pipeline() as ppl:
            inp_df = pl.placeholder(pl.PlaceholderTypes.DATAFRAME)
            inp_col = pl.placeholder(pl.PlaceholderTypes.VALUE)
            assembler = pl.vector_assembler(inp_df, [''], inp_col)
            pl.linear_regression(assembler.output_df, features_col='features',
                                 label_col='caliper', prediction_col='caliper_predict', reg_param=0.001)

        # run just to create the pipeline on the server
        ppl.run()
        self.assertIsNotNone(ppl.id)

        test_tag = 'pycebes-test-local-ppl:default'
        try:
            s1.pipeline.untag(test_tag)
        except ServerException:
            pass
        s1.pipeline.tag(ppl, test_tag)
        self.assertTrue(any(entry.tag == test_tag for entry in s1.pipeline.list().tagged_objects))

        # push with the pipeline object or the tag
        print(s1.pipeline.push(ppl))
        print(s1.pipeline.push(test_tag))

        with self.assertRaises(ServerException) as ex:
            s1.pipeline.pull(test_tag)
        self.assertIn('Tag pycebes-test-local-ppl:default already exists', '{}'.format(ex.exception))

        # untag the existing pipeline, then pull
        s1.pipeline.untag(test_tag)
        self.assertTrue(all(entry.tag != test_tag for entry in s1.pipeline.list().tagged_objects))

        s1.pipeline.pull(test_tag)
        self.assertTrue(any(entry.tag == test_tag for entry in s1.pipeline.list().tagged_objects))

        ppl = s1.pipeline.get(test_tag)
        self.assertIsInstance(ppl, Pipeline)
        s1.pipeline.untag(test_tag)


if __name__ == '__main__':
    unittest.main()
