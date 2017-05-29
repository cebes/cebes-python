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

import pandas as pd
import six

from pycebes.core.schema import Schema
from pycebes.internal.serializer import from_json


@six.python_2_unicode_compatible
class DataSample(object):
    """
    A sample of data, with a proper schema
    """

    def __init__(self, schema, data):
        """
        
        :type schema: Schema
        :param data: list of list. Each list is a column
        """
        if len(schema) != len(data):
            raise ValueError('Inconsistent data and schema: '
                             '{} fields in schema with {} data columns'.format(len(schema), len(data)))
        self.schema = schema
        self.data = data

    @property
    def columns(self):
        """
        Return a list of column names in this ``DataSample``
        """
        return self.schema.columns

    def __repr__(self):
        return '{}(schema={!r})'.format(self.__class__.__name__, self.schema)

    def to_pandas(self, raise_if_error=False):
        """
        Return a pandas DataFrame representation of this sample

        :param raise_if_error: whether to raise exception when there is a type-cast error
        :rtype: pd.DataFrame
        """
        df = pd.DataFrame()
        for f, c in zip(self.schema.fields, self.data):
            df[f.name] = c
            df[f.name] = df[f.name].astype(dtype=f.storage_type.python_type,
                                           errors='raise' if raise_if_error else 'ignore')
        return df

    @classmethod
    def from_json(cls, js_data):
        """
        Parse the JSON result from the server

        :param js_data: a dict with a key 'data' for the data part, and 'schema' for the data schema
        :rtype: DataSample
        """
        if 'schema' not in js_data or 'data' not in js_data:
            raise ValueError('Invalid JSON: {}'.format(js_data))

        schema = Schema.from_json(js_data['schema'])
        cols = []
        for c in js_data['data']:
            cols.append([from_json(x) for x in c])
        return DataSample(schema=schema, data=cols)
