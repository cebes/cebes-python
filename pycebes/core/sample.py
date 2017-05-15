from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import six
import pandas as pd
from pycebes.core.schema import Schema
import datetime


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
            cols.append([cls._parse_value(x) for x in c])
        return DataSample(schema=schema, data=cols)

    @classmethod
    def _parse_value(cls, v):
        """
        Private helper to parse json values from server into python types
        """
        if v is None or v is True or v is False or isinstance(v, six.text_type):
            # JsNull, JsBoolean, JsString
            return v

        if not isinstance(v, dict) or 'type' not in v or 'data' not in v:
            raise ValueError('Expected a dict, got {!r}'.format(v))

        t = v['type']
        data = v['data']

        if t in ('byte', 'short', 'int', 'long'):
            return int(data)

        if t in ('float', 'double'):
            return float(data)

        if t == 'timestamp':
            # server return timestamp in milliseconds, which is not the python convention
            return float(data) / 1E3

        if t == 'date':
            # server return timestamp in milliseconds
            return datetime.date.fromtimestamp(float(data) / 1E3)

        if t == 'byte_array':
            return bytearray([int(x) for x in data])

        if t in ('wrapped_array', 'seq'):
            return [cls._parse_value(x) for x in data]

        if t == 'map':
            d = {}
            for entry in data:
                if 'key' not in entry or 'val' not in entry:
                    raise ValueError('Invalid map entry: {!r}'.format(entry))
                d[cls._parse_value(entry['key'])] = cls._parse_value(entry['val'])

        raise ValueError('Failed to parse value: {!r}'.format(v))
