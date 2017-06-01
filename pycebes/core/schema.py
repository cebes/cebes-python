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

import datetime
import enum

import six


@enum.unique
class VariableTypes(enum.Enum):
    DISCRETE = 'Discrete'
    CONTINUOUS = 'Continuous'
    NOMINAL = 'Nominal'
    ORDINAL = 'Ordinal'
    TEXT = 'Text'
    DATETIME = 'DateTime'
    ARRAY = 'Array'
    MAP = 'Map'
    STRUCT = 'Struct'

    @classmethod
    def from_str(cls, s):
        v = next((e for e in cls.__members__.values() if e.value == s), None)
        if v is None:
            raise ValueError('Unknown variable type: {!r}'.format(s))
        return v


@enum.unique
class StorageTypes(enum.Enum):
    STRING = ('string', six.text_type)
    BINARY = ('binary', six.binary_type)

    DATE = ('date', datetime.date)
    TIMESTAMP = ('timestamp', int)
    CALENDAR_INTERVAL = ('calendarinterval', datetime.timedelta)

    BOOLEAN = ('boolean', bool)
    SHORT = ('short', int)
    INTEGER = ('integer', int)
    LONG = ('long', int)
    FLOAT = ('float', float)
    DOUBLE = ('double', float)
    VECTOR = ('vector', list)

    # TODO: re-do complex storage types
    ARRAY = ('array', list)
    MAP = ('map', dict)
    STRUCT = ('struct', list)

    @property
    def cebes_type(self):
        return self.value[0]

    @property
    def python_type(self):
        return self.value[1]

    def to_json(self):
        """
        Return the JSON representation of this storage type
        :return:
        """
        return self.cebes_type

    @classmethod
    def from_str(cls, s):
        v = next((e for e in cls.__members__.values() if e.cebes_type == s), None)
        if v is None:
            raise ValueError('Unknown storage type: {!r}'.format(s))
        return v


@six.python_2_unicode_compatible
class SchemaField(object):
    def __init__(self, name='', storage_type=StorageTypes.STRING, variable_type=VariableTypes.TEXT):
        self.name = name
        self.storage_type = storage_type
        self.variable_type = variable_type

    def __repr__(self):
        return '{}(name={!r},storage_type={},variable_type={})'.format(
            self.__class__.__name__, self.name, self.storage_type.name, self.variable_type.name)


@six.python_2_unicode_compatible
class Schema(object):
    def __init__(self, fields=None):
        self.fields = fields

    @property
    def columns(self):
        """A list of column names in this Schema"""
        return [f.name for f in self.fields]

    @property
    def simple_string(self):
        """Return a simple description of this schema, mainly used for brevity"""
        n = 3
        if len(self.fields) <= 0:
            s = '(empty)'
        else:
            s = ', '.join('{} {}'.format(f.name, f.storage_type.cebes_type) for f in self.fields[:n])
            if len(self.fields) > n:
                s += '... (+{} columns)'.format(len(self.fields) - n)
        return s

    def __repr__(self):
        return '{}(fields=[{}])'.format(
            self.__class__.__name__, ','.join('{!r}'.format(f) for f in self.fields))

    def __len__(self):
        return len(self.fields)

    @classmethod
    def from_json(cls, js_data):
        """
        Parse the Schema from its JSON representation

        :param js_data: a dict with a key named ``fields``, 
            which is a list of dict, containing the schema fields. Each field is a dict.
        :type js_data: dict
        :rtype: Schema
        """
        fields = [SchemaField(name=d.get('name', ''),
                              storage_type=StorageTypes.from_str(d.get('storageType', '')),
                              variable_type=VariableTypes.from_str(d.get('variableType', '')))
                  for d in js_data.get('fields', [])]
        return Schema(fields=fields)
