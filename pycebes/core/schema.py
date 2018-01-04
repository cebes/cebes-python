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
from pycebes.internal.helpers import require


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

    def to_json(self):
        """
        Return the JSON representation of this variable type
        :return:
        """
        return self.value


class StorageType(object):
    def __init__(self, cebes_type, python_type, name=None):
        self._name = name
        self._cebes_type = cebes_type
        self._python_type = python_type

    @property
    def name(self):
        return self._name or self._cebes_type

    @property
    def cebes_type(self):
        return self._cebes_type

    @property
    def python_type(self):
        return self._python_type

    def to_json(self):
        return self._cebes_type


class ArrayType(StorageType):

    def __init__(self, element_type):
        """
        # Arguments
        element_type (StorageType): type of the element in this array
        """
        super(ArrayType, self).__init__('Array[{}]'.format(element_type.cebes_type), list,
                                        name='Array[{}]'.format(element_type.name))
        self._element_type = element_type

    @property
    def element_type(self):
        return self._element_type

    def to_json(self):
        return {'elementType': self._element_type.to_json()}

    @staticmethod
    def from_json(js_val):
        require(isinstance(js_val, dict) and 'elementType' in js_val)
        return ArrayType(StorageTypes.from_json(js_val['elementType']))


class MapType(StorageType):

    def __init__(self, key_type, value_type):
        """
        # Arguments
        key_type (StorageType): type of the key
        value_type (StorageType): type of the value
        """
        super(MapType, self).__init__('Map[{}, {}]'.format(key_type.cebes_type, value_type.cebes_type), dict,
                                      name='Map[{}, {}]'.format(key_type.name, value_type.name))
        self._key_type = key_type
        self._value_type = value_type

    @property
    def key_type(self):
        return self._key_type

    @property
    def value_type(self):
        return self._value_type

    def to_json(self):
        return {'keyType': self._key_type.to_json(), 'valueType': self._value_type.to_json()}

    @staticmethod
    def from_json(js_val):
        require(isinstance(js_val, dict) and 'keyType' in js_val and 'valueType' in js_val)
        return MapType(StorageTypes.from_json(js_val['keyType']),
                       StorageTypes.from_json(js_val['valueType']))


class StructField(object):
    def __init__(self, name, storage_type):
        self._name = name
        self._storage_type = storage_type

    @property
    def name(self):
        return self._name

    @property
    def storage_type(self):
        return self._storage_type

    def to_json(self):
        return {'name': self._name, 'storageType': self._storage_type.to_json(), 'metadata': {}}

    @staticmethod
    def from_json(js_val):
        require(isinstance(js_val, dict) and 'name' in js_val and 'storageType' in js_val)
        return StructField(js_val['name'],
                           StorageTypes.from_json(js_val['storageType']))


class StructType(StorageType):

    def __init__(self, fields):
        super(StructType, self).__init__(
            'Struct[{}]'.format(','.join(f.storage_type.cebes_type for f in fields)),
            dict,
            name='Struct[{}]'.format(','.join(f.storage_type.name for f in fields)))
        self._fields = fields

    @property
    def fields(self):
        return self._fields[:]

    def to_json(self):
        return {'fields': [f.to_json() for f in self._fields]}

    @staticmethod
    def from_json(js_val):
        require(isinstance(js_val, dict) and 'fields' in js_val)
        return StructType([StructField.from_json(f) for f in js_val['fields']])


class StorageTypes(object):
    STRING = StorageType('string', six.text_type, name='STRING')
    BINARY = StorageType('binary', six.binary_type, name='BINARY')

    DATE = StorageType('date', datetime.date, name='DATE')
    TIMESTAMP = StorageType('timestamp', int, name='TIMESTAMP')
    CALENDAR_INTERVAL = StorageType('calendarinterval', datetime.timedelta, name='CALENDAR_INTERVAL')

    BOOLEAN = StorageType('boolean', bool, name='BOOLEAN')
    SHORT = StorageType('short', int, name='SHORT')
    INTEGER = StorageType('integer', int, name='INTEGER')
    LONG = StorageType('long', int, name='LONG')
    FLOAT = StorageType('float', float, name='FLOAT')
    DOUBLE = StorageType('double', float, name='DOUBLE')
    VECTOR = StorageType('vector', list, name='VECTOR')

    __atomic_types__ = [STRING, BINARY,
                        DATE, TIMESTAMP, CALENDAR_INTERVAL,
                        BOOLEAN, SHORT, INTEGER, LONG, FLOAT, DOUBLE, VECTOR]

    @staticmethod
    def array(element_type):
        """
        # Arguments
        element_type (StorageType):
        """
        return ArrayType(element_type)

    @staticmethod
    def map(key_type, value_type):
        return MapType(key_type, value_type)

    @staticmethod
    def struct(fields):
        return StructType(fields)

    @classmethod
    def from_json(cls, js_val):
        v = next((e for e in cls.__atomic_types__ if e.cebes_type == js_val), None)
        if v is not None:
            return v

        for clz in [ArrayType, MapType, StructType]:
            try:
                return clz.from_json(js_val)
            except ValueError:
                pass
        raise ValueError('Unknown storage type: {!r}'.format(js_val))


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

    def __getitem__(self, item):
        try:
            return next(f for f in self.fields if f.name == item)
        except StopIteration:
            raise KeyError('Column not found: {!r}'.format(item))

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
                              storage_type=StorageTypes.from_json(d.get('storageType', '')),
                              variable_type=VariableTypes.from_str(d.get('variableType', '')))
                  for d in js_data.get('fields', [])]
        return Schema(fields=fields)
