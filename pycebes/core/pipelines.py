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

import enum
import functools
from collections import namedtuple

import six

from pycebes.core.column import Column
from pycebes.core.dataframe import Dataframe
from pycebes.core.sample import DataSample
from pycebes.internal.serializer import to_json

_Slot = namedtuple('_Slot', ('name', 'message_type', 'server_name'))
_SlotDescriptor = namedtuple('_SlotDescriptor', ('parent', 'name', 'message_type'))


class _MessageTypes(enum.Enum):
    VALUE = 'ValueDef'
    STAGE_OUTPUT = 'StageOutputDef'
    DATAFRAME = 'DataframeMessageDef'
    SAMPLE = 'SampleMessageDef'
    MODEL = 'ModelMessageDef'
    COLUMN = 'ColumnDef'

    def is_valid(self, value):
        """
        Check if the given value is valid with the Message type
        :param value: value to be checked
        """
        _types = {_MessageTypes.VALUE: (int, float, six.text_type),
                  _MessageTypes.DATAFRAME: Dataframe,
                  _MessageTypes.SAMPLE: DataSample,
                  _MessageTypes.MODEL: None,
                  _MessageTypes.COLUMN: Column}

        for k, v in _types.items():
            if self == k:
                assert v is not None
                return isinstance(value, v)

        assert self == _MessageTypes.STAGE_OUTPUT
        return isinstance(value, _SlotDescriptor) and value.message_type == self

    def to_json(self, value):
        """
        Serialize the given value to JSON format, given the message type
        """
        assert value is None or self.is_valid(value)

        if value is None:
            js = None
        elif self == _MessageTypes.VALUE:
            js = to_json(value)
        elif self == _MessageTypes.STAGE_OUTPUT:
            js = {'stageName': value.parent, 'outputName': value.name}
        elif self == _MessageTypes.DATAFRAME:
            js = value.id
        elif self == _MessageTypes.SAMPLE:
            raise NotImplementedError()
        elif self == _MessageTypes.MODEL:
            raise NotImplementedError()
        else:
            assert self == _MessageTypes.COLUMN
            js = value.to_json()
        return [self.value, js]


def input_slot(name='', message_type=_MessageTypes.VALUE, server_name=None):
    def decorate(cls):
        class_name = cls.__name__
        if class_name not in cls.INPUT_SLOTS:
            cls.INPUT_SLOTS[class_name] = []

        s = _Slot(name, message_type, name if server_name is None else server_name)
        if next((p for p in cls.INPUT_SLOTS[class_name] if p.name == s.name), None) is not None:
            raise ValueError('Duplicated input slot named {} in class {}'.format(s.name, class_name))
        cls.INPUT_SLOTS[class_name].append(s)

        def prop_get(slot_name, self):
            return getattr(self, '_{}'.format(slot_name), None)

        def prop_set(slot_name, self, value):
            sl = cls.input_slot(slot_name)
            if not sl.message_type.is_valid(value):
                raise ValueError('Invalid value for slot {}.{}: expect type {}, got {!r}'.format(
                    self.__class__.__name__, slot_name, s.message_type.value, value))
            setattr(self, '_{}'.format(slot_name), value)

        setattr(cls, s.name, property(fget=functools.partial(prop_get, s.name),
                                      fset=functools.partial(prop_set, s.name)))
        return cls

    return decorate


def output_slot(name='', message_type=_MessageTypes.VALUE, server_name=None):
    def decorate(cls):
        assert issubclass(cls, Stage)

        class_name = cls.__name__
        if class_name not in cls.OUTPUT_SLOTS:
            cls.OUTPUT_SLOTS[class_name] = []

        s = _Slot(name, message_type, name if server_name is None else server_name)
        if next((p for p in cls.OUTPUT_SLOTS[class_name] if p.name == s.name), None) is not None:
            raise ValueError('Duplicated output slot named {} in class {}'.format(s.name, class_name))
        cls.OUTPUT_SLOTS[class_name].append(s)

        def prop_get(slot_name, self):
            sl = cls.output_slot(slot_name)
            return _SlotDescriptor(parent=self.name, name=slot_name, message_type=sl.message_type)

        setattr(cls, s.name, property(fget=functools.partial(prop_get, s.name)))
        return cls

    return decorate


@input_slot('name', _MessageTypes.VALUE)
@six.python_2_unicode_compatible
class Stage(object):
    INPUT_SLOTS = {}
    OUTPUT_SLOTS = {}

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            self.input_slot(k)
            setattr(self, k, v)

    def __repr__(self):
        slot_desc = ','.join('{}={}'.format(s.name, getattr(self, s.name)) for s in self._slots(True))
        return '{}({})'.format(self.__class__.__name__, slot_desc)

    @classmethod
    def _slots(cls, input_slots=False):
        d = cls.INPUT_SLOTS if input_slots else cls.OUTPUT_SLOTS
        slots = []
        for parent_class in cls.__mro__:
            slots.extend(d.get(parent_class.__name__, []))
        return slots

    @classmethod
    def input_slot(cls, slot_name=''):
        """
        Return the input slot with the given name
        :rtype: _Slot
        """
        s = next((s for s in cls._slots(True) if s.name == slot_name), None)
        if s is None:
            raise ValueError('Input slot name {} not found in class {}'.format(slot_name, cls.__name__))
        return s

    @classmethod
    def output_slot(cls, slot_name=''):
        """
        Return the output slot with the given name
        :rtype:: _Slot
        """
        s = next((s for s in cls._slots(False) if s.name == slot_name), None)
        if s is None:
            raise ValueError('Output slot name {} not found in class {}'.format(slot_name, cls.__name__))
        return s

    def to_json(self):
        return {'name': self.name,
                'stageClass': self.__class__.__name__,
                'inputs': {s.name: s.message_type.to_json(getattr(self, s.name)) for s in self._slots(True)},
                'outputs': {}}


@input_slot('input_df', _MessageTypes.DATAFRAME, 'inputDf')
@output_slot('output_df', _MessageTypes.DATAFRAME, 'outputDf')
class _UnaryTransformer(Stage):
    def __init__(self, **kwargs):
        super(_UnaryTransformer, self).__init__(**kwargs)


@input_slot('left_df', _MessageTypes.DATAFRAME, 'leftDf')
@input_slot('right_df', _MessageTypes.DATAFRAME, 'rightDf')
@output_slot('output_df', _MessageTypes.DATAFRAME, 'outputDf')
class _BinaryTransformer(Stage):
    def __init__(self, **kwargs):
        super(_BinaryTransformer, self).__init__(**kwargs)
