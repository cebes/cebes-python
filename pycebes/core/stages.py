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
from pycebes.internal.helpers import require
from pycebes.internal.serializer import to_json

# _Slot is the internal representation of a slot, belong to the class, not the object
# It is used to keep information about how to communicate with the server
_Slot = namedtuple('_Slot', ('name', 'message_type', 'is_input', 'server_name'))


class MessageTypes(enum.Enum):
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
        _types = {MessageTypes.VALUE: (int, float, six.text_type, list, tuple, dict),
                  MessageTypes.DATAFRAME: Dataframe,
                  MessageTypes.SAMPLE: DataSample,
                  MessageTypes.COLUMN: Column}

        for k, value_type in _types.items():
            if self == k:
                if isinstance(value, SlotDescriptor):
                    # in case  the value is a output slot, its type
                    # must be the same with the type of this message type
                    return value.message_type == k
                return isinstance(value, value_type)

        if self == MessageTypes.MODEL:
            # TODO: implement models
            raise NotImplementedError()

        assert self == MessageTypes.STAGE_OUTPUT
        return isinstance(value, SlotDescriptor)

    def to_json(self, value):
        """
        Serialize the given value to JSON format, given the message type
        """
        assert value is None or self.is_valid(value)

        if value is None:
            js = None
        elif self == MessageTypes.VALUE:
            # special json logic
            if isinstance(value, (list, tuple)):
                js = to_json(value, param_type='array')
            else:
                js = to_json(value)
        elif self == MessageTypes.STAGE_OUTPUT:
            js = value.to_json()
        elif self == MessageTypes.DATAFRAME:
            js = {'dfId': value.id}
        elif self == MessageTypes.SAMPLE:
            raise NotImplementedError()
        elif self == MessageTypes.MODEL:
            raise NotImplementedError()
        else:
            assert self == MessageTypes.COLUMN
            js = value.to_json()
        return [self.value, js]


class SlotDescriptor(object):
    """
    SlotDescriptor belong to the object, not the class
    It contains the name of the stage it belongs to
    """

    def __init__(self, parent, name='', is_input=True):
        """
        Construct a new SlotDescriptor

        :param parent: the parent Stage object
        :type parent: Stage
        :param name: name of this slot
        :type name: six.text_type
        :param is_input: Whether this is an input slot
        """
        self.parent = parent
        self.name = name
        self.is_input = is_input

    def __repr__(self):
        return '{}(parent-name={!r},name={!r},is_input={!r})'.format(
            self.__class__.__name__, self.parent_name, self.name, self.is_input)

    @property
    def parent_name(self):
        """Returns the name of the parent stage"""
        return self.parent.get_input(self.parent.name)

    @property
    def _parent_slot(self):
        """The ``_Slot`` object correspond to this SlotDescriptor.
        Computed by looking up the parent"""
        return self.parent.slot(self.name, self.is_input)

    @property
    def server_name(self):
        """Return the server name of this slot. Computed by looking up the slot in the parent"""
        return self._parent_slot.server_name

    @property
    def full_name(self):
        """Return the fully qualified name, normally in the format ``<parent name>:<server name>``"""
        return '{}:{}'.format(self.parent_name, self.name)

    @property
    def message_type(self):
        """Return the message type of this slot.
        Computed by looking up the parent

        :rtype: MessageTypes
        """
        return self._parent_slot.message_type

    def to_json(self):
        """
        Return the JSON representation of this slot descriptor
        """
        return {'stageName': self.parent.get_input(self.parent.name),
                'outputName': self.server_name}

####################################################################################

####################################################################################


def _get_slots(cls, is_input=True):
    """
    Get the list of slots of the given class
    """
    slots = []
    for parent_class in cls.mro():
        ss = getattr(cls, 'SLOTS', {})
        cls_slots = [p for p in ss.get(parent_class.__name__, []) if p.is_input == is_input]
        slots.extend(cls_slots)
    return slots


def _add_slot(name='', message_type=MessageTypes.VALUE, server_name=None, is_input=True):
    def decorate(cls):
        class_name = cls.__name__
        if class_name not in cls.SLOTS:
            cls.SLOTS[class_name] = []

        s = _Slot(name, message_type, is_input, name if server_name is None else server_name)
        if next((p for p in _get_slots(cls, is_input) if p.name == s.name), None) is not None:
            raise ValueError('Duplicated slot named {} in class {}'.format(s.name, class_name))
        cls.SLOTS[class_name].append(s)

        def prop_get(slot_name, is_input_slot, self):
            slot_desc_attr = '_{}'.format(slot_name)
            slot_desc = getattr(self, slot_desc_attr, None)
            if slot_desc is None:
                slot_desc = SlotDescriptor(self, slot_name, is_input=is_input_slot)
                setattr(self, slot_desc_attr, slot_desc)
            return slot_desc

        setattr(cls, s.name, property(fget=functools.partial(prop_get, s.name, is_input)))
        return cls

    return decorate


def input_slot(name='', message_type=MessageTypes.VALUE, server_name=None):
    return _add_slot(name=name, message_type=message_type, server_name=server_name, is_input=True)


def output_slot(name='', message_type=MessageTypes.VALUE, server_name=None):
    return _add_slot(name=name, message_type=message_type, server_name=server_name, is_input=False)


####################################################################################

####################################################################################

@input_slot('name', MessageTypes.VALUE)
@six.python_2_unicode_compatible
class Stage(object):
    SLOTS = {}

    def __init__(self):
        self._slot_values = {}

        # Name is special. We need this to avoid infinite recursion when looking up other slots
        self._name = SlotDescriptor(self, 'name', True)

    def __repr__(self):
        slot_desc = ','.join('{}={!r}'.format(s.name, self.get_input(getattr(self, s.name)))
                             for s in _get_slots(self.__class__, True))
        return '{}({})'.format(self.__class__.__name__, slot_desc)

    @classmethod
    def slot(cls, slot_name='', is_input=True):
        """
        Return the slot with the given name
        :rtype: _Slot
        """
        s = next((s for s in _get_slots(cls, is_input) if s.name == slot_name), None)
        if s is None:
            raise ValueError('Slot name {} not found in class {}'.format(slot_name, cls.__name__))
        return s

    def set_inputs(self, **kwargs):
        """
        Set values to the input slots of this stage.
        Iteratively call ``self.set_input()`` on the kwargs arguments

        :param kwargs: list of ``<slot-name>=value`` entries
        :return: this stage
        """
        for k, v in kwargs.items():
            self.set_input(getattr(self, k), v)
        return self

    def set_name(self, new_name):
        """
        Set the name of this stage. Shortcut for ``self.set_input(self.name, new_name)``

        :param new_name: Name of the stage
        :type new_name: six.string
        """
        return self.set_input(self.name, new_name)

    def get_name(self):
        """Returns the name of this stage, shortcut for ``self.get_input(self.name)``
        """
        return self.get_input(self.name)

    def set_input(self, slot_desc, value):
        """
        Set the value of the given slot descriptor

        :param slot_desc: The slot descriptor to set the value to
        :type slot_desc: SlotDescriptor
        :param value:
        :return: this instance
        """
        require(isinstance(slot_desc, SlotDescriptor) and slot_desc.parent is self and slot_desc.is_input,
                'Not a slot descriptor or it does not belong to this instance: {!r}'.format(slot_desc))
        require(slot_desc.message_type.is_valid(value),
                'Invalid type of value {!r} for slot {}'.format(value, slot_desc.full_name))
        self._slot_values[slot_desc.name] = value
        return self

    def get_input(self, slot_desc, default=None):
        """
        Get the value of the given slot descriptor

        :param slot_desc:
        :type slot_desc: SlotDescriptor
        :param default: default value to  return if the given slot is not specified
        :return: the value
        """
        if not isinstance(slot_desc, SlotDescriptor) or slot_desc.parent is not self or (not slot_desc.is_input):
            raise ValueError('Not a slot descriptor or it does not belong to this instance: {}'.format(slot_desc))
        return self._slot_values.get(slot_desc.name, default)

    def to_json(self):
        inputs = {}
        for s in _get_slots(self.__class__, True):
            if s.name != 'name':
                # skip the name slot
                slot_desc = getattr(self, s.name)
                v = self.get_input(slot_desc)
                if v is not None:
                    inputs[s.server_name] = s.message_type.to_json(v)

        return {'name': self.get_input(self.name),
                'stageClass': self.__class__.__name__,
                'inputs': inputs,
                'outputs': {}}


@input_slot('input_df', MessageTypes.DATAFRAME, 'inputDf')
@output_slot('output_df', MessageTypes.DATAFRAME, 'outputDf')
class _UnaryTransformer(Stage):
    pass


@input_slot('left_df', MessageTypes.DATAFRAME, 'leftDf')
@input_slot('right_df', MessageTypes.DATAFRAME, 'rightDf')
@output_slot('output_df', MessageTypes.DATAFRAME, 'outputDf')
class _BinaryTransformer(Stage):
    pass


@input_slot('input_df', MessageTypes.DATAFRAME, 'inputDf')
@output_slot('model', MessageTypes.MODEL)
@output_slot('output_df', MessageTypes.DATAFRAME, 'outputDf')
class _Estimator(Stage):
    pass

####################################################################################

####################################################################################


@input_slot('col_names', MessageTypes.VALUE, server_name='colNames')
class Drop(_UnaryTransformer):
    pass
