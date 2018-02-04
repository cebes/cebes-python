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

import functools
from collections import namedtuple

import six

from pycebes.core.column import Column
from pycebes.core.dataframe import Dataframe
from pycebes.core.sample import DataSample
from pycebes.internal import serializer
from pycebes.internal.helpers import require
from pycebes.internal.implicits import get_default_session

# _Slot is the internal representation of a slot, belong to the class, not the object
# It is used to keep information about how to communicate with the server
_Slot = namedtuple('_Slot', ('name', 'message_type', 'is_input', 'server_name', 'is_default'))


class MessageType(object):
    VALUE_DEF = 'ValueDef'
    STAGE_OUTPUT_DEF = 'StageOutputDef'
    DATAFRAME_DEF = 'DataframeMessageDef'
    SAMPLE_DEF = 'SampleMessageDef'
    MODEL_DEF = 'ModelMessageDef'
    COLUMN_DEF = 'ColumnDef'

    __all_defs = [VALUE_DEF, STAGE_OUTPUT_DEF, DATAFRAME_DEF, SAMPLE_DEF, MODEL_DEF, COLUMN_DEF]

    def __init__(self, msg_type=VALUE_DEF, value_type=None):
        require(msg_type in self.__all_defs, 'Invalid msg_type: {!r}'.format(msg_type))
        self.msg_type = msg_type
        self.value_type = value_type

    def __repr__(self):
        return '{}(msg_type={!r},value_type={!r})'.format(self.__class__.__name__, self.msg_type, self.value_type)

    def is_valid(self, value):
        """
        Check if the given value is valid with the Message type
        :param value: value to be checked
        """
        if value is None:
            return True

        _types = {self.VALUE_DEF: (int, float, six.text_type, list, tuple, dict),
                  self.DATAFRAME_DEF: Dataframe,
                  self.SAMPLE_DEF: DataSample,
                  self.COLUMN_DEF: Column}

        for k, value_type in _types.items():
            if self.msg_type == k:
                if isinstance(value, SlotDescriptor):
                    # in case  the value is a output slot, its type
                    # must be the same with the type of this message type
                    return value.message_type.msg_type == k
                return isinstance(value, value_type)

        if self.msg_type == self.MODEL_DEF:
            # TODO: implement models
            raise NotImplementedError()

        require(self.msg_type == self.STAGE_OUTPUT_DEF, 'Unrecognized message type: {}'.format(self.msg_type))
        return isinstance(value, SlotDescriptor)

    def to_json(self, value):
        """
        Serialize the given value to JSON format, given the message type
        """
        require(self.is_valid(value), 'Invalid value for message type {!r}: {!r}'.format(self, value))

        returned_msg_type = self.msg_type
        if value is None:
            js = None

        elif isinstance(value, SlotDescriptor):
            # stage output message
            require(value.message_type.msg_type == self.msg_type,
                    'Incompatible message type between input slot of type {!r} and '
                    'output slot {!r}'.format(self, value))
            returned_msg_type = self.STAGE_OUTPUT_DEF
            js = value.to_json()

        elif self.msg_type == self.VALUE_DEF:
            js = serializer.to_json(value, param_type=self.value_type)
        elif self.msg_type == self.STAGE_OUTPUT_DEF:
            js = value.to_json()
        elif self.msg_type == self.DATAFRAME_DEF:
            js = {'dfId': value.id}
        elif self.msg_type == self.SAMPLE_DEF:
            raise NotImplementedError()
        elif self.msg_type == self.MODEL_DEF:
            raise NotImplementedError()
        else:
            require(self.msg_type == self.COLUMN_DEF, 'Unrecognized message type: {}'.format(self.msg_type))
            js = value.to_json()
        return [returned_msg_type, js]

    @classmethod
    def from_json(cls, js_data):
        """
        Read a PipelineMessageDef (from server) to the value

        :param js_data: a list of 2 elelements, see ``to_json()`` for more information
        :return: value read from the JSON result
        """
        require(len(js_data) == 2, 'Invalid JSON data: {!r}'.format(js_data))
        msg_type = js_data[0]
        msg_content = js_data[1]

        if msg_type == MessageType.VALUE_DEF:
            return serializer.from_json(msg_content)
        if msg_type == MessageType.DATAFRAME_DEF:
            return get_default_session().dataframe.get(msg_content['dfId'])
        if msg_type == MessageType.MODEL_DEF:
            return get_default_session().model.get(msg_content['modelId'])
        if msg_type == MessageType.COLUMN_DEF:
            raise NotImplementedError('{}'.format(js_data))
        if msg_type == MessageType.STAGE_OUTPUT_DEF:
            # we don't have enough information to return a SlotDescriptor here
            return None

        raise NotImplementedError('{}'.format(js_data))


class MessageTypes(object):
    VALUE = MessageType(MessageType.VALUE_DEF)
    STAGE_OUTPUT = MessageType(MessageType.STAGE_OUTPUT_DEF)
    DATAFRAME = MessageType(MessageType.DATAFRAME_DEF)
    SAMPLE = MessageType(MessageType.SAMPLE_DEF)
    MODEL = MessageType(MessageType.MODEL_DEF)
    COLUMN = MessageType(MessageType.COLUMN_DEF)

    @classmethod
    def value(cls, value_type=None):
        """Same with VALUE, but user has the chance to specify a custom value type for the value"""
        return MessageType(MessageType.VALUE_DEF, value_type=value_type)


##############################################################################

##############################################################################

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
        return '{}(name={!r},is_input={!r})'.format(self.__class__.__name__, self.name, self.is_input)

    @property
    def parent_name(self):
        """Returns the name of the parent stage"""
        return self.parent.get_name()

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
        """Return the fully qualified name, normally in the format ``<parent name>:<name>``"""
        return '{}:{}'.format(self.parent_name, self.name)

    @property
    def full_server_name(self):
        """Return the fully qualified name, normally in the format ``<parent name>:<server name>``"""
        return '{}:{}'.format(self.parent_name, self.server_name)

    @property
    def message_type(self):
        """Return the message type of this slot.
        Computed by looking up the parent

        :rtype: MessageType
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

    :type cls: Type
    """
    slots = []
    for parent_class in cls.mro():
        ss = getattr(cls, 'SLOTS', {})
        cls_slots = [p for p in ss.get(parent_class.__name__, []) if p.is_input == is_input]
        slots.extend(cls_slots)
    return slots


def _add_slot(name='', message_type=MessageTypes.VALUE, server_name=None, doc='', is_input=True, is_default=False):
    def decorate(cls):
        class_name = cls.__name__
        if class_name not in cls.SLOTS:
            cls.SLOTS[class_name] = []

        s = _Slot(name, message_type, is_input, name if server_name is None else server_name, is_default=is_default)
        require(next((p for p in _get_slots(cls, is_input) if p.name == s.name), None) is None,
                'Duplicated slot named {} in class {}'.format(s.name, class_name))
        cls.SLOTS[class_name].append(s)

        def prop_get(slot_name, is_input_slot, self):
            slot_desc_attr = '_{}'.format(slot_name)
            slot_desc = getattr(self, slot_desc_attr, None)
            if slot_desc is None:
                slot_desc = SlotDescriptor(self, slot_name, is_input=is_input_slot)
                setattr(self, slot_desc_attr, slot_desc)
            return slot_desc

        prop_doc = '{} slot of type {}: {}'.format('Input' if is_input else 'Output',
                                                   message_type.msg_type, doc)
        setattr(cls, s.name, property(fget=functools.partial(prop_get, s.name, is_input), doc=prop_doc))
        return cls

    return decorate


def input_slot(name='', message_type=MessageTypes.VALUE, server_name=None, doc=''):
    """
    Create an input slot for the Stage

    :param name: name of the slot
    :param message_type: type of the message used to transport the slot value
    :param server_name: optionally, the name of this input on the server. If not specified,
        it is the same with ``name``.
    :param doc: Brief documentation explaining the slot
    """
    return _add_slot(name=name, message_type=message_type,
                     server_name=server_name, doc=doc, is_input=True)


def output_slot(name='', message_type=MessageTypes.VALUE, server_name=None, doc='', is_default=False):
    """
    Create an output slot for the Stage

    :param name: name of the slot
    :param message_type: type of the message used to transport the slot value
    :param server_name: optionally, the name of this input on the server. If not specified,
        it is the same with ``name``.
    :param doc: Brief documentation explaining the slot
    :param is_default: is this default output slot
    """
    return _add_slot(name=name, message_type=message_type, server_name=server_name, doc=doc,
                     is_input=False, is_default=is_default)


####################################################################################

####################################################################################

@input_slot('name', MessageTypes.VALUE)
@six.python_2_unicode_compatible
class Stage(object):
    SLOTS = {}

    def __init__(self, name='stage', **kwargs):
        """
        Initialize the stage

        :param name: name given to this stage
        :param kwargs: values assigned to the input slots
        """
        self._slot_values = {}

        # Name is special. We need this to avoid infinite recursion when looking up other slots
        require(name is not None, 'Stage name should not be None at creation time')
        self._name = SlotDescriptor(self, 'name', True)
        self._set_input(self.name, name)

        for k, v in kwargs.items():
            self._set_input(self.slot_descriptor(k), v)

    def __repr__(self):
        slot_desc = ','.join('{}={!r}'.format(s.name, self.get_input(self.slot_descriptor(s.name)))
                             for s in _get_slots(self.__class__, True))
        return '{}({})'.format(self.__class__.__name__, slot_desc)

    def __str__(self):
        return super(Stage, self).__str__()

    def __dir__(self):
        return dir(type(self)) + [p.name for p in _get_slots(self.__class__, True) + _get_slots(self.__class__, False)]

    def _set_input(self, slot_desc, value):
        """
        Set the value of the given slot descriptor

        This is a private function for a reason: users are not supposed to change the input slots
        of a stage, once it is already created. The only way to update the slots is to use
        the `feeds` argument in the call to Pipeline.run().

        :param slot_desc: The slot descriptor to set the value to
        :type slot_desc: SlotDescriptor
        :param value:
        :return: this instance
        """
        require(isinstance(slot_desc, SlotDescriptor) and slot_desc.parent is self and slot_desc.is_input,
                'Not a slot descriptor or it does not belong to this instance: {}'.format(slot_desc.name))
        if isinstance(value, SlotDescriptor):
            # output slot of another stage
            require((not value.is_input) and value.message_type.msg_type == slot_desc.message_type.msg_type,
                    'Slot {!r} is not an output or of incompatible type for input slot {}'.format(
                        value, slot_desc.name))
        else:
            require(slot_desc.message_type.is_valid(value),
                    'Invalid type of value {!r} for slot {}'.format(value, slot_desc.name))
        self._slot_values[slot_desc.name] = value
        return self

    """
    Public APIs
    """

    @property
    def default_output_slot_descriptor(self):
        """
        Return the default output slot descriptor of this stage.
        This is used to provide better UX for end-user.

        :rtype: SlotDescriptor
        """
        all_output_slots = _get_slots(self.__class__, is_input=False)
        require(len(all_output_slots) > 0,
                'Stage {}(name={}) has no output slot'.format(self.__class__.__name__, self.get_name()))

        if len(all_output_slots) == 1:
            default_slot = all_output_slots[0]
        else:
            default_slot = next((s for s in all_output_slots if s.is_default), None)
            require(default_slot is not None, 'No default output slot defined for stage {}(name={})'.format(
                self.__class__.__name__, self.get_name()))

        return self.slot_descriptor(default_slot.name)

    def slot(self, slot_name='', is_input=True):
        """
        Return the slot with the given name

        :rtype: _Slot
        """
        s = next((s for s in _get_slots(self.__class__, is_input) if s.name == slot_name), None)
        require(s is not None, 'Slot name {} not found in class {}'.format(slot_name, self.__class__.__name__))
        return s

    def slot_descriptor(self, slot_name=''):
        """
        Return the SlotDescriptor with the given name

        Different from :func:`slot`, this function return a SlotDescriptor, which is
        a "pointer" representation of the slot, and attaches to a particular Stage object.
        This also works if user provides slot_name as the server_name of the slot

        The _Slot object returned by :func:`slot` is the representation of the slot, attaches to each Stage class

        :rtype: SlotDescriptor
        """
        slot_desc = getattr(self, slot_name, None)
        if slot_desc is not None and isinstance(slot_desc, SlotDescriptor):
            return slot_desc

        # users might be providing the server name. Look it up!
        for c in dir(self):

            if c == Stage.default_output_slot_descriptor.fget.__name__:
                # skip the default output slot descriptor
                continue

            p = getattr(self, c)
            if isinstance(p, SlotDescriptor) and self.slot(c, is_input=p.is_input).server_name == slot_name:
                return p
        raise ValueError('Slot descriptor {} not found in class {}'.format(slot_name, self.__class__.__name__))

    def get_name(self):
        """Returns the name of this stage, shortcut for ``self.get_input(self.name)``
        """
        return self.get_input(self.name)

    def get_input(self, slot_desc, default=None):
        """
        Get the value of the given slot descriptor

        :param slot_desc:
        :type slot_desc: SlotDescriptor
        :param default: default value to  return if the given slot is not specified
        :return: the value
        """
        require(isinstance(slot_desc, SlotDescriptor) and (slot_desc.parent is self) and slot_desc.is_input,
                'Not a slot descriptor or it does not belong to this instance: {}'.format(slot_desc))
        return self._slot_values.get(slot_desc.name, default)

    def to_json(self):
        """
        Serialize the stage into JSON
        """
        inputs = {}
        for s in _get_slots(self.__class__, True):
            if s.name != 'name':
                # skip the name slot
                slot_desc = self.slot_descriptor(s.name)
                v = self.get_input(slot_desc)
                if v is not None:
                    inputs[s.server_name] = s.message_type.to_json(v)

        return {'name': self.get_input(self.name),
                'stageClass': self.__class__.__name__,
                'inputs': inputs,
                'outputs': {},
                'models': {},
                'schemas': {},
                'newInputs': []}

    @staticmethod
    def _find_subclass(cls, name):
        """Find subclass of cls that has the given name, recursively.
        Return None if cannot find any class for this name

        :type cls: Type
        """
        if cls.__name__ == name:
            return cls

        for c in cls.__subclasses__():
            cc = Stage._find_subclass(c, name)
            if cc is not None:
                return cc
        return None

    @classmethod
    def from_json(cls, js_data):
        """
        Return a Stage from its JSON representation

        :param js_data: JSON data (received from server)
        :rtype: Stage
        """
        stage_class = js_data['stageClass']
        clazz = Stage._find_subclass(cls, stage_class)
        require(clazz is not None, 'Could not find any Stage class named {}'.format(stage_class))

        name = js_data['name']
        inputs = {}
        for k, v in js_data['inputs'].items():
            if k != 'name':
                inputs[k] = MessageType.from_json(v)

        return clazz(name=name, **inputs)


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
@output_slot('output_df', MessageTypes.DATAFRAME, 'outputDf', is_default=True)
class _Estimator(Stage):
    pass


@input_slot('input_col', MessageTypes.VALUE, 'inputCol')
class _HasInputCol(Stage):
    pass


@input_slot('input_cols', MessageTypes.value(value_type='array'), 'inputCols')
class _HasInputCols(Stage):
    pass


@input_slot('output_col', MessageTypes.VALUE, 'outputCol')
class _HasOutputCol(Stage):
    pass


@input_slot('input_df', MessageTypes.DATAFRAME, 'inputDf')
@output_slot('metric_value', MessageTypes.value(value_type='double'), 'metricValue', is_default=True)
class _Evaluator(Stage):
    pass

####################################################################################

####################################################################################


class Placeholder(Stage):
    """
    General parent class for all placeholders.
    Mostly for coding convenience, not actually used in the APIs.
    """
    pass


@input_slot('input_val', MessageTypes.VALUE, server_name='inputVal')
@output_slot('output_val', MessageTypes.VALUE, server_name='outputVal')
class ValuePlaceholder(Placeholder):
    def __init__(self, name='Stage', value_type=None):
        # overwrite the actual input slot and output slot, to incorporate custom `value_type`
        msg_type = MessageTypes.VALUE if value_type is None else MessageTypes.value(value_type=value_type)
        self._input_val_slot = _Slot('input_val', msg_type, True, 'inputVal', False)
        self._output_val_slot = _Slot('output_val', msg_type, False, 'outputVal', True)

        super(ValuePlaceholder, self).__init__(name=name)

    def slot(self, slot_name='', is_input=True):
        # override this function to return the custom slots created in the constructor
        # this is to order to take into account custom `value_type` and make sure serialization works
        # especially for tricky types like list and so on.
        if slot_name == 'input_val':
            return self._input_val_slot
        if slot_name == 'output_val':
            return self._output_val_slot
        return super(ValuePlaceholder, self).slot(slot_name=slot_name, is_input=is_input)


@input_slot('input_val', MessageTypes.DATAFRAME, server_name='inputVal')
@output_slot('output_val', MessageTypes.DATAFRAME, server_name='outputVal')
class DataframePlaceholder(Placeholder):
    pass


@input_slot('input_val', MessageTypes.COLUMN, server_name='inputVal')
@output_slot('output_val', MessageTypes.COLUMN, server_name='outputVal')
class ColumnPlaceholder(Placeholder):
    pass


"""
ETL stages
"""


@input_slot('col_names', MessageTypes.value(value_type='array'), server_name='colNames')
class Drop(_UnaryTransformer):
    pass


"""
Feature extractors
"""


@input_slot('labels', MessageTypes.value(value_type='array'))
class IndexToString(_UnaryTransformer, _HasInputCol, _HasOutputCol):
    pass


@output_slot('model', MessageTypes.MODEL)
class StringIndexer(_UnaryTransformer, _HasInputCol, _HasOutputCol):
    pass


class VectorAssembler(_UnaryTransformer, _HasInputCols, _HasOutputCol):
    pass


"""
ML stages
"""


@input_slot('features_col', MessageTypes.VALUE, server_name='featuresCol')
class _HasFeaturesCol(Stage):
    pass


@input_slot('label_col', MessageTypes.VALUE, server_name='labelCol')
class _HasLabelCol(Stage):
    pass


@input_slot('prediction_col', MessageTypes.VALUE, server_name='predictionCol')
class _HasPredictionCol(Stage):
    pass


@input_slot('probability_col', MessageTypes.VALUE, server_name='probabilityCol')
class _HasProbabilityCol(Stage):
    pass


@input_slot('aggregation_depth', MessageTypes.VALUE, server_name='aggregationDepth')
@input_slot('elastic_net_param', MessageTypes.value(value_type='double'), server_name='elasticNetParam')
@input_slot('fit_intercept', MessageTypes.VALUE, server_name='fitIntercept')
@input_slot('max_iter', MessageTypes.VALUE, server_name='maxIter')
@input_slot('reg_param', MessageTypes.value(value_type='double'), server_name='regParam')
@input_slot('standardization', MessageTypes.VALUE, server_name='standardization')
@input_slot('tolerance', MessageTypes.value(value_type='double'), server_name='tolerance')
@input_slot('weight_col', MessageTypes.VALUE, server_name='weightCol')
@input_slot('solver', MessageTypes.VALUE, server_name='solver')
class _LinearRegressionInputs(_HasFeaturesCol, _HasLabelCol, _HasPredictionCol):
    pass


class LinearRegression(_Estimator, _LinearRegressionInputs):
    pass


"""
Evaluators
"""


@input_slot('metric_name', MessageTypes.VALUE, server_name='metricName')
class RegressionEvaluator(_Evaluator, _HasLabelCol, _HasPredictionCol):
    pass
