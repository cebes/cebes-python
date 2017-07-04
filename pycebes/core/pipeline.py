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

import six

from pycebes.core.stages import SlotDescriptor, Stage, MessageType, Placeholder
from pycebes.internal.helpers import require
from pycebes.internal.implicits import get_pipeline_stack, get_default_session


class Model(object):
    """
    Represent a trained model
    """

    def __init__(self, _id, model_class, inputs, metadata):
        self._id = _id
        self._model_class = model_class
        self._metadata = metadata
        self._inputs = {}
        for k, v in inputs.items():
            param_name = ''.join('_{}'.format(c.lower()) if c.isupper() else c for c in k)
            self._inputs[param_name] = v

    def __repr__(self):
        return '{}(id={!r},model_class={!r})'.format(self.__class__.__name__, self.id, self._model_class)

    @property
    def id(self):
        return self._id

    @property
    def metadata(self):
        return dict(**self._metadata)

    @property
    def inputs(self):
        return dict(**self._inputs)

    @classmethod
    def from_json(cls, js_data):
        """
        Deserialize a Model received from the server

        :param js_data: JSON data received from server
        :rtype: Model
        """
        inputs = {}
        for k, v in js_data.get('inputs', {}).items():
            inputs[k] = MessageType.from_json(v)
        return Model(_id=js_data['id'], model_class=js_data['modelClass'], inputs=inputs,
                     metadata=js_data.get('metaData', {}))


class Pipeline(object):
    def __init__(self, _id=None, stages=None):
        self._id = _id
        self._stages = [] if stages is None else stages
        self._context_manager = None

    @property
    def id(self):
        return self._id

    def __getitem__(self, item):
        try:
            return next(s for s in self._stages if s.name == item)
        except StopIteration:
            raise KeyError('Stage not found: {!r}'.format(item))

    def __contains__(self, item):
        """
        Check if the given stage or stage name belong to this pipeline

        :param item: stage name (string) or a stage object
        :return: true if the given stage belong to this pipeline
        """
        if isinstance(item, Stage):
            item_name = item.name
        else:
            require(isinstance(item, six.text_type), 'Only support stage object or a string, got {!r}'.format(item))
            item_name = item
        return next((s for s in self._stages if s.name == item_name), None) is not None

    def __enter__(self):
        self._context_manager = get_pipeline_stack().get_controller(self)
        return self._context_manager.__enter__()

    def __exit__(self, exc_type, exc_val, exc_tb):
        return self._context_manager.__exit__(exc_type, exc_val, exc_tb)

    def add(self, stage):
        """
        Add a stage into this pipeline
        :type stage: Stage
        :return: the given stage
        """
        if stage in self._stages:
            return stage

        if stage.get_name() is None:
            # give this a name
            name_template = '{}_{{}}'.format(stage.__class__.__name__.lower())
            idx = 0
            new_name = name_template.format(idx)
            while self.__contains__(new_name):
                idx += 1
                new_name = name_template.format(idx)
            stage.set_name(new_name)
        else:
            if next((s for s in self._stages if s.get_name() == stage.get_name()), None) is not None:
                raise ValueError('Duplicated stage name: {}'.format(stage.get_name()))

        self._stages.append(stage)
        return stage

    @classmethod
    def from_json(cls, js_data):
        """
        Return a ``Pipeline`` instance from its JSON representation

        :param js_data: a dict with ``id`` and ``stages``
        :rtype: Pipeline
        """
        # TODO: decide what to do here
        return Pipeline(js_data['id'], None)

    def to_json(self):
        """
        Return the JSON representation of this Pipeline
        """
        return {'id': self.id, 'stages': [s.to_json() for s in self._stages]}

    def run(self, outputs=(), feeds=None, timeout=-1):
        """
        Run this pipeline, given the feeds and take the outputs

        :param outputs: list of SlotDescriptor from which to take the value
        :param feeds: a dictionary of {SlotDescriptor -> value} giving the values
            to some slots in the pipeline
        :param timeout: timeout in seconds. Negative means wait indefinitely
        :return: tuple of values of the output slots
        """
        single_output = False
        if not isinstance(outputs, (tuple, list)):
            single_output = True
            outputs = [outputs]
        output_slots = []

        for o in outputs:
            # convenience: automatically translate placeholders into its input slot
            slot_desc = o
            if isinstance(slot_desc, Placeholder):
                slot_desc = slot_desc.input_val
            require(isinstance(slot_desc, SlotDescriptor), 'Expected an output slot, got {!r}'.format(o))
            output_slots.append(slot_desc.to_json())

        feeds = feeds or {}
        feeds_json = {}
        for k, v in feeds.items():
            # convenience: automatically translate placeholders into its input slot
            slot_desc = k
            if isinstance(slot_desc, Placeholder):
                slot_desc = slot_desc.input_val

            require(isinstance(slot_desc, SlotDescriptor),
                    'Expected slots as keys in `feeds`, got {!r}'.format(slot_desc))
            require(slot_desc.message_type.is_valid(v), 'Invalid value {!r} for slot {!r}'.format(v, slot_desc))

            feeds_json[slot_desc.full_server_name] = slot_desc.message_type.to_json(v)

        # intentionally set the pipeline ID in the request to be None, so that a new pipeline will be created
        # and a new ID is generated
        ppl_json = self.to_json()
        ppl_json['id'] = None

        data = {'pipeline': ppl_json,
                'feeds': feeds_json,
                'outputs': output_slots,
                'timeout': timeout}

        run_result = get_default_session().client.post_and_wait('pipeline/run', data)

        self._id = run_result['pipelineId']

        # parse the results
        require(len(run_result['results']) == len(output_slots), 'Invalid result from server')
        parsed_results = []
        for out_slot in output_slots:
            r = next((v for k, v in run_result['results'] if k == out_slot), None)
            require(r is not None, 'Could not find result for output slot {}'.format(out_slot))
            parsed_results.append(MessageType.from_json(r))

        return parsed_results[0] if single_output else tuple(parsed_results)
