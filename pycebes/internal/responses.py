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

import tabulate

from pycebes.core.schema import Schema


class _TaggedResponseEntry(object):
    def __init__(self, js_entry):
        self.tag = js_entry['tag']
        self.id = js_entry['id']
        self.created_at = datetime.datetime.utcfromtimestamp(js_entry['createdAt'] / 1E3)

    def to_dict(self):
        return {'Tag': self.tag, 'UUID': self.id, 'Created': self.created_at}


class _TaggedDataframeResponseEntry(_TaggedResponseEntry):
    def __init__(self, js_entry):
        super(_TaggedDataframeResponseEntry, self).__init__(js_entry)
        self.schema = Schema.from_json(js_entry['schema'])

    def to_dict(self):
        d = super(_TaggedDataframeResponseEntry, self).to_dict()
        d['Schema'] = self.schema.simple_string
        return d


class _TaggedModelResponseEntry(_TaggedResponseEntry):
    def __init__(self, js_entry):
        super(_TaggedModelResponseEntry, self).__init__(js_entry)
        self.model_class = js_entry['modelClass']

    def to_dict(self):
        d = super(_TaggedModelResponseEntry, self).to_dict()
        d['Model class'] = self.model_class
        return d


class _TaggedPipelineResponseEntry(_TaggedResponseEntry):
    def __init__(self, js_entry):
        super(_TaggedPipelineResponseEntry, self).__init__(js_entry)
        self.n_stages = len(js_entry['stages'])

    def to_dict(self):
        d = super(_TaggedPipelineResponseEntry, self).to_dict()
        d['# of stages'] = self.n_stages
        return d


"""
The response objects
"""


class _TaggedResponse(object):
    def __init__(self, js_data, tag_entry_class):
        self.tagged_objects = []
        for entry in js_data:
            self.tagged_objects.append(tag_entry_class(entry))

    def __repr__(self):
        return tabulate.tabulate((e.to_dict() for e in self.tagged_objects), headers='keys')


class TaggedDataframeResponse(_TaggedResponse):
    """Result of "df/tags"""

    def __init__(self, js_data):
        super(TaggedDataframeResponse, self).__init__(js_data, _TaggedDataframeResponseEntry)


class TaggedModelResponse(_TaggedResponse):
    """Result of "model/tags"""

    def __init__(self, js_data):
        super(TaggedModelResponse, self).__init__(js_data, _TaggedModelResponseEntry)


class TaggedPipelineResponse(_TaggedResponse):
    """Result of "pipeline/tags"""

    def __init__(self, js_data):
        super(TaggedPipelineResponse, self).__init__(js_data, _TaggedPipelineResponseEntry)
