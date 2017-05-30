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
from collections import namedtuple
from pycebes.core.schema import Schema


_TaggedDataframeInfo = namedtuple('_TaggedDataframeInfo', ('tag', 'id', 'created_at', 'schema'))


class TaggedDataframeResponse(object):
    """
    Result of "df/tags"
    """
    def __init__(self, js_data):
        self.tagged_dfs = []
        for entry in js_data:
            ts = datetime.datetime.utcfromtimestamp(entry['createdAt'] / 1E3)
            schema = Schema.from_json(entry['schema'])
            self.tagged_dfs.append(_TaggedDataframeInfo(entry['tag'], entry['id'], ts, schema))

    def __repr__(self):
        return '{!r}'.format(self.tagged_dfs)
