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
import datetime


def from_json(js):
    """
    Helper to parse json values from server into python types
    """
    if js is None or js is True or js is False or isinstance(js, six.text_type):
        # JsNull, JsBoolean, JsString
        return js

    if not isinstance(js, dict) or 'type' not in js or 'data' not in js:
        raise ValueError('Expected a dict, got {!r}'.format(js))

    t = js['type']
    data = js['data']

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
        return [from_json(x) for x in data]

    if t == 'map':
        d = {}
        for entry in data:
            if 'key' not in entry or 'val' not in entry:
                raise ValueError('Invalid map entry: {!r}'.format(entry))
            d[from_json(entry['key'])] = from_json(entry['val'])

    raise ValueError('Failed to parse value: {!r}'.format(js))


def _to_js_object(data_type='', data=None):
    """
    Seriaize the given data with the given type.
    Following "weird" convention of the server
    """
    return {'type': data_type, 'data': data}


def to_json(value, param_type=None):
    """
    Serialize (generic) python value into JSON, to be read by server
    """
    if value is None or isinstance(value, (six.text_type, bool)):
        return value

    if isinstance(value, int):
        param_type = param_type or 'int'
        assert param_type in ('byte', 'short', 'int', 'long')
        return _to_js_object(param_type, value)

    if isinstance(value, float):
        param_type = param_type or 'float'
        assert param_type in ('float', 'double', 'timestamp')
        if param_type == 'timestamp':
            # server expects timestamp in milliseconds
            value *= 1E3
        return _to_js_object(param_type, value)

    if isinstance(value, datetime.date):
        return _to_js_object('date', (value - datetime.date(1970, 1, 1)).total_seconds())

    if isinstance(value, bytearray):
        return _to_js_object('byte_array', [int(x) for x in value])

    if isinstance(value, (list, tuple)):
        param_type = param_type or 'seq'
        assert param_type in ('array', 'wrapped_array', 'seq')
        return _to_js_object(param_type, [to_json(vv) for vv in value])
    if isinstance(value, dict):
        return _to_js_object('map', [{'key': to_json(k), 'val': to_json(v)} for k, v in value.items()])

    raise ValueError('Failed to serialize value: {!r}'.format(value))
