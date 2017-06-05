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

from pycebes.internal.default_stack import DefaultStack

_default_session_stack = DefaultStack()
_default_pipeline_stack = DefaultStack()


def _get_default(stack, error_msg='No default object found'):
    ret = stack.get_default()
    if ret is None:
        raise LookupError(error_msg)
    return ret


def get_default_session():
    """
    Get the current default session, raise Exception if there is no Session created
    """
    return _get_default(_default_session_stack, 'No default session found. You need to create a Session')


def get_session_stack():
    """
    Get the default session stack

    :rtype: DefaultStack
    """
    return _default_session_stack


def get_default_pipeline():
    """
    Get the current pipeline, raise Exception if there is no Pipeline created
    """
    return _get_default(_default_pipeline_stack, 'No pipeline found. You need to create a Pipeline')


def get_pipeline_stack():
    """
    Get the pipeline stack

    :rtype: DefaultStack
    """
    return _default_pipeline_stack
