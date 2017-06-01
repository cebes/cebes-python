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


def get_default_session():
    """
    
    :return: 
    """
    ret = _default_session_stack.get_default()
    if ret is None:
        raise Exception('No default session found. You need to create a Session')
    return ret


def get_session_stack():
    """
    Get the default session stack

    :rtype: DefaultStack 
    """
    return _default_session_stack
