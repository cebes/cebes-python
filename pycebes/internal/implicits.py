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
