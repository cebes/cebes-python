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

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import


class ServerException(Exception):
    """
    Exception happened on the server, got catched and returned to the client
    """

    def __init__(self, message='', server_stack_trace='', request_uri='', request_entity=None):
        super(ServerException, self).__init__(message, server_stack_trace, request_uri, request_entity)
        self.server_stack_trace = server_stack_trace
        self.request_uri = request_uri
        self.request_entity = request_entity
