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
