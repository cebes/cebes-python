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

import base64

import six

from pycebes.core.client import Client
from pycebes.core.dataframe import Dataframe
from pycebes.internal.implicits import get_session_stack
from pycebes.internal.responses import TaggedDataframeResponse


@six.python_2_unicode_compatible
class Session(object):

    def __init__(self, host='localhost', port=21000, user_name='',
                 password='', api_version='v1', interactive=True):
        """
        Construct a new `Session` to the server at the given host and port, with the given user name and password.
        
        :param interactive: whether this is an interactive session, 
            in which case some diagnosis logs will be printed to stdout.
        """
        self._client = Client(host=host, port=port, user_name=user_name,
                              password=password, api_version=api_version, interactive=interactive)

        # the first session created
        session_stack = get_session_stack()
        if session_stack.get_default() is None:
            session_stack.stack.append(self)

    def __repr__(self):
        return '{}(host={!r},port={!r},user_name={!r},api_version={!r})'.format(
            self.__class__.__name__, self._client.host, self._client.port,
            self._client.user_name, self._client.api_version)

    @property
    def client(self):
        """
        Return the client which can be used to send requests to server

        :rtype: Client
        """
        return self._client

    def as_default(self):
        """
        Returns a context manager that makes this object the default session.
        
        Use with the `with` keyword to specify that all remote calls to server
        should be executed in this session.
        
        .. code-block:: python

            sess = cb.Session()
            with sess.as_default():
                ....

        To get the current default session, use `get_default_session`.
        
        .. note::

            The default session is a property of the current thread. If you
            create a new thread, and wish to use the default session in that
            thread, you must explicitly add a `with sess.as_default():` in that
            thread's function.
        
        :return: A context manager using this session as the default session.
        """
        return get_session_stack().get_controller(self)

    """
    Storage APIs
    """

    def _read(self, request):
        """
        Read a Dataframe from the given request

        :rtype: Dataframe
        """
        return Dataframe.from_json(self._client.post_and_wait('storage/read', data=request))

    def from_s3(self):
        pass

    def from_csv(self):
        pass

    def from_hdfs(self):
        pass

    def from_jdbc(self, url, table_name, user_name='', password=''):
        """
        Read a Dataframe from a JDBC table

        :param url: URL to the JDBC server
        :param table_name: name of the table
        :param user_name: JDBC user name
        :param password: JDBC password

        :rtype: Dataframe 
        """
        return self._read({
            'jdbc': {'url': url, 'tableName': table_name,
                     'userName': user_name,
                     'passwordBase64': base64.urlsafe_b64encode(password)}})

    def from_hive(self, table_name=''):
        """
        Read a Dataframe from Hive table of the given name

        :param table_name: name of the Hive table to read data from
        :rtype: Dataframe 
        """
        return self._read({'hive': {'tableName': table_name}})

    def from_id(self, identifier):
        """
        Get a ``Dataframe`` from the given tag or ID

        :param identifier: either a tag or an ID of a ``Dataframe`` to be retrieved.
        :type identifier: six.text_type
        :rtype: Dataframe 
        """
        return Dataframe.from_json(self._client.post_and_wait('df/get', identifier))

    """
    Dataframe APIs
    """

    def tag(self, obj, tag):
        """
        Tag the given object

        :param obj: can be a ``Dataframe`` or a ``Pipeline``
        :param tag: a string represent the tag
        :type tag: six.text_type
        :return: the given object if success
        """
        if isinstance(obj, Dataframe):
            self._client.post_and_wait('df/tagadd', {'tag': tag, 'df': obj.id})
            return obj
        raise ValueError('Unsupported object of type {}'.format(type(obj)))

    def untag_dataframe(self, tag):
        """
        Untag the ``Dataframe`` of the given tag. Note that if the ``Dataframe``
        has more than 1 tag, it can still be accessed using other tags.

        :param tag: the tag of the Dataframe to be removed
        :type tag: six.text_type
        :return: the Dataframe object if success
        """
        return Dataframe.from_json(self._client.post_and_wait('df/tagdelete', {'tag': tag}))

    def untag_pipeline(self, tag):
        """
        Untag the ``Pipeline`` of the given tag. Note that if the ``Pipeline``
        has more than 1 tag, it can still be accessed using other tags.

        :param tag: the tag of the ``Pipeline`` to be removed
        :type tag: six.text_type
        :return: the Pipeline object if success
        """
        return self._client.post_and_wait('pipeline/tagdelete', {'tag': tag})

    def dataframes(self, pattern=None, max_count=100):
        """
        Get the list of tagged Dataframes.

        :param pattern: a pattern string to match the tags.
            Simple wildcards are supported: use ``?`` to match 0 or 1 arbitrary character,
            ``*`` to match 0 or more arbitrary characters.
        :type pattern: six.text_type
        :param max_count: maximum number of entries to be returned

        :return:
        """
        data = {'maxCount': max_count}
        if pattern is not None:
            data['pattern'] = pattern
        return TaggedDataframeResponse(self._client.post_and_wait('df/tags', data))

    def pipelines(self, pattern=None, max_count=100):
        """

        :param pattern:
        :param max_count:
        :return:
        """
        raise NotImplementedError()


"""
Functional APIs for Session
"""
