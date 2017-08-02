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

import json
import random
import time

import requests
from future.utils import raise_from
from requests import exceptions as requests_exceptions
from requests_toolbelt import MultipartEncoderMonitor

from pycebes.core.exceptions import ServerException
from pycebes.internal.helpers import require


class Client(object):
    """
    Represent a connection to the Cebes server. Normally created by a session.
    """

    def __init__(self, host='localhost', port=21000, user_name='',
                 password='', api_version='v1', interactive=True):
        self.host = host
        self.port = port
        self.user_name = user_name
        self.api_version = api_version
        self.interactive = interactive

        self.session = requests.Session()
        self.session.headers.update({'Content-Type': 'application/json'})

        self._check_server_version()

        # login
        r = self.session.post(self._server_url('auth/login'),
                              data=json.dumps({'userName': user_name, 'passwordHash': password}))
        self.session.headers.update({'Authorization': r.headers.get('Set-Authorization'),
                                     'Refresh-Token': r.headers.get('Set-Refresh-Token'),
                                     'X-XSRF-TOKEN': r.cookies.get('XSRF-TOKEN')})

    def upload(self, path):
        """
        Upload the given path to the server, return the JSON response

        :param path: path to the file to be uploaded
        :return: a dict object with 'path' and 'size'
        """

        def callback(encoder):
            if self.interactive:
                n = 20
                pct = encoder.bytes_read / encoder.len
                l = int(round(n * pct))
                s = '\rUploading: {}{} {:.0f}%'.format('.' * l, ' ' * (max(0, n - l)), pct * 100)
                print(s, end='')

        with open(path, 'rb') as f:
            monitor = MultipartEncoderMonitor.from_fields(fields={'file': f}, callback=callback)
            headers = {'Content-Type': monitor.content_type}

            response = self.session.put(self._server_url('storage/upload'), data=monitor, headers=headers)
            require(response.status_code == requests.codes.ok, 'Unsuccessful request: {}'.format(response.text))
            if self.interactive:
                print('')
            return response.json()

    def post(self, uri, data):
        """
        Send a POST request to the given uri, with the given data
        This function catches the exceptions.

        :return: a JSON object of the response
        :exception ConnectionError: if a connection to the server can't be established.
        :exception ValueError: if the response code is not OK
        """
        try:
            response = self.session.post(self._server_url(uri), data=json.dumps(data))
            require(response.status_code == requests.codes.ok, 'Unsuccessful request: {}'.format(response.text))
            return response.json()

        except requests_exceptions.ConnectionError as e:
            # wrap this in the standard ConnectionError to ease end-users
            raise_from(ConnectionError('{}'.format(e)), e)

    def wait(self, request_id, sleep_base=0.5, max_count=100):
        """
        Wait for the given request ID to complete, using an exponential back-off scheme, where:
         - wait for at most `max_count` iterations
         - at each iteration `i`: 
            - if result is ready, return
            - if not, sleep for `sleep_base * randint(0, [2 ** min(i, 7)] - 1)` seconds

        :param request_id: ID of the request to wait for
        :param sleep_base: base of 1 sleep, in seconds
        :param max_count: maximum number of iterations to wait for
        :return: the JSON object
         
        :raises TimeoutError: if ``max_count`` is exceeded
        :raises ServerException: if the request failed, e.g. an exception thrown on the server
        :raises ValueError: invalid status received from the server 
        """
        if self.interactive:
            print('Request ID: {}'.format(request_id))

        status = self.post('request/{}'.format(request_id), {})
        cnt = 0

        while status.get('status', '') == 'scheduled':
            time.sleep(sleep_base * random.randint(0, (2 ** min(cnt, 7)) - 1))
            cnt += 1
            if cnt >= max_count:
                raise TimeoutError('Timed out waiting for request ID {} after {} sleeps'.format(request_id, cnt))

            status = self.post('request/{}'.format(request_id), {})

        request_status = status.get('status', '')
        if request_status == 'finished':
            return status.get('response', {})

        if request_status == 'failed':
            fail_response = status.get('response', {})
            raise ServerException(message=fail_response.get('message', 'Unknown server exception'),
                                  server_stack_trace=fail_response.get('stackTrace', ''),
                                  request_uri=status.get('requestUri', ''),
                                  request_entity=status.get('requestEntity'))

        raise ValueError('Request ID {}: invalid status response from server: {}'.format(request_id, status.text))

    def post_and_wait(self, uri, data):
        """
        POST a request to the server, which is expected to return an ID that will be 
        used to checking the results in an exponential-backoff fashion.

        :return: a JSON object of the response
        """
        response = self.post(uri, data=data)
        request_id = response.get('requestId', None)
        require(request_id is not None, 'Request ID not found. Maybe this is not an asynchronous command? '
                                        'uri={}, response={}'.format(uri, response))
        return self.wait(request_id)

    """
    Private helpers
    """

    def _server_url(self, uri):
        return 'http://{}:{}/{}/{}'.format(self.host, self.port, self.api_version, uri)

    def _check_server_version(self):
        """
        Private helper to check if the server supports the given API version
        Raise ValueError if that is not the case.
        """
        r = self.session.get('http://{}:{}/version'.format(self.host, self.port))
        require(r.status_code == requests.codes.ok, 'Unable to query server API version: {}'.format(r.text))
        server_version = r.json()
        server_api_version = server_version.get('api', '')
        require(server_api_version == self.api_version,
                'Mismatch API version: server={}, client={}'.format(server_api_version, self.api_version))
