from __future__ import print_function
from __future__ import unicode_literals

import json

import requests


class Client(object):

    def __init__(self, host='localhost', port=21000, user_name='', password=''):
        self.host = host
        self.port = port

        self.session = requests.Session()
        self.session.headers.update({'Content-Type': 'application/json'})

        # login
        self.post('auth/login', {'userName': user_name, 'passwordHash': password})

    def server_url(self, uri):
        return 'http://{}:{}/v1/{}'.format(self.host, self.port, uri)

    def post(self, uri, data):
        response = self.session.post(self.server_url(uri), data=json.dumps(data))
        self.session.headers.update({'Authorization': response.headers.get('Set-Authorization'),
                                     'Refresh-Token': response.headers.get('Set-Refresh-Token'),
                                     'X-XSRF-TOKEN': response.cookies.get('XSRF-TOKEN')})
        return response.text
