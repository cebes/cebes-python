from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import os

SERVER_HOST = os.getenv('PYCEBES_SERVER_HOST', 'localhost')
SERVER_PORT = int(os.getenv('PYCEBES_SERVER_PORT', '21000'))
USERNAME = os.getenv('PYCEBES_USERNAME', '')
PASSWORD = os.getenv('PYCEBES_PASSWORD', '')
API_VERSION = os.getenv('PYCEBES_API_VERSION', 'v1')
