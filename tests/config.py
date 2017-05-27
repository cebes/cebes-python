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

import os

SERVER_HOST = os.getenv('PYCEBES_SERVER_HOST', 'localhost')
SERVER_PORT = int(os.getenv('PYCEBES_SERVER_PORT', '21000'))
USERNAME = os.getenv('PYCEBES_USERNAME', '')
PASSWORD = os.getenv('PYCEBES_PASSWORD', '')
API_VERSION = os.getenv('PYCEBES_API_VERSION', 'v1')
