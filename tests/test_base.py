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

import unittest
from pycebes.core.session import Session
from pycebes.core.dataframe import Dataframe
from tests import config


class TestBase(unittest.TestCase):

    session = Session(host=config.SERVER_HOST, port=config.SERVER_PORT,
                      user_name=config.USERNAME, password=config.PASSWORD, interactive=False)
    cylinder_bands = None

    @classmethod
    def setUpClass(cls):
        cls.cylinder_bands = TestBase.load_data(cls.session)[0]

    @staticmethod
    def load_data(s):
        """
        Load the test data in the given Session

        :type s: Session
        :return: list of Dataframes
        """
        response = s.client.post_and_wait('test/loaddata', data={'datasets': ['cylinder_bands']})
        return [Dataframe.from_json(r) for r in response['dataframes']]
