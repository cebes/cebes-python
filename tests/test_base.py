from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import unittest
from pycebes.core.session import Session
from tests import config


class TestBase(unittest.TestCase):

    session = Session(host=config.SERVER_HOST, port=config.SERVER_PORT,
                      user_name=config.USERNAME, password=config.PASSWORD,
                      api_version=config.API_VERSION)
