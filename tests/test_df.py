from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import unittest

from tests import test_base


class TestDataframe(test_base.TestBase):

    def test_get_tags(self):
        tags = self.session.tags(max_count=10)
        self.assertIsInstance(tags, list)


if __name__ == '__main__':
    unittest.main()
