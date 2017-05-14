from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals


import six
import pandas as pd


@six.python_2_unicode_compatible
class DataSample(object):
    """
    A sample of data, with a proper schema
    """

    def __init__(self, schema, data):
        """
        
        :param schema: 
        :param data: list of list. Each list is a column
        """
        assert(len(schema) == len(data))
        self.schema = schema
        self.data = data

    def __repr__(self):
        return '{}(schema={!r})'.format(self.__class__.__name__, self.schema)

    def to_pandas(self):
        """
        Return a pandas DataFrame representation of this sample
        :rtype: pd.DataFrame 
        """
        pass

    @classmethod
    def from_json(cls, js_data):
        pass
