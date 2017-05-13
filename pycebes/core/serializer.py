from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import pandas as pd


def deserialize_sample(sample):
    """
    Return a pandas' Dataframe from the JSON serialization given in `sample`.
    :param sample: a dict with keys `data` and `schema`
    :rtype: pd.DataFrame 
    """
