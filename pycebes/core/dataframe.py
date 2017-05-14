from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import six

from pycebes.core.schema import Schema
from pycebes.core.implicits import get_default_session
from pycebes.core.sample import DataSample


@six.python_2_unicode_compatible
class Dataframe(object):
    def __init__(self, _id, _schema):
        self._id = _id
        self._schema = _schema

    @property
    def id(self):
        return self._id

    @property
    def schema(self):
        """
        The Schema of this data frame
        :rtype: Schema
        """
        return self._schema

    @property
    def _client(self):
        """
        Default client, taken from the current default session
        :rtype: pycebes.core.client.Client
        """
        return get_default_session().client

    def __len__(self):
        return self._client.post_and_wait('df/count', data={'df': self.id})

    def __repr__(self):
        return '{}(id={!r})'.format(self.__class__.__name__, self.id)

    @classmethod
    def from_json(cls, js_data):
        """
        Return a ``Dataframe`` instance from its JSON representation
        :param js_data: a dict with ``id`` and ``schema``
        :rtype: Dataframe 
        """
        if 'id' not in js_data or 'schema' not in js_data:
            raise ValueError('Invalid Dataframe JSON: {!r}'.format(js_data))

        return Dataframe(_id=js_data['id'], _schema=Schema.from_json(js_data['schema']))

    """
    Sampling functions
    """

    def take(self, n=10):
        """
        Take a sample of this ``Dataframe``
        :param n: maximum number of rows to be taken
        :rtype: DataSample
        """
        r = self._client.post_and_wait('df/take', {'df': self.id, 'n': n})
        return DataSample.from_json(r)

    def sample(self, fraction=0.1, replacement=True, seed=42):
        """
        Take a sample from this ``Dataframe``
        :param fraction: 
        :param replacement: 
        :param seed: 
        :return: 
        :rtype: Dataframe
        """
        r = self._client.post_and_wait(
            'df/sample', {'df': self.id, 'fraction': fraction,
                          'withReplacement': replacement, 'seed': seed})
        return Dataframe.from_json(r)
