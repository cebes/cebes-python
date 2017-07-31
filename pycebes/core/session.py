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

import base64
import os
import tempfile

import pandas as pd
import six

from pycebes.core.client import Client
from pycebes.core.dataframe import Dataframe
from pycebes.core.pipeline import Model, Pipeline
from pycebes.internal import responses
from pycebes.internal.helpers import require
from pycebes.internal.implicits import get_session_stack


@six.python_2_unicode_compatible
class Session(object):
    def __init__(self, host='localhost', port=21000, user_name='',
                 password='', api_version='v1', interactive=True):
        """
        Construct a new `Session` to the server at the given host and port, with the given user name and password.
        
        :param interactive: whether this is an interactive session, 
            in which case some diagnosis logs will be printed to stdout.
        """
        self._client = Client(host=host, port=port, user_name=user_name,
                              password=password, api_version=api_version, interactive=interactive)

        # the first session created
        session_stack = get_session_stack()
        if session_stack.get_default() is None:
            session_stack.stack.append(self)

    def __repr__(self):
        return '{}(host={!r},port={!r},user_name={!r},api_version={!r})'.format(
            self.__class__.__name__, self._client.host, self._client.port,
            self._client.user_name, self._client.api_version)

    @property
    def client(self):
        """
        Return the client which can be used to send requests to server

        :rtype: Client
        """
        return self._client

    @property
    def dataframe(self):
        """
        Return a helper for working with tagged and cached :class:`Dataframe`

        :rtype: _TagHelper
        """
        return _TagHelper(client=self._client, cmd_prefix='df',
                          object_class=Dataframe, response_class=responses.TaggedDataframeResponse)

    @property
    def model(self):
        """
        Return a helper for working with tagged and cached :class:`Model`

        :rtype: _TagHelper
        """
        return _TagHelper(client=self._client, cmd_prefix='model',
                          object_class=Model, response_class=responses.TaggedModelResponse)

    @property
    def pipeline(self):
        """
        Return a helper for working with tagged and cached :class:`Pipeline`

        :rtype: _TagHelper
        """
        return _TagHelper(client=self._client, cmd_prefix='pipeline',
                          object_class=Pipeline, response_class=responses.TaggedPipelineResponse)

    def as_default(self):
        """
        Returns a context manager that makes this object the default session.
        
        Use with the `with` keyword to specify that all remote calls to server
        should be executed in this session.
        
        .. code-block:: python

            sess = cb.Session()
            with sess.as_default():
                ....

        To get the current default session, use `get_default_session`.
        
        .. note::

            The default session is a property of the current thread. If you
            create a new thread, and wish to use the default session in that
            thread, you must explicitly add a `with sess.as_default():` in that
            thread's function.
        
        :return: A context manager using this session as the default session.
        """
        return get_session_stack().get_controller(self)

    """
    Storage APIs
    """

    def _read(self, request):
        """
        Read a Dataframe from the given request

        :rtype: Dataframe
        """
        return Dataframe.from_json(self._client.post_and_wait('storage/read', data=request))

    @staticmethod
    def _verify_data_format(fmt='csv', options=None):
        """
        Helper to verify the data format and options
        Return the JSON representation of the given options

        :type options: ReadOptions
        """
        valid_fmts = ['csv', 'json', 'parquet', 'orc', 'text']
        fmt = fmt.lower()
        require(fmt in valid_fmts, 'Unrecognized data format: {}. '
                                   'Supported values are: {}'.format(fmt, ', '.join(valid_fmts)))
        if options is not None:
            if fmt == valid_fmts[0]:
                require(isinstance(options, CsvReadOptions),
                        'options must be a {} object. Got {!r}'.format(CsvReadOptions.__name__, options))
            elif fmt == valid_fmts[1]:
                require(isinstance(options, JsonReadOptions),
                        'options must be a {} object. Got {!r}'.format(JsonReadOptions.__name__, options))
            elif fmt == valid_fmts[2]:
                require(isinstance(options, ParquetReadOptions),
                        'options must be a {} object. Got {!r}'.format(ParquetReadOptions.__name__, options))
            else:
                raise ValueError('options must be None when fmt={}'.format(fmt))
            return options.to_json()
        return {}

    def read_jdbc(self, url, table_name, user_name='', password=''):
        """
        Read a Dataframe from a JDBC table

        :param url: URL to the JDBC server
        :param table_name: name of the table
        :param user_name: JDBC user name
        :param password: JDBC password

        :rtype: Dataframe
        """
        return self._read({'jdbc': {'url': url, 'tableName': table_name,
                                    'userName': user_name,
                                    'passwordBase64': base64.urlsafe_b64encode(password)}})

    def read_hive(self, table_name=''):
        """
        Read a Dataframe from Hive table of the given name

        :param table_name: name of the Hive table to read data from
        :rtype: Dataframe
        """
        return self._read({'hive': {'tableName': table_name}})

    def read_s3(self, fmt='csv', options=None):
        pass

    def read_hdfs(self, fmt='csv', options=None):
        pass

    def read_local(self, path, fmt='csv', options=None):
        """
        Upload a file from the local machine to the server, and create a :class:`Dataframe` out of it.

        :param path: path to the local file
        :param fmt: format of the file, can be `csv`, `json`, `orc`, `parquet`, `text`
        :param options: Additional options that dictate how the files are going to be read.
            If specified, this can be:

            - :class:`CsvReadOptions` when `fmt='csv'`,
            - :class:`JsonReadOptions` when `fmt='json'`, or
            - :class:`ParquetReadOptions` when `fmt='parquet'`

         Other formats do not need additional options
        :rtype: Dataframe
        """
        options_dict = Session._verify_data_format(fmt=fmt, options=options)
        server_path = self._client.upload(path)['path']
        return self._read({'localFs': {'path': server_path, 'format': fmt}, 'readOptions': options_dict})

    def read_csv(self, path, options=None):
        """
        Upload a local CSV file to the server, and create a Dataframe out of it.

        :param path: path to the local CSV file
        :param options: Additional options that dictate how the files are going to be read.
            Must be either None or a :class:`CsvReadOptions` object
        :type options: CsvReadOptions
        :rtype: Dataframe
        """
        return self.read_local(path=path, fmt='csv', options=options)

    def read_json(self, path, options=None):
        """
        Upload a local JSON file to the server, and create a Dataframe out of it.

        :param path: path to the local JSON file
        :param options: Additional options that dictate how the files are going to be read.
            Must be either None or a :class:`JsonReadOptions` object
        :type options: JsonReadOptions
        :rtype: Dataframe
        """
        return self.read_local(path=path, fmt='json', options=options)

    def from_pandas(self, df):
        """
        Upload the given `pandas` DataFrame to the server and create a Cebes Dataframe out of it.
        Types are preserved on a best-efforts basis.

        :param df: a pandas DataFrame object
        :type df: pd.DataFrame
        :rtype: Dataframe
        """
        require(isinstance(df, pd.DataFrame), 'Must be a pandas DataFrame object. Got {}'.format(type(df)))
        with tempfile.NamedTemporaryFile('w', prefix='cebes', delete=False) as f:
            df.to_csv(path_or_buf=f, index=False, sep=',', quotechar='"', escapechar='\\', header=True,
                      na_rep='', date_format='yyyy-MM-dd\'T\'HH:mm:ss.SSSZZ')
            file_name = f.name

        csv_options = CsvReadOptions(infer_schema=True,
                                     sep=',', quote='"', escape='\\', header=True,
                                     null_value='', date_format='yyyy-MM-dd\'T\'HH:mm:ss.SSSZZ',
                                     timestamp_format='yyyy-MM-dd\'T\'HH:mm:ss.SSSZZ')
        cebes_df = self.read_csv(file_name, csv_options)
        try:
            os.remove(file_name)
        except IOError:
            pass
        return cebes_df


########################################################################

########################################################################


class _TagHelper(object):
    """
    Helper providing tag-related commands
    """

    def __init__(self, client, cmd_prefix='df', object_class=Dataframe,
                 response_class=responses.TaggedDataframeResponse):
        """

        :param client:
        :param cmd_prefix:
        :param object_class:
        :type object_class: Type
        :param response_class:
        :type response_class: Type
        """
        self._object_cls = object_class
        self._client = client
        self._cmd_prefix = cmd_prefix
        self._response_class = response_class

    def get(self, identifier):
        """
        Get the object from the given identifier, which can be a tag or a UUID

        :param identifier: either a tag or an ID of the object to be retrieved.
        :type identifier: six.text_type
        """
        return self._object_cls.from_json(self._client.post_and_wait('{}/get'.format(self._cmd_prefix), identifier))

    def tag(self, obj, tag):
        """
        Add the given tag to the given object, return the object itself

        :param obj: the object to be tagged
        :param tag: new tag for the object
        :type tag: six.text_type
        :return: the object itself if success
        """
        require(isinstance(obj, self._object_cls), 'Unsupported object of type {}'.format(type(obj)))
        self._client.post_and_wait('{}/tagadd'.format(self._cmd_prefix), {'tag': tag, 'objectId': obj.id})
        return obj

    def untag(self, tag):
        """
        Untag the object of the given tag. Note that if the object
        has more than 1 tag, it can still be accessed using other tags.

        :param tag: the tag of the object to be removed
        :type tag: six.text_type
        :return: the object itself if success
        """
        return self._object_cls.from_json(self._client.post_and_wait(
            '{}/tagdelete'.format(self._cmd_prefix), {'tag': tag}))

    def list(self, pattern=None, max_count=100):
        """
        Get the list of tagged objects.

        :param pattern: a pattern string to match the tags.
            Simple wildcards are supported: use ``?`` to match zero or one character,
            ``*`` to match zero or more characters.
        :type pattern: six.text_type
        :param max_count: maximum number of entries to be returned
        :rtype: responses._TaggedResponse
        """
        data = {'maxCount': max_count}
        if pattern is not None:
            data['pattern'] = pattern
        return self._response_class(self._client.post_and_wait('{}/tags'.format(self._cmd_prefix), data))


########################################################################

########################################################################

class ReadOptions(object):
    PERMISSIVE = 'PERMISSIVE'
    DROPMALFORMED = 'DROPMALFORMED'
    FAILFAST = 'FAILFAST'

    """
    Contain options for read commands
    """

    def __init__(self, **kwargs):
        self.options = kwargs

    def to_json(self):
        """
        Convert into a dict of options, with keys suitable for the server
        Concretely, this function will convert the keys from snake_convention to camelConvention
        """
        d = {}
        for k, v in self.options.items():
            # skip values that are None
            if v is None:
                continue

            # convert the key from snake_convention to camelConvention (for server)
            k_str = ''
            i = 0
            while i < len(k):
                if k[i] == '_' and i < len(k) - 1:
                    k_str += k[i + 1].upper()
                    i += 2
                else:
                    k_str += k[i]
                    i += 1

            # convert the value into its string representation
            if isinstance(v, bool):
                v_str = 'true' if v else 'false'
            else:
                v_str = '{}'.format(v)

            d[k_str] = v_str
        return d


class CsvReadOptions(ReadOptions):
    def __init__(self, sep=',', encoding='UTF-8', quote='"', escape='\\', comment=None, header=False,
                 infer_schema=False, ignore_leading_white_space=False, null_value=None, nan_value='NaN',
                 positive_inf='Inf', negative_inf='-Inf', date_format='yyyy-MM-dd',
                 timestamp_format='yyyy-MM-dd\'T\'HH:mm:ss.SSSZZ', max_columns=20480,
                 max_chars_per_column=-1, max_malformed_log_per_partition=10, mode=ReadOptions.PERMISSIVE):
        """
        :param sep: sets the single character as a separator for each field and value.
        :param encoding: decodes the CSV files by the given encoding type
        :param quote: sets the single character used for escaping quoted values where
            the separator can be part of the value. If you would like to turn off quotations, you need to
            set not `null` but an empty string.
        :param escape: sets the single character used for escaping quotes inside an already quoted value.
        :param comment: sets the single character used for skipping lines beginning with this character.
            By default, it is disabled
        :param header: uses the first line as names of columns.
        :param infer_schema: infers the input schema automatically from data. It requires one extra pass over the data.
        :param ignore_leading_white_space: defines whether or not leading whitespaces
            from values being read should be skipped.
        :param null_value: sets the string representation of a null value.
            This applies to all supported types including the string type.
        :param nan_value: sets the string representation of a "non-number" value
        :param positive_inf: sets the string representation of a positive infinity value
        :param negative_inf: sets the string representation of a negative infinity value
        :param date_format: sets the string that indicates a date format.
            Custom date formats follow the formats at `java.text.SimpleDateFormat`.
            This applies to date type.
        :param timestamp_format: sets the string that indicates a timestamp format.
            Custom date formats follow the formats at `java.text.SimpleDateFormat`. This applies to timestamp type.
        :param max_columns: defines a hard limit of how many columns a record can have
        :param max_chars_per_column: defines the maximum number of characters allowed
            for any given value being read. By default, it is -1 meaning unlimited length
        :param max_malformed_log_per_partition: sets the maximum number of malformed rows
            will be logged for each partition. Malformed records beyond this number will be ignored.
        :param mode: allows a mode for dealing with corrupt records during parsing.

            - :ref:`ReadOptions.PERMISSIVE`: sets other fields to `null` when it meets a corrupted record.
                    When a schema is set by user, it sets `null` for extra fields
            - :ref:`ReadOptions.DROPMALFORMED`: ignores the whole corrupted records
            - :ref:`ReadOptions.FAILFAST`: throws an exception when it meets corrupted records

        """
        super(CsvReadOptions, self).__init__(sep=sep, encoding=encoding, quote=quote,
                                             escape=escape, comment=comment, header=header,
                                             infer_schema=infer_schema,
                                             ignore_leading_white_space=ignore_leading_white_space,
                                             null_value=null_value, nan_value=nan_value,
                                             positive_inf=positive_inf, negative_inf=negative_inf,
                                             date_format=date_format, timestamp_format=timestamp_format,
                                             max_columns=max_columns, max_chars_per_column=max_chars_per_column,
                                             max_malformed_log_per_partition=max_malformed_log_per_partition,
                                             mode=mode)


class JsonReadOptions(ReadOptions):
    def __init__(self, primitives_as_string=False, prefers_decimal=False, allow_comments=False,
                 allow_unquoted_field_names=False, allow_single_quotes=True, allow_numeric_leading_zeros=False,
                 allow_backslash_escaping_any_character=False, mode=ReadOptions.PERMISSIVE,
                 column_name_of_corrupt_record=None, date_format='yyyy-MM-dd',
                 timestamp_format="yyyy-MM-dd'T'HH:mm:ss.SSSZZ"):
        """
        Options for reading Json files

        :param primitives_as_string: infers all primitive values as a string type
        :param prefers_decimal: infers all floating-point values as a decimal type.
            If the values do not fit in decimal, then it infers them as doubles
        :param allow_comments: ignores Java/C++ style comment in JSON records
        :param allow_unquoted_field_names: allows unquoted JSON field names
        :param allow_single_quotes: allows single quotes in addition to double quotes
        :param allow_numeric_leading_zeros: allows leading zeros in numbers (e.g. 00012)
        :param allow_backslash_escaping_any_character: allows accepting quoting of all
            character using backslash quoting mechanism
        :param mode: allows a mode for dealing with corrupt records during parsing.

            - :ref:`ReadOptions.PERMISSIVE`: sets other fields to `null` when it meets a corrupted record.
                    When a schema is set by user, it sets `null` for extra fields
            - :ref:`ReadOptions.DROPMALFORMED`: ignores the whole corrupted records
            - :ref:`ReadOptions.FAILFAST`: throws an exception when it meets corrupted records

        :param column_name_of_corrupt_record: allows renaming the new field having malformed string
            created by :ref:`ReadOptions.PERMISSIVE` mode. This overrides `spark.sql.columnNameOfCorruptRecord`.
        :param date_format: sets the string that indicates a date format.
            Custom date formats follow the formats at `java.text.SimpleDateFormat`. This applies to date type
        :param timestamp_format: sets the string that indicates a timestamp format.
            Custom date formats follow the formats at `java.text.SimpleDateFormat`. This applies to timestamp type
        """
        super(JsonReadOptions,
              self).__init__(primitives_as_string=primitives_as_string,
                             prefers_decimal=prefers_decimal, allow_comments=allow_comments,
                             allow_unquoted_field_names=allow_unquoted_field_names,
                             allow_single_quotes=allow_single_quotes,
                             allow_numeric_leading_zeros=allow_numeric_leading_zeros,
                             allow_backslash_escaping_any_character=allow_backslash_escaping_any_character,
                             mode=mode, column_name_of_corrupt_record=column_name_of_corrupt_record,
                             date_format=date_format, timestamp_format=timestamp_format)


class ParquetReadOptions(ReadOptions):
    def __init__(self, merge_schema=True):
        """
        Options for reading Parquet files

        :param merge_schema: sets whether we should merge schemas collected from all Parquet part-files.
        """
        super(ParquetReadOptions, self).__init__(merge_schema=merge_schema)
