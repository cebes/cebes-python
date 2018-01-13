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
import fcntl
import getpass
import json
import os
import tempfile

import pandas as pd
import six

from pycebes.core.client import Client
from pycebes.core.dataframe import Dataframe
from pycebes.core.pipeline import Model, Pipeline
from pycebes.internal import docker_helpers
from pycebes.internal import responses
from pycebes.internal.helpers import require, get_logger
from pycebes.internal.implicits import get_session_stack

_logger = get_logger(__name__)


@six.python_2_unicode_compatible
class Session(object):
    def __init__(self, host=None, port=21000, user_name='',
                 password='', interactive=True):
        """
        Construct a new `Session` to the server at the given host and port, with the given user name and password.

        :param host: Hostname of the Cebes server.

            If `None` (default), cebes will try to launch a new
            docker container with a suitable version of Cebes server in it. Note that it requires you have
            a working docker daemon on your machine.

            Otherwise a string containing the host name or IP address of the Cebes server you want to connect to.

        :param port: The port on which Cebes server is listening. Ignored when ``host=None``.
        :param user_name: Username to log in to Cebes server
        :param password: Password of the user to log in to Cebes server
        :param interactive: whether this is an interactive session, 
            in which case some diagnosis logs will be printed to stdout.
        """

        # local Spark
        self.cebes_container = None
        self.repository_container = None
        if host is None:
            self.cebes_container = docker_helpers.get_cebes_http_server_container()
            host = 'localhost'
            port = self.cebes_container.cebes_port
            _logger.info('Connecting to Cebes container {}'.format(self.cebes_container))
            _logger.info('Spark UI can be accessed at http://localhost:{}'.format(self.cebes_container.spark_port))

        self._client = Client(host=host, port=port, user_name=user_name,
                              password=password, interactive=interactive)

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

        :rtype: _PipelineHelper
        """
        return _PipelineHelper(client=self._client, cebes_http_container=self.cebes_container,
                               local_repo=self.repository_container)

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

    def start_repository_container(self, host_port=None):
        """
        Start a local docker container running Cebes pipeline repository,
        with the repository listening on the host at the given port.

        If `host_port` is None, a new port will be automatically allocated
        by Docker. Therefore, to maintain consistency with your pipeline tags,
        it is recommended to specify a high port, e.g. 35000 or 36000.

        If one repository was started for this Session already, it will be returned.
        """
        if self.repository_container is None:
            self.repository_container = docker_helpers.get_cebes_repository_container(
                host_port=host_port)
        _logger.info('Pipeline repository started on port {}'.format(self.repository_container.cebes_port))
        return self.repository_container

    def stop_repository_container(self):
        """Stop the local pipeline repository if it is running.
        Do nothing if there is no local repository associated with this Session."""
        if self.repository_container is not None:
            self.repository_container.shutdown()
            self.repository_container = None

    def close(self):
        """
        Close this session. Will stop the Cebes container if this session was
        created against a local Cebes container. Otherwise it is a no-op.
        """
        if self.cebes_container is not None:
            self.cebes_container.shutdown()
            self.cebes_container = None
        self.stop_repository_container()

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

    def read_s3(self, bucket, key, access_key, secret_key, region=None, fmt='csv', options=None):
        """
        Read a Dataframe from files stored in Amazon S3.

        :param bucket: name of the S3 bucket
        :type bucket: str
        :param key: path to the file(s) on S3
        :type key: str
        :param access_key: Amazon S3 access key
        :type access_key: str
        :param secret_key: Amazon S3 secret key
        :type secret_key: str
        :param region: S3 region, if needed.
        :type region: str
        :param fmt: format of the files, can be `csv`, `json`, `orc`, `parquet`, `text`
        :param options: Additional options that dictate how the files are going to be read.
            If specified, this can be:

            - :class:`CsvReadOptions` when `fmt='csv'`,
            - :class:`JsonReadOptions` when `fmt='json'`, or
            - :class:`ParquetReadOptions` when `fmt='parquet'`

         Other formats do not need additional options
        :rtype: Dataframe
        """
        options_dict = Session._verify_data_format(fmt, options)
        s3_options = {'accessKey': access_key, 'secretKey': secret_key,
                      'bucketName': bucket, 'key': key,
                      'format': fmt}
        if region:
            s3_options['regionName'] = region
        return self._read({'s3': s3_options, 'readOptions': options_dict})

    def read_hdfs(self, path, server=None, fmt='csv', options=None):
        """
        Load a dataset from HDFS.

        :param path: path to the files on HDFS, e.g. `/data/dataset1`
        :param server: Host name and port, e.g. `hdfs://server:9000`
        :type server: str
        :param fmt: format of the files, can be `csv`, `json`, `orc`, `parquet`, `text`
        :param options: Additional options that dictate how the files are going to be read.
            If specified, this can be:

            - :class:`CsvReadOptions` when `fmt='csv'`,
            - :class:`JsonReadOptions` when `fmt='json'`, or
            - :class:`ParquetReadOptions` when `fmt='parquet'`

         Other formats do not need additional options
        :rtype: Dataframe
        """
        options_dict = Session._verify_data_format(fmt, options)
        hdfs_options = {'path': path, 'format': fmt}
        if server:
            hdfs_options['uri'] = server
        return self._read({'hdfs': hdfs_options, 'readOptions': options_dict})

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

    def load_test_datasets(self):
        """
        Load test datasets that come by default in Cebes server.

        # Returns
        dict: a dict of datasets, currently has only one element:
            `{'cylinder_bands': Dataframe}`
        """
        response = self._client.post_and_wait('test/loaddata', data={'datasets': ['cylinder_bands']})
        return {'cylinder_bands': Dataframe.from_json(response['dataframes'][0])}


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


class _PipelineHelper(_TagHelper):
    """
    Helper for pipeline commands
    """

    def __init__(self, client, cebes_http_container, local_repo):
        super(_PipelineHelper, self).__init__(
            client=client, cmd_prefix='pipeline',
            object_class=Pipeline, response_class=responses.TaggedPipelineResponse)
        self._cebes_http_container = cebes_http_container
        self._local_repo = local_repo

    def login(self, host=None, port=80, username='', password=None):
        """
        Login to the repository at the given host and port, with the given name and password

        If username is non-empty and password is None, pycebes will ask for a password.
        This way the password will not be stored in plain-text in the (I)Python interpreter history.
        """
        if username and password is None:
            password = getpass.getpass('{}\' password: '.format(username))
        data = {'userName': username, 'passwordHash': password or ''}

        if host:
            data.update({'host': host, 'port': port})
        else:
            # if host is not provided, try to get reasonable defaults
            default_host, default_port = self._get_default_repo_host_and_port()
            if default_host:
                data.update({'host': default_host, 'port': default_port})

        result = self._client.post_and_wait('pipeline/login', data)

        self._update_repo_creds(host=result['host'], port=result['port'],
                                token=result['token'])
        _logger.info('Logged into repository {}:{}'.format(result['host'], result['port']))

    def push(self, ppl):
        """
        Push the pipeline to the repository
        """
        data = self._get_tag_info(ppl)
        result = self._client.post_and_wait('pipeline/push', data)
        _logger.info('Pushed successfully: {}'.format(result))

    def pull(self, tag):
        """
        Pull the pipeline with the given tag, return it as an object

        # Returns
        Pipeline: the pipeline object
        """
        data = self._get_tag_info(tag)
        result = self._client.post_and_wait('pipeline/pull', data)
        _logger.info('Pulled successfully: {}'.format(data['tag']))
        return Pipeline.from_json(result)

    def _get_default_repo_host_and_port(self):
        """Get the default repository host and port"""
        local_cebes_http_server = self._client.host in ('127.0.0.1', 'localhost')
        if local_cebes_http_server and self._local_repo:
            if self._cebes_http_container:
                # both cebes-http-server and cebes-repository are running in Docker containers
                # use docker ip address and default repository port
                return self._local_repo.container_ip, 22000
            else:
                # Cebes-http-server is running locally but not in Docker (e.g. with ./bin/start-cebes.sh)
                # use localhost and mapped port
                return 'localhost', self._local_repo.cebes_port
        return None, None

    def _get_tag_info(self, tag_or_ppl):
        """Get information of the given tag."""
        data = {}
        if isinstance(tag_or_ppl, Pipeline):
            data['identifier'] = tag_or_ppl.id
        else:
            data['identifier'] = six.text_type(tag_or_ppl)

        default_host, default_port = self._get_default_repo_host_and_port()
        if default_host:
            data.update({'defaultRepoHost': default_host, 'defaultRepoPort': default_port})

        tag_info = self._client.post_and_wait('pipeline/taginfo', data)
        repo_port = int(tag_info['port'])
        _logger.info('Pushing to repository {}:{}'.format(tag_info['host'], repo_port))
        token = self._read_repo_creds(tag_info['host'], repo_port)

        data = {'tag': tag_info['tag'], 'host': tag_info['host'], 'port': repo_port}
        if token:
            data['token'] = token
        return data

    def _open_creds_file(self):
        cebes_data_path = os.path.expanduser('~/.cebes')
        os.makedirs(cebes_data_path, mode=0o700, exist_ok=True)
        creds_file_path = os.path.join(cebes_data_path, 'repositories.json')
        try:
            return open(creds_file_path, 'r+')
        except FileNotFoundError:
            return open(creds_file_path, 'a+')

    def _read_repo_creds(self, host, port):
        with self._open_creds_file() as f:
            fcntl.flock(f, fcntl.LOCK_EX)
            try:
                try:
                    repositories = json.load(f)
                except json.JSONDecodeError:
                    repositories = []

                return next((entry.get('token', '') for entry in repositories
                            if entry['host'] == host and entry['port'] == port), '')
            finally:
                fcntl.flock(f, fcntl.LOCK_UN)

    def _update_repo_creds(self, host, port, token):
        with self._open_creds_file() as f:
            fcntl.flock(f, fcntl.LOCK_EX)
            try:
                try:
                    repositories = json.load(f)
                except json.JSONDecodeError:
                    repositories = []

                updated_repos = []
                for entry in repositories:
                    if entry['host'] != host or entry['port'] != port:
                        updated_repos.append(entry)
                updated_repos.append({'host': host, 'port': port, 'token': token})

                f.seek(0)
                f.write(json.dumps(updated_repos))
                f.truncate()
            finally:
                fcntl.flock(f, fcntl.LOCK_UN)


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
