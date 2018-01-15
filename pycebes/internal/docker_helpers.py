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

import os
import time

import docker
import requests
import six
from docker import errors as docker_errors
from future import utils
from requests import exceptions as requests_exceptions

from pycebes import config
from pycebes.internal import helpers as pycebes_helper


def _get_docker_client():
    """Returns the docker client"""
    try:
        return docker.from_env(version='auto')
    except docker_errors.DockerException as e:
        err_msg = 'Could not create docker client: {}. Have you started the docker daemon?'.format(e)
        utils.raise_from(ValueError(err_msg), e)


@six.python_2_unicode_compatible
class _CebesContainerInfo(object):

    def __init__(self, name='', image='', container_ip='', cebes_port=0):
        self.name = name
        self.image = image
        self.container_ip = container_ip
        self.cebes_port = cebes_port
        self._logger = pycebes_helper.get_logger(self.__class__.__module__ + '.' + self.__class__.__name__)

    def __str__(self):
        return '{}[{}] at port {}'.format(self.name, self.image, self.cebes_port)

    def shutdown(self):
        """Shutdown this container."""
        client = _get_docker_client()
        try:
            container = client.containers.get(self.name)
            container.stop()
            self._logger.info('Cebes container stopped: {}'.format(self))
        except docker_errors.NotFound:
            self._logger.error('Failed to stop container {}'.format(self), exc_info=1)
        except docker_errors.APIError:
            self._logger.error('Failed to stop container {}'.format(self), exc_info=1)
        finally:
            client.api.close()


@six.python_2_unicode_compatible
class _CebesHttpServerContainerInfo(_CebesContainerInfo):
    def __init__(self, name='', image='', container_ip='', cebes_port=0, spark_port=0):
        super(_CebesHttpServerContainerInfo, self).__init__(
            name=name, image=image, container_ip=container_ip, cebes_port=cebes_port)
        self.spark_port = spark_port


class _DockerContainerStarter(object):
    """
    A helper for starting a new docker container, or return
    its details if it is running (based on its label)
    """

    def __init__(self, img_repo='phvu/cebes', img_tag='0.10.0',
                 container_name_prefix='cebes-http-server',
                 container_data_dir='/cebes/data',
                 data_sub_dir=''):
        self._logger = pycebes_helper.get_logger(self.__class__.__module__ + '.' + self.__class__.__name__)
        self._img_repo = img_repo
        self._img_tag = img_tag
        self._container_name_prefix = container_name_prefix
        self._container_data_dir = container_data_dir
        self._data_sub_dir = data_sub_dir

    def start(self):
        client = _get_docker_client()
        container_info = self._find_running_container(client)
        if container_info is None:
            container_info = self._start_cebes_container(client)

        client.api.close()
        return container_info

    def _port_mapping(self):
        return {'21000/tcp': None, '4040/tcp': None}

    def _parse_container_attrs(self, attrs):
        return _CebesHttpServerContainerInfo(
            name=attrs['Name'][1:], image=attrs['Config']['Image'],
            container_ip=attrs['NetworkSettings']['IPAddress'],
            cebes_port=int(attrs['NetworkSettings']['Ports']['21000/tcp'][0]['HostPort']),
            spark_port=int(attrs['NetworkSettings']['Ports']['4040/tcp'][0]['HostPort']))

    def _find_running_container(self, client):
        """Find all running containers having the same label."""
        running_containers = []
        for c in client.containers.list(filters={'label': 'type={}'.format(self._container_name_prefix)}):
            container_info = self._parse_container_attrs(c.attrs)
            if container_info.image == '{}:{}'.format(self._img_repo, self._img_tag):
                running_containers.append(container_info)

        if len(running_containers) > 1:
            self._logger.info('Detected multiple containers: {}'.format(
                ', '.join('{}'.format(c) for c in running_containers)))

        if len(running_containers) > 0:
            return running_containers[0]
        return None

    def _start_cebes_container(self, client):
        """
        Start a new Cebes container

        :type client: DockerClient
        :rtype: _CebesContainerInfo
        """

        # data dir
        data_path = os.path.join(os.path.expanduser('~/.cebes'), self._img_tag)
        if self._data_sub_dir:
            data_path = os.path.join(data_path, self._data_sub_dir)
        os.makedirs(data_path, mode=0o700, exist_ok=True)

        # invent a name
        name_template = '{}-{}-{{}}'.format(self._container_name_prefix, self._img_tag)
        i = 0
        container_name = name_template.format(i)
        while True:
            running_containers = client.containers.list(all=True, filters={'name': container_name})

            # if no container has the same name, then we are good
            if len(running_containers) == 0:
                break

            # if there is a container with the same name but it exited, remove it and use the name
            container = running_containers[0]
            if container.status == 'exited':
                container.remove()
                break

            # increase the id and get a new name
            i += 1
            container_name = name_template.format(i)

        self._logger.info('Starting container {}[{}:{}] with data path at {}'.format(
            container_name, self._img_repo, self._img_tag, data_path))

        container = client.containers.run('{}:{}'.format(self._img_repo, self._img_tag),
                                          detach=True, ports=self._port_mapping(),
                                          volumes={data_path: {'bind': self._container_data_dir, 'mode': 'rw'}},
                                          labels={'type': self._container_name_prefix},
                                          name=container_name)

        while container.status == 'created':
            time.sleep(0.1)
            container.reload()

        if container.status != 'running':
            self._logger.warning('Container log:\n{}'.format(container.logs().decode('utf-8')))
            raise ValueError('Unable to launch Cebes container. See logs above for more information')

        container_info = self._parse_container_attrs(container.attrs)

        # wait until the service is up
        c = 0
        max_tries = 200
        sess = requests.Session()
        connected = False
        while c < max_tries and not connected:
            try:
                sess.get('http://localhost:{}'.format(container_info.cebes_port))
                connected = True
            except requests_exceptions.ConnectionError:
                time.sleep(0.5)
                c += 1
        sess.close()

        if connected:
            self._logger.info('Cebes container started, listening at localhost:{}'.format(container_info.cebes_port))
        else:
            self._logger.warning('Cebes container {} takes more time than usual to start. '
                                 'You might want to wait a bit more before trying again'.format(container_info))
        return container_info


class _RepositoryContainerStarter(_DockerContainerStarter):
    def __init__(self, img_repo='phvu/cebes', img_tag='0.10.0',
                 container_name_prefix='cebes-http-server',
                 container_data_dir='/cebes/data',
                 data_sub_dir='',
                 host_port=None):
        super(_RepositoryContainerStarter, self).__init__(
            img_repo=img_repo, img_tag=img_tag,
            container_name_prefix=container_name_prefix,
            container_data_dir=container_data_dir,
            data_sub_dir=data_sub_dir)
        self._host_port = host_port

    def _port_mapping(self):
        return {'22000/tcp': self._host_port}

    def _parse_container_attrs(self, attrs):
        return _CebesHttpServerContainerInfo(
            name=attrs['Name'][1:], image=attrs['Config']['Image'],
            container_ip=attrs['NetworkSettings']['IPAddress'],
            cebes_port=int(attrs['NetworkSettings']['Ports']['22000/tcp'][0]['HostPort']),
            spark_port=0)


def get_cebes_http_server_container():
    """
    Get the details of a running Cebes HTTP server container,
    or start a new container if none is running
    """
    starter = _DockerContainerStarter(config.LOCAL_HTTP_SERVER_REPO, config.LOCAL_HTTP_SERVER_TAG,
                                      container_name_prefix='cebes-http-server',
                                      container_data_dir='/cebes/data',
                                      data_sub_dir='http-server')
    return starter.start()


def get_cebes_repository_container(host_port=None):
    """
    Get the details of a running Cebes repository container, or start a new one.
    """
    starter = _RepositoryContainerStarter(config.LOCAL_REPOSITORY_REPO, config.LOCAL_REPOSITORY_TAG,
                                          container_name_prefix='cebes-pipeline-repository',
                                          container_data_dir='/cebes/repository',
                                          data_sub_dir='repository',
                                          host_port=host_port)
    return starter.start()
