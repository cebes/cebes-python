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
from future.utils import raise_from
from requests import exceptions as requests_exceptions

from pycebes import config
from pycebes.internal.helpers import get_logger

__logger = get_logger(__name__)


@six.python_2_unicode_compatible
class _CebesContainerInfo(object):
    def __init__(self, name='', image='', cebes_port=0, spark_port=0):
        self.name = name
        self.image = image
        self.cebes_port = cebes_port
        self.spark_port = spark_port

    def __str__(self):
        return '{}[{}] at port {}'.format(self.name, self.image, self.cebes_port)


def _get_docker_client():
    """Returns the docker client"""
    try:
        return docker.from_env(version='auto')
    except docker_errors.DockerException as e:
        err_msg = 'Could not create docker client: {}. Have you started the docker daemon?'.format(e)
        raise_from(ValueError(err_msg), e)


def _parse_container_attrs(attrs):
    """
    Parse the container attrs and returns the info

    :param attrs: `attrs` field of the container
    :rtype: _CebesContainerInfo
    """
    return _CebesContainerInfo(name=attrs['Name'][1:], image=attrs['Config']['Image'],
                               cebes_port=int(attrs['NetworkSettings']['Ports']['21000/tcp'][0]['HostPort']),
                               spark_port=int(attrs['NetworkSettings']['Ports']['4040/tcp'][0]['HostPort']))


def _start_cebes_container(client):
    """
    Start a new Cebes container

    :type client: DockerClient
    :rtype: _CebesContainerInfo
    """

    # data dir
    data_path = os.path.join(os.path.expanduser('~/.cebes'), config.LOCAL_DOCKER_TAG)
    os.makedirs(data_path, mode=0o700, exist_ok=True)

    # invent a name
    name_template = 'cebes-server-{}-{{}}'.format(config.LOCAL_DOCKER_TAG)
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

    __logger.info('Starting Cebes container {}[{}:{}] with data path at {}'.format(
        container_name, config.LOCAL_DOCKER_REPO, config.LOCAL_DOCKER_TAG, data_path))

    container = client.containers.run('{}:{}'.format(config.LOCAL_DOCKER_REPO, config.LOCAL_DOCKER_TAG),
                                      detach=True, ports={'21000/tcp': None, '4040/tcp': None},
                                      volumes={data_path: {'bind': '/cebes/data', 'mode': 'rw'}},
                                      labels={'type': 'cebes-local-docker-image'},
                                      name=container_name)

    while container.status == 'created':
        time.sleep(0.1)
        container.reload()

    if container.status != 'running':
        __logger.warning('Container log:\n{}'.format(container.logs().decode('utf-8')))
        raise ValueError('Unable to launch Cebes container. See logs above for more information')

    container_info = _parse_container_attrs(container.attrs)

    # wait until the service is up
    c = 0
    max_tries = 100
    sess = requests.Session()
    while c < max_tries:
        try:
            sess.get('http://localhost:{}'.format(container_info.cebes_port))
            break
        except requests_exceptions.ConnectionError:
            time.sleep(0.5)
            c += 1
    sess.close()

    if c == max_tries:
        __logger.warning('Cebes container {} takes more time than usual to start. '
                         'You might want to wait a bit more before trying again'.format(container_info))
    else:
        __logger.info('Cebes container started, listening at localhost:{}'.format(container_info.cebes_port))
    return container_info


def get_cebes_container():
    """
    Get the details of a running Cebes container, or start a new container if none is running

    :rtype: _CebesContainerInfo
    """
    client = _get_docker_client()
    running_containers = []
    for c in client.containers.list(filters={'label': 'type=cebes-local-docker-image'}):
        container_info = _parse_container_attrs(c.attrs)
        __logger.debug('Found a probable Cebes container {}'.format(container_info))

        if container_info.image == '{}:{}'.format(config.LOCAL_DOCKER_REPO, config.LOCAL_DOCKER_TAG):
            running_containers.append(container_info)

    if len(running_containers) > 1:
        __logger.info('Detected multiple Cebes containers: {}'.format(
            ', '.join('{}'.format(c) for c in running_containers)))

    if len(running_containers) > 0:
        container_info = running_containers[0]
    else:
        container_info = _start_cebes_container(client)

    client.api.close()
    return container_info


def shutdown(container_info):
    """
    Shutdown the container given in the argument

    :type container_info: _CebesContainerInfo
    """
    client = _get_docker_client()
    try:
        container = client.containers.get(container_info.name)
        container.stop()
        __logger.info('Cebes container stopped: {}'.format(container_info))
    except docker_errors.NotFound:
        __logger.error('Failed to stop container {}'.format(container_info), exc_info=1)
    except docker_errors.APIError:
        __logger.error('Failed to stop container {}'.format(container_info), exc_info=1)
    finally:
        client.api.close()
