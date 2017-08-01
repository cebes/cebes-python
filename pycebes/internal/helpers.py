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

import logging


def require(condition, msg='Requirement failed'):
    """
    Helper to raise ValueError exception if the given condition is not satisfied

    :param condition: a boolean expression to check
    :param msg: message of the exception
    :return: nothing
    """
    if not condition:
        raise ValueError(msg)


def get_logger(name):
    """
    Get a logger with default configuration and given name

    :rtype: logging.Logger
    """
    logger = logging.getLogger(name)

    logger.setLevel(logging.INFO)

    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    formatter = logging.Formatter('%(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return logger
