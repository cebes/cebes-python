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

from pycebes.internal.implicits import get_default_pipeline
from pycebes.core import stages


"""
Dataframe ETL APIs
"""


def _create_stage(stage, stage_name=None, **kwargs):
    if stage_name is not None:
        stage.set_name(stage_name)
    return get_default_pipeline().add(stage.set_inputs(**kwargs))


def drop(df, col_names, stage_name=None):
    """

    :param df:
    :param col_names:
    :param stage_name:
    :return:
    """
    return _create_stage(stages.Drop(), stage_name, input_df=df, col_names=col_names)

