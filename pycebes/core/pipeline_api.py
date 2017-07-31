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

import enum

from pycebes.core import stages
from pycebes.internal.helpers import require
from pycebes.internal.implicits import get_default_pipeline


def _create_stage(stage_cls, stage_name=None, **kwargs):
    """
    Internal helper for creating a stage and add it into the current default pipeline
    """
    ppl = get_default_pipeline()

    # invent a name for this stage
    if stage_name is None:
        name_template = '{}_{{}}'.format(stage_cls.__name__.lower())
        idx = 0
        stage_name = name_template.format(idx)
        while ppl.__contains__(stage_name):
            idx += 1
            stage_name = name_template.format(idx)

    inputs = {}
    for k, v in kwargs.items():
        if isinstance(v, stages.Stage):
            # convenience: users can specify a slot, we take the default output slot
            v = v.default_output_slot_descriptor
        if isinstance(v, stages.SlotDescriptor):
            # if v is a slot descriptor, make sure its parent is in the pipeline
            ppl.add(v.parent)
        inputs[k] = v

    return ppl.add(stage_cls(name=stage_name, **inputs))


class PlaceholderTypes(enum.Enum):
    VALUE = 0
    DATAFRAME = 1
    COLUMN = 2


def placeholder(placeholder_type=PlaceholderTypes.DATAFRAME, value_type=None, name=None):
    """

    :param placeholder_type:
    :param value_type:
    :param name:
    :return:
    """
    if placeholder_type == PlaceholderTypes.VALUE:
        return _create_stage(stages.ValuePlaceholder, name, value_type=value_type)
    else:
        placeholders = {PlaceholderTypes.DATAFRAME: stages.DataframePlaceholder,
                        PlaceholderTypes.COLUMN: stages.ColumnPlaceholder}
        require(placeholder_type in placeholders, 'Invalid placeholder type: {}'.format(placeholder_type))
        return _create_stage(placeholders[placeholder_type], name)


"""
ETL stages
"""


def drop(df, col_names, name=None):
    """

    :param df:
    :param col_names:
    :param name:
    :return:
    :rtype: stages.Drop
    """
    return _create_stage(stages.Drop, name, input_df=df, col_names=col_names)


"""
Feature extractors
"""


def index_to_string(df, input_col, output_col, name=None):
    """

    :param df:
    :param input_col:
    :param output_col:
    :param name:
    :return:
    :rtype: stages.IndexToString
    """
    return _create_stage(stages.IndexToString, name, input_df=df, input_col=input_col, output_col=output_col)


def string_indexer(df, input_col, output_col, name=None):
    """

    :param df:
    :param input_col:
    :param output_col:
    :param name:
    :return:
    :rtype: stages.StringIndexer
    """
    return _create_stage(stages.StringIndexer, name, input_df=df, input_col=input_col, output_col=output_col)


def vector_assembler(df, input_cols, output_col='output', name=None):
    """

    :param df:
    :param input_cols: list of column names
    :param output_col: name of the output column
    :param name: name of this stage. Will be automatically generated if not provided
    :return:
    :rtype: stages.VectorAssembler
    """
    return _create_stage(stages.VectorAssembler, name, input_df=df, input_cols=input_cols, output_col=output_col)


"""
ML stages
"""


def linear_regression(df, features_col, label_col, prediction_col='prediction',
                      aggregation_depth=2, elastic_net_param=0.0, fit_intercept=True,
                      max_iter=10, reg_param=0.0, standardization=True,
                      tolerance=1e-6, weight_col=None, solver='auto', name=None):
    """

    :param df:
    :param features_col:
    :param label_col:
    :param prediction_col:
    :param aggregation_depth:
    :param elastic_net_param:
    :param fit_intercept:
    :param max_iter:
    :param reg_param:
    :param standardization:
    :param tolerance:
    :param weight_col:
    :param solver:
    :param name:
    :return:
    :rtype: stages.LinearRegression
    """
    return _create_stage(stages.LinearRegression, name, input_df=df,
                         features_col=features_col, label_col=label_col, prediction_col=prediction_col,
                         aggregation_depth=aggregation_depth, elastic_net_param=elastic_net_param,
                         fit_intercept=fit_intercept, max_iter=max_iter, reg_param=reg_param,
                         standardization=standardization, tolerance=tolerance,
                         weight_col=weight_col, solver=solver)
