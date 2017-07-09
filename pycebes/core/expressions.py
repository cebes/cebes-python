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

from collections import namedtuple

import six

from pycebes.internal.serializer import to_json
from pycebes.core.schema import StorageTypes

_ParamConfig = namedtuple('_ParamConfig', ['name', 'param_type', 'server_name'])


def param(name='param', param_type=None, server_name=None):
    """
    Decorator used for parameters of Expressions

    :param name: Name of the parameter (i.e. slot) 
    :param param_type: optionally, a string represent the type of this param (on the server)
        mostly used to disambiguous primitive types on server
    :type param_type: str
    :param server_name: name of this parameter on server, if different from 'name'
    :return: 
    """

    def decorate(cls):
        assert issubclass(cls, Expression)
        if getattr(cls, 'PARAMS', None) is None:
            cls.PARAMS = {}

        class_name = cls.__name__
        if class_name not in cls.PARAMS:
            cls.PARAMS[class_name] = []

        pc = _ParamConfig(name, param_type, name if server_name is None else server_name)
        if next((p for p in cls.PARAMS[class_name] if p.name == pc.name), None) is not None:
            raise ValueError('Duplicated parameter named {} in class {}'.format(pc.name, cls.__name__))
        cls.PARAMS[class_name].append(pc)
        return cls

    return decorate


def server_namespace(ns='io.cebes.df.expressions'):
    """
    Decorator used for Expressions, to specify the namespace of this class on the server
    """

    def decorate(cls):
        assert issubclass(cls, Expression)
        setattr(cls, '_server_namespace', ns)
        return cls

    return decorate


############################################################################


@six.python_2_unicode_compatible
class Expression(object):
    def __init__(self, **kwargs):
        param_names = {pc.name for pc in self._get_params()}
        for k, v in kwargs.items():
            if k not in param_names:
                raise ValueError('Invalid param name {} in class {}'.format(k, self.__class__.__name__))
            setattr(self, k, v)

    def to_json(self):
        js = {'className': '{}.{}'.format(self._get_server_namespace(), self.__class__.__name__)}
        for pc in self._get_params():
            assert isinstance(pc, _ParamConfig)
            value = getattr(self, pc.name, None)
            js[pc.server_name] = self._param_to_json(pc, value)
        return js

    def __repr__(self):
        params_str = ','.join('{}={!r}'.format(pc.name, getattr(self, pc.name, None)) for pc in self._get_params())
        return '{}({})'.format(self.__class__.__name__, params_str)

    @staticmethod
    def _param_to_json(pc, value):
        """
        Serialize a parameter to JSON, can be overriden by subclasses
        :type pc: _ParamConfig
        :return: a JSON value
        """
        return value.to_json() if isinstance(value, Expression) else to_json(value, pc.param_type)

    def _get_params(self):
        """
        Return the list of params of this expression
        :rtype: list[_ParamConfig]
        """
        params = []
        for parent_class in self.__class__.__mro__:
            params.extend(self.PARAMS.get(parent_class.__name__, []))
        return params

    @classmethod
    def _get_server_namespace(cls):
        return getattr(cls, '_server_namespace', 'io.cebes.df.expressions')


############################################################################
# Helpers subclasses of Expression

@param('children')
class _WithChildren(Expression):
    """
    Subclass of Expression with serialization logic for "children" parameter,
    which is a list of Expressions
    """

    def __init__(self, **kwargs):
        super(_WithChildren, self).__init__(**kwargs)

    @staticmethod
    def _param_to_json(pc, value):
        if pc.name == 'children':
            assert isinstance(value, (list, tuple)) and all(isinstance(ex, Expression) for ex in value)
            return [ex.to_json() for ex in value]
        raise ValueError('Missing serialization logic for {} of class {!r}'.format(pc.name, value))


@param('child')
class _UnaryExpression(Expression):
    """
    Subclass of Expression with serialization logic for "child" parameter
    """

    def __init__(self, child, **kwargs):
        super(_UnaryExpression, self).__init__(child=child, **kwargs)


@param('left')
@param('right')
class _BinaryExpression(Expression):
    """
    Subclass of Expression with serialization logic for "left" and "right" parameters
    """

    def __init__(self, left, right):
        super(_BinaryExpression, self).__init__(left=left, right=right)


"""
Aggregation
"""


@param('relative_sd', param_type='double', server_name='relativeSD')
class ApproxCountDistinct(_UnaryExpression):
    def __init__(self, child, relative_sd=0.05):
        super(ApproxCountDistinct, self).__init__(child=child, relative_sd=relative_sd)


class Average(_UnaryExpression):
    def __init__(self, child):
        super(Average, self).__init__(child=child)


class CollectList(_UnaryExpression):
    def __init__(self, child):
        super(CollectList, self).__init__(child=child)


class CollectSet(_UnaryExpression):
    def __init__(self, child):
        super(CollectSet, self).__init__(child=child)


class Corr(_BinaryExpression):
    def __init__(self, left, right):
        super(Corr, self).__init__(left=left, right=right)


class Count(_UnaryExpression):
    def __init__(self, child):
        super(Count, self).__init__(child=child)


@param('expr')
@param('exprs')
class CountDistinct(Expression):
    def __init__(self, *exprs):
        if len(exprs) == 0:
            raise ValueError('Empty list of expressions for {}'.format(self.__class__.__name__))
        expr = exprs[0]
        exprs = exprs[1:]
        super(CountDistinct, self).__init__(expr=expr, exprs=exprs)

    @staticmethod
    def _param_to_json(pc, value):
        if pc.name == 'expr':
            assert isinstance(value, Expression)
            return value.to_json()
        if pc.name == 'exprs':
            return [v.to_json() for v in value]
        return Expression._param_to_json(pc, value)


class CovPopulation(_BinaryExpression):
    def __init__(self, left, right):
        super(CovPopulation, self).__init__(left=left, right=right)


class CovSample(_BinaryExpression):
    def __init__(self, left, right):
        super(CovSample, self).__init__(left=left, right=right)


@param('ignore_nulls', param_type=bool, server_name='ignoreNulls')
class First(_UnaryExpression):
    def __init__(self, child, ignore_nulls=False):
        super(First, self).__init__(child=child, ignore_nulls=ignore_nulls)


class Grouping(_UnaryExpression):
    def __init__(self, child):
        super(Grouping, self).__init__(child=child)


class GroupingID(_WithChildren):
    def __init__(self, children=()):
        super(GroupingID, self).__init__(children=children)


class Kurtosis(_UnaryExpression):
    def __init__(self, child):
        super(Kurtosis, self).__init__(child=child)


@param('ignore_nulls', param_type=bool, server_name='ignoreNulls')
class Last(_UnaryExpression):
    def __init__(self, child, ignore_nulls=False):
        super(Last, self).__init__(child=child, ignore_nulls=ignore_nulls)


class Max(_UnaryExpression):
    def __init__(self, child):
        super(Max, self).__init__(child=child)


class Min(_UnaryExpression):
    def __init__(self, child):
        super(Min, self).__init__(child=child)


class Skewness(_UnaryExpression):
    def __init__(self, child):
        super(Skewness, self).__init__(child=child)


class StddevSamp(_UnaryExpression):
    def __init__(self, child):
        super(StddevSamp, self).__init__(child=child)


class StddevPop(_UnaryExpression):
    def __init__(self, child):
        super(StddevPop, self).__init__(child=child)


@param('is_distinct', param_type=bool, server_name='isDistinct')
class Sum(_UnaryExpression):
    def __init__(self, child, is_distinct=False):
        super(Sum, self).__init__(child=child, is_distinct=is_distinct)


class VarianceSamp(_UnaryExpression):
    def __init__(self, child):
        super(VarianceSamp, self).__init__(child=child)


class VariancePop(_UnaryExpression):
    def __init__(self, child):
        super(VariancePop, self).__init__(child=child)


"""
alias
"""


@param('alias')
class Alias(_UnaryExpression):
    def __init__(self, child, alias='alias'):
        super(Alias, self).__init__(child=child, alias=alias)


@param('aliases')
class MultiAlias(_UnaryExpression):
    def __init__(self, child, aliases=()):
        super(MultiAlias, self).__init__(child=child, aliases=aliases)


@param('to')
class Cast(_UnaryExpression):
    def __init__(self, child, to):
        super(Cast, self).__init__(child=child, to=to)

    @staticmethod
    def _param_to_json(pc, value):
        if pc.name == 'child':
            assert isinstance(value, Expression)
            return value.to_json()

        assert pc.name == 'to' and isinstance(value, StorageTypes)
        return value.to_json()


"""
arithmetic
"""


class UnaryMinus(_UnaryExpression):
    def __init__(self, child):
        super(UnaryMinus, self).__init__(child=child)


class Add(_BinaryExpression):
    def __init__(self, left, right):
        super(Add, self).__init__(left=left, right=right)


class Subtract(_BinaryExpression):
    def __init__(self, left, right):
        super(Subtract, self).__init__(left=left, right=right)


class Multiply(_BinaryExpression):
    def __init__(self, left, right):
        super(Multiply, self).__init__(left=left, right=right)


class Divide(_BinaryExpression):
    def __init__(self, left, right):
        super(Divide, self).__init__(left=left, right=right)


class Remainder(_BinaryExpression):
    def __init__(self, left, right):
        super(Remainder, self).__init__(left=left, right=right)


class Abs(_UnaryExpression):
    def __init__(self, child):
        super(Abs, self).__init__(child=child)


class Sqrt(_UnaryExpression):
    def __init__(self, child):
        super(Sqrt, self).__init__(child=child)


class Acos(_UnaryExpression):
    def __init__(self, child):
        super(Acos, self).__init__(child=child)


class Asin(_UnaryExpression):
    def __init__(self, child):
        super(Asin, self).__init__(child=child)


class Atan(_UnaryExpression):
    def __init__(self, child):
        super(Atan, self).__init__(child=child)


class Atan2(_BinaryExpression):
    def __init__(self, left, right):
        super(Atan2, self).__init__(left=left, right=right)


class Bin(_UnaryExpression):
    def __init__(self, child):
        super(Bin, self).__init__(child=child)


class Cbrt(_UnaryExpression):
    def __init__(self, child):
        super(Cbrt, self).__init__(child=child)


class Ceil(_UnaryExpression):
    def __init__(self, child):
        super(Ceil, self).__init__(child=child)


@param('from_base', param_type='int', server_name='fromBase')
@param('to_base', param_type='int', server_name='toBase')
class Conv(_UnaryExpression):
    def __init__(self, child, from_base=10, to_base=2):
        super(Conv, self).__init__(child=child, from_base=from_base, to_base=to_base)


class Cos(_UnaryExpression):
    def __init__(self, child):
        super(Cos, self).__init__(child=child)


class Cosh(_UnaryExpression):
    def __init__(self, child):
        super(Cosh, self).__init__(child=child)


class Exp(_UnaryExpression):
    def __init__(self, child):
        super(Exp, self).__init__(child=child)


class Expm1(_UnaryExpression):
    def __init__(self, child):
        super(Expm1, self).__init__(child=child)


class Factorial(_UnaryExpression):
    def __init__(self, child):
        super(Factorial, self).__init__(child=child)


class Floor(_UnaryExpression):
    def __init__(self, child):
        super(Floor, self).__init__(child=child)


class Greatest(_WithChildren):
    def __init__(self, children=()):
        super(Greatest, self).__init__(children=children)


class Hex(_UnaryExpression):
    def __init__(self, child):
        super(Hex, self).__init__(child=child)


class Unhex(_UnaryExpression):
    def __init__(self, child):
        super(Unhex, self).__init__(child=child)


class Hypot(_BinaryExpression):
    def __init__(self, left, right):
        super(Hypot, self).__init__(left=left, right=right)


class Least(_WithChildren):
    def __init__(self, children=()):
        super(Least, self).__init__(children=children)


class Log(_UnaryExpression):
    def __init__(self, child):
        super(Log, self).__init__(child=child)


@param('base', param_type='double')
class Logarithm(_UnaryExpression):
    def __init__(self, child, base=10.0):
        super(Logarithm, self).__init__(child=child, base=base)


class Log10(_UnaryExpression):
    def __init__(self, child):
        super(Log10, self).__init__(child=child)


class Log1p(_UnaryExpression):
    def __init__(self, child):
        super(Log1p, self).__init__(child=child)


class Log2(_UnaryExpression):
    def __init__(self, child):
        super(Log2, self).__init__(child=child)


class Pow(_BinaryExpression):
    def __init__(self, left, right):
        super(Pow, self).__init__(left=left, right=right)


class Pmod(_BinaryExpression):
    def __init__(self, left, right):
        super(Pmod, self).__init__(left=left, right=right)


class Rint(_UnaryExpression):
    def __init__(self, child):
        super(Rint, self).__init__(child=child)


class Round(_BinaryExpression):
    def __init__(self, left, right):
        super(Round, self).__init__(left=left, right=right)


@param('scale', param_type='int')
class BRound(_UnaryExpression):
    def __init__(self, child, scale=5):
        super(BRound, self).__init__(child=child, scale=scale)


@param('num_bits', param_type='int', server_name='numBits')
class ShiftLeft(_UnaryExpression):
    def __init__(self, child, num_bits=5):
        super(ShiftLeft, self).__init__(child=child, num_bits=num_bits)


@param('num_bits', param_type='int', server_name='numBits')
class ShiftRight(_UnaryExpression):
    def __init__(self, child, num_bits=5):
        super(ShiftRight, self).__init__(child=child, num_bits=num_bits)


@param('num_bits', param_type='int', server_name='numBits')
class ShiftRightUnsigned(_UnaryExpression):
    def __init__(self, child, num_bits=5):
        super(ShiftRightUnsigned, self).__init__(child=child, num_bits=num_bits)


class Signum(_UnaryExpression):
    def __init__(self, child):
        super(Signum, self).__init__(child=child)


class Sin(_UnaryExpression):
    def __init__(self, child):
        super(Sin, self).__init__(child=child)


class Sinh(_UnaryExpression):
    def __init__(self, child):
        super(Sinh, self).__init__(child=child)


class Tan(_UnaryExpression):
    def __init__(self, child):
        super(Tan, self).__init__(child=child)


class Tanh(_UnaryExpression):
    def __init__(self, child):
        super(Tanh, self).__init__(child=child)


class ToDegrees(_UnaryExpression):
    def __init__(self, child):
        super(ToDegrees, self).__init__(child=child)


class ToRadians(_UnaryExpression):
    def __init__(self, child):
        super(ToRadians, self).__init__(child=child)


"""
collections
"""


class ArrayContains(_BinaryExpression):
    def __init__(self, left, right):
        super(ArrayContains, self).__init__(left=left, right=right)


class Explode(_UnaryExpression):
    def __init__(self, child):
        super(Explode, self).__init__(child=child)


class PosExplode(_UnaryExpression):
    def __init__(self, child):
        super(PosExplode, self).__init__(child=child)


@param('path')
class GetJsonObject(_UnaryExpression):
    def __init__(self, child, path=''):
        super(GetJsonObject, self).__init__(child=child, path=path)


@param('fields')
class JsonTuple(_UnaryExpression):
    def __init__(self, child, fields=()):
        super(JsonTuple, self).__init__(child=child, fields=fields)


class Size(_UnaryExpression):
    def __init__(self, child):
        super(Size, self).__init__(child=child)


@param('asc')
class SortArray(_UnaryExpression):
    def __init__(self, child, asc=True):
        super(SortArray, self).__init__(child=child, asc=asc)


"""
complex types
"""


class CreateArray(_WithChildren):
    def __init__(self, children=()):
        super(CreateArray, self).__init__(children=children)


class CreateMap(_WithChildren):
    def __init__(self, children=()):
        super(CreateMap, self).__init__(children=children)


class CreateStruct(_WithChildren):
    def __init__(self, children=()):
        super(CreateStruct, self).__init__(children=children)


"""
conditional
"""


@param('branches')
@param('else_value', server_name='elseValue')
class CaseWhen(Expression):
    def __init__(self, branches, else_value=None):
        """
        :param branches: a list [(expr, expr)] where each tuple is a condition and the corresponding value
        :param else_value: another expression for the "else" value, or None if unspecified
        """
        super(CaseWhen, self).__init__(branches=branches, else_value=else_value)

    @staticmethod
    def _param_to_json(pc, value):
        if pc.name == 'branches':
            result = []
            for pair in value:
                result.append([ex.to_json() for ex in pair])
            return result

        assert pc.name == 'else_value'
        assert value is None or isinstance(value, Expression)
        return value.to_json() if isinstance(value, Expression) else None


class GetItem(_BinaryExpression):
    def __init__(self, left, right):
        super(GetItem, self).__init__(left=left, right=right)


@param('field_name', server_name='fieldName')
class GetField(_UnaryExpression):
    def __init__(self, child, field_name=''):
        super(GetField, self).__init__(child=child, field_name=field_name)


"""
datetime
"""


@param('num_months', param_type='int', server_name='numMonths')
class AddMonths(_UnaryExpression):
    def __init__(self, child, num_months=1):
        super(AddMonths, self).__init__(child=child, num_months=num_months)


class CurrentDate(Expression):
    def __init__(self):
        super(CurrentDate, self).__init__()


class CurrentTimestamp(Expression):
    def __init__(self):
        super(CurrentTimestamp, self).__init__()


@param('fmt', server_name='format')
class DateFormatClass(_UnaryExpression):
    def __init__(self, child, fmt=''):
        super(DateFormatClass, self).__init__(child=child, fmt=fmt)


@param('days', param_type='int')
class DateAdd(_UnaryExpression):
    def __init__(self, child, days=1):
        super(DateAdd, self).__init__(child=child, days=days)


@param('days', param_type='int')
class DateSub(_UnaryExpression):
    def __init__(self, child, days=1):
        super(DateSub, self).__init__(child=child, days=days)


class DateDiff(_BinaryExpression):
    def __init__(self, left, right):
        super(DateDiff, self).__init__(left=left, right=right)


class Year(_UnaryExpression):
    def __init__(self, child):
        super(Year, self).__init__(child=child)


class Quarter(_UnaryExpression):
    def __init__(self, child):
        super(Quarter, self).__init__(child=child)


class Month(_UnaryExpression):
    def __init__(self, child):
        super(Month, self).__init__(child=child)


class DayOfMonth(_UnaryExpression):
    def __init__(self, child):
        super(DayOfMonth, self).__init__(child=child)


class DayOfYear(_UnaryExpression):
    def __init__(self, child):
        super(DayOfYear, self).__init__(child=child)


class Hour(_UnaryExpression):
    def __init__(self, child):
        super(Hour, self).__init__(child=child)


class LastDay(_UnaryExpression):
    def __init__(self, child):
        super(LastDay, self).__init__(child=child)


class Minute(_UnaryExpression):
    def __init__(self, child):
        super(Minute, self).__init__(child=child)


class MonthsBetween(_BinaryExpression):
    def __init__(self, left, right):
        super(MonthsBetween, self).__init__(left=left, right=right)


@param('day_of_week', server_name='dayOfWeek')
class NextDay(_UnaryExpression):
    def __init__(self, child, day_of_week=''):
        super(NextDay, self).__init__(child=child, day_of_week=day_of_week)


class Second(_UnaryExpression):
    def __init__(self, child):
        super(Second, self).__init__(child=child)


class WeekOfYear(_UnaryExpression):
    def __init__(self, child):
        super(WeekOfYear, self).__init__(child=child)


@param('fmt', server_name='format')
class FromUnixTime(_UnaryExpression):
    def __init__(self, child, fmt=''):
        super(FromUnixTime, self).__init__(child=child, fmt=fmt)


@param('fmt', server_name='format')
class UnixTimestamp(_UnaryExpression):
    def __init__(self, child, fmt=''):
        super(UnixTimestamp, self).__init__(child=child, fmt=fmt)


class ToDate(_UnaryExpression):
    def __init__(self, child):
        super(ToDate, self).__init__(child=child)


@param('fmt', server_name='format')
class TruncDate(_UnaryExpression):
    def __init__(self, child, fmt=''):
        super(TruncDate, self).__init__(child=child, fmt=fmt)


@param('tz')
class FromUTCTimestamp(_UnaryExpression):
    def __init__(self, child, tz=''):
        super(FromUTCTimestamp, self).__init__(child=child, tz=tz)


@param('tz')
class ToUTCTimestamp(_UnaryExpression):
    def __init__(self, child, tz=''):
        super(ToUTCTimestamp, self).__init__(child=child, tz=tz)


@param('window_duration', server_name='windowDuration')
@param('slide_duration', server_name='slideDuration')
@param('start_time', server_name='startTime')
class TimeWindow(_UnaryExpression):
    def __init__(self, child, window_duration='', slide_duration='', start_time=''):
        super(TimeWindow, self).__init__(child=child, window_duration=window_duration,
                                         slide_duration=slide_duration,
                                         start_time=start_time)


"""
misc
"""


class Md5(_UnaryExpression):
    def __init__(self, child):
        super(Md5, self).__init__(child=child)


class Sha1(_UnaryExpression):
    def __init__(self, child):
        super(Sha1, self).__init__(child=child)


@param('num_bits', param_type='int', server_name='numBits')
class Sha2(_UnaryExpression):
    def __init__(self, child, num_bits=0):
        super(Sha2, self).__init__(child=child, num_bits=num_bits)


class Crc32(_UnaryExpression):
    def __init__(self, child):
        super(Crc32, self).__init__(child=child)


class Murmur3Hash(_WithChildren):
    def __init__(self, children=()):
        super(Murmur3Hash, self).__init__(children=children)


"""
non-aggregation
"""


class Coalesce(_WithChildren):
    def __init__(self, children=()):
        super(Coalesce, self).__init__(children=children)


class InputFileName(Expression):
    def __init__(self):
        super(InputFileName, self).__init__()


class MonotonicallyIncreasingID(Expression):
    def __init__(self):
        super(MonotonicallyIncreasingID, self).__init__()


class NaNvl(_BinaryExpression):
    def __init__(self, left, right):
        super(NaNvl, self).__init__(left=left, right=right)


@param('seed', param_type='long')
class Rand(Expression):
    def __init__(self, seed=42):
        super(Rand, self).__init__(seed=seed)


@param('seed', param_type='long')
class Randn(Expression):
    def __init__(self, seed=42):
        super(Randn, self).__init__(seed=seed)


class SparkPartitionID(Expression):
    def __init__(self):
        super(SparkPartitionID, self).__init__()


@param('expr')
class RawExpression(Expression):
    def __init__(self, expr=''):
        super(RawExpression, self).__init__(expr=expr)


"""
predicates
"""


class Not(_UnaryExpression):
    def __init__(self, child):
        super(Not, self).__init__(child=child)


class EqualTo(_BinaryExpression):
    def __init__(self, left, right):
        super(EqualTo, self).__init__(left=left, right=right)


class GreaterThan(_BinaryExpression):
    def __init__(self, left, right):
        super(GreaterThan, self).__init__(left=left, right=right)


class LessThan(_BinaryExpression):
    def __init__(self, left, right):
        super(LessThan, self).__init__(left=left, right=right)


class GreaterThanOrEqual(_BinaryExpression):
    def __init__(self, left, right):
        super(GreaterThanOrEqual, self).__init__(left=left, right=right)


class LessThanOrEqual(_BinaryExpression):
    def __init__(self, left, right):
        super(LessThanOrEqual, self).__init__(left=left, right=right)


class EqualNullSafe(_BinaryExpression):
    def __init__(self, left, right):
        super(EqualNullSafe, self).__init__(left=left, right=right)


class IsNaN(_UnaryExpression):
    def __init__(self, child):
        super(IsNaN, self).__init__(child=child)


class IsNull(_UnaryExpression):
    def __init__(self, child):
        super(IsNull, self).__init__(child=child)


class IsNotNull(_UnaryExpression):
    def __init__(self, child):
        super(IsNotNull, self).__init__(child=child)


class Or(_BinaryExpression):
    def __init__(self, left, right):
        super(Or, self).__init__(left=left, right=right)


class And(_BinaryExpression):
    def __init__(self, left, right):
        super(And, self).__init__(left=left, right=right)


@param('value')
@param('ls', server_name='list')
class In(Expression):
    def __init__(self, value, ls=()):
        super(In, self).__init__(value=value, ls=ls)

    @staticmethod
    def _param_to_json(pc, value):
        if pc.name == 'value':
            assert isinstance(value, Expression)
            return value.to_json()
        assert pc.name == 'ls'
        assert isinstance(value, (tuple, list)) and all(isinstance(ex, Expression) for ex in value)
        return [ex.to_json() for ex in value]


class BitwiseOr(_BinaryExpression):
    def __init__(self, left, right):
        super(BitwiseOr, self).__init__(left=left, right=right)


class BitwiseAnd(_BinaryExpression):
    def __init__(self, left, right):
        super(BitwiseAnd, self).__init__(left=left, right=right)


class BitwiseXor(_BinaryExpression):
    def __init__(self, left, right):
        super(BitwiseXor, self).__init__(left=left, right=right)


class BitwiseNot(_UnaryExpression):
    def __init__(self, child):
        super(BitwiseNot, self).__init__(child=child)


"""
primitives
"""


@param('value')
class Literal(Expression):
    def __init__(self, value):
        super(Literal, self).__init__(value=value)


@param('col_name', server_name='colName')
class UnresolvedColumnName(Expression):
    def __init__(self, col_name):
        super(UnresolvedColumnName, self).__init__(col_name=col_name)


"""
SortOrder
"""


@param('direction')
class SortOrder(_UnaryExpression):
    Ascending = 'Ascending'
    Descending = 'Descending'

    def __init__(self, child, direction=Ascending):
        super(SortOrder, self).__init__(child=child, direction=direction)


"""
strings
"""


class Ascii(_UnaryExpression):
    def __init__(self, child):
        super(Ascii, self).__init__(child=child)


class Base64(_UnaryExpression):
    def __init__(self, child):
        super(Base64, self).__init__(child=child)


class Concat(_WithChildren):
    def __init__(self, children=()):
        super(Concat, self).__init__(children=children)


@param('sep')
class ConcatWs(_WithChildren):
    def __init__(self, children=(), sep=''):
        super(ConcatWs, self).__init__(sep=sep, children=children)

    @staticmethod
    def _param_to_json(pc, value):
        if pc.name == 'sep':
            return to_json(value, pc.param_type)
        return _WithChildren._param_to_json(pc, value)


class Contains(_BinaryExpression):
    def __init__(self, left, right):
        super(Contains, self).__init__(left=left, right=right)


@param('charset')
class Decode(_UnaryExpression):
    def __init__(self, child, charset='ascii'):
        super(Decode, self).__init__(child=child, charset=charset)


@param('charset')
class Encode(_UnaryExpression):
    def __init__(self, child, charset='ascii'):
        super(Encode, self).__init__(child=child, charset=charset)


class EndsWith(_BinaryExpression):
    def __init__(self, left, right):
        super(EndsWith, self).__init__(left=left, right=right)


@param('precision', param_type='int')
class FormatNumber(_UnaryExpression):
    def __init__(self, child, precision=2):
        super(FormatNumber, self).__init__(child=child, precision=precision)


@param('fmt', server_name='format')
class FormatString(_WithChildren):
    def __init__(self, fmt='', children=()):
        super(FormatString, self).__init__(fmt=fmt, children=children)

    @staticmethod
    def _param_to_json(pc, value):
        if pc.name == 'fmt':
            return to_json(value, pc.param_type)
        return _WithChildren._param_to_json(pc, value)


class InitCap(_UnaryExpression):
    def __init__(self, child):
        super(InitCap, self).__init__(child=child)


@param('sub_str', server_name='subStr')
class StringInstr(_UnaryExpression):
    def __init__(self, child, sub_str=''):
        super(StringInstr, self).__init__(child=child, sub_str=sub_str)


class Length(_UnaryExpression):
    def __init__(self, child):
        super(Length, self).__init__(child=child)


@param('literal')
class Like(_UnaryExpression):
    def __init__(self, child, literal=''):
        super(Like, self).__init__(child=child, literal=literal)


class Lower(_UnaryExpression):
    def __init__(self, child):
        super(Lower, self).__init__(child=child)


class Levenshtein(_BinaryExpression):
    def __init__(self, left, right):
        super(Levenshtein, self).__init__(left=left, right=right)


@param('substr')
@param('pos', param_type='int')
class StringLocate(_UnaryExpression):
    def __init__(self, child, substr='', pos=0):
        super(StringLocate, self).__init__(child=child, substr=substr, pos=pos)


@param('length', param_type='int', server_name='len')
@param('padding', server_name='pad')
class StringLPad(_UnaryExpression):
    def __init__(self, child, length=0, padding=''):
        super(StringLPad, self).__init__(child=child, length=length, padding=padding)


class StringTrimLeft(_UnaryExpression):
    def __init__(self, child):
        super(StringTrimLeft, self).__init__(child=child)


@param('expr')
@param('group_idx', param_type='int', server_name='groupIdx')
class RegExpExtract(_UnaryExpression):
    def __init__(self, child, expr='', group_idx=0):
        super(RegExpExtract, self).__init__(child=child, expr=expr, group_idx=group_idx)


@param('pattern')
@param('replacement')
class RegExpReplace(_UnaryExpression):
    def __init__(self, child, pattern='', replacement=''):
        super(RegExpReplace, self).__init__(child=child, pattern=pattern, replacement=replacement)


@param('literal')
class RLike(_UnaryExpression):
    def __init__(self, child, literal=''):
        super(RLike, self).__init__(child=child, literal=literal)


class StartsWith(_BinaryExpression):
    def __init__(self, left, right):
        super(StartsWith, self).__init__(left=left, right=right)


@param('length', param_type='int', server_name='len')
@param('padding', server_name='pad')
class StringRPad(_UnaryExpression):
    def __init__(self, child, length=0, padding=''):
        super(StringRPad, self).__init__(child=child, length=length, padding=padding)


@param('n', param_type='int')
class StringRepeat(_UnaryExpression):
    def __init__(self, child, n=1):
        super(StringRepeat, self).__init__(child=child, n=n)


class StringReverse(_UnaryExpression):
    def __init__(self, child):
        super(StringReverse, self).__init__(child=child)


class StringTrimRight(_UnaryExpression):
    def __init__(self, child):
        super(StringTrimRight, self).__init__(child=child)


class SoundEx(_UnaryExpression):
    def __init__(self, child):
        super(SoundEx, self).__init__(child=child)


@param('pattern')
class StringSplit(_UnaryExpression):
    def __init__(self, child, pattern=''):
        super(StringSplit, self).__init__(child=child, pattern=pattern)


@param('matching')
@param('replace')
class StringTranslate(_UnaryExpression):
    def __init__(self, child, matching='', replace=''):
        super(StringTranslate, self).__init__(child=child, matching=matching, replace=replace)


class StringTrim(_UnaryExpression):
    def __init__(self, child):
        super(StringTrim, self).__init__(child=child)


@param('s', server_name='str')
@param('pos')
@param('length', server_name='len')
class Substring(Expression):
    def __init__(self, s, pos, length):
        super(Substring, self).__init__(s=s, pos=pos, length=length)


@param('delim')
@param('cnt', param_type='int', server_name='count')
class SubstringIndex(_UnaryExpression):
    def __init__(self, child, delim, cnt=1):
        super(SubstringIndex, self).__init__(child=child, delim=delim, cnt=cnt)


class UnBase64(_UnaryExpression):
    def __init__(self, child):
        super(UnBase64, self).__init__(child=child)


class Upper(_UnaryExpression):
    def __init__(self, child):
        super(Upper, self).__init__(child=child)


"""
Spark-specific
"""


@param('df_id', server_name='dfId')
@param('col_name', server_name='colName')
@server_namespace('io.cebes.spark.df.expressions')
class SparkPrimitiveExpression(Expression):
    def __init__(self, df_id='', col_name=''):
        super(SparkPrimitiveExpression, self).__init__(df_id=df_id, col_name=col_name)
