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

import six

import pycebes.core.expressions as exprs
from pycebes.core.schema import StorageTypes
from pycebes.internal.helpers import require


def lit(literal):
    """
    Return a Column object containing the given literal
    """
    if isinstance(literal, Column):
        return literal
    return Column(exprs.Literal(literal))


@six.python_2_unicode_compatible
class Column(object):
    def __init__(self, expr):
        """
        Represent a column of a #Dataframe, backed by an expression.

        # Arguments
        expr (exprs.Expression): Expression behind this column
        """
        require(isinstance(expr, exprs.Expression), 'expr has to be an Expression, got {!r}'.format(expr))
        self.expr = expr

    def __repr__(self):
        return '{}(expr={!r})'.format(self.__class__.__name__, self.expr)

    """
    helpers
    """

    def _with_expr(self, expr_class, *args):
        """
        Helper to return a new column with the given expression class, constructed
        with the current expression as the first argument, and
        other arguments in *args
        """
        return Column(expr_class(self.expr, *args))

    def _bin_op(self, expr_class, other):
        """
        Binary operation with this expression (self.expr) and the other (functions.lit(other).expr)
        """
        return Column(expr_class(self.expr, lit(other).expr))

    def to_json(self):
        """
        Serialize the Column into JSON format

        # Returns
         JSON representation of this Column
        """
        return {'expr': self.expr.to_json()}

    """
    Building expressions
    """

    @property
    def desc(self):
        """
        Returns an ordering used in sorting.

        # Example
        ```python
        df.sort(df.col1.desc)
        ```
        """
        return self._with_expr(exprs.SortOrder, exprs.SortOrder.Descending)

    @property
    def asc(self):
        """
        Returns an ordering used in sorting.

        # Example
        ```python
        df.sort(df.col1.asc)
        ```
        """
        return self._with_expr(exprs.SortOrder, exprs.SortOrder.Ascending)

    def __neg__(self):
        """
        Unary minus, i.e. negate the expression.

        # Example
        ```python
        df.select(-df.cost)
        ```
        """
        return self._with_expr(exprs.UnaryMinus)

    def __nonzero__(self):
        raise ValueError("Cannot convert column into bool: please use '&' for 'and', '|' for 'or', "
                         "'~' for 'not' when building Dataframe boolean expressions.")

    __bool__ = __nonzero__

    def __invert__(self):
        """
        Inversion of boolean expression, i.e. NOT.

        # Example
        ```python
        # select rows that are not rich (isRich === false)
        df.filter(~(df.isRich))
        ```
        """
        return self._with_expr(exprs.Not)

    def __eq__(self, other):
        """
        Equality test.

        # Example
        ```python
        df.filter(df.colA == df.colB)
        ```
        """
        return self._bin_op(exprs.EqualTo, other)

    def __ne__(self, other):
        """
        Inequality test.

        # Example
        ```python
        df.select(df.colA != df.colB)
        ```
        """
        return Column(exprs.Not(self.__eq__(other)))

    def __gt__(self, other):
        """
        # Example
        ```python
        # The following selects people older than 25.
        people.select(people.age > 25)
        ```
        """
        return self._bin_op(exprs.GreaterThan, other)

    def __lt__(self, other):
        """
        # Example
        ```python
        # The following selects people younger than 25.
        people.select(people.age < 25)
        ```
        """
        return self._bin_op(exprs.LessThan, other)

    def __le__(self, other):
        """
        # Example
        ```python
        # The following selects people younger than 25.
        people.select(people.age <= 25)
        ```
        """
        return self._bin_op(exprs.LessThanOrEqual, other)

    def __ge__(self, other):
        """
        # Example
        ```python
        # The following selects people younger than 25.
        people.select(people.age >= 25)
        ```
        """
        return self._bin_op(exprs.GreaterThanOrEqual, other)

    def equal_null_safe(self, other):
        """
        Equality test that is safe for null values.
        Returns same result with EQUAL(=) operator for non-null operands,
        but returns ``true`` if both are ``NULL``, ``false`` if one of the them is ``NULL``.
        """
        return self._bin_op(exprs.EqualNullSafe, other)

    def when(self, condition, value):
        """
        Evaluates a list of conditions and returns one of multiple possible result expressions.
        If otherwise() is not defined at the end, `null` is returned for unmatched conditions.

        # Arguments
        condition: the condition to be checked
        value (Column): the output value if condition is true

        # Example
        ```python
        # encoding gender string column into integer.
        people.select(functions.when(people.gender == "male", 0)
            .when(people.gender == "female", 1)
            .otherwise(2))
        ```
        """
        if not isinstance(condition, Column):
            raise ValueError('Expect condition as a Column')
        if not isinstance(self.expr, exprs.CaseWhen):
            raise ValueError('when() can only be applied on a Column previously generated by when() function')
        if self.expr.else_value is not None:
            raise ValueError('when() cannot be applied once otherwise() is applied')
        return Column(exprs.CaseWhen(self.expr[:] + [(condition, lit(value).expr)]))

    def otherwise(self, value):
        """
        Evaluates a list of conditions and returns one of multiple possible result expressions.
        If otherwise() is not defined at the end, `null` is returned for unmatched conditions.

        # Example
        ```python
        # encoding gender string column into integer.
        people.select(functions.when(people.gender == "male", 0)
            .when(people.gender == "female", 1)
            .otherwise(2))
        ```
        """
        if not isinstance(self.expr, exprs.CaseWhen):
            raise ValueError('otherwise() can only be applied on a Column previously generated by when() function')
        if self.expr.else_value is not None:
            raise ValueError('otherwise() can only be applied once on a Column previously generated by when()')
        return Column(exprs.CaseWhen(self.expr[:], lit(value).expr))

    def between(self, lower_bound, upper_bound):
        """
        True if the current column is between the lower bound and upper bound, inclusive.
        """
        return (self >= lower_bound) & (self <= upper_bound)

    def is_nan(self):
        """
        True if the current expression is NaN.
        """
        return self._with_expr(exprs.IsNaN)

    def is_null(self):
        """
        True if the current expression is null.
        """
        return self._with_expr(exprs.IsNull)

    def is_not_null(self):
        """
        True if the current expression is not null.
        """
        return self._with_expr(exprs.IsNotNull)

    def __or__(self, other):
        """
        Boolean OR.

        # Example
        ```python
        # Selects people that are in school or employed.
        people.filter(people.inSchool | people.isEmployed)
        ```
        """
        return self._bin_op(exprs.Or, other)

    def __and__(self, other):
        """
        Boolean AND.

        # Example
        ```python
        # Selects people that are in school and employed at the same time.
        people.select(people.inSchool & people.isEmployed)
        ```
        """
        return self._bin_op(exprs.And, other)

    def __add__(self, other):
        """
        Sum of this expression and another expression. Only work on numeric columns.

        # Example
        ```python
        # Selects the sum of a person's height and weight.
        people.select(people.height + people.weight)
        ```
        """
        return self._bin_op(exprs.Add, other)

    def __sub__(self, other):
        """
        Subtraction. Subtract the other expression from this expression. Only work on numeric columns.

        # Example
        ```python
        # Selects the difference between people's height and their weight.
        people.select(people.height - people.weight)
        ```
        """
        return self._bin_op(exprs.Subtract, other)

    def __mul__(self, other):
        """
        Multiplication of this expression and another expression. Only work on numeric columns.

        # Example
        ```python
        # Multiplies a person's height by their weight
        people.select(people.height * people.weight)
        ```
        """
        return self._bin_op(exprs.Multiply, other)

    def __truediv__(self, other):
        """
         Division this expression by another expression. Only work on numeric columns.

        # Example
        ```python
        # Divides a person's height by their weight
        people.select(people.height / people.weight)
        ```
        """
        return self._bin_op(exprs.Divide, other)

    def __mod__(self, other):
        """
        Modulo (a.k.a. remainder) expression.
        """
        return self._bin_op(exprs.Remainder, other)

    def isin(self, values):
        """
        A boolean expression that is evaluated to true if the value of this expression is contained
        by the evaluated values of the arguments.
        """
        return self._with_expr(exprs.In, [lit(v).expr for v in values])

    def like(self, literal):
        """
        SQL like expression. Result will be a BooleanType column

        # Arguments
        literal: a string
        """
        if not isinstance(literal, six.text_type):
            raise ValueError('Expected a string, got {!r}'.format(literal))
        return self._with_expr(exprs.Like, literal)

    def rlike(self, literal):
        """
        SQL RLIKE expression (LIKE with Regex). Result will be a BooleanType column

        # Arguments
        literal: a string
        """
        if not isinstance(literal, six.text_type):
            raise ValueError('Expected a string, got {!r}'.format(literal))
        return self._with_expr(exprs.RLike, literal)

    def get_item(self, key):
        """
        An expression that gets an item at position `ordinal` out of an array,
        or gets a value by key `key` in a MapType column.
        """
        return self._bin_op(exprs.GetItem, key)

    def get_field(self, field_name='key'):
        """
        An expression that gets a field by name in a StructType column.
        """
        return self._bin_op(exprs.GetField, field_name)

    def substr(self, start_pos, length):
        """
        An expression that returns a substring.

        This is not zero-based, but 1-based index. The first character in str has index 1.

        `start_pos` and `length` are handled specially:

          - ``"Content".substr(1, 3)`` gives ``"Con"``
          - ``"Content".substr(-100, 2)`` gives ``""``
          - ``"Content".substr(-100, 102)`` gives ``"Content"``
          - ``"Content".substr(2, 100)`` gives ``"ontent"``

        # Arguments
        start_pos: starting position, can be an integer, or a ``Column`` that gives an integer
        length: length of the sub-string, can be an integer, or a ``Column`` that gives an integer
        """
        return self._with_expr(exprs.Substring, lit(start_pos).expr, lit(length).expr)

    def contains(self, other):
        """
        Contains the other element
        """
        return self._bin_op(exprs.Contains, other)

    def starts_with(self, other):
        """
        String starts with.

        # Arguments
        other: string to test, can be a string, or a ``Column``  that gives a string
        """
        return self._bin_op(exprs.StartsWith, other)

    def ends_with(self, other):
        """
        String ends with.

        # Arguments
        other: string to test, can be a string, or a ``Column``  that gives a string
        """
        return self._bin_op(exprs.StartsWith, other)

    def alias(self, *alias):
        """
        Gives the column an alias, or multiple aliases in case it is used on an expression
        that returns more than one column (such as ``explode``).

        # Example
        ```python
        # Renames colA to colB in select output.
        df.select(df.colA.alias('colB'))

        # multiple alias
        df.select(functions.explode(df.myMap).alias('key', 'value'))
        ```
        """
        if len(alias) == 1:
            return self._with_expr(exprs.Alias, alias[0])
        return self._with_expr(exprs.MultiAlias, alias)

    def name(self, *alias):
        """
        Gives the column a name. Same as :func:`alias`
        """
        return self.alias(*alias)

    def cast(self, to):
        """
        Casts the column to a different data type.

        # Arguments
        to (StorageType): the StorageType to cast to

        # Example
        ```python
        # Casts colA to [[IntegerType]].
        df.select(df.colA.cast(IntegerType))
        df.select(df.colA.cast("int"))
        ```
        """
        if isinstance(to, six.text_type):
            to = StorageTypes.from_json(to)
        return self._bin_op(exprs.Cast, to)

    def bitwise_or(self, other):
        """
        Compute bitwise OR of this expression with another expression.

        # Example
        ```python
            df.select(df.colA.bitwise_or(df.colB))
        ```
        """
        return self._bin_op(exprs.BitwiseOr, other)

    def bitwise_and(self, other):
        """
        Compute bitwise AND of this expression with another expression.

        # Example
        ```python
        df.select(df.colA.bitwise_and(df.colB))
        ```
        """
        return self._bin_op(exprs.BitwiseAnd, other)

    def bitwise_xor(self, other):
        """
        Compute bitwise XOR of this expression with another expression.

        # Example
        ```python
        df.select(df.colA.bitwise_xor(df.colB))
        ```
        """
        return self._bin_op(exprs.BitwiseXor, other)

    @property
    def bitwise_not(self):
        """
        Compute bitwise NOT of this expression

        # Example
        ```python
        df.select(df.colA.bitwise_not)
        ```
        """
        return self._with_expr(exprs.BitwiseNot)
