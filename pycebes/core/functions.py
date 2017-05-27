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
import random
import pycebes.core.expressions as exprs
from pycebes.core.column import Column, lit


def col(col_name='column'):
    """
    Returns a :ref:`Column` based on the given column name.
    """
    return Column(exprs.UnresolvedColumnName(col_name))


def _get_column(column):
    """
    Helper to get a ``Column`` object if the given argument is a string
    """
    if isinstance(column, six.text_type):
        column = col(column)
    if not isinstance(column, Column):
        raise ValueError('A Column or a column name (as a string) is expected. Got {!r}'.format(column))
    return column


def _with_expr(expr_class, cols, *args):
    """
    Helper to return a column constructed from the given expression class,
    with Column arguments in cols and additional arguments in args
    `cols` will be transformed into Column using :ref:`_get_column`,
    other arguments in args will be passed as-it-is into the constructor of the expression class
    """
    if not isinstance(cols, (tuple, list)):
        cols = [cols]
    all_args = [_get_column(c).expr for c in cols] + list(args)
    return Column(expr_class(*all_args))


"""
Aggregate functions
"""


def approx_count_distinct(column, rsd=0.05):
    """
    Returns the approximate number of distinct items in a group.
    :param column: the column to compute
    :param rsd: maximum estimation error allowed (default = 0.05)
    """
    return _with_expr(exprs.ApproxCountDistinct, column, rsd)


def avg(column):
    """
    Returns the average of the values in a group
    """
    return _with_expr(exprs.Average, column)


def collect_list(column):
    """
    Returns a list of objects with duplicates
    """
    return _with_expr(exprs.CollectList, column)


def collect_set(column):
    """
    Returns a set of objects with duplicate elements eliminated
    """
    return _with_expr(exprs.CollectSet, column)


def corr(column1, column2):
    """
    Returns the Pearson Correlation Coefficient for two columns
    """
    return _with_expr(exprs.Corr, cols=[column1, column2])


def count(column):
    """
    returns the number of items in a group
    """
    return _with_expr(exprs.Count, column)


def count_distinct(*args):
    """
    returns the number of distinct items in a group
    """
    return _with_expr(exprs.CountDistinct, cols=args)


def covar_pop(column1, column2):
    """
    Returns the population covariance for two columns
    """
    return _with_expr(exprs.CovPopulation, cols=[column1, column2])


def covar_samp(column1, column2):
    """
    Returns the sample covariance for two columns
    """
    return _with_expr(exprs.CovSample, cols=[column1, column2])


def first(column, ignore_nulls=False):
    """
    Returns the first value in a group.

    The function by default returns the first values it sees. It will return the first non-null
    value it sees when ``ignore_nulls`` is set to true. If all values are null, then null is returned.
    """
    return _with_expr(exprs.First, column, ignore_nulls)


def last(column, ignore_nulls=False):
    """
    Returns the last value in a group.

    The function by default returns the last values it sees. It will return the last non-null
    value it sees when ``ignore_nulls`` is set to true. If all values are null, then null is returned.
    """
    return _with_expr(exprs.Last, column, ignore_nulls)


def grouping(column):
    """
    indicates whether a specified column in a GROUP BY list is aggregated
    or not, returns 1 for aggregated or 0 for not aggregated in the result set.
    """
    return _with_expr(exprs.Grouping, column)


def grouping_id(*columns):
    """
    returns the level of grouping, equals to

    ``(grouping(c1) << (n-1)) + (grouping(c2) << (n-2)) + ... + grouping(cn)``

    .. :note: the list of columns should match with grouping columns exactly, or empty (means all the
        grouping columns).
    """
    return _with_expr(exprs.GroupingID, columns)


def kurtosis(column):
    """
    Returns the kurtosis of the values in a group
    """
    return _with_expr(exprs.Kurtosis, column)


def max(column):
    """
    Returns the kurtosis of the values in a group
    """
    return _with_expr(exprs.Max, column)


mean = avg


def min(column):
    """
    Returns the kurtosis of the values in a group
    """
    return _with_expr(exprs.Min, column)


def skewness(column):
    """
    Returns the kurtosis of the values in a group
    """
    return _with_expr(exprs.Skewness, column)


def stddev_samp(column):
    """
    Returns the sample standard deviation of the expression in a group.
    """
    return _with_expr(exprs.StddevSamp, column)


stddev = stddev_samp


def stddev_pop(column):
    """
    Returns the population standard deviation of the expression in a group.
    """
    return _with_expr(exprs.StddevPop, column)


def sum(column, is_distinct=False):
    """
    Returns the sum of all values in the expression
    :param column: the column to compute the sum
    :param is_distinct: whether to only compute the sum of distinct values. False by default,
    meaning computing the sum of all values
    """
    return _with_expr(exprs.Sum, column, is_distinct)


def var_samp(column):
    """
    Returns the unbiased variance of the values in a grou
    """
    return _with_expr(exprs.VarianceSamp, column)


variance = var_samp


def var_pop(column):
    """
    Returns the population variance of the values in a group
    """
    return _with_expr(exprs.VariancePop, column)


"""
Non-aggregate functions
"""


def when(condition, value):
    """
    Evaluates a list of conditions and returns one of multiple possible result expressions.
    If otherwise is not defined at the end, null is returned for unmatched conditions.
    :param condition: the condition
    :type condition: Column
    :param value: value to take when condition is true

    :Example:
        # encoding gender string column into integer
        people.select(functions.when(people.gender == 'male', 0)
            .when(people.gender == 'female', 1)
            .otherwise(2))
    """
    if not isinstance(condition, Column):
        raise ValueError('condition must be a Column expression')
    return _with_expr(exprs.CaseWhen, (), [(condition.expr, lit(value).expr)])


def array(*columns):
    """
    Creates a new array column. The input columns must all have the same data type.
    """
    return _with_expr(exprs.CreateArray, cols=columns)


def create_map(*columns):
    """
    Creates a new map column. The input columns must be grouped as key-value pairs, e.g.
    (key1, value1, key2, value2, ...). The key columns must all have the same data type, and can't
    be null. The value columns must all have the same data type.
    """
    return _with_expr(exprs.CreateMap, cols=columns)


def coalesce(*columns):
    """
    Returns the first column that is not null, or null if all inputs are null.

    For example, ``coalesce(a, b, c)`` will return a if a is not null,
    or b if a is null and b is not null, or c if both a and b are null but c is not null.
    """
    return _with_expr(exprs.Coalesce, cols=columns)


def input_file_name():
    """
    Spark-specific: Creates a string column for the file name of the current Spark task
    """
    return _with_expr(exprs.InputFileName, cols=())


def is_nan(column):
    """
    Return true iff the column is NaN
    :type column: Column
    """
    return _with_expr(exprs.IsNaN, column)


def is_null(column):
    """
    Return true iff the column is null
    :type column: Column
    """
    return _with_expr(exprs.IsNull, column)


def monotonically_increasing_id():
    """
    Spark-specific: A column expression that generates monotonically increasing 64-bit integers.

    The generated ID is guaranteed to be monotonically increasing and unique, but not consecutive.
    The current implementation puts the partition ID in the upper 31 bits, and the record number
    within each partition in the lower 33 bits. The assumption is that the data frame has
    less than 1 billion partitions, and each partition has less than 8 billion records.

    As an example, consider a ``Dataframe`` with two partitions, each with 3 records.
    This expression would return the following IDs:
    0, 1, 2, 8589934592 (1L << 33), 8589934593, 8589934594.
    """
    return _with_expr(exprs.MonotonicallyIncreasingID, cols=())


def nanvl(column1, column2):
    """
    Returns col1 if it is not NaN, or col2 if col1 is NaN.
    Both inputs should be floating point columns (DoubleType or FloatType).
    """
    return _with_expr(exprs.NaNvl, cols=[column1, column2])


def negate(column):
    """
    Unary minus, i.e. negate the expression.
    :Example:
        # Select the amount column and negates all values.
        df.select(-df.amount)
        df.select(functions.negate(df.amount))
        df.select(functions.negate("amount"))
    """
    return _with_expr(exprs.UnaryMinus, column)


def rand(seed=None):
    """
    Generate a random column with i.i.d. samples from U[0.0, 1.0].
    ``seed`` will be automatically randomly generated by ``random.randint(0, 1000)`` if not specified.
    .. :note: this is un-deterministic when data partitions are not fixed.
    """
    if seed is None:
        seed = random.randint(0, 1000)
    return _with_expr(exprs.Rand, (), seed)


def randn(seed=None):
    """
    Generate a column with i.i.d. samples from the standard normal distribution.
    ``seed`` will be automatically randomly generated by ``random.randint(0, 1000)`` if not specified.
    .. :note: this is un-deterministic when data partitions are not fixed.
    """
    if seed is None:
        seed = random.randint(0, 1000)
    return _with_expr(exprs.Randn, (), seed)


def spark_partition_id():
    """
    Spark-specific: Partition ID of the Spark task.
    .. :note: that this is un-deterministic because it depends on data partitioning and task scheduling.
    """
    return _with_expr(exprs.SparkPartitionID, cols=())


def struct(*columns):
    """
    Creates a new struct column.
    If the input column is a column in a ``Dataframe``, or a derived column expression
    that is named (i.e. aliased), its name would be remained as the StructField's name,
    otherwise, the newly generated StructField's name would be auto generated as col${index + 1},
    i.e. col1, col2, col3, ...
    """
    return _with_expr(exprs.CreateStruct, cols=columns)


def expr(expr_str=''):
    """
    Parses the expression string into the column that it represents
    :Example:
        # get the number of words of each length
        df.group_by(functions.expr("length(word)")).count()
    """

    return _with_expr(exprs.RawExpression, (), expr_str)


"""
Math functions
"""


def abs(column):
    """
    Computes the absolute value
    """
    return _with_expr(exprs.Abs, column)


"""  

  //////////////////////////////////////////////////////////////////////////////////////////////
  // Math functions
  //////////////////////////////////////////////////////////////////////////////////////////////

  /**
    * Computes the absolute value.
    *
    * @group normal_funcs
    */
  def abs(e: Column): Column = withExpr {
    Abs(e.expr)
  }

  /**
    * Computes the square root of the specified float value.
    *
    * @group math_funcs
    */
  def sqrt(e: Column): Column = withExpr {
    Sqrt(e.expr)
  }

  /**
    * Computes the square root of the specified float value.
    *
    * @group math_funcs
    */
  def sqrt(colName: String): Column = sqrt(col(colName))

  /**
    * Computes bitwise NOT.
    *
    * @group normal_funcs
    */
  def bitwiseNOT(e: Column): Column = withExpr {
    BitwiseNot(e.expr)
  }


  /**
    * Computes the cosine inverse of the given value; the returned angle is in the range
    * 0.0 through pi.
    *
    * @group math_funcs
    */
  def acos(e: Column): Column = withExpr {
    Acos(e.expr)
  }

  /**
    * Computes the cosine inverse of the given column; the returned angle is in the range
    * 0.0 through pi.
    *
    * @group math_funcs
    */
  def acos(columnName: String): Column = acos(col(columnName))

  /**
    * Computes the sine inverse of the given value; the returned angle is in the range
    * -pi/2 through pi/2.
    *
    * @group math_funcs
    */
  def asin(e: Column): Column = withExpr {
    Asin(e.expr)
  }

  /**
    * Computes the sine inverse of the given column; the returned angle is in the range
    * -pi/2 through pi/2.
    *
    * @group math_funcs
    */
  def asin(columnName: String): Column = asin(col(columnName))

  /**
    * Computes the tangent inverse of the given value.
    *
    * @group math_funcs
    */
  def atan(e: Column): Column = withExpr {
    Atan(e.expr)
  }

  /**
    * Computes the tangent inverse of the given column.
    *
    * @group math_funcs
    */
  def atan(columnName: String): Column = atan(col(columnName))

  /**
    * Returns the angle theta from the conversion of rectangular coordinates (x, y) to
    * polar coordinates (r, theta).
    *
    * @group math_funcs
    */
  def atan2(l: Column, r: Column): Column = withExpr {
    Atan2(l.expr, r.expr)
  }

  /**
    * Returns the angle theta from the conversion of rectangular coordinates (x, y) to
    * polar coordinates (r, theta).
    *
    * @group math_funcs
    */
  def atan2(l: Column, rightName: String): Column = atan2(l, col(rightName))

  /**
    * Returns the angle theta from the conversion of rectangular coordinates (x, y) to
    * polar coordinates (r, theta).
    *
    * @group math_funcs
    */
  def atan2(leftName: String, r: Column): Column = atan2(col(leftName), r)

  /**
    * Returns the angle theta from the conversion of rectangular coordinates (x, y) to
    * polar coordinates (r, theta).
    *
    * @group math_funcs
    */
  def atan2(leftName: String, rightName: String): Column =
    atan2(col(leftName), col(rightName))

  /**
    * Returns the angle theta from the conversion of rectangular coordinates (x, y) to
    * polar coordinates (r, theta).
    *
    * @group math_funcs
    */
  def atan2(l: Column, r: Double): Column = atan2(l, lit(r))

  /**
    * Returns the angle theta from the conversion of rectangular coordinates (x, y) to
    * polar coordinates (r, theta).
    *
    * @group math_funcs
    */
  def atan2(leftName: String, r: Double): Column = atan2(col(leftName), r)

  /**
    * Returns the angle theta from the conversion of rectangular coordinates (x, y) to
    * polar coordinates (r, theta).
    *
    * @group math_funcs
    * @since 1.4.0
    */
  def atan2(l: Double, r: Column): Column = atan2(lit(l), r)

  /**
    * Returns the angle theta from the conversion of rectangular coordinates (x, y) to
    * polar coordinates (r, theta).
    *
    * @group math_funcs
    */
  def atan2(l: Double, rightName: String): Column = atan2(l, col(rightName))

  /**
    * An expression that returns the string representation of the binary value of the given long
    * column. For example, bin("12") returns "1100".
    *
    * @group math_funcs
    */
  def bin(e: Column): Column = withExpr {
    Bin(e.expr)
  }

  /**
    * An expression that returns the string representation of the binary value of the given long
    * column. For example, bin("12") returns "1100".
    *
    * @group math_funcs
    */
  def bin(columnName: String): Column = bin(col(columnName))

  /**
    * Computes the cube-root of the given value.
    *
    * @group math_funcs
    */
  def cbrt(e: Column): Column = withExpr {
    Cbrt(e.expr)
  }

  /**
    * Computes the cube-root of the given column.
    *
    * @group math_funcs
    */
  def cbrt(columnName: String): Column = cbrt(col(columnName))

  /**
    * Computes the ceiling of the given value.
    *
    * @group math_funcs
    */
  def ceil(e: Column): Column = withExpr {
    Ceil(e.expr)
  }

  /**
    * Computes the ceiling of the given column.
    *
    * @group math_funcs
    */
  def ceil(columnName: String): Column = ceil(col(columnName))

  /**
    * Convert a number in a string column from one base to another.
    *
    * @group math_funcs
    */
  def conv(num: Column, fromBase: Int, toBase: Int): Column = withExpr {
    Conv(num.expr, fromBase, toBase)
  }

  /**
    * Computes the cosine of the given value.
    *
    * @group math_funcs
    */
  def cos(e: Column): Column = withExpr {
    Cos(e.expr)
  }

  /**
    * Computes the cosine of the given column.
    *
    * @group math_funcs
    */
  def cos(columnName: String): Column = cos(col(columnName))

  /**
    * Computes the hyperbolic cosine of the given value.
    *
    * @group math_funcs
    */
  def cosh(e: Column): Column = withExpr {
    Cosh(e.expr)
  }

  /**
    * Computes the hyperbolic cosine of the given column.
    *
    * @group math_funcs
    */
  def cosh(columnName: String): Column = cosh(col(columnName))

  /**
    * Computes the exponential of the given value.
    *
    * @group math_funcs
    */
  def exp(e: Column): Column = withExpr {
    Exp(e.expr)
  }

  /**
    * Computes the exponential of the given column.
    *
    * @group math_funcs
    */
  def exp(columnName: String): Column = exp(col(columnName))

  /**
    * Computes the exponential of the given value minus one.
    *
    * @group math_funcs
    */
  def expm1(e: Column): Column = withExpr {
    Expm1(e.expr)
  }

  /**
    * Computes the exponential of the given value minus one.
    *
    * @group math_funcs
    */
  def expm1(columnName: String): Column = expm1(col(columnName))

  /**
    * Computes the factorial of the given value.
    *
    * @group math_funcs
    */
  def factorial(e: Column): Column = withExpr {
    Factorial(e.expr)
  }

  /**
    * Computes the floor of the given value.
    *
    * @group math_funcs
    */
  def floor(e: Column): Column = withExpr {
    Floor(e.expr)
  }

  /**
    * Computes the floor of the given column.
    *
    * @group math_funcs
    */
  def floor(columnName: String): Column = floor(col(columnName))

  /**
    * Returns the greatest value of the list of values, skipping null values.
    * This function takes at least 2 parameters. It will return null iff all parameters are null.
    *
    * @group normal_funcs
    */
  def greatest(exprs: Column*): Column = withExpr {
    require(exprs.length > 1, "greatest requires at least 2 arguments.")
    Greatest(exprs.map(_.expr))
  }

  /**
    * Returns the greatest value of the list of column names, skipping null values.
    * This function takes at least 2 parameters. It will return null iff all parameters are null.
    *
    * @group normal_funcs
    */
  def greatest(columnName: String, columnNames: String*): Column = {
    greatest((columnName +: columnNames).map(col): _*)
  }

  /**
    * Computes hex value of the given column.
    *
    * @group math_funcs
    */
  def hex(column: Column): Column = withExpr {
    Hex(column.expr)
  }

  /**
    * Inverse of hex. Interprets each pair of characters as a hexadecimal number
    * and converts to the byte representation of number.
    *
    * @group math_funcs
    */
  def unhex(column: Column): Column = withExpr {
    Unhex(column.expr)
  }

  /**
    * Computes `sqrt(a^2^ + b^2^)` without intermediate overflow or underflow.
    *
    * @group math_funcs
    */
  def hypot(l: Column, r: Column): Column = withExpr {
    Hypot(l.expr, r.expr)
  }

  /**
    * Computes `sqrt(a^2^ + b^2^)` without intermediate overflow or underflow.
    *
    * @group math_funcs
    */
  def hypot(l: Column, rightName: String): Column = hypot(l, col(rightName))

  /**
    * Computes `sqrt(a^2^ + b^2^)` without intermediate overflow or underflow.
    *
    * @group math_funcs
    */
  def hypot(leftName: String, r: Column): Column = hypot(col(leftName), r)

  /**
    * Computes `sqrt(a^2^ + b^2^)` without intermediate overflow or underflow.
    *
    * @group math_funcs
    */
  def hypot(leftName: String, rightName: String): Column =
    hypot(col(leftName), col(rightName))

  /**
    * Computes `sqrt(a^2^ + b^2^)` without intermediate overflow or underflow.
    *
    * @group math_funcs
    */
  def hypot(l: Column, r: Double): Column = hypot(l, lit(r))

  /**
    * Computes `sqrt(a^2^ + b^2^)` without intermediate overflow or underflow.
    *
    * @group math_funcs
    */
  def hypot(leftName: String, r: Double): Column = hypot(col(leftName), r)

  /**
    * Computes `sqrt(a^2^ + b^2^)` without intermediate overflow or underflow.
    *
    * @group math_funcs
    */
  def hypot(l: Double, r: Column): Column = hypot(lit(l), r)

  /**
    * Computes `sqrt(a^2^ + b^2^)` without intermediate overflow or underflow.
    *
    * @group math_funcs
    */
  def hypot(l: Double, rightName: String): Column = hypot(l, col(rightName))

  /**
    * Returns the least value of the list of values, skipping null values.
    * This function takes at least 2 parameters. It will return null iff all parameters are null.
    *
    * @group normal_funcs
    */
  def least(exprs: Column*): Column = withExpr {
    require(exprs.length > 1, "least requires at least 2 arguments.")
    Least(exprs.map(_.expr))
  }

  /**
    * Returns the least value of the list of column names, skipping null values.
    * This function takes at least 2 parameters. It will return null iff all parameters are null.
    *
    * @group normal_funcs
    */
  def least(columnName: String, columnNames: String*): Column = {
    least((columnName +: columnNames).map(col): _*)
  }

  /**
    * Computes the natural logarithm of the given value.
    *
    * @group math_funcs
    */
  def log(e: Column): Column = withExpr {
    Log(e.expr)
  }

  /**
    * Computes the natural logarithm of the given column.
    *
    * @group math_funcs
    */
  def log(columnName: String): Column = log(col(columnName))

  /**
    * Returns the first argument-base logarithm of the second argument.
    *
    * @group math_funcs
    */
  def log(base: Double, a: Column): Column = withExpr {
    Logarithm(base, a.expr)
  }

  /**
    * Returns the first argument-base logarithm of the second argument.
    *
    * @group math_funcs
    */
  def log(base: Double, columnName: String): Column = log(base, col(columnName))

  /**
    * Computes the logarithm of the given value in base 10.
    *
    * @group math_funcs
    */
  def log10(e: Column): Column = withExpr {
    Log10(e.expr)
  }

  /**
    * Computes the logarithm of the given value in base 10.
    *
    * @group math_funcs
    */
  def log10(columnName: String): Column = log10(col(columnName))

  /**
    * Computes the natural logarithm of the given value plus one.
    *
    * @group math_funcs
    */
  def log1p(e: Column): Column = withExpr {
    Log1p(e.expr)
  }

  /**
    * Computes the natural logarithm of the given column plus one.
    *
    * @group math_funcs
    */
  def log1p(columnName: String): Column = log1p(col(columnName))

  /**
    * Computes the logarithm of the given column in base 2.
    *
    * @group math_funcs
    */
  def log2(expr: Column): Column = withExpr {
    Log2(expr.expr)
  }

  /**
    * Computes the logarithm of the given value in base 2.
    *
    * @group math_funcs
    */
  def log2(columnName: String): Column = log2(col(columnName))

  /**
    * Returns the value of the first argument raised to the power of the second argument.
    *
    * @group math_funcs
    */
  def pow(l: Column, r: Column): Column = withExpr {
    Pow(l.expr, r.expr)
  }

  /**
    * Returns the value of the first argument raised to the power of the second argument.
    *
    * @group math_funcs
    */
  def pow(l: Column, rightName: String): Column = pow(l, col(rightName))

  /**
    * Returns the value of the first argument raised to the power of the second argument.
    *
    * @group math_funcs
    */
  def pow(leftName: String, r: Column): Column = pow(col(leftName), r)

  /**
    * Returns the value of the first argument raised to the power of the second argument.
    *
    * @group math_funcs
    */
  def pow(leftName: String, rightName: String): Column = pow(col(leftName), col(rightName))

  /**
    * Returns the value of the first argument raised to the power of the second argument.
    *
    * @group math_funcs
    */
  def pow(l: Column, r: Double): Column = pow(l, lit(r))

  /**
    * Returns the value of the first argument raised to the power of the second argument.
    *
    * @group math_funcs
    */
  def pow(leftName: String, r: Double): Column = pow(col(leftName), r)

  /**
    * Returns the value of the first argument raised to the power of the second argument.
    *
    * @group math_funcs
    */
  def pow(l: Double, r: Column): Column = pow(lit(l), r)

  /**
    * Returns the value of the first argument raised to the power of the second argument.
    *
    * @group math_funcs
    */
  def pow(l: Double, rightName: String): Column = pow(l, col(rightName))

  /**
    * Returns the positive value of dividend mod divisor.
    *
    * @group math_funcs
    */
  def pmod(dividend: Column, divisor: Column): Column = withExpr {
    Pmod(dividend.expr, divisor.expr)
  }

  /**
    * Returns the double value that is closest in value to the argument and
    * is equal to a mathematical integer.
    *
    * @group math_funcs
    */
  def rint(e: Column): Column = withExpr {
    Rint(e.expr)
  }

  /**
    * Returns the double value that is closest in value to the argument and
    * is equal to a mathematical integer.
    *
    * @group math_funcs
    */
  def rint(columnName: String): Column = rint(col(columnName))

  /**
    * Returns the value of the column `e` rounded to 0 decimal places.
    *
    * @group math_funcs
    */
  def round(e: Column): Column = round(e, 0)

  /**
    * Round the value of `e` to `scale` decimal places if `scale` >= 0
    * or at integral part when `scale` < 0.
    *
    * @group math_funcs
    */
  def round(e: Column, scale: Int): Column = withExpr {
    Round(e.expr, Literal(scale))
  }

  /**
    * Returns the value of the column `e` rounded to 0 decimal places with HALF_EVEN round mode.
    *
    * @group math_funcs
    */
  def bround(e: Column): Column = bround(e, 0)

  /**
    * Round the value of `e` to `scale` decimal places with HALF_EVEN round mode
    * if `scale` >= 0 or at integral part when `scale` < 0.
    *
    * @group math_funcs
    */
  def bround(e: Column, scale: Int): Column = withExpr {
    BRound(e.expr, scale)
  }

  /**
    * Shift the given value numBits left. If the given value is a long value, this function
    * will return a long value else it will return an integer value.
    *
    * @group math_funcs
    */
  def shiftLeft(e: Column, numBits: Int): Column = withExpr {
    ShiftLeft(e.expr, numBits)
  }

  /**
    * Shift the given value numBits right. If the given value is a long value, it will return
    * a long value else it will return an integer value.
    *
    * @group math_funcs
    */
  def shiftRight(e: Column, numBits: Int): Column = withExpr {
    ShiftRight(e.expr, numBits)
  }

  /**
    * Unsigned shift the given value numBits right. If the given value is a long value,
    * it will return a long value else it will return an integer value.
    *
    * @group math_funcs
    */
  def shiftRightUnsigned(e: Column, numBits: Int): Column = withExpr {
    ShiftRightUnsigned(e.expr, numBits)
  }

  /**
    * Computes the signum of the given value.
    *
    * @group math_funcs
    */
  def signum(e: Column): Column = withExpr {
    Signum(e.expr)
  }

  /**
    * Computes the signum of the given column.
    *
    * @group math_funcs
    */
  def signum(columnName: String): Column = signum(col(columnName))

  /**
    * Computes the sine of the given value.
    *
    * @group math_funcs
    */
  def sin(e: Column): Column = withExpr {
    Sin(e.expr)
  }

  /**
    * Computes the sine of the given column.
    *
    * @group math_funcs
    */
  def sin(columnName: String): Column = sin(col(columnName))

  /**
    * Computes the hyperbolic sine of the given value.
    *
    * @group math_funcs
    */
  def sinh(e: Column): Column = withExpr {
    Sinh(e.expr)
  }

  /**
    * Computes the hyperbolic sine of the given column.
    *
    * @group math_funcs
    */
  def sinh(columnName: String): Column = sinh(col(columnName))

  /**
    * Computes the tangent of the given value.
    *
    * @group math_funcs
    */
  def tan(e: Column): Column = withExpr {
    Tan(e.expr)
  }

  /**
    * Computes the tangent of the given column.
    *
    * @group math_funcs
    */
  def tan(columnName: String): Column = tan(col(columnName))

  /**
    * Computes the hyperbolic tangent of the given value.
    *
    * @group math_funcs
    */
  def tanh(e: Column): Column = withExpr {
    Tanh(e.expr)
  }

  /**
    * Computes the hyperbolic tangent of the given column.
    *
    * @group math_funcs
    */
  def tanh(columnName: String): Column = tanh(col(columnName))

  /**
    * Converts an angle measured in radians to an approximately equivalent angle measured in degrees.
    *
    * @group math_funcs
    */
  def toDegrees(e: Column): Column = withExpr {
    ToDegrees(e.expr)
  }

  /**
    * Converts an angle measured in radians to an approximately equivalent angle measured in degrees.
    *
    * @group math_funcs
    */
  def toDegrees(columnName: String): Column = toDegrees(col(columnName))

  /**
    * Converts an angle measured in degrees to an approximately equivalent angle measured in radians.
    *
    * @group math_funcs
    */
  def toRadians(e: Column): Column = withExpr {
    ToRadians(e.expr)
  }

  /**
    * Converts an angle measured in degrees to an approximately equivalent angle measured in radians.
    *
    * @group math_funcs
    */
  def toRadians(columnName: String): Column = toRadians(col(columnName))

  //////////////////////////////////////////////////////////////////////////////////////////////
  // Misc functions
  //////////////////////////////////////////////////////////////////////////////////////////////

  /**
    * Calculates the MD5 digest of a binary column and returns the value
    * as a 32 character hex string.
    *
    * @group misc_funcs
    */
  def md5(e: Column): Column = withExpr {
    Md5(e.expr)
  }

  /**
    * Calculates the SHA-1 digest of a binary column and returns the value
    * as a 40 character hex string.
    *
    * @group misc_funcs
    */
  def sha1(e: Column): Column = withExpr {
    Sha1(e.expr)
  }

  /**
    * Calculates the SHA-2 family of hash functions of a binary column and
    * returns the value as a hex string.
    *
    * @param e       column to compute SHA-2 on.
    * @param numBits one of 224, 256, 384, or 512.
    * @group misc_funcs
    */
  def sha2(e: Column, numBits: Int): Column = {
    require(Seq(0, 224, 256, 384, 512).contains(numBits),
      s"numBits $numBits is not in the permitted values (0, 224, 256, 384, 512)")
    withExpr {
      Sha2(e.expr, numBits)
    }
  }

  /**
    * Calculates the cyclic redundancy check value  (CRC32) of a binary column and
    * returns the value as a bigint.
    *
    * @group misc_funcs
    */
  def crc32(e: Column): Column = withExpr {
    Crc32(e.expr)
  }

  /**
    * Calculates the hash code of given columns, and returns the result as an int column.
    *
    * @group misc_funcs
    */
  def hash(cols: Column*): Column = withExpr {
    Murmur3Hash(cols.map(_.expr))
  }

  //////////////////////////////////////////////////////////////////////////////////////////////
  // String functions
  //////////////////////////////////////////////////////////////////////////////////////////////

  /**
    * Computes the numeric value of the first character of the string column, and returns the
    * result as an int column.
    *
    * @group string_funcs
    */
  def ascii(e: Column): Column = withExpr {
    Ascii(e.expr)
  }

  /**
    * Computes the BASE64 encoding of a binary column and returns it as a string column.
    * This is the reverse of unbase64.
    *
    * @group string_funcs
    */
  def base64(e: Column): Column = withExpr {
    Base64(e.expr)
  }

  /**
    * Concatenates multiple input string columns together into a single string column.
    *
    * @group string_funcs
    */
  def concat(exprs: Column*): Column = withExpr {
    Concat(exprs.map(_.expr))
  }

  /**
    * Concatenates multiple input string columns together into a single string column,
    * using the given separator.
    *
    * @group string_funcs
    */
  def concat_ws(sep: String, exprs: Column*): Column = withExpr {
    ConcatWs(sep, exprs.map(_.expr))
  }

  /**
    * Computes the first argument into a string from a binary using the provided character set
    * (one of 'US-ASCII', 'ISO-8859-1', 'UTF-8', 'UTF-16BE', 'UTF-16LE', 'UTF-16').
    * If either argument is null, the result will also be null.
    *
    * @group string_funcs
    */
  def decode(value: Column, charset: String): Column = withExpr {
    Decode(value.expr, charset)
  }

  /**
    * Computes the first argument into a binary from a string using the provided character set
    * (one of 'US-ASCII', 'ISO-8859-1', 'UTF-8', 'UTF-16BE', 'UTF-16LE', 'UTF-16').
    * If either argument is null, the result will also be null.
    *
    * @group string_funcs
    */
  def encode(value: Column, charset: String): Column = withExpr {
    Encode(value.expr, charset)
  }

  /**
    * Formats numeric column x to a format like '#,###,###.##', rounded to d decimal places,
    * and returns the result as a string column.
    *
    * If d is 0, the result has no decimal point or fractional part.
    * If d < 0, the result will be null.
    *
    * @group string_funcs
    */
  def format_number(x: Column, d: Int): Column = withExpr {
    FormatNumber(x.expr, d)
  }

  /**
    * Formats the arguments in printf-style and returns the result as a string column.
    *
    * @group string_funcs
    */
  def format_string(format: String, arguments: Column*): Column = withExpr {
    FormatString(format, arguments.map(_.expr))
  }

  /**
    * Returns a new string column by converting the first letter of each word to uppercase.
    * Words are delimited by whitespace.
    *
    * For example, "hello world" will become "Hello World".
    *
    * @group string_funcs
    */
  def initcap(e: Column): Column = withExpr {
    InitCap(e.expr)
  }

  /**
    * Locate the position of the first occurrence of substr column in the given string.
    * Returns null if either of the arguments are null.
    *
    * NOTE: The position is not zero based, but 1 based index, returns 0 if substr
    * could not be found in str.
    *
    * @group string_funcs
    */
  def instr(str: Column, substring: String): Column = withExpr {
    StringInstr(str.expr, substring)
  }

  /**
    * Computes the length of a given string or binary column.
    *
    * @group string_funcs
    */
  def length(e: Column): Column = withExpr {
    Length(e.expr)
  }

  /**
    * Converts a string column to lower case.
    *
    * @group string_funcs
    */
  def lower(e: Column): Column = withExpr {
    Lower(e.expr)
  }

  /**
    * Computes the Levenshtein distance of the two given string columns.
    *
    * @group string_funcs
    */
  def levenshtein(l: Column, r: Column): Column = withExpr {
    Levenshtein(l.expr, r.expr)
  }

  /**
    * Locate the position of the first occurrence of substr.
    * NOTE: The position is not zero based, but 1 based index, returns 0 if substr
    * could not be found in str.
    *
    * @group string_funcs
    */
  def locate(substr: String, str: Column): Column = locate(substr, str, 1)

  /**
    * Locate the position of the first occurrence of substr in a string column, after position pos.
    *
    * NOTE: The position is not zero based, but 1 based index. returns 0 if substr
    * could not be found in str.
    *
    * @group string_funcs
    */
  def locate(substr: String, str: Column, pos: Int): Column = withExpr {
    StringLocate(substr, str.expr, pos)
  }

  /**
    * Left-pad the string column with
    *
    * @group string_funcs
    */
  def lpad(str: Column, len: Int, pad: String): Column = withExpr {
    StringLPad(str.expr, len, pad)
  }

  /**
    * Trim the spaces from left end for the specified string value.
    *
    * @group string_funcs
    */
  def ltrim(e: Column): Column = withExpr {
    StringTrimLeft(e.expr)
  }

  /**
    * Extract a specific group matched by a Java regex, from the specified string column.
    * If the regex did not match, or the specified group did not match, an empty string is returned.
    *
    * @group string_funcs
    */
  def regexp_extract(e: Column, exp: String, groupIdx: Int): Column = withExpr {
    RegExpExtract(e.expr, exp, groupIdx)
  }

  /**
    * Replace all substrings of the specified string value that match regexp with rep.
    *
    * @group string_funcs
    */
  def regexp_replace(e: Column, pattern: String, replacement: String): Column = withExpr {
    RegExpReplace(e.expr, pattern, replacement)
  }

  /**
    * Decodes a BASE64 encoded string column and returns it as a binary column.
    * This is the reverse of base64.
    *
    * @group string_funcs
    */
  def unbase64(e: Column): Column = withExpr {
    UnBase64(e.expr)
  }

  /**
    * Right-padded with pad to a length of len.
    *
    * @group string_funcs
    */
  def rpad(str: Column, len: Int, pad: String): Column = withExpr {
    StringRPad(str.expr, len, pad)
  }

  /**
    * Repeats a string column n times, and returns it as a new string column.
    *
    * @group string_funcs
    */
  def repeat(str: Column, n: Int): Column = withExpr {
    StringRepeat(str.expr, n)
  }

  /**
    * Reverses the string column and returns it as a new string column.
    *
    * @group string_funcs
    */
  def reverse(str: Column): Column = withExpr {
    StringReverse(str.expr)
  }

  /**
    * Trim the spaces from right end for the specified string value.
    *
    * @group string_funcs
    */
  def rtrim(e: Column): Column = withExpr {
    StringTrimRight(e.expr)
  }

  /**
    * Return the soundex code for the specified expression.
    *
    * @group string_funcs
    */
  def soundex(e: Column): Column = withExpr {
    SoundEx(e.expr)
  }

  /**
    * Splits str around pattern (pattern is a regular expression).
    * NOTE: pattern is a string representation of the regular expression.
    *
    * @group string_funcs
    */
  def split(str: Column, pattern: String): Column = withExpr {
    StringSplit(str.expr, pattern)
  }

  /**
    * Substring starts at `pos` and is of length `len` when str is String type or
    * returns the slice of byte array that starts at `pos` in byte and is of length `len`
    * when str is Binary type
    *
    * @group string_funcs
    */
  def substring(str: Column, pos: Int, len: Int): Column = withExpr {
    Substring(str.expr, lit(pos).expr, lit(len).expr)
  }

  /**
    * Returns the substring from string str before count occurrences of the delimiter delim.
    * If count is positive, everything the left of the final delimiter (counting from left) is
    * returned. If count is negative, every to the right of the final delimiter (counting from the
    * right) is returned. substring_index performs a case-sensitive match when searching for delim.
    *
    * @group string_funcs
    */
  def substring_index(str: Column, delim: String, count: Int): Column = withExpr {
    SubstringIndex(str.expr, delim, count)
  }

  /**
    * Translate any character in the src by a character in replaceString.
    * The characters in replaceString correspond to the characters in matchingString.
    * The translate will happen when any character in the string matches the character
    * in the `matchingString`.
    *
    * @group string_funcs
    */
  def translate(src: Column, matchingString: String, replaceString: String): Column = withExpr {
    StringTranslate(src.expr, matchingString, replaceString)
  }

  /**
    * Trim the spaces from both ends for the specified string column.
    *
    * @group string_funcs
    */
  def trim(e: Column): Column = withExpr {
    StringTrim(e.expr)
  }

  /**
    * Converts a string column to upper case.
    *
    * @group string_funcs
    */
  def upper(e: Column): Column = withExpr {
    Upper(e.expr)
  }

  //////////////////////////////////////////////////////////////////////////////////////////////
  // DateTime functions
  //////////////////////////////////////////////////////////////////////////////////////////////

  /**
    * Returns the date that is numMonths after startDate.
    *
    * @group datetime_funcs
    */
  def add_months(startDate: Column, numMonths: Int): Column = withExpr {
    AddMonths(startDate.expr, numMonths)
  }

  /**
    * Returns the current date as a date column.
    *
    * @group datetime_funcs
    */
  def current_date(): Column = withExpr {
    CurrentDate()
  }

  /**
    * Returns the current timestamp as a timestamp column.
    *
    * @group datetime_funcs
    */
  def current_timestamp(): Column = withExpr {
    CurrentTimestamp()
  }

  /**
    * Converts a date/timestamp/string to a value of string in the format specified by the date
    * format given by the second argument.
    *
    * A pattern could be for instance `dd.MM.yyyy` and could return a string like '18.03.1993'. All
    * pattern letters of [[java.text.SimpleDateFormat]] can be used.
    *
    * NOTE: Use when ever possible specialized functions like [[year]]. These benefit from a
    * specialized implementation.
    *
    * @group datetime_funcs
    */
  def date_format(dateExpr: Column, format: String): Column = withExpr {
    DateFormatClass(dateExpr.expr, format)
  }

  /**
    * Returns the date that is `days` days after `start`
    *
    * @group datetime_funcs
    */
  def date_add(start: Column, days: Int): Column = withExpr {
    DateAdd(start.expr, days)
  }

  /**
    * Returns the date that is `days` days before `start`
    *
    * @group datetime_funcs
    */
  def date_sub(start: Column, days: Int): Column = withExpr {
    DateSub(start.expr, days)
  }

  /**
    * Returns the number of days from `start` to `end`.
    *
    * @group datetime_funcs
    */
  def datediff(end: Column, start: Column): Column = withExpr {
    DateDiff(end.expr, start.expr)
  }

  /**
    * Extracts the year as an integer from a given date/timestamp/string.
    *
    * @group datetime_funcs
    */
  def year(e: Column): Column = withExpr {
    Year(e.expr)
  }

  /**
    * Extracts the quarter as an integer from a given date/timestamp/string.
    *
    * @group datetime_funcs
    */
  def quarter(e: Column): Column = withExpr {
    Quarter(e.expr)
  }

  /**
    * Extracts the month as an integer from a given date/timestamp/string.
    *
    * @group datetime_funcs
    */
  def month(e: Column): Column = withExpr {
    Month(e.expr)
  }

  /**
    * Extracts the day of the month as an integer from a given date/timestamp/string.
    *
    * @group datetime_funcs
    */
  def dayofmonth(e: Column): Column = withExpr {
    DayOfMonth(e.expr)
  }

  /**
    * Extracts the day of the year as an integer from a given date/timestamp/string.
    *
    * @group datetime_funcs
    */
  def dayofyear(e: Column): Column = withExpr {
    DayOfYear(e.expr)
  }

  /**
    * Extracts the hours as an integer from a given date/timestamp/string.
    *
    * @group datetime_funcs
    */
  def hour(e: Column): Column = withExpr {
    Hour(e.expr)
  }

  /**
    * Given a date column, returns the last day of the month which the given date belongs to.
    * For example, input "2015-07-27" returns "2015-07-31" since July 31 is the last day of the
    * month in July 2015.
    *
    * @group datetime_funcs
    */
  def last_day(e: Column): Column = withExpr {
    LastDay(e.expr)
  }

  /**
    * Extracts the minutes as an integer from a given date/timestamp/string.
    *
    * @group datetime_funcs
    */
  def minute(e: Column): Column = withExpr {
    Minute(e.expr)
  }

  /**
    * Returns number of months between dates `date1` and `date2`.
    *
    * @group datetime_funcs
    */
  def months_between(date1: Column, date2: Column): Column = withExpr {
    MonthsBetween(date1.expr, date2.expr)
  }

  /**
    * Given a date column, returns the first date which is later than the value of the date column
    * that is on the specified day of the week.
    *
    * For example, `next_day('2015-07-27', "Sunday")` returns 2015-08-02 because that is the first
    * Sunday after 2015-07-27.
    *
    * Day of the week parameter is case insensitive, and accepts:
    * "Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun".
    *
    * @group datetime_funcs
    */
  def next_day(date: Column, dayOfWeek: String): Column = withExpr {
    NextDay(date.expr, dayOfWeek)
  }

  /**
    * Extracts the seconds as an integer from a given date/timestamp/string.
    *
    * @group datetime_funcs
    */
  def second(e: Column): Column = withExpr {
    Second(e.expr)
  }

  /**
    * Extracts the week number as an integer from a given date/timestamp/string.
    *
    * @group datetime_funcs
    */
  def weekofyear(e: Column): Column = withExpr {
    WeekOfYear(e.expr)
  }

  /**
    * Converts the number of seconds from unix epoch (1970-01-01 00:00:00 UTC) to a string
    * representing the timestamp of that moment in the current system time zone in the given
    * format.
    *
    * @group datetime_funcs
    */
  def from_unixtime(ut: Column): Column = from_unixtime(ut, "yyyy-MM-dd HH:mm:ss")

  /**
    * Converts the number of seconds from unix epoch (1970-01-01 00:00:00 UTC) to a string
    * representing the timestamp of that moment in the current system time zone in the given
    * format.
    *
    * @group datetime_funcs
    */
  def from_unixtime(ut: Column, f: String): Column = withExpr {
    FromUnixTime(ut.expr, f)
  }

  /**
    * Gets current Unix timestamp in seconds.
    *
    * @group datetime_funcs
    */
  def unix_timestamp(): Column = unix_timestamp(withExpr(CurrentTimestamp()), "yyyy-MM-dd HH:mm:ss")

  /**
    * Converts time string in format yyyy-MM-dd HH:mm:ss to Unix timestamp (in seconds),
    * using the default timezone and the default locale, return null if fail.
    *
    * @group datetime_funcs
    */
  def unix_timestamp(s: Column): Column = unix_timestamp(s, "yyyy-MM-dd HH:mm:ss")

  /**
    * Convert time string with given pattern
    * (see [http://docs.oracle.com/javase/tutorial/i18n/format/simpleDateFormat.html])
    * to Unix time stamp (in seconds), return null if fail.
    *
    * @group datetime_funcs
    */
  def unix_timestamp(s: Column, p: String): Column = withExpr {
    UnixTimestamp(s.expr, p)
  }

  /**
    * Converts the column into DateType.
    *
    * @group datetime_funcs
    */
  def to_date(e: Column): Column = withExpr {
    ToDate(e.expr)
  }

  /**
    * Returns date truncated to the unit specified by the format.
    *
    * @param format : 'year', 'yyyy', 'yy' for truncate by year,
    *               or 'month', 'mon', 'mm' for truncate by month
    * @group datetime_funcs
    */
  def trunc(date: Column, format: String): Column = withExpr {
    TruncDate(date.expr, format)
  }

  /**
    * Assumes given timestamp is UTC and converts to given timezone.
    *
    * @group datetime_funcs
    */
  def from_utc_timestamp(ts: Column, tz: String): Column = withExpr {
    FromUTCTimestamp(ts.expr, tz)
  }

  /**
    * Assumes given timestamp is in given timezone and converts to UTC.
    *
    * @group datetime_funcs
    */
  def to_utc_timestamp(ts: Column, tz: String): Column = withExpr {
    ToUTCTimestamp(ts.expr, tz)
  }

  /**
    * Bucketize rows into one or more time windows given a timestamp specifying column. Window
    * starts are inclusive but the window ends are exclusive, e.g. 12:05 will be in the window
    * [12:05,12:10) but not in [12:00,12:05). Windows can support microsecond precision. Windows in
    * the order of months are not supported. The following example takes the average stock price for
    * a one minute window every 10 seconds starting 5 seconds after the hour:
    *
    * {{{
    *   val df = ... // schema => timestamp: TimestampType, stockId: StringType, price: DoubleType
    *   df.groupBy(window($"time", "1 minute", "10 seconds", "5 seconds"), $"stockId")
    *     .agg(mean("price"))
    * }}}
    *
    * The windows will look like:
    *
    * {{{
    *   09:00:05-09:01:05
    *   09:00:15-09:01:15
    *   09:00:25-09:01:25 ...
    * }}}
    *
    * For a streaming query, you may use the function `current_timestamp` to generate windows on
    * processing time.
    *
    * @param timeColumn     The column or the expression to use as the timestamp for windowing by time.
    *                       The time column must be of TimestampType.
    * @param windowDuration A string specifying the width of the window, e.g. `10 minutes`,
    *                       `1 second`. Check [[io.cebes.df.types.storage.CalendarIntervalType]] for
    *                       valid duration identifiers. Note that the duration is a fixed length of
    *                       time, and does not vary over time according to a calendar. For example,
    *                       `1 day` always means 86,400,000 milliseconds, not a calendar day.
    * @param slideDuration  A string specifying the sliding interval of the window, e.g. `1 minute`.
    *                       A new window will be generated every `slideDuration`. Must be less than
    *                       or equal to the `windowDuration`. Check
    *                       [[io.cebes.df.types.storage.CalendarIntervalType]] for valid duration
    *                      identifiers. This duration is likewise absolute, and does not vary
    *                       according to a calendar.
    * @param startTime      The offset with respect to 1970-01-01 00:00:00 UTC with which to start
    *                       window intervals. For example, in order to have hourly tumbling windows that
    *                       start 15 minutes past the hour, e.g. 12:15-13:15, 13:15-14:15... provide
    *                       `startTime` as `15 minutes`.
    * @group datetime_funcs
    */
  def window(
              timeColumn: Column,
              windowDuration: String,
              slideDuration: String,
              startTime: String): Column = withExpr {
    TimeWindow(timeColumn.expr, windowDuration, slideDuration, startTime)
  }


  /**
    * Bucketize rows into one or more time windows given a timestamp specifying column. Window
    * starts are inclusive but the window ends are exclusive, e.g. 12:05 will be in the window
    * [12:05,12:10) but not in [12:00,12:05). Windows can support microsecond precision. Windows in
    * the order of months are not supported. The windows start beginning at 1970-01-01 00:00:00 UTC.
    * The following example takes the average stock price for a one minute window every 10 seconds:
    *
    * {{{
    *   val df = ... // schema => timestamp: TimestampType, stockId: StringType, price: DoubleType
    *   df.groupBy(window($"time", "1 minute", "10 seconds"), $"stockId")
    *     .agg(mean("price"))
    * }}}
    *
    * The windows will look like:
    *
    * {{{
    *   09:00:00-09:01:00
    *   09:00:10-09:01:10
    *   09:00:20-09:01:20 ...
    * }}}
    *
    * For a streaming query, you may use the function `current_timestamp` to generate windows on
    * processing time.
    *
    * @param timeColumn     The column or the expression to use as the timestamp for windowing by time.
    *                       The time column must be of TimestampType.
    * @param windowDuration A string specifying the width of the window, e.g. `10 minutes`,
    *                       `1 second`. Check [[io.cebes.df.types.storage.CalendarIntervalType]] for
    *                       valid duration identifiers. Note that the duration is a fixed length of
    *                       time, and does not vary over time according to a calendar. For example,
    *                       `1 day` always means 86,400,000 milliseconds, not a calendar day.
    * @param slideDuration  A string specifying the sliding interval of the window, e.g. `1 minute`.
    *                       A new window will be generated every `slideDuration`. Must be less than
    *                       or equal to the `windowDuration`. Check
    *                       [[io.cebes.df.types.storage.CalendarIntervalType]] for valid duration
    *                      identifiers. This duration is likewise absolute, and does not vary
    *                       according to a calendar.
    * @group datetime_funcs
    */
  def window(timeColumn: Column, windowDuration: String, slideDuration: String): Column = {
    window(timeColumn, windowDuration, slideDuration, "0 second")
  }

  /**
    * Generates tumbling time windows given a timestamp specifying column. Window
    * starts are inclusive but the window ends are exclusive, e.g. 12:05 will be in the window
    * [12:05,12:10) but not in [12:00,12:05). Windows can support microsecond precision. Windows in
    * the order of months are not supported. The windows start beginning at 1970-01-01 00:00:00 UTC.
    * The following example takes the average stock price for a one minute tumbling window:
    *
    * {{{
    *   val df = ... // schema => timestamp: TimestampType, stockId: StringType, price: DoubleType
    *   df.groupBy(window($"time", "1 minute"), $"stockId")
    *     .agg(mean("price"))
    * }}}
    *
    * The windows will look like:
    *
    * {{{
    *   09:00:00-09:01:00
    *   09:01:00-09:02:00
    *   09:02:00-09:03:00 ...
    * }}}
    *
    * For a streaming query, you may use the function `current_timestamp` to generate windows on
    * processing time.
    *
    * @param timeColumn     The column or the expression to use as the timestamp for windowing by time.
    *                       The time column must be of TimestampType.
    * @param windowDuration A string specifying the width of the window, e.g. `10 minutes`,
    *                       `1 second`. Check [[io.cebes.df.types.storage.CalendarIntervalType]] for
    *                       valid duration identifiers.
    * @group datetime_funcs
    */
  def window(timeColumn: Column, windowDuration: String): Column = {
    window(timeColumn, windowDuration, windowDuration, "0 second")
  }

  //////////////////////////////////////////////////////////////////////////////////////////////
  // Collection functions
  //////////////////////////////////////////////////////////////////////////////////////////////

  /**
    * Returns true if the array contains `value`
    *
    * @group collection_funcs
    */
  def array_contains(column: Column, value: Any): Column = withExpr {
    ArrayContains(column.expr, Literal(value))
  }

  /**
    * Creates a new row for each element in the given array or map column.
    *
    * @group collection_funcs
    */
  def explode(e: Column): Column = withExpr {
    Explode(e.expr)
  }

  /**
    * Creates a new row for each element with position in the given array or map column.
    *
    * @group collection_funcs
    */
  def posexplode(e: Column): Column = withExpr {
    PosExplode(e.expr)
  }

  /**
    * Extracts json object from a json string based on json path specified, and returns json string
    * of the extracted json object. It will return null if the input json string is invalid.
    *
    * @group collection_funcs
    */
  def get_json_object(e: Column, path: String): Column = withExpr {
    GetJsonObject(e.expr, path)
  }

  /**
    * Creates a new row for a json column according to the given field names.
    *
    * @group collection_funcs
    */
  def json_tuple(json: Column, fields: String*): Column = withExpr {
    require(fields.nonEmpty, "at least 1 field name should be given.")
    JsonTuple(json.expr, fields)
  }

  /**
    * Returns length of array or map.
    *
    * @group collection_funcs
    */
  def size(e: Column): Column = withExpr {
    Size(e.expr)
  }

  /**
    * Sorts the input array for the given column in ascending order,
    * according to the natural ordering of the array elements.
    *
    * @group collection_funcs
    */
  def sort_array(e: Column): Column = sort_array(e, asc = true)

  /**
    * Sorts the input array for the given column in ascending / descending order,
    * according to the natural ordering of the array elements.
    *
    * @group collection_funcs
    */
  def sort_array(e: Column, asc: Boolean): Column = withExpr {
    SortArray(e.expr, asc)
  }
"""
