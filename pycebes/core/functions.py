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
    Returns a ``Column`` based on the given column name.
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
    `cols` will be transformed into Column using :func:`_get_column`,
    other arguments in args will be passed as-it-is into the constructor of the expression class
    """
    if isinstance(cols, (tuple, list)):
        cols = tuple(_get_column(c).expr for c in cols)
    else:
        cols = _get_column(cols).expr
    return Column(expr_class(cols, *args))


def _with_binary_expr(expr_class, left, right, *args):
    """
    Helper to return a column constructed from the given binary expression class.
    """
    all_args = [_get_column(left).expr, _get_column(right).expr] + list(args)
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
    return _with_binary_expr(exprs.Corr, left=column1, right=column2)


def count(column):
    """
    returns the number of items in a group
    """
    return _with_expr(exprs.Count, column)


def count_distinct(*args):
    """
    returns the number of distinct items in a group
    """
    cols = [_get_column(c).expr for c in args]
    return Column(exprs.CountDistinct(*cols))


def covar_pop(column1, column2):
    """
    Returns the population covariance for two columns
    """
    return _with_binary_expr(exprs.CovPopulation, left=column1, right=column2)


def covar_samp(column1, column2):
    """
    Returns the sample covariance for two columns
    """
    return _with_binary_expr(exprs.CovSample, left=column1, right=column2)


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

    .. note::

        the list of columns should match with grouping columns exactly, or empty (means all the
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
    :param is_distinct: whether to only compute the sum of distinct values.
            False by default, meaning computing the sum of all values
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

    .. code-block:: python

        # encoding gender string column into integer
        people.select(functions.when(people.gender == 'male', 0)
            .when(people.gender == 'female', 1)
            .otherwise(2))
    """
    if not isinstance(condition, Column):
        raise ValueError('condition must be a Column expression')
    return Column(exprs.CaseWhen([(condition.expr, lit(value).expr)]))


def array(*columns):
    """
    Creates a new array column. The input columns must all have the same data type.
    """
    return _with_expr(exprs.CreateArray, columns)


def create_map(*columns):
    """
    Creates a new map column. The input columns must be grouped as key-value pairs, e.g.
    (key1, value1, key2, value2, ...). The key columns must all have the same data type, and can't
    be null. The value columns must all have the same data type.
    """
    return _with_expr(exprs.CreateMap, columns)


def coalesce(*columns):
    """
    Returns the first column that is not null, or null if all inputs are null.

    For example, ``coalesce(a, b, c)`` will return a if a is not null,
    or b if a is null and b is not null, or c if both a and b are null but c is not null.
    """
    return _with_expr(exprs.Coalesce, columns)


def input_file_name():
    """
    Spark-specific: Creates a string column for the file name of the current Spark task
    """
    return Column(exprs.InputFileName())


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
    return Column(exprs.MonotonicallyIncreasingID())


def nanvl(column1, column2):
    """
    Returns col1 if it is not NaN, or col2 if col1 is NaN.
    Both inputs should be floating point columns (DoubleType or FloatType).
    """
    return _with_binary_expr(exprs.NaNvl, left=column1, right=column2)


def negate(column):
    """
    Unary minus, i.e. negate the expression.

    :Example:

    .. code-block:: python

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

    .. note::

        This is un-deterministic when data partitions are not fixed.
    """
    if seed is None:
        seed = random.randint(0, 1000)
    return Column(exprs.Rand(seed))


def randn(seed=None):
    """
    Generate a column with i.i.d. samples from the standard normal distribution.
    ``seed`` will be automatically randomly generated by ``random.randint(0, 1000)`` if not specified.

    .. note::

        This is un-deterministic when data partitions are not fixed.
    """
    if seed is None:
        seed = random.randint(0, 1000)
    return Column(exprs.Randn(seed))


def spark_partition_id():
    """
    Spark-specific: Partition ID of the Spark task.

    .. note::

        That this is un-deterministic because it depends on data partitioning and task scheduling.
    """
    return Column(exprs.SparkPartitionID())


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

    .. code-block:: python

        # get the number of words of each length
        df.group_by(functions.expr("length(word)")).count()
    """

    return Column(exprs.RawExpression(expr_str))


"""
Math functions
"""


def abs(column):
    """
    Computes the absolute value
    """
    return _with_expr(exprs.Abs, column)


def sqrt(column):
    """
    Computes the square root of the specified float value.
    """
    return _with_expr(exprs.Sqrt, column)


def bitwise_not(column):
    """
    Computes bitwise NOT.
    """
    return _with_expr(exprs.BitwiseNot, column)


def acos(column):
    """
    Computes the cosine inverse of the given value; the returned angle is in the range 0.0 through pi.
    """
    return _with_expr(exprs.Acos, column)


def asin(column):
    """
    Computes the sine inverse of the given value; the returned angle is in the range -pi/2 through pi/2.
    """
    return _with_expr(exprs.Asin, column)


def atan(column):
    """
    Computes the tangent inverse of the given value
    """
    return _with_expr(exprs.Atan, column)


def atan2(x, y):
    """
    Returns the angle theta from the conversion of rectangular coordinates (x, y) to polar coordinates (r, theta)

    :param x: the x-component, can be a ``Column``, a string (column name), or a float value
    :param y: the y-component, can be a ``Column``, a string (column name), or a float value
    """
    if isinstance(x, float):
        x = lit(x)
    if isinstance(y, float):
        y = lit(y)
    return _with_binary_expr(exprs.Atan2, left=x, right=y)


def bin(column):
    """
    An expression that returns the string representation of the binary value of the given long column.
    For example, bin("12") returns "1100".
    """
    return _with_expr(exprs.Bin, column)


def cbrt(column):
    """
    Computes the cube-root of the given value
    """
    return _with_expr(exprs.Cbrt, column)


def ceil(column):
    """
    Computes the ceiling of the given value.
    """
    return _with_expr(exprs.Ceil, column)


def conv(column, from_base=10, to_base=2):
    """
    Convert a number in a string column from one base to another
    """
    return _with_expr(exprs.Conv, column, from_base, to_base)


def cos(column):
    """
    Computes the cosine of the given value
    """
    return _with_expr(exprs.Cos, column)


def cosh(column):
    """
    Computes the hyperbolic cosine of the given value
    """
    return _with_expr(exprs.Cosh, column)


def exp(column):
    """
    Computes the exponential of the given value
    """
    return _with_expr(exprs.Exp, column)


def expm1(column):
    """
    Computes the exponential of the given value minus one
    """
    return _with_expr(exprs.Expm1, column)


def factorial(column):
    """
    Computes the factorial of the given value
    """
    return _with_expr(exprs.Factorial, column)


def floor(column):
    """
    Computes the floor of the given value
    """
    return _with_expr(exprs.Floor, column)


def greatest(*columns):
    """
    Returns the greatest value of the list of values, skipping null values.
    This function takes at least 2 parameters. It will return null iff all parameters are null.
    """
    return _with_expr(exprs.Greatest, columns)


def hex(column):
    """
    Computes hex value of the given column
    """
    return _with_expr(exprs.Hex, column)


def unhex(column):
    """
    Inverse of hex. Interprets each pair of characters as a hexadecimal number
    and converts to the byte representation of number.
    """
    return _with_expr(exprs.Unhex, column)


def hypot(x, y):
    """
    Computes ``sqrt(x^2 + y^2)`` without intermediate overflow or underflow
    See :func:`atan2` to compute the angle.

    :param x: can be a ``Column``, a string (column name), or a double value
    :param y: can be a ``Column``, a string (column name), or a double value
    """
    if isinstance(x, float):
        x = lit(x)
    if isinstance(y, float):
        y = lit(y)
    return _with_binary_expr(exprs.Hypot, left=x, right=y)


def least(*columns):
    """
    Returns the least value of the list of values, skipping null values.
    This function takes at least 2 parameters. It will return null iff all parameters are null.
    """
    return _with_expr(exprs.Least, columns)


def log(column, base=None):
    """
    Computes the natural logarithm of the given value if ``base`` is None.
    If ``base`` is not None, compute the logarithm with the given base of the column.
    """
    if base is None:
        return _with_expr(exprs.Log, column)

    if not isinstance(base, float):
        raise ValueError('Expected a floating number base, got {!r}'.format(base))
    return _with_expr(exprs.Logarithm, column, base)


def log10(column):
    """
    Computes the logarithm of the given value in base 10
    """
    return _with_expr(exprs.Log10, column)


def log1p(column):
    """
    Computes the natural logarithm of the given value plus one
    """
    return _with_expr(exprs.Log1p, column)


def log2(column):
    """
    Computes the logarithm of the given column in base 2
    """
    return _with_expr(exprs.Log2, column)


def pow(left, right):
    """
    Returns the value of the first argument raised to the power of the second argument

    :param left: can be a ``Column``, a string (column name), or a double value
    :param right: can be a ``Column``, a string (column name), or a double value
    """
    if isinstance(left, float):
        left = lit(left)
    if isinstance(right, float):
        right = lit(right)
    return _with_binary_expr(exprs.Pow, left=left, right=right)


def pmod(dividend, divisor):
    """
    Returns the positive value of dividend mod divisor.
    ``dividend`` and ``divisor`` can be ``Column`` or strings (column names)
    """
    return _with_binary_expr(exprs.Pmod, left=dividend, right=divisor)


def rint(column):
    """
    Returns the double value that is closest in value to the argument and
    is equal to a mathematical integer.
    """
    return _with_expr(exprs.Rint, column)


def round(column, scale=0):
    """
    Round the value of ``column`` to ``scale`` decimal places if ``scale >= 0``
    or at integral part when ``scale < 0``
    """
    return _with_binary_expr(exprs.Round, left=column, right=lit(scale))


def bround(column, scale=0):
    """
    Round the value of ``column`` to ``scale`` decimal places with HALF_EVEN round mode if ``scale >= 0``
    or at integral part when ``scale < 0``
    """
    return _with_binary_expr(exprs.BRound, left=column, right=lit(scale))


def shift_left(column, num_bits=1):
    """
    Shift the given value ``num_bits`` left. If the given value is a long value, this function
    will return a long value else it will return an integer value.
    """
    return _with_expr(exprs.ShiftLeft, column, num_bits)


def shift_right(column, num_bits=1):
    """
    Shift the given value ``num_bits`` right. If the given value is a long value, this function
    will return a long value else it will return an integer value.
    """
    return _with_expr(exprs.ShiftRight, column, num_bits)


def shift_right_unsigned(column, num_bits=1):
    """
    Unsigned shift the given value ``num_bits`` right. If the given value is a long value, this function
    will return a long value else it will return an integer value.
    """
    return _with_expr(exprs.ShiftRightUnsigned, column, num_bits)


def signum(column):
    """
    Computes the signum of the given value
    """
    return _with_expr(exprs.Signum, column)


def sin(column):
    """
    Computes the sine of the given value
    """
    return _with_expr(exprs.Sin, column)


def sinh(column):
    """
    Computes the hyperbolic sine of the given value
    """
    return _with_expr(exprs.Sinh, column)


def tan(column):
    """
    Computes the tangent of the given value
    """
    return _with_expr(exprs.Tan, column)


def tanh(column):
    """
    Computes the hyperbolic tangent of the given value
    """
    return _with_expr(exprs.Tanh, column)


def to_degrees(column):
    """
    Converts an angle measured in radians to an approximately equivalent angle measured in degrees
    """
    return _with_expr(exprs.ToDegrees, column)


def to_radians(column):
    """
    Converts an angle measured in degrees to an approximately equivalent angle measured in radians
    """
    return _with_expr(exprs.ToRadians, column)


"""
Misc functions
"""


def md5(column):
    """
    Calculates the MD5 digest of a binary column and returns the value
    as a 32 character hex string
    """
    return _with_expr(exprs.Md5, column)


def sha1(column):
    """
    Calculates the SHA-1 digest of a binary column and returns the value
    as a 40 character hex string
    """
    return _with_expr(exprs.Sha1, column)


def sha2(column, num_bits=0):
    """
    Calculates the SHA-2 family of hash functions of a binary column and
    returns the value as a hex string

    :param column: column to compute SHA-2 on. Can be a ``Column`` or a string (a column name)
    :param num_bits: one of 224, 256, 384, or 512
    """
    if num_bits not in (0, 224, 256, 384, 512):
        raise ValueError('Invalid value of num_bits')
    return _with_expr(exprs.Sha2, column, num_bits)


def crc32(column):
    """
    Calculates the cyclic redundancy check value  (CRC32) of a binary column and
    returns the value as a bigint
    """
    return _with_expr(exprs.Crc32, column)


def hash(column):
    """
    Calculates the hash code of given columns, and returns the result as an int column
    """
    return _with_expr(exprs.Murmur3Hash, column)


"""
String functions
"""


def ascii(column):
    """
    Computes the numeric value of the first character of the string column, and returns the
    result as an int column
    """
    return _with_expr(exprs.Ascii, column)


def base64(column):
    """
    Computes the BASE64 encoding of a binary column and returns it as a string column.
    This is the reverse of unbase64
    """
    return _with_expr(exprs.Base64, column)


def concat(*columns):
    """
    Concatenates multiple input string columns together into a single string column
    """
    return _with_expr(exprs.Concat, columns)


def concat_ws(sep=' ', *columns):
    """
    Concatenates multiple input string columns together into a single string column,
    using the given separator
    """
    return _with_expr(exprs.ConcatWs, columns, sep)


def decode(column, charset='US-ASCII'):
    """
    Computes the first argument into a string from a binary using the provided character set
    (one of 'US-ASCII', 'ISO-8859-1', 'UTF-8', 'UTF-16BE', 'UTF-16LE', 'UTF-16').
    If either argument is null, the result will also be null.
    """
    return _with_expr(exprs.Decode, column, charset)


def encode(column, charset='US-ASCII'):
    """
    Computes the first argument into a binary from a string using the provided character set
    (one of 'US-ASCII', 'ISO-8859-1', 'UTF-8', 'UTF-16BE', 'UTF-16LE', 'UTF-16').
    If either argument is null, the result will also be null.
    """
    return _with_expr(exprs.Encode, column, charset)


def format_number(column, d=2):
    """
    Formats numeric column x to a format like '#,###,###.##', rounded to d decimal places,
    and returns the result as a string column.

        * If d is 0, the result has no decimal point or fractional part.
        * If d < 0, the result will be null.
    """
    return _with_expr(exprs.FormatNumber, column, d)


def format_string(fmt, *columns):
    """
    Formats the arguments in printf-style and returns the result as a string column
    """
    columns = (_get_column(c) for c in columns)
    return Column(exprs.FormatString(fmt, columns))


def initcap(column):
    """
    Returns a new string column by converting the first letter of each word to uppercase.
    Words are delimited by whitespace.
    For example, "hello world" will become "Hello World".
    """
    return _with_expr(exprs.InitCap, column)


def instr(column, substr=''):
    """
    Locate the position of the first occurrence of substr column in the given string.
    Returns null if either of the arguments are null.

    .. note::

        The position is not zero based, but 1 based index, returns 0 if substr
        could not be found in ``column``
    """
    return _with_expr(exprs.StringInstr, column, substr)


def length(column):
    """
    Computes the length of a given string or binary column
    """
    return _with_expr(exprs.Length, column)


def lower(column):
    """
    Converts a string column to lower case
    """
    return _with_expr(exprs.Lower, column)


def levenshtein(left, right):
    """
    Computes the Levenshtein distance of the two given string columns
    """
    return _with_binary_expr(exprs.Levenshtein, left=left, right=right)


def locate(substr, column, pos=None):
    """
    Locate the position of the first occurrence of substr.
    If ``pos`` is not None, only search after position ``pos``.

    .. note::

        The position is not zero based, but 1 based index, returns 0 if substr
        could not be found in str.
    """
    if not isinstance(substr, six.text_type):
        raise ValueError('Expected substr as a string, got {!r}'.format(substr))

    return _with_expr(exprs.StringLocate, column, substr, pos)


def lpad(column, pad_length=1, pad=''):
    """
    Left-pad the string column
    """
    return _with_expr(exprs.StringLPad, column, pad_length, pad)


def ltrim(column):
    """
    Trim the spaces from left end for the specified string value
    """
    return _with_expr(exprs.StringTrimLeft, column)


def regexp_extract(column, regexp='', group_idx=0):
    """
    Extract a specific group matched by a Java regex, from the specified string column.
    If the regex did not match, or the specified group did not match, an empty string is returned.
    """
    return _with_expr(exprs.RegExpExtract, column, regexp, group_idx)


def regexp_replace(column, pattern='', replacement=''):
    """
    Replace all substrings of the specified string value that match regexp with rep.
    """
    return _with_expr(exprs.RegExpReplace, column, pattern, replacement)


def unbase64(column):
    """
    Decodes a BASE64 encoded string column and returns it as a binary column.
    This is the reverse of base64.
    """
    return _with_expr(exprs.UnBase64, column)


def rpad(column, pad_length=1, pad=''):
    """
    Left-pad the string column
    """
    return _with_expr(exprs.StringRPad, column, pad_length, pad)


def repeat(column, n=1):
    """
    Repeats a string column n times, and returns it as a new string column
    """
    return _with_expr(exprs.StringRepeat, column, n)


def reverse(column):
    """
    Reverses the string column and returns it as a new string column
    """
    return _with_expr(exprs.StringReverse, column)


def rtrim(column):
    """
    Trim the spaces from right end for the specified string value
    """
    return _with_expr(exprs.StringTrimRight, column)


def soundex(column):
    """
    Return the soundex code for the specified expression
    """
    return _with_expr(exprs.SoundEx, column)


def split(column, pattern=''):
    """
    Splits str around pattern (pattern is a regular expression)

    .. note::
        pattern is a string representation of the regular expression
    """
    return _with_expr(exprs.StringSplit, column, pattern)


def substring(column, pos=0, l=1):
    """
    Substring starts at ``pos`` and is of length ``l`` when str is String type or
    returns the slice of byte array that starts at ``pos`` in byte and is of length ``l``
    when ``column`` is Binary type
    """
    return _with_expr(exprs.Substring, column, pos, l)


def substring_index(column, delim=' ', cnt=1):
    """
    Returns the substring from string ``column`` before ``cnt`` occurrences of the delimiter ``delim``.
    If ``cnt`` is positive, everything the left of the final delimiter (counting from left) is
    returned. If ``cnt`` is negative, every to the right of the final delimiter (counting from the
    right) is returned. substring_index performs a case-sensitive match when searching for ``delim``.
    """
    return _with_expr(exprs.SubstringIndex, column, delim, cnt)


def translate(column, matching_str='', replace_str=''):
    """
    Translate any character in the ``column`` by a character in ``replace_str``.
    The characters in ``replace_str`` correspond to the characters in ``matching_str``.
    The translate will happen when any character in the string matches the character
    in the ``matching_str``.
    """
    return _with_expr(exprs.StringTranslate, column, matching_str, replace_str)


def trim(column):
    """
    Trim the spaces from both ends for the specified string column
    """
    return _with_expr(exprs.StringTrim, column)


def upper(column):
    """
    Converts a string column to upper case
    """
    return _with_expr(exprs.Upper, column)


"""
DateTime functions
"""


def add_months(column, num_months=1):
    """
    Returns the date that is ``num_months`` after the date in ``column``
    """
    return _with_expr(exprs.AddMonths, column, num_months)


def current_date():
    """
    Returns the current date as a date column
    """
    return Column(exprs.CurrentDate())


def current_timestamp():
    """
    Returns the current timestamp as a timestamp column
    """
    return Column(exprs.CurrentTimestamp())


def date_format(column, fmt=''):
    """
    Converts a date/timestamp/string to a value of string in the format specified by the date
    format given by the second argument.

    A pattern could be for instance `dd.MM.yyyy` and could return a string like '18.03.1993'. All
    pattern letters of [[java.text.SimpleDateFormat]] can be used.

    .. note::

        Use when ever possible specialized functions like :func:`year`. These benefit from a
        specialized implementation.
    """
    return _with_expr(exprs.DateFormatClass, column, fmt)


def date_add(column, days=1):
    """
    Returns the date that is `days` days after the date in ``column``
    """
    return _with_expr(exprs.DateAdd, column, days)


def date_sub(column, days=1):
    """
    eturns the date that is `days` days before the date in ``column``
    """
    return _with_expr(exprs.DateSub, column, days)


def datediff(end_column, start_column):
    """
    Returns the number of days from `start` to `end`
    """
    return _with_binary_expr(exprs.DateDiff, end_column, start_column)


def year(column):
    """
    Extracts the year as an integer from a given date/timestamp/string
    """
    return _with_expr(exprs.Year, column)


def quarter(column):
    """
    Extracts the quarter as an integer from a given date/timestamp/string
    """
    return _with_expr(exprs.Quarter, column)


def month(column):
    """
    Extracts the month as an integer from a given date/timestamp/string
    """
    return _with_expr(exprs.Month, column)


def dayofmonth(column):
    """
    Extracts the day of the month as an integer from a given date/timestamp/string
    """
    return _with_expr(exprs.DayOfMonth, column)


def dayofyear(column):
    """
    Extracts the day of the year as an integer from a given date/timestamp/string
    """
    return _with_expr(exprs.DayOfYear, column)


def hour(column):
    """
    Extracts the hours as an integer from a given date/timestamp/string
    """
    return _with_expr(exprs.Hour, column)


def last_day(column):
    """
    Given a date column, returns the last day of the month which the given date belongs to.
    For example, input "2015-07-27" returns "2015-07-31" since July 31 is the last day of the
    month in July 2015.
    """
    return _with_expr(exprs.LastDay, column)


def minute(column):
    """
    Extracts the minutes as an integer from a given date/timestamp/string
    """
    return _with_expr(exprs.Minute, column)


def months_between(column1, column2):
    """
    Returns number of months between dates `column1` and `column2`
    """
    return _with_binary_expr(exprs.MonthsBetween, column1, column2)


def next_day(column, day_of_week='mon'):
    """
    Given a date column, returns the first date which is later than the value of the date column
    that is on the specified day of the week.

    For example, `next_day('2015-07-27', "Sunday")` returns 2015-08-02 because that is the first
    Sunday after 2015-07-27.

    Day of the week parameter is case insensitive, and accepts:

        "Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun".
    """
    return _with_expr(exprs.NextDay, column, day_of_week)


def second(column):
    """
    Extracts the seconds as an integer from a given date/timestamp/string
    """
    return _with_expr(exprs.Second, column)


def weekofyear(column):
    """
    Extracts the week number as an integer from a given date/timestamp/string
    """
    return _with_expr(exprs.WeekOfYear, column)


def from_unixtime(column, fmt='yyyy-MM-dd HH:mm:ss'):
    """
    Converts the number of seconds from unix epoch (1970-01-01 00:00:00 UTC) to a string
    representing the timestamp of that moment in the current system time zone in the given
    format.
    """
    return _with_expr(exprs.FromUnixTime, column, fmt)


def unix_timestamp(column=None, pattern='yyyy-MM-dd HH:mm:ss'):
    """
    Convert time string in ``column`` with given ``pattern``
    (see [http://docs.oracle.com/javase/tutorial/i18n/format/simpleDateFormat.html])
    to Unix time stamp (in seconds), return null if fail.

    If ``column=None``, the current Unix timestamp (computed by :func:`current_timestamp`) will be used
    """
    if column is None:
        column = current_timestamp()
    return _with_expr(exprs.UnixTimestamp, column, pattern)


def to_date(column):
    """
    Converts the column into DateType
    """
    return _with_expr(exprs.ToDate, column)


def trunc(column, fmt='year'):
    """
    Returns date truncated to the unit specified by the format.

    :param column: the source column, can be a ``Column`` object or a string (column name)
    :param fmt: 'year', 'yyyy', 'yy' for truncate by year,
            or 'month', 'mon', 'mm' for truncate by month
    """
    return _with_expr(exprs.TruncDate, column, fmt)


def from_utc_timestamp(column, tz=''):
    """
    Assumes given timestamp is UTC and converts to given timezone
    """
    return _with_expr(exprs.FromUTCTimestamp, column, tz)


def to_utc_timestamp(column, tz=''):
    """
    Assumes given timestamp is in given timezone and converts to UTC
    """
    return _with_expr(exprs.ToUTCTimestamp, column, tz)


def window(column, window_duration='', slide_duration=None, start_time='0 second'):
    """
    Bucketize rows into one or more time windows given a timestamp specifying column. Window
    starts are inclusive but the window ends are exclusive, e.g. ``12:05`` will be in the window
    ``[12:05,12:10)`` but not in ``[12:00,12:05)``.

    Windows can support microsecond precision. Windows in the order of months are not supported.

    The following example takes the average stock price for a one minute window every 10 seconds
    starting 5 seconds after the hour:

    :Example:

    .. code-block:: python

        df = ...  # schema: timestamp: TimestampType, stockId: StringType, price: DoubleType
        df.group_by(functions.window(df.timestamp, "1 minute", "10 seconds", "5 seconds"), df.stockId)
            .agg(mean("price"))


    The windows will look like:

    .. code-block:: python

        09:00:05-09:01:05
        09:00:15-09:01:15
        09:00:25-09:01:25 ...


    For a streaming query, you may use the function ``current_timestamp`` to generate windows on
    processing time.

    :param column: The column or the expression to use as the timestamp for windowing by time.
                   The time column must be of TimestampType.
    :param window_duration: A string specifying the width of the window, e.g. `10 minutes`,
                            ``1 second``. Check ``CalendarIntervalType`` for
                            valid duration identifiers. Note that the duration is a fixed length of
                            time, and does not vary over time according to a calendar. For example,
                            ``1 day`` always means ``86,400,000 milliseconds``, not a calendar day.
    :param slide_duration:  A string specifying the sliding interval of the window, e.g. ``1 minute``.
                            A new window will be generated every ``slide_duration``. Must be less than
                            or equal to the ``window_duration``.
                            This duration is likewise absolute, and does not vary according to a calendar.
                            If unspecified, ``slide_duration`` will be equal to ``window_duration``
    :param start_time: The offset with respect to ``1970-01-01 00:00:00 UTC`` with which to start
                        window intervals. For example, in order to have hourly tumbling windows that
                        start 15 minutes past the hour, e.g. ``12:15-13:15``, ``13:15-14:15``... provide
                        ``start_time`` as ``15 minutes``.
    """
    if slide_duration is None:
        slide_duration = window_duration
    return _with_expr(exprs.TimeWindow, column, window_duration, slide_duration, start_time)


"""
Collection functions
"""


def array_contains(column, value):
    """
    Returns true if the array contains ``value``
    """
    return _with_binary_expr(exprs.ArrayContains, left=column, right=lit(value))


def explode(column):
    """
    Creates a new row for each element in the given array or map column
    """
    return _with_expr(exprs.Explode, column)


def posexplode(column):
    """
    Creates a new row for each element with position in the given array or map column
    """
    return _with_expr(exprs.PosExplode, column)


def get_json_object(column, path=''):
    """
    Extracts json object from a json string based on json path specified, and returns json string
    of the extracted json object. It will return null if the input json string is invalid.
    """
    return _with_expr(exprs.GetJsonObject, column, path)


def json_tuple(column, *fields):
    """
    Creates a new row for a json column according to the given field names
    """
    if len(fields) == 0:
        raise ValueError('at least 1 field name should be given')
    return _with_expr(exprs.JsonTuple, column, fields)


def size(column):
    """
    Returns length of array or map
    """
    return _with_expr(exprs.Size, column)


def sort_array(column, asc=True):
    """
    Sorts the input array for the given column in ascending / descending order,
    according to the natural ordering of the array elements.
    """
    return _with_expr(exprs.SortArray, column, asc)
