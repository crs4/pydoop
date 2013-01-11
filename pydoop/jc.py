# BEGIN_COPYRIGHT
# 
# Copyright 2009-2013 CRS4.
# 
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy
# of the License at
# 
#   http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
# 
# END_COPYRIGHT

"""
Provides a wrapper for the JobConf object.
"""


class jc_wrapper(object):
  """
  Simple JobConf wrapper to cache type-converted items.

  .. method:: jc_wrapper[k]

    Use the ``[]`` operator to get the original conf string value for
    property ``k``.

    :param k: name of the property.
    :raises KeyError: if the requested property doesn't exist.
  """
  INT = 1
  FLOAT = 2
  BOOL = 3

  def __init__(self, jc):
    """
    :param jc:  the ``pydoop.pipes.JobConf`` object to be wrapped.
    """
    self.jc = jc
    self.cache = {}  # TYPE_CODE: TYPE_CONVERTED_VALUE

  def has_key(self, k):
    """
    Test for the presence of the property ``k`` in the configuration.

    :param k: name of the property.
    """
    return self.jc.hasKey(k)

  def __getitem__(self, k):
    """
    Get the original conf string value for property ``k``.

    :param k: name of the property.
    :raises KeyError: if the requested property doesn't exist.
    """
    try:
      return self.jc.get(k)
    except RuntimeError:
      raise KeyError("No such configuration key: %s" % k)

  def get(self, k, default=None):
    """
    Return the original conf string, if ``k`` exists in the configuration.
    Else return the value of the argument `default`.
    
    :param k: name of the property.
    :param default: value to return if the requested property doesn't exist.
    """
    if self.jc.hasKey(k):
      return self.jc.get(k)
      # LP: I suspect that checking for the key twice (once in hasKey
      # and once in jc.get) may be faster than relying on the
      # exception thrown by jc.get, which needs to cross the
      # python-c++ boundary.  So, I implemented the above with an if
      # rather than a try/except.
    else:
      return default

  def get_int(self, k, default=0):
    """
    Fetch the property named ``k`` and convert it to an ``int``.

    :param k: name of the property.
    :param default: value to return if the requested property doesn't exist.
    :raises ValueError: if the value can't be converted to an ``int``.
    """
    return self.__fetch_through_cache(self.INT, k, default)

  def get_float(self, k, default=0.0):
    """
    Fetch the property named ``k`` and convert it to a ``float``.

    :param k: name of the property.
    :param default: value to return if the requested property doesn't exist.
    :raises ValueError: if the value can't be converted to a ``float``.
    """
    return self.__fetch_through_cache(self.FLOAT, k, default)

  def get_boolean(self, k, default=False):
    """
    Fetch the property named ``k`` and convert it to a ``bool``.

    The values 't', 'true', and '1' (upper or lower case) are accepted
    for True.  Similarly, 'f', 'false' and '0' are accepted for False.

    :param k: name of the property.
    :param default: value to return if the requested property doesn't exist.
    :raises ValueError: if the value can't be converted to a ``bool``.
    """
    return self.__fetch_through_cache(self.BOOL, k, default)

  def __fetch_and_cache(self, value_type, key, default):
    """
    Fetch the raw string value from the JobConf and interpret
    it as the type specified by value_type.  The result is cached
    in self.cache, along with the specified type, so subsequent
    queries can re-use it.

    Precondition:  the key must not already be in the cache
    """
    if not self.jc.hasKey(key):
      return default
    raw_value = self.jc.get(key)
    # The JobConf can't return None for any parameter value.  An empty value
    # is returned as an empty string.
    if value_type == self.INT:
      # tests show that converting a string to float and then to int is
      # faster than converting directly to int
      #
      # In [1]: import timeit
      # 
      # In [5]: timeit.timeit('int("123")', number=10000000)
      # Out[5]: 4.447897911071777
      # 
      # In [6]: timeit.timeit('float("123.3")', number=10000000)
      # Out[6]: 1.4062819480895996
      # 
      # In [7]: timeit.timeit('int(float("123.3"))', number=10000000)
      # Out[7]: 2.7725789546966553
      value = int(float(raw_value))
    elif value_type == self.FLOAT:
      value = float(raw_value)
    elif value_type == self.BOOL:
      if raw_value:
        if raw_value.lower() in ('f', 'false', '0'):
          value = False
        elif raw_value.lower() in ('t', 'true', '1'):
          value = True
        else:
          raise ValueError("Unrecognized boolean value '%s'" % raw_value)
      else:
        raise ValueError("Unrecognized boolean value '%s'" % raw_value)
    else:
      raise ValueError("Unrecognized value_type argument %s" % value_type)
    self.cache[key] = (value_type, value)
    return value

  def __fetch_through_cache(self, value_type, key, default):
    t = self.cache.get(key)
    if t and t[0] == value_type:
      return t[1]
    else:
      return self.__fetch_and_cache(value_type, key, default)
