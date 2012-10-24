# BEGIN_COPYRIGHT
# 
# Copyright 2012 CRS4.
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

class jc_wrapper(object):
  """
  Simple JobConf wrapper to cache type-converted items.
  """
  INT = 1
  FLOAT = 2
  BOOL = 3

  def __init__(self, jc):
    self.jc = jc
    # in the cache, we store type-converted values as
    #   (TYPE_CODE, value)
    self.cache = {}

  def has_key(self, k):
    """
    Test for the presence of the property k in the configuration.
    """
    return self.jc.hasKey(k)

  def __getitem__(self, k):
    # XXX: how to document the [] operator?
    """
    Returns the original conf string.

    Raises IndexError if the key doesn't exist
    """
    if not self.jc.hasKey(k):
      raise IndexError("No such configuration key: %s" % k)
    return self.jc.get(k)

  def get(self, k, default=None):
    """
    Returns the original conf string, if k exists in the configuration.
    Else returns the value of the argument `default`
    """
    if self.jc.hasKey(k):
      return self.jc.get(k)
    else:
      return default

  def get_int(self, k):
    """
    Fetch the property named k and convert it to an ``int``.

    Raises an ``IndexError`` if the requested property doesn't exist.
    Raises a ``ValueError`` if the value can't be converted to an ``int``.
    """
    return self.__fetch_through_cache(self.INT, k)

  def get_float(self, k):
    """
    Fetch the property named k and convert it to a ``float``.

    Raises an ``IndexError`` if the requested property doesn't exist.
    Raises a ``ValueError`` if the value can't be converted to a ``float``.
    """
    return self.__fetch_through_cache(self.FLOAT, k)

  def get_boolean(self, k):
    """
    Fetch the property named k and convert it to a ``bool``.

    Raises an ``IndexError`` if the requested property doesn't exist.
    Raises a ``ValueError`` if the value can't be converted to a ``bool``.

    The values 't', 'true', and '1' (upper or lower case) are accepted for True.
    Similarly, 'f', 'false' and '0' are accepted for False.
    """
    return self.__fetch_through_cache(self.BOOL, k)

  def __fetch_and_cache(self, value_type, key):
    """
    Fetch the raw string value from the JobConf and interpret
    it as the type specified by value_type.  The result is cached
    in self.cache, along with the specified type, so subsequent
    queries can re-use it.
    """
    # XXX: can the JobConf return None for parameters?
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
      value = int(float(self[key]))
    elif value_type == self.FLOAT:
      value = float(self[key])
    elif value_type == self.BOOL:
      string = self[key]
      if string:
        if string.lower() in ('f', 'false', '0'):
          value = False
        elif string.lower() in ('t', 'true', '1'):
          value = True
        else:
          raise ValueError("Unrecognized boolean value '%s'" % string)
      else:
        raise ValueError("Unrecognized boolean value '%s'" % string)
    else:
      raise ValueError("Unrecognized value_type argument %s" % value_type)

    self.cache[key] = (value_type, value)
    return value

  def __fetch_through_cache(self, value_type, key):
    t = self.cache.get(key)
    if t and t[0] == value_type:
        return t[1]
    else:
      return self.__fetch_and_cache(value_type, key)
