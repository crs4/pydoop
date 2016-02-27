# BEGIN_COPYRIGHT
#
# Copyright 2009-2016 CRS4.
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
General purpose utilities.
"""

__all__ = [
    'raise_pydoop_exception',
    'jc_configure',
    'jc_configure_int',
    'jc_configure_bool',
    'jc_configure_float',
    'jc_configure_log_level',
    'make_input_split',
    'NullHandler',
    'NullLogger',
    'make_random_str',
    'split_hdfs_path',
]

from .misc import (  # backward compatibility
    raise_pydoop_exception,
    jc_configure,
    jc_configure_int,
    jc_configure_bool,
    jc_configure_float,
    jc_configure_log_level,
    make_input_split,
    NullHandler,
    NullLogger,
    make_random_str,
    split_hdfs_path,
)
