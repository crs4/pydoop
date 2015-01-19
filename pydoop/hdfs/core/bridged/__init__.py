# BEGIN_COPYRIGHT
#
# Copyright 2009-2015 CRS4.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy
# of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
#
# END_COPYRIGHT

"""
JPype-bridged HDFS core implementation.
"""
import os

import pydoop
from pydoop.utils.bridge.factory import JavaWrapperFactory


def get_implementation_module():
    from . import hadoop
    return hadoop


def init(bridge_type):
    hadoop_classpath = pydoop.hadoop_classpath()
    if hadoop_classpath is None:
        raise RuntimeError('Hadoop classpath not set')
    classpath = os.environ.get('classpath', '.') + ':' + hadoop_classpath
    return JavaWrapperFactory(
        classpath=classpath, java_bridge_name=bridge_type
    )


BRIDGE_TYPE = 'JPype'
try:
    _WRAPPER_FACTORY
except NameError:
    _WRAPPER_FACTORY = init(BRIDGE_TYPE)


def get_wrapper_factory():
    return _WRAPPER_FACTORY
