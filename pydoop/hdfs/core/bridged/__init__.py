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
