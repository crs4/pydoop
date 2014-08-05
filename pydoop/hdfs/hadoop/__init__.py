__author__ = 'kikkomep'

import os
import logging
import importlib

from pydoop.hadoop_utils import PathFinder
from pydoop.utils.bridge.factory import JavaWrapperFactory


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("pydoop.hdfs.hadoop")

# Hadoop ClassPath detection
HADOOP_CLASSPATH = PathFinder().hadoop_classpath()

if HADOOP_CLASSPATH is None:
    raise EnvironmentError(
        "HADOOP_CLASSPATH not found! Use hadoop classpath to detect it and to set it into your environment")

# Adds the HADOOP_CLASSPATH to the default Java CLASSPATH
CLASSPATH = os.environ.get("CLASSPATH", ".") + ":" + HADOOP_CLASSPATH
HADOOP_VERSION_INFO_JAVA_CLASS = "org.apache.hadoop.util.VersionInfo"

# the factory instance for creating Python wrappers of Java classes
factory = JavaWrapperFactory(classpath=CLASSPATH, java_bridge_name="JPype")
logger.debug("JavaWrapperFactory created!")


def wrap_class(fully_qualified_class_name):
    """

    :param fully_qualified_class_name:
    :return:
    """
    return factory.get_wrapper(fully_qualified_class_name)


def wrap_class_instance(fully_qualified_class_name, *args):
    """

    :param fully_qualified_class_name:
    :param args:
    :return:
    """
    return factory.get_wrapper(fully_qualified_class_name)(*args)


def get_hadoop_version():
    """
    Detects the current available Hadoop distribution
    :return:
    """
    version_info = wrap_class(HADOOP_VERSION_INFO_JAVA_CLASS)
    return version_info.getVersion()
    # return PathFinder().hadoop_version()


def get_implementation_instance(class_name_prefix, *args, **kwargs):
    try:
        class_name = class_name_prefix + HADOOP_HDFS_IMPL_SUFFIX
        logger.debug("Trying to load the implementation class %s from the package %s" % (class_name, HADOOP_HDFS_WRAPPER_MODULE_NAME))
        cl = getattr(implementation_module, class_name)
        return cl(*args, **kwargs)
    except Exception, e:
        print "Unable to load the class %s" % class_name
        print e.message


def get_implementation_module():
    return implementation_module


# Loads the proper version of the hadoop hdfs wrapper
HADOOP_VERSION = get_hadoop_version().split('.')
DEFAULT_HADOOP_HDFS_WRAPPER_MODULE_NAME = "pydoop.hdfs.hadoop.hadoop_" + HADOOP_VERSION[0]
HADOOP_HDFS_WRAPPER_MODULE_NAME = "pydoop.hdfs.hadoop.hadoop_" + "_".join(HADOOP_VERSION)
HADOOP_HDFS_IMPL_SUFFIX = "Impl"

logger.debug("HADOOP_HDFS_WRAPPER_MODULE: %s" % HADOOP_HDFS_WRAPPER_MODULE_NAME)
try:
    implementation_module = importlib.import_module(HADOOP_HDFS_WRAPPER_MODULE_NAME)
    logger.info("loaded module %s", HADOOP_HDFS_WRAPPER_MODULE_NAME)
except ImportError:
    implementation_module = importlib.import_module(DEFAULT_HADOOP_HDFS_WRAPPER_MODULE_NAME)
    logger.info("loaded module %s", DEFAULT_HADOOP_HDFS_WRAPPER_MODULE_NAME)
