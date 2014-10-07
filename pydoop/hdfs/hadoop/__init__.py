import os
import logging
import importlib

from pydoop.hadoop_utils import PathFinder
from pydoop.utils.bridge.factory import JavaWrapperFactory


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("pydoop.hdfs.hadoop")

HADOOP_HDFS_IMPL_SUFFIX = "Impl"
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


def wrap_array(fully_qualified_array_item_class, array_dimensions=1):
    """

    :param fully_qualified_array_item_class:
    :param array_dimensions:
    :return:
    """
    return factory.get_array_wrapper(fully_qualified_array_item_class, array_dimensions)


def wrap_array_instance(fully_qualified_array_item_class, array_dimension=1, array_items=[]):
    """

    :param fully_qualified_array_item_class:
    :param array_dimension:
    :param array_items:
    :return:
    """
    return factory.get_array_wrapper_instance(fully_qualified_array_item_class, array_dimension, array_items)


def get_hadoop_version():
    """
    Detects the current available Hadoop distribution
    :return:
    """
    version_info = wrap_class(HADOOP_VERSION_INFO_JAVA_CLASS)
    return version_info.getVersion()
    # return PathFinder().hadoop_version()


def get_implementation_instance(class_name_prefix, *args, **kwargs):
    class_name = class_name_prefix + HADOOP_HDFS_IMPL_SUFFIX
    logger.debug("class_name %s", class_name)
    logger.debug("class_name_prefix %s", class_name_prefix)
    logger.debug("implementation_module%s", implementation_module)
    logger.debug("HADOOP_HDFS_IMPL_SUFFIX %s", HADOOP_HDFS_IMPL_SUFFIX)
    cl = getattr(implementation_module, class_name)
    return cl(*args, **kwargs)


def get_implementation_module():
    return implementation_module

# Loads the proper version of the hadoop hdfs wrapper
h_version = [''] + get_hadoop_version().split('.')

while h_version:
    module_path = "pydoop.hdfs.hadoop.hadoop" + "_".join(h_version)
    try:
        implementation_module = importlib.import_module(module_path)
        logger.info("loaded module %s", module_path)
        break
    except ImportError:
        h_version.pop()

else:
    logger.critical("INTERNAL ERROR, MISSING CRITICAL MODULE pydoop.hdfs.hadoop.hadoop")
