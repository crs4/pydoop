import os
import logging
import importlib
from pydoop.hadoop_utils import PathFinder
from pydoop.utils.bridge.factory import JavaWrapperFactory


logger = logging.getLogger("pydoop.hdfs.core.bridged")
logger.setLevel(logging.CRITICAL)


_supported_bridges = ["JPype"]

_bridge_type = _supported_bridges[0]

_implementation_module = None

_wrapper_factory = None


def get_supported_bridges():
    return _supported_bridges


def get_bridge_type():
    return _bridge_type


def get_factory_wrapper_factory():
    return _wrapper_factory


def get_implementation_module():
    return _implementation_module


def _init_wrapper_factory_(bridge_type=_supported_bridges[0]):

    if not bridge_type in _supported_bridges:
        raise ValueError("Bridge type '%s' not supported: only %s are supported." % (bridge_type, _supported_bridges))

    # Hadoop ClassPath detection
    HADOOP_CLASSPATH = PathFinder().hadoop_classpath()

    if HADOOP_CLASSPATH is None:
        raise EnvironmentError(
            "HADOOP_CLASSPATH not found! Use hadoop classpath to detect it and to set it into your environment")

    # Adds the HADOOP_CLASSPATH to the default Java CLASSPATH
    CLASSPATH = os.environ.get("CLASSPATH", ".") + ":" + HADOOP_CLASSPATH

    # the factory instance for creating Python wrappers of Java classes
    wrapper_factory = JavaWrapperFactory(classpath=CLASSPATH, java_bridge_name=bridge_type)

    logger.debug("JavaWrapperFactory created!")
    return wrapper_factory


def _init_(bridge_type=_supported_bridges[0]):

    import hadoop
    return hadoop

    # Loads the proper version of the hadoop hdfs wrapper
    hadoop_version = PathFinder().hadoop_version()
    h_version = [''] + hadoop_version.split('.')

    print "Hadoop versions: %s" % h_version

    while h_version:
        module_path = "pydoop.hdfs.core.bridged.hadoop" + "_".join(h_version)
        try:
            loaded_module = importlib.import_module(module_path)
            logger.info("loaded module %s", loaded_module)
            break
        except ImportError, e:
            h_version.pop()
            print "Error", e.message

    else:
        logger.critical("INTERNAL ERROR, MISSING CRITICAL MODULE pydoop.hdfs.core.hadoop")

    return loaded_module

if _wrapper_factory is None:
    _wrapper_factory = _init_wrapper_factory_()

if _implementation_module is None:
    _implementation_module = _init_()

print "Implementation module", _implementation_module
print "Wrappert Factory", _wrapper_factory