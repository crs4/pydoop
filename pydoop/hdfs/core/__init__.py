import logging

logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger("pydoop.hdfs.core")


NATIVE = "native"
JPYPE_BRIDGED = "jpype-bridged"

_supported_implementations = [NATIVE, JPYPE_BRIDGED]

_implementation_module = None


def get_supported_implementations():
    return _supported_implementations


def core_hdfs_fs(host, port, user):
    if not _implementation_module:
        raise RuntimeError("hdfs_core implementation not initialized")
    return _implementation_module.CoreHdfsFs(host, port, user)


def _init_(implementation_type=_supported_implementations[0]):
    try:

        if implementation_type == _supported_implementations[0]:

            import pydoop.utils.jvm as jvm
            jvm.load_jvm_lib()

            import native_core_hdfs  # Notice: when you import _hdfs JVM has to be already instanciated
            return native_core_hdfs

        elif implementation_type == _supported_implementations[1]:
            from pydoop.hdfs.core.bridged import get_implementation_module
            return get_implementation_module()

        raise ValueError("Implementation %s not supported!!!" % implementation_type)

    except Exception, e:
        logger.error("Unable to load the module %s: %s", "core_hdfs", e.message)
        raise ImportError("Unable to load the module %s: %s" % ("core_hdfs", e.message))


if _implementation_module is None:
    try:
        import pydoop.config
        _implementation_module = _init_(pydoop.config.HDFS_CORE_IMPL)
    except:
        logger.debug("No config.py found: it must exist after pydoop installation")
