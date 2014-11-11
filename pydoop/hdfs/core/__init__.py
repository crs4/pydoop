"""
HDFS core implementation.
"""

from . import impl


def init(backend=impl.DEFAULT):
    if backend == impl.NATIVE:
        import pydoop.utils.jvm as jvm
        jvm.load_jvm_lib()
        try:
            import native_core_hdfs  # NOTE: JVM must be already instantiated
        except ImportError:
            return None  # should only happen at compile time
        else:
            return native_core_hdfs
    elif backend == impl.JPYPE_BRIDGED:
        from pydoop.hdfs.core.bridged import get_implementation_module
        return get_implementation_module()
    else:
        raise ValueError("%r: unsupported hdfs backend" % (backend,))


try:
    from pydoop.config import HDFS_CORE_IMPL
except ImportError:
    _CORE_MODULE = None  # should only happen at compile time
else:
    _CORE_MODULE = init(backend=HDFS_CORE_IMPL)


def core_hdfs_fs(host, port, user):
    if _CORE_MODULE is None:
        raise RuntimeError(
            'module not initialized, check that Pydoop is correctly installed'
        )
    return _CORE_MODULE.CoreHdfsFs(host, port, user)
