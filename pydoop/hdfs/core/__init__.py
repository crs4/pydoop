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
HDFS core implementation.
"""

from . import impl


def init(backend=impl.DEFAULT):
    if backend == impl.NATIVE:
        import pydoop.utils.jvm as jvm
        jvm.load_jvm_lib()
        try:
            # NOTE: JVM must be already instantiated
            import pydoop.native_core_hdfs
        except ImportError:
            return None  # should only happen at compile time
        else:
            return pydoop.native_core_hdfs
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
