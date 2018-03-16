# BEGIN_COPYRIGHT
#
# Copyright 2009-2018 CRS4.
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

import os


def init():
    import pydoop.utils.jvm as jvm
    jvm.load_jvm_lib()
    try:
        # NOTE: JVM must be already instantiated
        import pydoop.native_core_hdfs
    except ImportError:
        return None  # should only happen at compile time
    else:
        return pydoop.native_core_hdfs


def core_hdfs_fs(host, port, user):
    _CORE_MODULE = init()
    if _CORE_MODULE is None:
        if os.path.isdir("pydoop"):
            msg = "Trying to import from the source directory?"
        else:
            msg = "Check that Pydoop is correctly installed"
        raise RuntimeError("Core module unavailable. %s" % msg)
    return _CORE_MODULE.CoreHdfsFs(host, port, user)
