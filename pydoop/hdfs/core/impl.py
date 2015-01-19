# BEGIN_COPYRIGHT
#
# Copyright 2009-2015 CRS4.
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

# DEV NOTE: this module is used by the setup script, so it MUST be
# importable even if Pydoop has not been installed (yet).

"""
HDFS core implementation info.
"""

NATIVE = "native"
JPYPE_BRIDGED = "jpype-bridged"

SUPPORTED = frozenset([NATIVE, JPYPE_BRIDGED])
DEFAULT = NATIVE
