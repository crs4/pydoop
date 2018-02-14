# BEGIN_COPYRIGHT
#
# Copyright 2009-2018 CRS4.
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

"""
Emit strings that contain the substring provided by the property
'grep-expression' (script raises an exception if the property is
missing).  We use the fourth 'conf' argument to retrieve the custom
'grep-expression' parameter.

When running this example, set --kv-separator to the empty string and
--num-reducers 0.
"""


# DOCS_INCLUDE_START
def mapper(_, text, writer, conf):
    if text.find(conf['grep-expression']) >= 0:
        writer.emit("", text)
