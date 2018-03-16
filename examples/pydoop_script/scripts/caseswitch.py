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
Convert text to upper or lower case.  By default, the program will
switch text to upper case.  Set the config property
'caseswitch.case=lower' if you prefer to switch to lower case.

Set --kv-separator to the empty string when running this example.
"""


def mapper(_, record, writer, conf):
    if conf['caseswitch.case'] == 'upper':
        value = record.upper()
    elif conf['caseswitch.case'] == 'lower':
        value = record.lower()
    else:
        raise RuntimeError(
            "Invalid caseswitch value %s" % conf['caseswitch.case']
        )
    writer.emit("", value)
