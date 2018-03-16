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

import hmac
import hashlib
import base64
import string


DIGITS = frozenset(string.digits)
QUOTE_MAP = {
    '\\': '\\\\',
    '\t': '\\t',
    '\n': '\\n',
    ' ': '\\s',
}
UNQUOTE_MAP = {
    't': '\t',
    'n': '\n',
    's': ' ',
    'X': '\\',
}


def quote_string(in_string, deliminators='\\'):
    return ''.join(s if (32 < ord(s) < 127) and s not in deliminators
                   else QUOTE_MAP.get(s, r'\%02x' % ord(s))
                   for s in in_string)


def unquote_head(p):
    return (UNQUOTE_MAP[p[0]] + p[1:] if p[0] not in DIGITS
            else chr(int(p[0:2], 16)) + p[2:])


def unquote_string(in_string):
    # FIXME HACK HACK
    s = in_string.replace('\\\\', '\\X')
    parts = s.split('\\')
    return ''.join([parts[0]] + [unquote_head(p) for p in parts[1:]])


def create_digest(key, msg):
    h = hmac.new(key, msg, hashlib.sha1)
    return base64.b64encode(h.digest())
