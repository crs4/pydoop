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

"""
Common hdfs utilities.
"""

import getpass
import pwd
import grp


BUFSIZE = 16384
DEFAULT_PORT = 8020  # org/apache/hadoop/hdfs/server/namenode/NameNode.java
DEFAULT_USER = getpass.getuser()
DEFAULT_LIBHDFS_OPTS = "-Xmx48m"  # enough for most applications

# Unicode objects are encoded using this encoding:
TEXT_ENCODING = 'utf-8'
# We use UTF-8 since this is what the Hadoop TextFileFormat uses
# NOTE:  If you change this, you'll also need to fix the encoding
# used by the native extension.


def encode_path(path):
    if isinstance(path, unicode):
        path = path.encode('utf-8')
    return path


def decode_path(path):
    if isinstance(path, str):
        path = path.decode('utf-8')
    return path


def encode_host(host):
    if isinstance(host, unicode):
        host = host.encode('idna')
    return host


def decode_host(host):
    if isinstance(host, str):
        host = host.decode('idna')
    return host


def get_groups(user=DEFAULT_USER):
    groups = set(_.gr_name for _ in grp.getgrall() if user in set(_.gr_mem))
    primary_gid = pwd.getpwnam(user).pw_gid
    groups.add(grp.getgrgid(primary_gid).gr_name)
    return groups
